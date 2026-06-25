#!/usr/bin/env python3
"""Generate the S1-style timeline chart from a snapshot dir.

Usage:
    python infra/generate_chart.py /tmp/runs/<label> [out.png]

Reads:  api.log, jobs.json, profiles.json   (produced by snapshot_run.sh)
Writes: PNG timeline chart (default: <snapshot>/chart.png)

Parses events from the API log:
- ``Scheduled job X: mode=profiling, config={...}, node=...``  --> profile dispatch
- ``Optimizer assigned job X --> node Y {...}``                --> standard dispatch
- ``Remote run for job X on URL``                              --> dispatch went out
  (URL :8001=matemagician, :8002=polimi-gpu — used to backfill node when needed)
- ``Issued preempt /stop for job X``                           --> segment end
- ``Job X completed successfully``                             --> terminal
"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

NODE_COLOR = {"polimi-gpu": "#3b82f6", "matemagician": "#ef4444"}
NODE_FILL = {"polimi-gpu": "#dbeafe", "matemagician": "#fee2e2"}
URL_TO_NODE = {"8001": "matemagician", "8002": "polimi-gpu"}

LINE = re.compile(r"^(\d{2}:\d{2}:\d{2}\.\d+)\s+\S+\s+\S+:\s+(.*)$")
OPT_CLASSIFY = re.compile(r"OPT CLASSIFY new=\[([^\]]*)\] migrate=\[([^\]]*)\] kept-same=\[([^\]]*)\] drop-preempt=\[([^\]]*)\]")
PROF_SCHED = re.compile(r"Scheduled job (\w+):\s*mode=profiling,\s*config=\{([^}]+)\},\s*node=(\S+)")
OPT_ASSIGN = re.compile(r"Optimizer assigned job (\w+).*?node (\S+)\s+\{([^}]+)\}")
REMOTE_RUN = re.compile(r"Remote run for job (\w+) on http://[^:]+:(\d+)")
DISPATCHED = re.compile(r"Dispatched job (\w+) on (\S+) \{([^}]+)\}")
PREEMPT = re.compile(r"Issued preempt /stop for job (\w+)")
COMPLETED = re.compile(r"Job (\w+) completed successfully")
GPU_RE = re.compile(r"['\"]?(\w+)['\"]?:\s*(\d+)")

# Worker-side log lines (different format: 2026-05-19 17:13:27 - module - LEVEL - msg)
WORKER_LINE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+-\s+\S+\s+-\s+\S+\s+-\s+(.*)$")
PROF_COMPLETE = re.compile(r"Profile-complete (\w+):")
PROF_COMPLETE_LEGACY = re.compile(r"Profiling complete for job (\w+)")
# The worker's GPU allocator logs the moment a job's physical GPUs are freed
# — the authoritative end of a run segment (profiling or standard).  This is
# the current-format replacement for the older "Profile-complete" line: a
# profile cell finishes, the GPUs release, and the job re-dispatches to the
# next config.  Without it, a profiling segment is only closed at the job's
# NEXT dispatch, so a 30 s profile cell is drawn as a multi-minute bar that
# spuriously overlaps other jobs (apparent oversubscription).
GPU_RELEASE = re.compile(r"GPU allocator: job (\w+) releases indices")
EPOCH_LINE = re.compile(r"Epoch (\d+)/(\d+)\s+-\s+(?:Loss|starting)")


def parse_gpu(s: str) -> dict[str, int]:
    return {m.group(1): int(m.group(2)) for m in GPU_RE.finditer(s)}


def parse_time(s: str, day: datetime) -> datetime:
    t = datetime.strptime(s.split(".", 1)[0], "%H:%M:%S")
    out = day.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=0)
    if "." in s:
        out = out.replace(microsecond=int(s.split(".", 1)[1].ljust(6, "0")[:6]))
    return out


def tz_offset_from_drift(mtime: datetime, latest_ts: datetime) -> timedelta:
    """TZ offset to apply to a log's timestamps, inferred from clock drift.

    A UTC-clock containerised process leaves its in-line timestamps ≥1.5 h
    behind the host-local file mtime; snap to the host's exact TZ offset
    (rounding ``drift/3600`` mis-snaps when the snapshot was saved well after
    the last log event).  Smaller drift is just local-clock slack ⇒ zero.
    Shared by the API-log parser, the worker-log parser, and the TeX
    submit-time parser so all three stay in one coordinate system.
    """
    drift = (mtime - latest_ts).total_seconds()
    if abs(drift) >= 5400:
        tz_off = datetime.now().astimezone().utcoffset() or timedelta(0)
        return tz_off if drift > 0 else -tz_off
    return timedelta(0)


def load_jobs_and_anchor(snap_dir: Path) -> tuple[list, datetime]:
    """Load ``jobs.json`` and derive the anchor day: today's date, with H:M:S
    taken from the first ``scenario.log`` timestamp when present."""
    jobs = json.loads((snap_dir / "jobs.json").read_text())
    sc = (snap_dir / "scenario.log").read_text() if (snap_dir / "scenario.log").exists() else ""
    first_t = re.search(r"\[(\d{2}:\d{2}:\d{2})\]", sc)
    anchor_day = datetime.now().replace(microsecond=0)
    if first_t:
        h, m, s = map(int, first_t.group(1).split(":"))
        anchor_day = anchor_day.replace(hour=h, minute=m, second=s)
    return jobs, anchor_day


def append_worker_events(snap_dir: Path, events: list[dict]) -> list[dict]:
    """Merge ``worker_*.log`` events into *events* (which must be non-empty),
    keeping only those inside the api.log window, then return it sorted by ts.

    Worker logs span multiple runs (matemagician's container holds days of
    history); the ``api_first`` lower bound + a generous +4 h upper bound
    constrain them to the current run.  Callers MUST guard the empty-events
    case before calling — the ``api_first`` ``min()`` raises on an empty list,
    and the two callers diverge in how they report it.
    """
    api_first = min(e["ts"] for e in events)
    api_last = max(e["ts"] for e in events)
    window_end = api_last + timedelta(hours=4)
    for worker_log in snap_dir.glob("worker_*.log"):
        for ev in parse_worker_log(worker_log):
            if api_first <= ev["ts"] <= window_end:
                events.append(ev)
    events.sort(key=lambda e: e["ts"])
    return events


def chart_t0(events: list[dict], jobs: list, anchor_day: datetime) -> datetime:
    """t=0 for the chart: first dispatch of a job still present in jobs.json
    (so a chart captured after an earlier run was cleared doesn't anchor on
    dead events)."""
    current_jids = {j["id"][:8] for j in jobs}
    return min(
        (ev["ts"] for ev in events if ev["kind"] == "dispatch" and ev.get("jid") in current_jids),
        default=anchor_day,
    )


def first_dispatch_times(events: list[dict]) -> dict[str, datetime]:
    """Map each job's 8-char id to the ts of its first ``dispatch`` event
    (used to order chart rows by submission)."""
    job_first_ts: dict[str, datetime] = {}
    for ev in events:
        if ev["kind"] == "dispatch" and "jid" in ev:
            job_first_ts.setdefault(ev["jid"], ev["ts"])
    return job_first_ts


def parse_log(log_path: Path, anchor_day: datetime) -> list[dict]:
    """Return a flat list of dispatch/preempt/complete events keyed by timestamp."""
    events: list[dict] = []
    # Track per-job "intent" (most recent Scheduled mode=profiling or Optimizer assigned).
    # Used to backfill (node, gpus) when a Remote run line arrives without them.
    last_intent: dict[str, dict] = {}

    raw_lines = log_path.read_text(errors="replace").splitlines()
    # Detect TZ offset: dockerized API logs in UTC (its container's clock),
    # but the snapshot file's mtime is host-local (CEST).  Without this,
    # api.log events stay 2 h behind worker events / jobs.json updated_at
    # (also normalized to UTC), and SUCCEEDED bars stretch to the worker
    # completion time + the TZ offset.  Compare file mtime to the latest
    # in-line ts (parsed as naive H:M:S against today); drift ≥ 30 min ⇒
    # shift by the rounded hour offset.
    api_offset = timedelta(0)
    if log_path.exists():
        latest_inline = None
        for raw in raw_lines:
            m = LINE.match(raw.strip())
            if m:
                cand = parse_time(m.group(1), anchor_day)
                if latest_inline is None or cand > latest_inline:
                    latest_inline = cand
        if latest_inline is not None:
            mtime = datetime.fromtimestamp(log_path.stat().st_mtime)
            api_offset = tz_offset_from_drift(mtime, latest_inline)

    for raw in raw_lines:
        m = LINE.match(raw.strip())
        if not m:
            continue
        ts = parse_time(m.group(1), anchor_day) + api_offset
        msg = m.group(2)

        if d := PROF_SCHED.search(msg):
            jid, gpus_s, node = d.group(1), d.group(2), d.group(3)
            last_intent[jid] = {"node": node, "gpus": parse_gpu(gpus_s), "profile": True}
        elif d := OPT_ASSIGN.search(msg):
            jid, node, gpus_s = d.group(1), d.group(2), d.group(3)
            last_intent[jid] = {"node": node, "gpus": parse_gpu(gpus_s), "profile": False}
        elif d := REMOTE_RUN.search(msg):
            jid = d.group(1)
            intent = last_intent.get(jid)
            if not intent:
                continue  # no prior schedule info — skip
            events.append({
                "ts": ts, "kind": "dispatch", "jid": jid,
                "node": intent["node"], "gpus": intent["gpus"], "profile": intent["profile"],
            })
        elif d := DISPATCHED.search(msg):
            # Sometimes a Remote run wasn't logged (or arrived later); use this as
            # canonical dispatch event with explicit (node, gpus).
            jid, node, gpus_s = d.group(1), d.group(2), d.group(3)
            gpus = parse_gpu(gpus_s)
            intent = last_intent.get(jid, {})
            profile = intent.get("profile", False)
            # Update intent so later Remote-run can backfill correctly.
            last_intent[jid] = {"node": node, "gpus": gpus, "profile": profile}
            events.append({
                "ts": ts, "kind": "dispatch", "jid": jid,
                "node": node, "gpus": gpus, "profile": profile,
            })
        elif d := PREEMPT.search(msg):
            events.append({"ts": ts, "kind": "preempt", "jid": d.group(1)})
        elif d := COMPLETED.search(msg):
            events.append({"ts": ts, "kind": "completed", "jid": d.group(1)})
        elif d := OPT_CLASSIFY.search(msg):
            # Flag trivial (no-op) rounds separately so the chart can show
            # which rounds produced an actual placement change.
            non_trivial = bool(d.group(1).strip() or d.group(2).strip() or d.group(4).strip())
            events.append({"ts": ts, "kind": "opt", "non_trivial": non_trivial})

    return events


def parse_worker_log(log_path: Path) -> list[dict]:
    """Profile-complete and completion events from a worker log.

    Worker logs carry an explicit ``YYYY-MM-DD HH:MM:SS`` timestamp, but the
    timezone varies: matemagician's worker runs in a docker container with
    ``TZ=UTC``, while polimi-gpu's native worker logs in the host's CEST
    clock.  ``tz_offset_from_drift`` detects the offset by comparing the
    file's mtime (always local) to the latest in-line timestamp.  Without
    this, the ``api_first <= ev["ts"] <= window_end`` filter mis-rejects
    local-TZ worker events as 2 h late and ``profile_done`` / ``completed``
    signals never close their segments.
    """
    out: list[dict] = []
    if not log_path.exists():
        return out
    raw_lines = log_path.read_text(errors="replace").splitlines()
    parsed: list[tuple[datetime, str]] = []
    for raw in raw_lines:
        m = WORKER_LINE.match(raw)
        if not m:
            continue
        ts = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%Y-%m-%d %H:%M:%S")
        parsed.append((ts, m.group(3)))
    if not parsed:
        return out
    mtime = datetime.fromtimestamp(log_path.stat().st_mtime)
    last_log_ts = max(ts for ts, _ in parsed)
    offset = tz_offset_from_drift(mtime, last_log_ts)
    for ts, msg in parsed:
        ts_local = ts + offset
        if d := PROF_COMPLETE.search(msg):
            out.append({"ts": ts_local, "kind": "profile_done", "jid": d.group(1)})
        elif d := PROF_COMPLETE_LEGACY.search(msg):
            out.append({"ts": ts_local, "kind": "profile_done", "jid": d.group(1)})
        elif d := GPU_RELEASE.search(msg):
            out.append({"ts": ts_local, "kind": "release", "jid": d.group(1)})
        elif d := COMPLETED.search(msg):
            out.append({"ts": ts_local, "kind": "completed", "jid": d.group(1)})
    return out


def parse_progress(jobs_dir: Path) -> dict[str, tuple[int, int]]:
    """Return {job_id_8: (last_epoch, total_epochs)} from per-job trainer logs."""
    out: dict[str, tuple[int, int]] = {}
    if not jobs_dir.exists():
        return out
    for log in jobs_dir.glob("*.log"):
        sid = log.stem
        last: tuple[int, int] | None = None
        for line in log.read_text(errors="replace").splitlines():
            m = EPOCH_LINE.search(line)
            if m:
                last = (int(m.group(1)), int(m.group(2)))
        if last:
            out[sid] = last
    return out


def derive_segments(events: list[dict], jobs: list[dict]) -> dict[str, list[dict]]:
    """Per job_id (8-char): list of segments {start, end, node, gpus, profile}."""
    by_id: dict[str, list[dict]] = defaultdict(list)

    job_status = {j["id"][:8]: j.get("status", "") for j in jobs}

    # Per-job ``updated_at`` — used as the close time for terminal jobs when
    # api.log lacks an explicit completion event (e.g. completions are
    # reported by the worker logs only, or the snapshot's window cuts off
    # before the run ends).  Falling back to ``last_ts`` would close every
    # still-open bar at the last logged event, collapsing post-final-dispatch
    # work down to zero width.
    job_close_ts: dict[str, datetime] = {}
    for j in jobs:
        u = j.get("updated_at")
        if not u:
            continue
        try:
            # Convert to naive local time to match api.log (which is shifted
            # to local by ``parse_log``'s TZ-offset detection) and worker logs
            # (which carry explicit dates and are also rebased to local).
            ts = (
                datetime.fromisoformat(u.replace("Z", "+00:00"))
                .astimezone()
                .replace(tzinfo=None)
            )
        except ValueError:
            continue
        job_close_ts[j["id"][:8]] = ts

    open_seg: dict[str, dict] = {}

    def close(jid: str, end_ts: datetime) -> None:

        if jid in open_seg:
            seg = open_seg.pop(jid)
            seg["end"] = end_ts
            # Dedupe: ignore zero-length segments (a Remote-run followed by an
            # immediate Dispatched for the same (node, gpus) often double-fires).
            if end_ts > seg["start"]:
                by_id[jid].append(seg)

    seen_seg_key: dict[str, tuple] = {}
    for ev in events:
        if "jid" not in ev:
            continue  # opt events have no jid
        jid = ev["jid"]
        if ev["kind"] == "dispatch":
            key = (ev["node"], tuple(sorted(ev["gpus"].items())), ev["profile"])
            # Suppress duplicate consecutive dispatches (Remote-run + Dispatched
            # for the same (node, gpus, profile) — they're the same event).
            if seen_seg_key.get(jid) == key and jid in open_seg:
                continue
            close(jid, ev["ts"])
            open_seg[jid] = {
                "start": ev["ts"], "end": None,
                "node": ev["node"], "gpus": ev["gpus"], "profile": ev["profile"],
            }
            seen_seg_key[jid] = key
        elif ev["kind"] == "preempt":
            close(jid, ev["ts"])
            seen_seg_key.pop(jid, None)
        elif ev["kind"] == "completed":
            close(jid, ev["ts"])
            seen_seg_key.pop(jid, None)
        elif ev["kind"] == "profile_done":
            # Worker says a profile cell finished — close any open segment
            # for this job so the next dispatch starts a fresh bar.
            close(jid, ev["ts"])
            seen_seg_key.pop(jid, None)
        elif ev["kind"] == "release":
            # Worker freed this job's physical GPUs — the true end of the
            # current segment.  Closing here (rather than waiting for the
            # job's next dispatch) prevents a short profile cell from being
            # drawn as a long bar overlapping other jobs on the same node.
            close(jid, ev["ts"])
            seen_seg_key.pop(jid, None)

    # Close still-open segments.  Preferences:
    #   1. Terminal job with a jobs.json ``updated_at``: close at that ts
    #      (this is the canonical completion time when no completion event
    #      was logged in api.log).
    #   2. Otherwise the latest event ts (with a small floor so a job with
    #      only one event still draws a visible bar).
    last_ts = max((ev["ts"] for ev in events), default=None)
    for jid in list(open_seg.keys()):
        terminal = job_status.get(jid) in ("SUCCEEDED", "FAILED", "PREEMPTED")
        close_ts = None
        if terminal:
            close_ts = job_close_ts.get(jid)
        if close_ts is None:
            close_ts = last_ts or (open_seg[jid]["start"] + timedelta(seconds=30))
        # Never close before the segment started.
        if close_ts < open_seg[jid]["start"]:
            close_ts = open_seg[jid]["start"] + timedelta(seconds=30)
        close(jid, close_ts)
    return by_id


def make_chart(snap_dir: Path, out: Path) -> None:
    import matplotlib.patches as mpatches  # noqa: PLC0415
    import matplotlib.pyplot as plt  # noqa: PLC0415

    jobs, anchor_day = load_jobs_and_anchor(snap_dir)

    events = parse_log(snap_dir / "api.log", anchor_day)
    if not events:
        print("No events parsed from api.log", file=sys.stderr)
        return
    events = append_worker_events(snap_dir, events)
    progress = parse_progress(snap_dir / "jobs")
    segs = derive_segments(events, jobs)

    t0 = chart_t0(events, jobs, anchor_day)

    def to_min(t: datetime) -> float:
        return (t - t0).total_seconds() / 60.0

    # Sort jobs by first dispatch time (so chart rows match submission order).
    job_first_ts = first_dispatch_times(events)
    sorted_jobs = sorted(jobs, key=lambda j: job_first_ts.get(j["id"][:8], anchor_day))

    # Job numbers by submission order (created_at).
    def _parse_iso(s: str) -> datetime:
        # jobs.json uses ISO 8601 in UTC; convert to naive *local* time so it
        # lines up with t0 (api.log clock-time stamps, also rebased to local
        # by parse_log's TZ-offset detection).
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone().replace(tzinfo=None)

    by_created = sorted(jobs, key=lambda j: _parse_iso(j["created_at"]))
    job_num = {j["id"][:8]: i + 1 for i, j in enumerate(by_created)}

    # Pre-compute chart x range from segment ends only — deadlines that fall
    # far in the future (patient jobs with +120 min) would otherwise stretch
    # the chart and squash the action.
    seg_x_max = max((to_min(s["end"]) for sl in segs.values() for s in sl), default=10) + 5

    fig, ax = plt.subplots(figsize=(15, max(3.5, 0.85 * len(sorted_jobs) + 1.5)))
    yticks: list[float] = []
    ylabels: list[str] = []
    for i, j in enumerate(sorted_jobs):
        sid = j["id"][:8]
        y = i + 1
        yticks.append(y)
        deadline_str = ""
        if j.get("created_at") and j.get("deadline"):
            offset = (_parse_iso(j["deadline"]) - _parse_iso(j["created_at"])).total_seconds() / 60.0
            deadline_str = f" d=+{offset:.0f}m"
        ylabels.append(f"J{job_num[sid]}\np={j['priority']} ep={j['epochs_total']}{deadline_str}")
        for seg in segs.get(sid, []):
            start = to_min(seg["start"])
            end = to_min(seg["end"])
            n = seg["node"]
            n_gpu = sum(seg["gpus"].values())
            height = 0.55 if n_gpu == 1 else 0.85
            color = NODE_COLOR.get(n, "#888")
            facec = NODE_FILL.get(n, "#ddd")
            rect = mpatches.Rectangle(
                (start, y - height / 2),
                max(end - start, 0.05),
                height,
                linewidth=0.7,
                edgecolor=color,
                facecolor=facec,
                hatch="////" if seg["profile"] else None,
            )
            ax.add_patch(rect)
            cfg_label = "×".join(f"{k}{v}" for k, v in seg["gpus"].items())
            ax.text((start + end) / 2, y, cfg_label, ha="center", va="center", fontsize=6.5, color=color)
        # Deadline tick on the timeline (only if it lands in the action
        # window) — the "+Xm" offset already shows in the row label.
        if j.get("created_at") and j.get("deadline"):
            dx = to_min(_parse_iso(j["deadline"]))
            if 0 <= dx <= seg_x_max:
                ax.vlines(dx, y - 0.45, y + 0.45, color="#a855f7",
                          linewidth=1.6, linestyles=(0, (4, 2)), zorder=5)

        # final status + epoch progress
        status = j.get("status", "")
        prog = progress.get(sid)
        prog_str = f" {prog[0]}/{prog[1]}" if prog else f" /{j['epochs_total']}"
        last_end = max((to_min(s["end"]) for s in segs.get(sid, [])), default=0)
        if segs.get(sid):
            ax.text(last_end + 0.15, y, f"  {status}{prog_str}", va="center", fontsize=7,
                    color="green" if status == "SUCCEEDED" else "red" if status == "FAILED" else "gray")

    # Mark every OPT CLASSIFY round on a strip just below the timeline.
    opt_y = len(sorted_jobs) + 0.5
    for ev in events:
        if ev["kind"] != "opt":
            continue
        x = to_min(ev["ts"])
        color = "#dc2626" if ev["non_trivial"] else "#9ca3af"
        lw = 0.9 if ev["non_trivial"] else 0.4
        ax.vlines(x, opt_y - 0.15, opt_y + 0.15, color=color, linewidth=lw)
    ax.text(-0.4, opt_y, "OPT", fontsize=7, color="#444", va="center", ha="right")

    x_max = seg_x_max
    ax.set_xlim(-0.5, x_max)
    ax.set_ylim(0.3, opt_y + 0.5)
    ax.set_yticks(yticks)
    ax.set_yticklabels(ylabels, fontsize=8)
    ax.set_xlabel("time [min] (t=0 at first dispatch)")
    tick_step = 1 if x_max <= 15 else 2 if x_max <= 40 else 5
    ax.set_xticks(list(range(0, int(x_max) + 1, tick_step)))
    ax.grid(True, axis="x", alpha=0.2)
    ax.invert_yaxis()

    legend_handles = [
        mpatches.Patch(facecolor=NODE_FILL["polimi-gpu"], edgecolor=NODE_COLOR["polimi-gpu"], label="polimi-gpu (A40)"),
        mpatches.Patch(facecolor=NODE_FILL["matemagician"], edgecolor=NODE_COLOR["matemagician"], label="matemagician (P600)"),
        mpatches.Patch(facecolor="white", edgecolor="black", hatch="////", label="profile dispatch"),
        mpatches.Patch(facecolor="#dc2626", label="OPT round (non-trivial)"),
        mpatches.Patch(facecolor="#9ca3af", label="OPT round (no-op)"),
        mpatches.Patch(facecolor="white", edgecolor="#a855f7", label="deadline (+min from submit)"),
    ]
    ax.legend(handles=legend_handles, loc="lower right", fontsize=8)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    run_started = t0.strftime("%Y-%m-%d %H:%M:%S")
    ax.set_title(
        f"Scenario timeline — {snap_dir.name}\n"
        f"run started {run_started}  ·  chart generated {generated_at}",
        fontsize=10,
    )

    plt.tight_layout()
    plt.savefig(out, dpi=140)
    print(f"Chart saved to {out}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: generate_chart.py <snapshot_dir> [out.png]", file=sys.stderr)
        sys.exit(2)
    snap = Path(sys.argv[1])
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else snap / "chart.png"
    make_chart(snap, out)
