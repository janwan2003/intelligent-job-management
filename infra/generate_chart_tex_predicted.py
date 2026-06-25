#!/usr/bin/env python3
"""Gantt chart annotated with the OPTIMISER'S PREDICTED finish vs the ACTUAL end.

A sibling of :mod:`generate_chart_tex` — it renders the *same* bars, queue
lines, and deadline ticks, then overlays, for every completed job, an orange
``predicted finish`` marker.  The two scripts are deliberately kept separate so
the plain timeline (``generate_chart_tex.py``) and this prediction-vs-actual
variant can both be regenerated independently.

The prediction is the optimiser's own ExecTime estimate for the placement the
job actually *completed* under (its final standard-run segment)::

    predicted_finish = final_standard_segment.start
                       + remaining_epochs * per_epoch(final_bundle)

``remaining_epochs`` is read from the ``OPT REQ`` line that drove that placement
(``api.log``), and ``per_epoch`` from ``profiles_<type>.json`` (the per-epoch
cost the optimiser was actually fed).  The horizontal gap between the marker and
the bar's end is the prediction error.

NOTE: snapshots captured before the 2026-06-25 profiler fix carry a per-epoch
that is ~0.6x the true value.  The worker matched both the "Epoch N/M -
starting" and "Epoch N/M - Loss: ..." log lines, so it recorded two timestamps
per epoch and ``compute_duration`` averaged each real epoch interval against a
near-zero start->end gap (E/(2E-1) -> ~0.5x).  On those snapshots the gap is
dominated by that measurement artefact and scales with ``remaining_epochs`` —
it is NOT resume/warmup/sustained-throughput effects.  See
``worker/execution.py`` (``EPOCH_DONE_RE``).  Re-run the e2e after the fix for a
per-epoch that reflects sustained wall-clock.

Usage::

    python infra/generate_chart_tex_predicted.py <snapshot_dir> <out.tex> \
        [--label <jid_prefix>=<display_name>] [--deadline <jid>=<offset>] ...
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generate_chart import (  # noqa: E402
    first_dispatch_times,
    tz_offset_from_drift,
)
from generate_chart_tex import (  # noqa: E402
    STYLE_SUFFIX,
    _load,
    _parse_iso,
    _parse_offset,
    _resolve_label,
)

# Reuse the OPT REQ job-tuple shape:  ('6a0707ca', 5, '200', ['A40', ...])
#                                       ^id        ^prio ^epochs-remaining
_OPT_REQ_JOB = re.compile(r"\('([0-9a-f]{6,})',\s*\d+,\s*'(\d+)'")


def _parse_opt_req_epochs(api_log: Path, anchor_day: datetime) -> list[tuple[datetime, dict[str, int]]]:
    """List of ``(ts, {jid8: remaining_epochs})`` from each ``OPT REQ`` line.

    Same TZ-offset handling as :func:`generate_chart.parse_log` so the
    timestamps share the chart's coordinate system.
    """
    from generate_chart import LINE, parse_time  # noqa: PLC0415

    out: list[tuple[datetime, dict[str, int]]] = []
    if not api_log.exists():
        return out
    raw_lines = api_log.read_text(errors="replace").splitlines()
    api_offset = timedelta(0)
    latest_inline = None
    for raw in raw_lines:
        m = LINE.match(raw.strip())
        if m:
            cand = parse_time(m.group(1), anchor_day)
            if latest_inline is None or cand > latest_inline:
                latest_inline = cand
    if latest_inline is not None:
        mtime = datetime.fromtimestamp(api_log.stat().st_mtime)
        api_offset = tz_offset_from_drift(mtime, latest_inline)
    for raw in raw_lines:
        m = LINE.match(raw.strip())
        if not m:
            continue
        msg = m.group(2)
        if "OPT REQ" not in msg:
            continue
        ts = parse_time(m.group(1), anchor_day) + api_offset
        epochs = {jid[:8]: int(ep) for jid, ep in _OPT_REQ_JOB.findall(msg)}
        if epochs:
            out.append((ts, epochs))
    return out


def _load_profiles(snap_dir: Path, jobs: list[dict]) -> dict[str, dict[tuple[str, int], float]]:
    """``{job_type: {(gpu_type, count): per_epoch_seconds}}`` from profiles_<type>.json.

    ``duration_seconds`` in the profile rows is the steady-state per-epoch
    wall-clock (warmup excluded) — exactly the value fed to the optimiser as
    the per-epoch cost, so ``per_epoch * remaining_epochs`` reproduces its
    ExecTime estimate.
    """
    out: dict[str, dict[tuple[str, int], float]] = {}
    for jtype in {j.get("job_id") for j in jobs if j.get("job_id")}:
        f = snap_dir / f"profiles_{jtype}.json"
        if not f.exists():
            continue
        per_epoch: dict[tuple[str, int], float] = {}
        for row in json.loads(f.read_text()):
            cfg = row.get("gpu_config") or {}
            dur = row.get("duration_seconds")
            if not cfg or dur is None:
                continue
            (gtype, cnt), = cfg.items()
            per_epoch[(gtype, int(cnt))] = float(dur)
        out[jtype] = per_epoch
    return out


def _predicted_finish(
    jid: str,
    j: dict,
    segs_j: list[dict],
    opt_reqs: list[tuple[datetime, dict[str, int]]],
    profiles: dict[str, dict[tuple[str, int], float]],
) -> dict | None:
    """Optimiser-predicted finish for the placement *jid* completed under.

    Returns ``{pred_ts, end_ts, R, per_epoch, half_h, bundle}`` or ``None`` when
    the job never ran a standard segment / has no profile for its final bundle.
    """
    pe_map = profiles.get(j.get("job_id", ""), {})
    standard = [s for s in segs_j if not s["profile"]]
    if not standard:
        return None
    final = standard[-1]
    gpus = final["gpus"]
    if not gpus:
        return None
    (gtype, cnt), = list(gpus.items())[:1]  # single (type, count) bundle
    per_epoch = pe_map.get((gtype, int(cnt)))
    if per_epoch is None:
        return None
    seg_start = final["start"]
    # Remaining epochs the optimiser saw at this placement: the OPT REQ with the
    # largest ts at-or-just-before the segment dispatch that lists this job.
    r: int | None = None
    best_ts: datetime | None = None
    for ts, ep in opt_reqs:
        if jid in ep and ts <= seg_start + timedelta(seconds=3) and (best_ts is None or ts > best_ts):
            best_ts, r = ts, ep[jid]
    if r is None:  # fall back to the first OPT REQ that lists it, else full epochs
        for _ts, ep in opt_reqs:
            if jid in ep:
                r = ep[jid]
                break
    if r is None:
        r = j.get("epochs_total")
    if not r:
        return None
    return {
        "pred_ts": seg_start + timedelta(seconds=r * per_epoch),
        "end_ts": final["end"],
        "R": r,
        "per_epoch": per_epoch,
        "half_h": 0.22 if sum(gpus.values()) == 1 else 0.70,
        "bundle": f"{gtype}x{cnt}",
    }


# ---------------------------------------------------------------------------
# TikZ emission (mirrors generate_chart_tex, plus the predicted-finish overlay)

PREAMBLE = r"""% Auto-generated by infra/generate_chart_tex_predicted.py — do not edit by hand.
% Source snapshot: {snap}   (predicted-vs-actual variant)
\begin{{tikzpicture}}[x={xscale}cm, y=0.75cm, font=\footnotesize,
    nodeA40/.style={{draw=blue!60!black, fill=blue!22, line width=0.4pt}},
    nodeP600/.style={{draw=red!60!black,  fill=red!22,  line width=0.4pt}},
    profhatchA/.style={{pattern=north east lines, pattern color=blue!60!black}},
    profhatchP/.style={{pattern=north east lines, pattern color=red!60!black}},
    queued/.style={{densely dotted, gray!55, line width=1.1pt}},
    deadlinev/.style={{dashed, violet!70!black, line width=0.7pt}},
    predfin/.style={{dash pattern=on 2pt off 1.4pt, orange!85!black, line width=0.9pt}},
  ]
"""

POSTAMBLE = r"""  % Two-row legend below the chart.
  \begin{{scope}}[shift={{(0,{legend_y1})}}, x=1.0cm, y=0.45cm]
    \draw[nodeA40] (0,0) rectangle (0.6,0.6);
    \node[anchor=west] at (0.7,0.3) {{\scriptsize polimi-gpu (A40)}};
    \draw[nodeP600] (4.0,0) rectangle (4.6,0.6);
    \node[anchor=west] at (4.7,0.3) {{\scriptsize matemagician (P600)}};
    \draw[queued] (8.0,0.3) -- (9.0,0.3);
    \node[anchor=west] at (9.1,0.3) {{\scriptsize queued}};
    \draw[deadlinev] (11.5,0.05) -- (11.5,0.55);
    \node[anchor=west] at (11.7,0.3) {{\scriptsize deadline}};
  \end{{scope}}
  \begin{{scope}}[shift={{(0,{legend_y2})}}, x=1.0cm, y=0.45cm]
    \draw[nodeA40] (0,0.15) rectangle (0.6,0.45);
    \draw[profhatchA] (0,0.15) rectangle (0.6,0.45);
    \node[anchor=west] at (0.7,0.3) {{\scriptsize hatched $=$ profile run}};
    \draw[nodeA40] (4.0,0) rectangle (4.6,0.6);
    \node[anchor=west] at (4.7,0.3) {{\scriptsize taller bar $=$ 2-GPU bundle}};
    \draw[predfin] (9.5,0.0) -- (9.5,0.6);
    \node[font=\tiny, text=orange!85!black] at (9.5,0.78) {{$\blacktriangledown$}};
    \node[anchor=west] at (9.75,0.3) {{\scriptsize optimiser-predicted finish ($\Delta=$ actual$-$predicted)}};
  \end{{scope}}
\end{{tikzpicture}}
"""


def emit_tikz(
    snap_dir: Path,
    label_map: dict[str, str],
    deadline_overrides: dict[str, float],
) -> str:
    events, jobs, derived, t0 = _load(snap_dir)
    segs = derived["segs"]
    progress = derived["progress"]
    submit_ts: dict[str, datetime] = derived["submit_ts"]

    opt_reqs = _parse_opt_req_epochs(snap_dir / "api.log", t0.replace(microsecond=0))
    profiles = _load_profiles(snap_dir, jobs)

    def to_min(ts: datetime) -> float:
        return (ts - t0).total_seconds() / 60.0

    job_first_ts = first_dispatch_times(events)
    sorted_jobs = sorted(jobs, key=lambda j: job_first_ts.get(j["id"][:8], t0))

    by_created = sorted(jobs, key=lambda j: _parse_iso(j["created_at"]))
    fallback_name = {j["id"][:8]: f"J{i + 1}" for i, j in enumerate(by_created)}

    snapshot_jids = {j["id"][:8] for j in jobs}
    seg_end_max = max(
        (to_min(s["end"]) for jid, sl in segs.items() if jid in snapshot_jids for s in sl),
        default=10.0,
    )

    job_deadline_x: dict[str, float] = {}
    for j in sorted_jobs:
        jid = j["id"][:8]
        override_min = None
        for prefix, off in deadline_overrides.items():
            if jid.startswith(prefix):
                override_min = off
                break
        anchor = submit_ts.get(jid) or job_first_ts.get(jid)
        if override_min is not None and anchor is not None:
            job_deadline_x[jid] = to_min(anchor) + override_min
        elif j.get("created_at") and j.get("deadline"):
            job_deadline_x[jid] = to_min(_parse_iso(j["deadline"]))

    # Predicted-finish per job (drawn over the actual bars below).  Compute
    # first so far-right predictions widen the x-range like a deadline would.
    pred: dict[str, dict] = {}
    for j in sorted_jobs:
        jid = j["id"][:8]
        if j.get("status") != "SUCCEEDED":
            continue
        pf = _predicted_finish(jid, j, segs.get(jid, []), opt_reqs, profiles)
        if pf is not None:
            pred[jid] = pf

    dl_visible = [d for d in job_deadline_x.values() if 0 <= d <= seg_end_max + 5]
    pred_x_visible = [to_min(p["pred_ts"]) for p in pred.values() if to_min(p["pred_ts"]) <= seg_end_max + 8]
    x_max = max(seg_end_max + 4.0, *(dl_visible or [0.0]), *(pred_x_visible or [0.0]))

    row_gap = 1.4
    rows: list[tuple[str, float, dict]] = []
    for i, j in enumerate(sorted_jobs):
        rows.append((j["id"][:8], round(1.0 + i * row_gap, 2), j))

    xscale = max(0.18, min(0.55, 14.0 / max(x_max, 1.0)))

    lines: list[str] = []
    lines.append(PREAMBLE.format(snap=snap_dir.name, xscale=f"{xscale:.3f}"))

    axis_max = x_max + 0.5
    if x_max <= 15:
        tick_step = 2
    elif x_max <= 30:
        tick_step = 4
    elif x_max <= 60:
        tick_step = 5
    else:
        tick_step = 10
    ticks = list(range(0, int(x_max) + 1, tick_step))
    lines.append(f"  \\draw[->] (0,0) -- ({axis_max:.2f},0);\n")
    for t in ticks:
        lines.append(f"  \\draw ({t},0) -- ({t},-0.10) node[below] {{{t}}};\n")
    lines.append(f"  \\node[anchor=west] at ({axis_max + 0.1:.2f},0) {{time [min]}};\n\n")

    for jid, y, j in rows:
        label = _resolve_label(jid, label_map, fallback_name[jid])
        lines.append(f"  % ---- {label} ({jid}, p={j['priority']} ep={j['epochs_total']})\n")
        lines.append(f"  \\node[anchor=east] at (-0.2,{y}) {{\\textbf{{{label}}}}};\n")

        first_disp = job_first_ts.get(jid)
        submit = submit_ts.get(jid)
        if submit is None and j.get("created_at"):
            submit = _parse_iso(j["created_at"])
        if first_disp is not None and submit is not None:
            q_start = max(0.0, to_min(submit))
            q_end = to_min(first_disp)
            if q_end - q_start > 0.05:
                lines.append(f"  \\draw[queued] ({q_start:.2f},{y}) -- ({q_end:.2f},{y});\n")

        for seg in segs.get(jid, []):
            start = to_min(seg["start"])
            end = max(to_min(seg["end"]), start + 0.05)
            n_gpu = sum(seg["gpus"].values())
            half_h = 0.22 if n_gpu == 1 else 0.70
            style = STYLE_SUFFIX.get(seg["node"], "A40")
            lines.append(
                f"  \\draw[node{style}] ({start:.2f},{y - half_h:.2f}) "
                f"rectangle ({end:.2f},{y + half_h:.2f});\n"
            )
            if seg["profile"]:
                lines.append(
                    f"    \\draw[profhatch{style[0]}] ({start:.2f},{y - half_h:.2f}) "
                    f"rectangle ({end:.2f},{y + half_h:.2f});\n"
                )
            if n_gpu == 2:
                mid = (start + end) / 2
                lines.append(
                    f"  \\node[font=\\tiny, anchor=center] at "
                    f"({mid:.2f},{y + half_h + 0.15:.2f}) {{$\\times 2$}};\n"
                )

        dx = job_deadline_x.get(jid)
        dl_suffix = ""
        if dx is not None:
            if 0 <= dx <= x_max:
                lines.append(
                    f"  \\draw[deadlinev] ({dx:.2f},{y - 0.45:.2f}) -- ({dx:.2f},{y + 0.45:.2f});\n"
                )
            elif dx > x_max:
                off_txt = f"$+${dx / 60:.0f}\\,h" if dx >= 60 else f"$+${dx:.0f}\\,m"
                dl_suffix = "\\ \\textcolor{violet!70!black}{d\\,$\\to$\\," + off_txt + "}"

        # --- Predicted-finish overlay -------------------------------------
        # Orange marker at the optimiser's predicted completion for the run
        # this job finished under; the gap to the bar's right edge is the error.
        pred_suffix = ""
        pf = pred.get(jid)
        if pf is not None:
            px = to_min(pf["pred_ts"])
            ax = to_min(pf["end_ts"])
            hh = pf["half_h"]
            top = y + hh + 0.12
            bot = y - hh - 0.12
            if px <= x_max:
                lines.append(f"  \\draw[predfin] ({px:.2f},{bot:.2f}) -- ({px:.2f},{top:.2f});\n")
                lines.append(
                    f"  \\node[font=\\tiny, text=orange!85!black, anchor=south] "
                    f"at ({px:.2f},{top - 0.02:.2f}) {{$\\blacktriangledown$}};\n"
                )
            # Signed error (actual − predicted) appended to the terminal label
            # so it never collides with bars or neighbouring rows.
            derr = ax - px
            pred_suffix = (
                "\\ \\textcolor{orange!75!black}{\\scriptsize ($\\Delta{=}"
                + f"{derr:+.1f}" + "$\\,m)}"
            )

        status = j.get("status", "")
        if status in ("SUCCEEDED", "FAILED"):
            last_end = max((to_min(s["end"]) for s in segs.get(jid, [])), default=0.0)
            prog = progress.get(jid)
            done = prog[0] if prog else j["epochs_total"]
            label_txt = "SUCC." if status == "SUCCEEDED" else "FAIL"
            color = "green!45!black" if status == "SUCCEEDED" else "red!70!black"
            lines.append(
                f"  \\node[anchor=west, font=\\tiny, text={color}] "
                f"at ({last_end + 0.2:.2f},{y}) "
                f"{{{label_txt} {done}/{j['epochs_total']}{pred_suffix}{dl_suffix}}};\n"
            )
        elif dl_suffix:
            last_end = max((to_min(s["end"]) for s in segs.get(jid, [])), default=0.0)
            lines.append(
                f"  \\node[anchor=west, font=\\tiny] "
                f"at ({last_end + 0.2:.2f},{y}) {{{dl_suffix.lstrip()}}};\n"
            )

        lines.append("\n")

    lines.append(POSTAMBLE.format(legend_y1=-1.5, legend_y2=-2.6))
    return "".join(lines)


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("snap_dir", type=Path)
    p.add_argument("out_tex", type=Path)
    p.add_argument("--label", action="append", default=[], metavar="JID=NAME")
    p.add_argument("--deadline", action="append", default=[], metavar="JID=OFFSET")
    return p.parse_args(argv)


def main(argv: list[str]) -> None:
    args = parse_args(argv)
    label_map: dict[str, str] = {}
    for spec in args.label:
        if "=" not in spec:
            raise SystemExit(f"Bad --label spec (need JID=NAME): {spec!r}")
        k, v = spec.split("=", 1)
        label_map[k] = v
    deadline_overrides: dict[str, float] = {}
    for spec in args.deadline:
        if "=" not in spec:
            raise SystemExit(f"Bad --deadline spec (need JID=OFFSET): {spec!r}")
        k, v = spec.split("=", 1)
        deadline_overrides[k] = _parse_offset(v)
    tex = emit_tikz(args.snap_dir, label_map, deadline_overrides)
    args.out_tex.write_text(tex)
    print(f"Wrote {args.out_tex} ({len(tex)} bytes)")


if __name__ == "__main__":
    main(sys.argv[1:])
