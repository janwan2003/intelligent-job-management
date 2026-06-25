"""Regression tests for profiling per-epoch measurement.

History of this code path (two bugs, two fixes):

1. (2026-06-25a) The runtime logs *two* lines per epoch that both match
   ``PROGRESS_RE`` (``Epoch N/M - starting`` then ``Epoch N/M - Loss: ...``).
   The worker appended a profiling sample on every match, so a 3-epoch run
   produced 6 samples and the per-epoch estimate was ~halved.  Fixed by keying
   the sample off ``EPOCH_DONE_RE`` (completion line only).

2. (2026-06-25b) The sample was the worker's ``time.monotonic()`` at log-line
   *arrival*; intervals between arrivals absorb checkpoint-save I/O and
   log/network jitter, which run far heavier during the concurrent profile
   sweep than during the steady standard run (A40x1 read 8.0s by arrival vs
   ~5.2s real compute), so predictions ran early.  Fixed by parsing the
   runtime's own per-epoch compute time (the trailing ``- 5.21s``).
"""

from execution import EPOCH_DONE_RE, PROGRESS_RE
from profiling import compute_duration

# Real container stdout (post-strip), as _stream_output sees it.
START_LINE = "2026-06-03 09:42:00 - base - INFO - Epoch 13/50 - starting"
DONE_LINE = "2026-06-03 09:43:17 - base - INFO - Epoch 13/50 - Loss: 0.49 - Acc: 9.38% - 77.42s"
CKPT_LINE = "2026-06-03 09:43:17 - base - INFO - Checkpoint saved at epoch 13"


def test_progress_re_matches_both_epoch_lines() -> None:
    # Progress reporting intentionally fires on both start and completion.
    assert PROGRESS_RE.search(START_LINE)
    assert PROGRESS_RE.search(DONE_LINE)


def test_epoch_done_re_matches_completion_only_and_extracts_compute() -> None:
    assert not EPOCH_DONE_RE.search(START_LINE)  # no trailing duration
    assert not EPOCH_DONE_RE.search(CKPT_LINE)
    m = EPOCH_DONE_RE.search(DONE_LINE)
    assert m is not None
    assert int(m.group(1)) == 13  # epoch number
    assert float(m.group(2)) == 77.42  # runtime's own compute time, not arrival clock


def test_epoch_done_re_grabs_last_duration_not_loss_or_acc() -> None:
    # Loss/Acc numbers must never be mistaken for the per-epoch time.
    m = EPOCH_DONE_RE.search("x - Epoch 1/3 - Loss: 2.330128 - Acc: 12.50% - 5.51s")
    assert m is not None and float(m.group(2)) == 5.51


def _samples_for_run(lines: list[str]) -> list[tuple[int, float]]:
    """Replicate _stream_output's profiling-sample gate over a line stream."""
    out: list[tuple[int, float]] = []
    for s in lines:
        if PROGRESS_RE.search(s):
            done = EPOCH_DONE_RE.search(s)
            if done:
                out.append((int(done.group(1)), float(done.group(2))))
    return out


def test_one_sample_per_epoch_with_runtime_compute_time() -> None:
    lines = [
        "Epoch 1/3 - starting",
        "Epoch 1/3 - Loss: 0.48 - Acc: 95.1% - 9.12s",
        "Checkpoint saved at epoch 1",
        "Epoch 2/3 - starting",
        "Epoch 2/3 - Loss: 0.14 - Acc: 96.9% - 9.14s",
        "Epoch 3/3 - starting",
        "Epoch 3/3 - Loss: 0.09 - Acc: 97.3% - 9.19s",
    ]
    samples = _samples_for_run(lines)
    assert samples == [(1, 9.12), (2, 9.14), (3, 9.19)]  # one per epoch, runtime values


def test_compute_duration_averages_compute_and_excludes_warmup() -> None:
    from datetime import UTC, datetime

    # (epoch_num, runtime compute_seconds). Epoch 1 = warmup, dropped.
    samples = [(1, 9.99), (2, 9.10), (3, 9.20)]
    mean = compute_duration(samples, datetime.now(UTC))
    assert mean == (9.10 + 9.20) / 2  # 9.15; warmup 9.99 excluded

    # Below threshold -> falls back to wall-clock (not the single sample).
    assert compute_duration([(1, 9.10)], datetime.now(UTC)) >= 0.0


def test_compute_duration_uses_logged_value_not_arrival_interval() -> None:
    # The whole point of fix (2): if the worker had timed arrivals, a contended
    # sweep would inflate the result. Here we feed the runtime's stable compute
    # numbers and get exactly them back — no I/O/jitter term sneaks in.
    from datetime import UTC, datetime

    steady = 5.20
    samples = [(1, 7.00), (2, steady), (3, steady), (4, steady)]
    assert compute_duration(samples, datetime.now(UTC)) == steady
