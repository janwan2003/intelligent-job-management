"""Tests for the pending-migrate apply path.

Regression guard: previously, when the optimiser emitted a *migrate*
plan (same instance in both ``assignments`` and ``preempt``), the
apply step's ``UPDATE ... WHERE assigned_node IS NULL`` silently
skipped the row (it was still RUNNING on the source node), so the new
destination was never written.  With the AUTO_PREEMPT wake-suppression
in place, no later optimiser pass ran until an unrelated external
event (terminal NOTIFY, drift heartbeat, 60-min watcher), leaving the
URGENT instance idle for ~2 minutes after each migrate.

The fix stashes migrate assignments in ``state.pending_migrates`` and
lets the slot listener apply them once the source ``/stop`` drains
the row back to QUEUED+NULL.  These tests pin:

  1. The classification rule the apply step uses (overlap of
     ``.assignments`` and ``.preempt``) — if this contract ever
     changes the apply branch breaks silently.
  2. The conditional-UPDATE filter (``assigned_node IS NULL AND
     status = QUEUED``) so the listener can only apply a migrate
     after the source worker has actually drained.
"""

from __future__ import annotations

from src.optimizer import Assignment, OptimizerResult


class TestMigrateDetection:
    def test_new_placement_no_preempt_overlap(self) -> None:
        result = OptimizerResult(
            assignments=[Assignment(instance_id="A", node_id="n1", gpu_config={"A40": 1})],
            preempt=[],
        )
        a = result.assignments[0]
        assert a.instance_id not in result.preempt

    def test_drop_preempt_no_assignment(self) -> None:
        result = OptimizerResult(assignments=[], preempt=["B"])
        assert "B" in result.preempt
        assert not result.assignments

    def test_migrate_appears_in_both(self) -> None:
        # The contract the apply step relies on.  A migrate is the
        # single case where ``instance_id`` appears in BOTH lists.
        result = OptimizerResult(
            assignments=[Assignment(instance_id="C", node_id="n2", gpu_config={"A40": 2})],
            preempt=["C"],
        )
        a = result.assignments[0]
        assert a.instance_id in result.preempt

    def test_mixed_plan_classification(self) -> None:
        # Plan combining a new placement, a drop, and a migrate.  The
        # apply step must classify each correctly: the migrate goes to
        # pending_migrates; the new placement takes the UPDATE-WHERE-NULL
        # path; the drop only produces a preempt task.
        result = OptimizerResult(
            assignments=[
                Assignment(instance_id="new1", node_id="n1", gpu_config={"A40": 1}),
                Assignment(instance_id="mig1", node_id="n2", gpu_config={"QuadroP600": 2}),
            ],
            preempt=["drop1", "mig1"],
        )
        migrates = {a.instance_id for a in result.assignments if a.instance_id in result.preempt}
        new_placements = {a.instance_id for a in result.assignments if a.instance_id not in result.preempt}
        drops = set(result.preempt) - migrates
        assert migrates == {"mig1"}
        assert new_placements == {"new1"}
        assert drops == {"drop1"}


class TestPendingMigrateApplySQL:
    """The listener's apply UPDATE must be safe to fire on every NOTIFY.

    The conditional is ``WHERE id = ? AND status = 'QUEUED' AND
    assigned_node IS NULL``.  This means:

    - A pending migrate for instance X only commits after X's own
      auto-preempt has drained the row to QUEUED+NULL.  Other jobs'
      NOTIFYs do not match X's row, so they are no-ops.
    - If the row was re-assigned by some other path in the meantime
      (manual stop, drift recovery), assigned_node is non-null and the
      apply is a no-op — no clobber.

    These properties are enforced by the SQL itself; the unit-test
    here just documents them so a future refactor of the UPDATE filter
    has to revisit the contract.
    """

    def test_update_filter_excludes_non_queued(self) -> None:
        # If the listener's UPDATE were ``WHERE id = ?`` only (no status
        # guard), a migrate could land on a row that has since gone
        # PROFILING/RUNNING/SUCCEEDED, silently overwriting it.  The
        # filter must include status = 'QUEUED'.
        from src.app import parse_slot_payload  # noqa: F401  - import sanity

        # No DB needed: just assert the filter clause is what we expect.
        # If someone weakens this filter to e.g. ``WHERE id = ? AND
        # assigned_node IS NULL`` without the status guard, this test
        # breaks because the docstring above pins the contract.
        # (Behavioural verification is covered by infra/e2e_scenario.sh.)
        expected_clause = "WHERE id = %s AND status = %s AND assigned_node IS NULL"
        # Re-grep the source rather than refactor: the listener's apply
        # SQL is hot-path inside lifespan() and not directly importable.
        from pathlib import Path

        src = Path(__file__).resolve().parents[1] / "src" / "app.py"
        text = src.read_text()
        assert expected_clause in text, "pending-migrate UPDATE filter changed; revisit invariants"
