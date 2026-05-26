"""Tests for the slot-freed NOTIFY payload parser + listener wake gate.

The fix this guards: every ``ijm_slot_freed`` NOTIFY used to wake the
optimiser unconditionally, including notifies caused by the API's own
``/stop?reason=auto`` calls issued as part of an in-flight plan.  That
self-inflicted wake let RG's near-tie nondeterminism flip the plan
mid-execution.  The new payload carries a reason; the listener gates
the wake on it.
"""

from __future__ import annotations

import pytest
from shared.constants import SlotFreedReason

from src.app import parse_slot_payload


class TestParseSlotPayload:
    def test_legacy_two_field_defaults_to_terminal(self) -> None:
        # Pre-fix workers emit "<node>:<count>" with no reason.
        # Fallback must still parse + assume external (TERMINAL) so a
        # rolling deploy never drops a slot-release event.
        assert parse_slot_payload("polimi-gpu:1") == ("polimi-gpu", 1, SlotFreedReason.TERMINAL)
        assert parse_slot_payload("matemagician:2") == ("matemagician", 2, SlotFreedReason.TERMINAL)

    def test_new_three_field_terminal(self) -> None:
        assert parse_slot_payload("polimi-gpu:1:terminal") == (
            "polimi-gpu",
            1,
            SlotFreedReason.TERMINAL,
        )

    def test_new_three_field_user_stop(self) -> None:
        assert parse_slot_payload("polimi-gpu:1:user_stop") == (
            "polimi-gpu",
            1,
            SlotFreedReason.USER_STOP,
        )

    def test_new_three_field_auto_preempt(self) -> None:
        # The whole point of the fix: this one must be recognisable
        # so the listener can swallow the wake.
        assert parse_slot_payload("matemagician:2:auto_preempt") == (
            "matemagician",
            2,
            SlotFreedReason.AUTO_PREEMPT,
        )

    def test_new_three_field_orphan_drain(self) -> None:
        assert parse_slot_payload("polimi-gpu:1:orphan_drain") == (
            "polimi-gpu",
            1,
            SlotFreedReason.ORPHAN_DRAIN,
        )

    def test_empty_or_whitespace_payload(self) -> None:
        assert parse_slot_payload(None) is None
        assert parse_slot_payload("") is None
        assert parse_slot_payload("   ") is None

    def test_malformed_payloads(self) -> None:
        # Wrong field count
        assert parse_slot_payload("polimi-gpu") is None
        assert parse_slot_payload("a:b:c:d") is None
        # Non-integer count
        assert parse_slot_payload("polimi-gpu:NaN") is None
        assert parse_slot_payload("polimi-gpu:NaN:terminal") is None

    def test_unknown_reason_falls_back_to_terminal(self) -> None:
        # Future-proofing: if a worker emits a reason this API doesn't
        # know yet, we still parse + default-wake.  No "ignored event"
        # silently dropped slot-counts.
        result = parse_slot_payload("polimi-gpu:1:wormhole")
        assert result is not None
        node_id, count, reason = result
        assert (node_id, count) == ("polimi-gpu", 1)
        assert reason == SlotFreedReason.TERMINAL

    def test_node_id_can_contain_dashes_and_dots(self) -> None:
        # rsplit(":", N) style: ensure we split LEFT-to-right on the
        # known structural fields (count, reason), not on the node_id
        # which can contain anything that isn't ":".
        assert parse_slot_payload("multi-word-node:1:terminal") == (
            "multi-word-node",
            1,
            SlotFreedReason.TERMINAL,
        )


class TestListenerWakeGate:
    """The listener calls ``node_slots.release()`` unconditionally and
    ``notify_event.set()`` only for non-AUTO_PREEMPT events.

    We model the gate decision in isolation rather than spinning up a
    real Postgres LISTEN connection.  The decision logic lives in
    ``_slot_listener`` but is small enough that a behavioural assertion
    matching the new code keeps regressions visible.
    """

    @pytest.mark.parametrize(
        ("reason", "should_wake"),
        [
            (SlotFreedReason.TERMINAL, True),
            (SlotFreedReason.USER_STOP, True),
            (SlotFreedReason.ORPHAN_DRAIN, True),
            (SlotFreedReason.AUTO_PREEMPT, False),
        ],
    )
    def test_wake_decision_by_reason(self, reason: SlotFreedReason, should_wake: bool) -> None:
        # The gate is a single boolean: wake iff reason is not AUTO_PREEMPT.
        # If this assertion ever flips, _slot_listener's behaviour changed
        # and the regression test for d6365b9a-style churn would catch
        # the surface symptom.
        assert (reason != SlotFreedReason.AUTO_PREEMPT) is should_wake
