"""Legality of the ride state machine, both by enumeration and by fuzzing."""

from __future__ import annotations

from itertools import pairwise

import pytest
from hypothesis import given
from hypothesis import strategies as st

from generator.state_machine import (
    EVENT_SEQ,
    LEGAL_TRANSITIONS,
    TERMINAL_STATES,
    IllegalTransitionError,
    RideEventType,
    RideStateMachine,
)


def test_happy_path_completes() -> None:
    machine = RideStateMachine()
    for event in (
        RideEventType.REQUESTED,
        RideEventType.MATCHED,
        RideEventType.DRIVER_ARRIVED,
        RideEventType.STARTED,
        RideEventType.COMPLETED,
    ):
        machine.advance(event)
    assert machine.is_terminal


def test_first_event_must_be_requested() -> None:
    machine = RideStateMachine()
    with pytest.raises(IllegalTransitionError):
        machine.advance(RideEventType.COMPLETED)


def test_cannot_complete_without_start() -> None:
    machine = RideStateMachine()
    machine.advance(RideEventType.REQUESTED)
    machine.advance(RideEventType.MATCHED)
    with pytest.raises(IllegalTransitionError):
        machine.advance(RideEventType.COMPLETED)


def test_terminal_states_accept_nothing() -> None:
    for terminal in TERMINAL_STATES:
        assert LEGAL_TRANSITIONS[terminal] == frozenset()


def test_cancellation_possible_from_every_non_terminal_stage() -> None:
    for state, nexts in LEGAL_TRANSITIONS.items():
        if state not in TERMINAL_STATES:
            assert RideEventType.CANCELLED in nexts, state


def test_event_seq_is_injective() -> None:
    assert len(set(EVENT_SEQ.values())) == len(EVENT_SEQ)


@given(st.lists(st.sampled_from(list(RideEventType)), min_size=1, max_size=12))
def test_machine_never_accepts_an_illegal_sequence(events: list[RideEventType]) -> None:
    """Replay arbitrary event soup; every accepted prefix must be a legal path."""
    machine = RideStateMachine()
    accepted: list[RideEventType] = []
    for event in events:
        try:
            machine.advance(event)
        except IllegalTransitionError:
            break
        accepted.append(event)
    if accepted:
        assert accepted[0] is RideEventType.REQUESTED
        for prev, nxt in pairwise(accepted):
            assert nxt in LEGAL_TRANSITIONS[prev]
        assert all(s not in TERMINAL_STATES for s in accepted[:-1])
