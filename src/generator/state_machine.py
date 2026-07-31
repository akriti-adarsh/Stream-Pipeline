"""The ride lifecycle state machine: the single source of truth for legality.

A ride cannot be completed without being started, cannot receive events after a
terminal state, and must always begin with ``requested``. The simulator drives
every ride through this class, so an illegal transition is structurally
impossible to emit, and the property-based test in
tests/test_state_machine.py hammers that claim across arbitrary seeds.
"""

from __future__ import annotations

from enum import StrEnum


class RideEventType(StrEnum):
    REQUESTED = "requested"
    MATCHED = "matched"
    DRIVER_ARRIVED = "driver_arrived"
    STARTED = "started"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


TERMINAL_STATES = frozenset({RideEventType.COMPLETED, RideEventType.CANCELLED})

LEGAL_TRANSITIONS: dict[RideEventType, frozenset[RideEventType]] = {
    RideEventType.REQUESTED: frozenset({RideEventType.MATCHED, RideEventType.CANCELLED}),
    RideEventType.MATCHED: frozenset({RideEventType.DRIVER_ARRIVED, RideEventType.CANCELLED}),
    RideEventType.DRIVER_ARRIVED: frozenset({RideEventType.STARTED, RideEventType.CANCELLED}),
    RideEventType.STARTED: frozenset({RideEventType.COMPLETED, RideEventType.CANCELLED}),
    RideEventType.COMPLETED: frozenset(),
    RideEventType.CANCELLED: frozenset(),
}

# Lifecycle ordinal, constant per event type. Together with ride_id it forms the
# idempotency key used by the Postgres sink: a duplicate of the same event maps
# to the same (ride_id, event_seq) no matter when it arrives.
EVENT_SEQ: dict[RideEventType, int] = {
    RideEventType.REQUESTED: 1,
    RideEventType.MATCHED: 2,
    RideEventType.DRIVER_ARRIVED: 3,
    RideEventType.STARTED: 4,
    RideEventType.COMPLETED: 5,
    RideEventType.CANCELLED: 6,
}


class IllegalTransitionError(Exception):
    """Raised when a ride would move through an edge that does not exist."""


class RideStateMachine:
    def __init__(self) -> None:
        self._state: RideEventType | None = None

    @property
    def state(self) -> RideEventType | None:
        return self._state

    def advance(self, event: RideEventType) -> None:
        if self._state is None:
            if event is not RideEventType.REQUESTED:
                raise IllegalTransitionError(f"first event must be requested, got {event.value}")
        elif event not in LEGAL_TRANSITIONS[self._state]:
            raise IllegalTransitionError(
                f"{self._state.value} -> {event.value} is not a legal edge"
            )
        self._state = event

    @property
    def is_terminal(self) -> bool:
        return self._state in TERMINAL_STATES

    def legal_next(self) -> frozenset[RideEventType]:
        if self._state is None:
            return frozenset({RideEventType.REQUESTED})
        return LEGAL_TRANSITIONS[self._state]
