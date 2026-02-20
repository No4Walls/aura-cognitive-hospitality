from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class WhisperType(str, Enum):
    GUEST_CONTEXT = "guest_context"
    STRATEGY_HINT = "strategy_hint"
    SENTIMENT = "sentiment"
    MENU_SUGGESTION = "menu_suggestion"


class AgentRole(str, Enum):
    HISTORIAN = "historian"
    NEGOTIATOR = "negotiator"
    SOMMELIER = "sommelier"
    GATEWAY = "gateway"


@dataclass
class Whisper:
    agent: AgentRole
    whisper_type: WhisperType
    session_id: str
    payload: dict[str, Any]
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent": self.agent.value,
            "whisper_type": self.whisper_type.value,
            "session_id": self.session_id,
            "payload": self.payload,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Whisper:
        return cls(
            agent=AgentRole(data["agent"]),
            whisper_type=WhisperType(data["whisper_type"]),
            session_id=data["session_id"],
            payload=data["payload"],
            timestamp=float(data["timestamp"]),
        )


@dataclass
class GuestProfile:
    phone_number: str
    name: str
    last_call_date: str
    preference_tags: list[str] = field(default_factory=list)
    notes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "phone_number": self.phone_number,
            "name": self.name,
            "last_call_date": self.last_call_date,
            "preference_tags": self.preference_tags,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GuestProfile:
        return cls(
            phone_number=data["phone_number"],
            name=data["name"],
            last_call_date=data["last_call_date"],
            preference_tags=data.get("preference_tags", []),
            notes=data.get("notes", {}),
        )


@dataclass
class TranscriptEvent:
    session_id: str
    caller_number: str
    text: str
    direction: str  # "inbound" or "outbound"
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "caller_number": self.caller_number,
            "text": self.text,
            "direction": self.direction,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TranscriptEvent:
        return cls(
            session_id=data["session_id"],
            caller_number=data["caller_number"],
            text=data["text"],
            direction=data["direction"],
            timestamp=float(data["timestamp"]),
        )
