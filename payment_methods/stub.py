"""Stub payment method implementation."""
from __future__ import annotations

from typing import Any

from .base import PaymentMethod


class StubPaymentMethod(PaymentMethod):
    """Placeholder payment method that raises NotImplementedError."""

    name = "stub"

    def deposit(self, amount: float, currency: str, **kwargs: Any) -> dict[str, Any]:
        raise NotImplementedError("Payment method not implemented")

    def withdraw(self, amount: float, currency: str, **kwargs: Any) -> dict[str, Any]:
        raise NotImplementedError("Payment method not implemented")

    def get_balance(self, **kwargs: Any) -> dict[str, Any]:
        raise NotImplementedError("Payment method not implemented")
