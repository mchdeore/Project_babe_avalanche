"""Base interface for payment method integrations."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class PaymentMethod(ABC):
    """Interface for payment method integrations."""

    name: str

    @abstractmethod
    def deposit(self, amount: float, currency: str, **kwargs: Any) -> dict[str, Any]:
        """Deposit funds.

        Args:
            amount: Amount to deposit.
            currency: Currency code (e.g., "USD").
            **kwargs: Provider-specific options.

        Returns:
            Provider response payload.
        """
        raise NotImplementedError

    @abstractmethod
    def withdraw(self, amount: float, currency: str, **kwargs: Any) -> dict[str, Any]:
        """Withdraw funds.

        Args:
            amount: Amount to withdraw.
            currency: Currency code (e.g., "USD").
            **kwargs: Provider-specific options.

        Returns:
            Provider response payload.
        """
        raise NotImplementedError

    @abstractmethod
    def get_balance(self, **kwargs: Any) -> dict[str, Any]:
        """Get current balance.

        Args:
            **kwargs: Provider-specific options.

        Returns:
            Provider response payload.
        """
        raise NotImplementedError
