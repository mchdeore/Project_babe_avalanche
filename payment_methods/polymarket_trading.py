"""Polymarket trading service for manual execution of trades."""
from __future__ import annotations

import json
import os
from typing import Any, Optional

from dotenv import load_dotenv

from utils import init_db, load_config, upsert_orders, utc_now_iso


class PolymarketTradingService:
    """Polymarket trading service for manual execution of trades."""

    def __init__(
        self,
        config: Optional[dict[str, Any]] = None,
        db_path: Optional[str] = None,
    ) -> None:
        load_dotenv()
        self.config = config or load_config()
        if db_path is None:
            db_path = self.config.get("storage", {}).get("database", "odds.db")

        self.conn = init_db(db_path)

        self.host = os.getenv("POLY_HOST", "https://clob.polymarket.com")
        self.chain_id = int(os.getenv("POLY_CHAIN_ID", "137"))
        self.private_key = os.getenv("POLY_PRIVATE_KEY", "")
        self.signature_type = int(os.getenv("POLY_SIGNATURE_TYPE", "0"))
        self.funder = os.getenv("POLY_FUNDER", "")

        self.api_key = os.getenv("POLY_API_KEY", "")
        self.api_secret = os.getenv("POLY_API_SECRET", "")
        self.api_passphrase = os.getenv("POLY_API_PASSPHRASE", "")

        self._client = None
        self._sdk = None
        self._api_creds_set = False

    def __enter__(self) -> "PolymarketTradingService":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        """Close underlying resources."""
        self.conn.close()

    def _load_sdk(self) -> None:
        if self._sdk is not None:
            return
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds, MarketOrderArgs, OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY, SELL
        except ImportError as exc:
            raise ImportError(
                "py-clob-client is required for Polymarket trading. "
                "Install it via `pip install py-clob-client`."
            ) from exc

        self._sdk = {
            "ClobClient": ClobClient,
            "ApiCreds": ApiCreds,
            "MarketOrderArgs": MarketOrderArgs,
            "OrderArgs": OrderArgs,
            "OrderType": OrderType,
            "BUY": BUY,
            "SELL": SELL,
        }

    def _get_client(self):
        self._load_sdk()
        if self._client is None:
            ClobClient = self._sdk["ClobClient"]
            if self.private_key:
                self._client = ClobClient(
                    self.host,
                    key=self.private_key,
                    chain_id=self.chain_id,
                    signature_type=self.signature_type,
                    funder=self.funder or None,
                )
            else:
                self._client = ClobClient(self.host)
        return self._client

    def login(self) -> bool:
        """Authenticate with Polymarket and set API creds."""
        if self._api_creds_set:
            return True
        if not self.private_key:
            return False

        client = self._get_client()
        ApiCreds = self._sdk["ApiCreds"]
        try:
            if self.api_key and self.api_secret and self.api_passphrase:
                creds = ApiCreds(
                    api_key=self.api_key,
                    api_secret=self.api_secret,
                    api_passphrase=self.api_passphrase,
                )
                client.set_api_creds(creds)
            else:
                client.set_api_creds(client.create_or_derive_api_creds())
        except Exception:
            return False

        self._api_creds_set = True
        return True

    def market_infos(self) -> Optional[dict[str, Any]]:
        """Fetch market info records."""
        client = self._get_client()
        try:
            return client.get_simplified_markets()
        except Exception:
            return None

    def place_order(self, order: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Place an order using a raw Polymarket payload."""
        if not order:
            return None
        if not self.login():
            return None

        token_id = order.get("token_id")
        if not token_id:
            return None

        client = self._get_client()
        OrderArgs = self._sdk["OrderArgs"]
        MarketOrderArgs = self._sdk["MarketOrderArgs"]
        OrderType = self._sdk["OrderType"]

        side = _resolve_side(order.get("side"), self._sdk["BUY"], self._sdk["SELL"])
        order_type = _resolve_order_type(
            order.get("order_type") or order.get("orderType"),
            OrderType,
            OrderType.GTC,
        )

        resp = None
        try:
            if order.get("amount") is not None:
                args = _build_args(
                    MarketOrderArgs,
                    {
                        "token_id": token_id,
                        "amount": order.get("amount"),
                        "side": side,
                        "order_type": _resolve_order_type(
                            order.get("order_type") or order.get("orderType"),
                            OrderType,
                            OrderType.FOK,
                        ),
                    },
                )
                signed = client.create_market_order(args)
                resp = client.post_order(signed, OrderType.FOK)
            else:
                price = order.get("price")
                size = order.get("size") or order.get("quantity")
                if price is None or size is None:
                    return None
                args = _build_args(
                    OrderArgs,
                    {
                        "token_id": token_id,
                        "price": price,
                        "size": size,
                        "side": side,
                        "post_only": order.get("post_only"),
                    },
                )
                signed = client.create_order(args)
                resp = client.post_order(signed, order_type)
        except Exception:
            return None

        self._store_orders(resp, order, token_id)
        return resp

    def confirm_order(self, order: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Alias to place_order for STX-style ergonomics."""
        return self.place_order(order)

    def cancel_order(self, order_id: str) -> Optional[dict[str, Any]]:
        """Cancel a single order by ID."""
        if not order_id:
            return None
        if not self.login():
            return None

        client = self._get_client()
        try:
            resp = client.cancel(order_id)
        except Exception:
            return None

        status = "canceled"
        if isinstance(resp, dict) and resp.get("status"):
            status = resp.get("status")

        row = {
            "source": "polymarket",
            "provider": "polymarket",
            "order_id": order_id,
            "status": status,
            "updated_at": utc_now_iso(),
        }
        upsert_orders(self.conn, [row])
        self.conn.commit()

        return resp

    def cancel_all_orders(self) -> Optional[dict[str, Any]]:
        """Cancel all open orders."""
        if not self.login():
            return None

        client = self._get_client()
        try:
            resp = client.cancel_all()
        except Exception:
            return None

        rows = []
        now = utc_now_iso()
        if isinstance(resp, list):
            for item in resp:
                order_id = item.get("id") if isinstance(item, dict) else None
                if order_id:
                    rows.append({
                        "source": "polymarket",
                        "provider": "polymarket",
                        "order_id": order_id,
                        "status": "canceled",
                        "updated_at": now,
                    })
        if rows:
            upsert_orders(self.conn, rows)
            self.conn.commit()

        return resp

    def get_open_orders(self) -> None:
        """Stub: fetch open orders once the query is available."""
        # TODO: Implement once the API call is finalized.
        return None

    def get_positions(self) -> None:
        """Stub: fetch positions once the query is available."""
        # TODO: Implement once the API call is finalized.
        return None

    def get_balances(self) -> None:
        """Stub: fetch balances once the API call is finalized."""
        # TODO: Implement once the API call is finalized.
        return None

    def sync_inventory(self) -> None:
        """Stub: sync inventory once APIs are available."""
        self.get_open_orders()
        self.get_positions()
        self.get_balances()

    def _store_orders(self, resp: Any, order: dict[str, Any], token_id: str) -> None:
        rows = []
        now = utc_now_iso()
        if isinstance(resp, dict):
            row = _polymarket_response_to_row(resp, order, token_id, now)
            if row:
                rows.append(row)
        elif isinstance(resp, list):
            for item in resp:
                if isinstance(item, dict):
                    row = _polymarket_response_to_row(item, order, token_id, now)
                    if row:
                        rows.append(row)
        if rows:
            upsert_orders(self.conn, rows)
            self.conn.commit()


def _resolve_side(side: Any, buy_const: Any, sell_const: Any) -> Any:
    if side is None:
        return buy_const
    if side in (buy_const, sell_const):
        return side
    if isinstance(side, str):
        normalized = side.strip().upper()
        if normalized == "SELL":
            return sell_const
        return buy_const
    return buy_const


def _resolve_order_type(value: Any, order_type_enum: Any, default: Any) -> Any:
    if value is None:
        return default
    if value == default:
        return value
    if isinstance(value, str):
        key = value.strip().upper()
        return getattr(order_type_enum, key, default)
    return value


def _build_args(cls, kwargs: dict[str, Any]) -> Any:
    clean = {k: v for k, v in kwargs.items() if v is not None}
    try:
        return cls(**clean)
    except TypeError:
        clean.pop("post_only", None)
        clean.pop("order_type", None)
        return cls(**clean)


def _polymarket_response_to_row(
    resp: dict[str, Any],
    order: dict[str, Any],
    token_id: str,
    now: str,
) -> Optional[dict[str, Any]]:
    order_id = resp.get("id") or resp.get("order_id") or resp.get("orderId")
    if not order_id:
        return None

    price = resp.get("price") or order.get("price")
    quantity = resp.get("size") or order.get("size") or order.get("quantity")
    status = resp.get("status")

    return {
        "source": "polymarket",
        "provider": "polymarket",
        "order_id": str(order_id),
        "market_id": token_id,
        "side": order.get("side"),
        "price": price,
        "quantity": quantity,
        "status": status,
        "updated_at": now,
        "raw_json": json.dumps(resp),
    }
