"""Kalshi trading service for manual execution of trades (production default)."""
from __future__ import annotations

import json
import os
import uuid
from typing import Any, Optional

from dotenv import load_dotenv

from utils import init_db, load_config, upsert_orders, utc_now_iso


class KalshiTradingService:
    """Kalshi trading service for manual execution of trades."""

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

        self.base_url = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
        self.api_key_id = os.getenv("KALSHI_API_KEY_ID", "")
        self.private_key_pem = os.getenv("KALSHI_PRIVATE_KEY_PEM", "")
        self.private_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")

        self._client = None
        self._sdk = None

    def __enter__(self) -> "KalshiTradingService":
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
            import kalshi_python
        except ImportError as exc:
            raise ImportError(
                "kalshi-python is required for Kalshi trading. "
                "Install it via `pip install kalshi-python`."
            ) from exc

        self._sdk = kalshi_python

    def _load_private_key(self) -> str:
        if self.private_key_pem:
            return self.private_key_pem
        if self.private_key_path:
            try:
                with open(self.private_key_path, encoding="utf-8") as f:
                    return f.read()
            except OSError:
                return ""
        return ""

    def _get_client(self):
        self._load_sdk()
        if self._client is None:
            if not self.api_key_id:
                return None
            private_key = self._load_private_key()
            if not private_key:
                return None

            config = self._sdk.Configuration(host=self.base_url)
            config.api_key_id = self.api_key_id
            config.private_key_pem = private_key
            self._client = self._sdk.KalshiClient(config)
        return self._client

    def login(self) -> bool:
        """Initialize the Kalshi client with credentials."""
        return self._get_client() is not None

    def market_infos(self) -> Optional[dict[str, Any]]:
        """Fetch market info records."""
        client = self._get_client()
        if client is None:
            return None
        try:
            return client.get_markets()
        except Exception:
            return None

    def place_order(self, order: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Place an order using a raw Kalshi payload."""
        if not order:
            return None
        client = self._get_client()
        if client is None:
            return None

        if not order.get("client_order_id"):
            order["client_order_id"] = str(uuid.uuid4())

        try:
            req = self._sdk.CreateOrderRequest(**order)
            resp = client.create_order(req)
        except Exception:
            return None

        raw = _to_dict(resp)
        order_data = raw.get("order") if isinstance(raw, dict) else None
        if isinstance(order_data, dict):
            row = _kalshi_order_to_row(order_data, order)
        elif isinstance(raw, dict):
            row = _kalshi_order_to_row(raw, order)
        else:
            row = None

        if row:
            upsert_orders(self.conn, [row])
            self.conn.commit()

        return raw if isinstance(raw, dict) else None

    def confirm_order(self, order: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Alias to place_order for STX-style ergonomics."""
        return self.place_order(order)

    def cancel_order(self, order_id: str) -> Optional[dict[str, Any]]:
        """Cancel a single order by ID."""
        if not order_id:
            return None
        client = self._get_client()
        if client is None:
            return None

        try:
            resp = client.cancel_order(order_id)
        except Exception:
            return None

        status = "canceled"
        if isinstance(resp, dict) and resp.get("status"):
            status = resp.get("status")

        row = {
            "source": "kalshi",
            "provider": "kalshi",
            "order_id": order_id,
            "status": status,
            "updated_at": utc_now_iso(),
        }
        upsert_orders(self.conn, [row])
        self.conn.commit()

        return resp if isinstance(resp, dict) else None

    def cancel_all_orders(self) -> Optional[dict[str, Any]]:
        """Cancel all open orders."""
        client = self._get_client()
        if client is None:
            return None

        try:
            orders_resp = client.get_orders(status="resting")
        except Exception:
            return None

        orders = _extract_orders(orders_resp)
        order_ids = [o.get("order_id") for o in orders if o.get("order_id")]
        if not order_ids:
            return {"status": "no_orders"}

        try:
            req = self._sdk.BatchCancelOrdersRequest(order_ids=order_ids)
            resp = client.batch_cancel_orders(req)
        except Exception:
            resp = None

        now = utc_now_iso()
        rows = [
            {
                "source": "kalshi",
                "provider": "kalshi",
                "order_id": oid,
                "status": "canceled",
                "updated_at": now,
            }
            for oid in order_ids
        ]
        upsert_orders(self.conn, rows)
        self.conn.commit()

        return _to_dict(resp) if resp is not None else None

    def get_open_orders(self) -> None:
        """Stub: fetch open orders once the query is available."""
        # TODO: Implement once the API call is finalized.
        return None

    def get_positions(self) -> None:
        """Stub: fetch positions once the API call is finalized."""
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


def _to_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if hasattr(value, "to_dict"):
        try:
            return value.to_dict()
        except Exception:
            return {}
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump()
        except Exception:
            return {}
    try:
        return json.loads(json.dumps(value, default=str))
    except Exception:
        return {}


def _extract_orders(resp: Any) -> list[dict[str, Any]]:
    data = _to_dict(resp)
    if not data:
        return []
    orders = data.get("orders")
    if isinstance(orders, list):
        return [o for o in orders if isinstance(o, dict)]
    return []


def _kalshi_order_to_row(order_data: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    price = _kalshi_price(order_data, request)
    quantity = order_data.get("count") or request.get("count")
    order_id = order_data.get("order_id") or order_data.get("id")
    if not order_id:
        return {}
    return {
        "source": "kalshi",
        "provider": "kalshi",
        "order_id": str(order_id),
        "market_id": order_data.get("ticker") or request.get("ticker"),
        "side": order_data.get("side") or request.get("side"),
        "price": price,
        "quantity": quantity,
        "status": order_data.get("status"),
        "updated_at": utc_now_iso(),
        "raw_json": json.dumps(order_data),
    }


def _kalshi_price(order_data: dict[str, Any], request: dict[str, Any]) -> Optional[float]:
    for key in ("yes_price_dollars", "no_price_dollars"):
        if order_data.get(key) is not None:
            return float(order_data.get(key))
        if request.get(key) is not None:
            return float(request.get(key))
    for key in ("yes_price", "no_price"):
        if order_data.get(key) is not None:
            return float(order_data.get(key)) / 100
        if request.get(key) is not None:
            return float(request.get(key)) / 100
    return None
