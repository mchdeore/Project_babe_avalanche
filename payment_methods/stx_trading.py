"""STX trading service for manual execution of trades and inventory sync."""
from __future__ import annotations

import json
import os
import re
import uuid
from typing import Any, Optional

import requests
from dotenv import load_dotenv

from adapters.adapter_stx import DEFAULT_GRAPHQL_URL, STXClient
from utils import init_db, load_config, upsert_orders, utc_now_iso

GraphQLResponse = dict[str, Any]

_MARKET_INFOS_QUERY = """
query MarketInfos {
  marketInfos {
    marketId
    status
    sport
    shortTitle
    participants { abbreviation name role }
    position
    question
  }
}
"""

_CONFIRM_ORDER_MUTATION = """
mutation confirmOrder($order: UserOrder!, $geo: GeoLocationCode){
  confirmOrder(userOrder: $order, geoLocation: $geo) {
    order {
      time
      totalValue
      marketId
      price
      quantity
      avgPrice
      filledPercentage
      status
      insertedAt
      id
      clientOrderId
    }
    nextGeolocationAt
  }
}
"""

_CANCEL_ORDER_MUTATION = """
mutation cancelOrder($orderId: ID!) {
  cancelOrder(orderId: $orderId) {
    status
  }
}
"""

_CANCEL_ALL_ORDERS_MUTATION = """
mutation cancelAllOrders($geo: GeoLocationCode) {
  cancelAllOrders(geoLocation: $geo) {
    status
    orderId
  }
}
"""

_TNC_ACCEPTED_MUTATION = """
mutation tncAccepted($input: TncAcceptanceInput!) {
  account {
    tncAccepted(input: $input)
  }
}
"""


class STXTradingService:
    """STX trading service for manual execution of trades."""

    def __init__(
        self,
        config: Optional[dict[str, Any]] = None,
        db_path: Optional[str] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        load_dotenv()
        self.config = config or load_config()
        if db_path is None:
            db_path = self.config.get("storage", {}).get("database", "odds.db")

        self.conn = init_db(db_path)
        self.session = session or requests.Session()
        self._owns_session = session is None

        graphql_url = os.getenv("STX_GRAPHQL_URL", DEFAULT_GRAPHQL_URL)
        email = os.getenv("STX_EMAIL", "")
        password = os.getenv("STX_PASSWORD", "")
        device_id = os.getenv("STX_DEVICE_ID") or str(uuid.uuid4())

        self.geo_code = os.getenv("STX_GEO_CODE")
        self.client = STXClient(self.session, graphql_url, email, password, device_id)

    def __enter__(self) -> "STXTradingService":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        """Close underlying resources."""
        try:
            self.conn.close()
        finally:
            if self._owns_session:
                self.session.close()

    def login(self) -> bool:
        """Authenticate with STX."""
        return self.client.login()

    def market_infos(self) -> Optional[GraphQLResponse]:
        """Fetch market info records."""
        return self.client.graphql(_MARKET_INFOS_QUERY)

    def market_infos_with_count(self, filters: Optional[dict[str, Any]] = None) -> Optional[GraphQLResponse]:
        """Fetch market info records with count using optional filters."""
        if filters:
            input_literal = _graphql_literal(filters)
            query = (
                "query MarketInfosWithCount {"
                f" marketInfosWithCount(input: {input_literal}) "
                "{ count marketInfos { marketId status sport shortTitle participants"
                "{ abbreviation name role } position question } }"
                "}"
            )
        else:
            query = (
                "query MarketInfosWithCount {"
                " marketInfosWithCount { count marketInfos { marketId status sport shortTitle participants"
                "{ abbreviation name role } position question } }"
                "}"
            )
        return self.client.graphql(query)

    def confirm_order(self, user_order: dict[str, Any], geo: Optional[str] = None) -> Optional[GraphQLResponse]:
        """Confirm a buy/sell order using the provided UserOrder payload."""
        if not user_order:
            return None

        variables = {
            "order": user_order,
            "geo": geo or self.geo_code,
        }
        result = self.client.graphql(_CONFIRM_ORDER_MUTATION, variables)
        if not result:
            return None

        payload = (result.get("data") or {}).get("confirmOrder")
        if not payload:
            return result

        order = payload.get("order") or {}
        if order.get("id"):
            row = _order_to_row(order, user_order)
            upsert_orders(self.conn, [row])
            self.conn.commit()

        return payload

    def cancel_order(self, order_id: str) -> Optional[GraphQLResponse]:
        """Cancel a single order by ID."""
        if not order_id:
            return None

        result = self.client.graphql(_CANCEL_ORDER_MUTATION, {"orderId": order_id})
        if not result:
            return None

        status = ((result.get("data") or {}).get("cancelOrder") or {}).get("status")
        if status:
            row = {
                "source": "stx",
                "provider": "stx",
                "order_id": order_id,
                "status": status,
                "updated_at": utc_now_iso(),
            }
            upsert_orders(self.conn, [row])
            self.conn.commit()

        return result

    def cancel_all_orders(self, geo: Optional[str] = None) -> Optional[GraphQLResponse]:
        """Cancel all open orders."""
        result = self.client.graphql(_CANCEL_ALL_ORDERS_MUTATION, {"geo": geo or self.geo_code})
        if not result:
            return None

        canceled = (result.get("data") or {}).get("cancelAllOrders") or []
        rows = []
        now = utc_now_iso()
        for item in canceled:
            order_id = item.get("orderId")
            status = item.get("status")
            if not order_id:
                continue
            rows.append({
                "source": "stx",
                "provider": "stx",
                "order_id": order_id,
                "status": status,
                "updated_at": now,
            })

        if rows:
            upsert_orders(self.conn, rows)
            self.conn.commit()

        return result

    def accept_tnc(self, input_payload: dict[str, Any]) -> Optional[GraphQLResponse]:
        """Accept the STX terms and conditions."""
        if not input_payload:
            return None
        return self.client.graphql(_TNC_ACCEPTED_MUTATION, {"input": input_payload})

    def get_open_orders(self) -> None:
        """Stub: fetch open orders once the query is available."""
        # TODO: Implement once GraphQL operation is available.
        return None

    def get_positions(self) -> None:
        """Stub: fetch positions once the query is available."""
        # TODO: Implement once GraphQL operation is available.
        return None

    def get_balances(self) -> None:
        """Stub: fetch balances once the query is available."""
        # TODO: Implement once GraphQL operation is available.
        return None

    def sync_inventory(self) -> None:
        """Stub: sync inventory once queries are available."""
        self.get_open_orders()
        self.get_positions()
        self.get_balances()


def _order_to_row(order: dict[str, Any], user_order: dict[str, Any]) -> dict[str, Any]:
    now = utc_now_iso()
    side = (
        user_order.get("side")
        or user_order.get("orderSide")
        or user_order.get("direction")
        or ""
    )
    return {
        "source": "stx",
        "provider": "stx",
        "order_id": str(order.get("id") or ""),
        "market_id": order.get("marketId"),
        "side": side,
        "price": order.get("price"),
        "quantity": order.get("quantity"),
        "total_value": order.get("totalValue"),
        "avg_price": order.get("avgPrice"),
        "filled_percentage": order.get("filledPercentage"),
        "status": order.get("status"),
        "client_order_id": order.get("clientOrderId"),
        "created_at": order.get("time"),
        "inserted_at": order.get("insertedAt"),
        "updated_at": now,
        "raw_json": json.dumps(order),
    }


def _graphql_literal(value: Any) -> str:
    if isinstance(value, dict):
        items = [f"{k}: {_graphql_literal(v)}" for k, v in value.items()]
        return "{" + ", ".join(items) + "}"
    if isinstance(value, list):
        return "[" + ", ".join(_graphql_literal(v) for v in value) + "]"
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        if value and re.fullmatch(r"[A-Z0-9_]+", value):
            return value
        return json.dumps(value)
    return json.dumps(value)
