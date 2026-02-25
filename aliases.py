"""Canonical alias helpers for teams, players, providers, and markets."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import re
from typing import Any

import yaml

from utils import normalize_player, normalize_team

ROOT = Path(__file__).resolve().parent
ALIASES_DIR = ROOT / "data" / "aliases"


def _load_yaml(path: Path) -> Any:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _norm_token(value: str) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", "", value.lower())


@lru_cache(maxsize=1)
def load_alias_maps() -> dict[str, Any]:
    return {
        "teams": _load_yaml(ALIASES_DIR / "teams.yaml"),
        "providers": _load_yaml(ALIASES_DIR / "provider_aliases.yaml"),
        "markets": _load_yaml(ALIASES_DIR / "market_aliases.yaml"),
        "players": _load_yaml(ALIASES_DIR / "player_aliases.yaml"),
    }


@lru_cache(maxsize=1)
def _build_team_indexes() -> tuple[dict[str, dict[str, set[str]]], dict[str, set[str]], dict[str, dict[str, Any]]]:
    data = load_alias_maps().get("teams", {})
    by_league: dict[str, dict[str, set[str]]] = {}
    all_aliases: dict[str, set[str]] = {}
    records: dict[str, dict[str, Any]] = {}

    for league, teams in (data or {}).items():
        for team in teams or []:
            key = team.get("key") or normalize_team(team.get("name", ""))
            if not key:
                continue

            record = {
                "key": key,
                "name": team.get("name") or key,
                "city": team.get("city") or "",
                "abbrev": team.get("abbrev") or "",
                "lat": team.get("lat"),
                "lon": team.get("lon"),
                "league": league,
                "aliases": team.get("aliases") or [],
            }
            records[key] = record

            aliases = set(record["aliases"])
            aliases.update([
                record.get("name", ""),
                record.get("abbrev", ""),
                record.get("city", ""),
            ])

            for alias in aliases:
                if not alias:
                    continue
                norm = normalize_team(alias)
                if not norm:
                    continue

                by_league.setdefault(league, {}).setdefault(norm, set()).add(key)
                all_aliases.setdefault(norm, set()).add(key)

    return by_league, all_aliases, records


@lru_cache(maxsize=1)
def _build_alias_lookup(mapping: dict[str, list[str]]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for canonical, aliases in (mapping or {}).items():
        if not canonical:
            continue
        canonical_norm = _norm_token(canonical)
        if canonical_norm:
            lookup[canonical_norm] = canonical
        for alias in aliases or []:
            alias_norm = _norm_token(alias)
            if alias_norm:
                lookup[alias_norm] = canonical
    return lookup


@lru_cache(maxsize=1)
def _player_alias_lookup() -> dict[str, str]:
    data = load_alias_maps().get("players", {})
    return _build_alias_lookup(data)


@lru_cache(maxsize=1)
def _provider_alias_lookup() -> dict[str, str]:
    data = load_alias_maps().get("providers", {})
    return _build_alias_lookup(data)


@lru_cache(maxsize=1)
def _market_alias_lookup() -> dict[str, str]:
    data = load_alias_maps().get("markets", {})
    return _build_alias_lookup(data)


def canonical_team(name: str, league: str | None = None) -> str:
    if not name:
        return ""
    norm = normalize_team(name)
    if not norm:
        return ""

    by_league, all_aliases, _ = _build_team_indexes()
    if league:
        keys = by_league.get(league, {}).get(norm)
        if keys:
            return sorted(keys)[0]

    keys = all_aliases.get(norm)
    if keys and len(keys) == 1:
        return next(iter(keys))

    return norm


def canonical_player(name: str) -> str:
    if not name:
        return ""
    norm = normalize_player(name)
    if not norm:
        return ""

    lookup = _player_alias_lookup()
    return lookup.get(norm, norm)


def canonical_provider(name: str) -> str:
    if not name:
        return ""
    norm = _norm_token(name)
    lookup = _provider_alias_lookup()
    return lookup.get(norm, name.strip().lower())


def canonical_market(name: str) -> str:
    if not name:
        return ""
    norm = _norm_token(name)
    lookup = _market_alias_lookup()
    if norm in lookup:
        return lookup[norm]
    return name.strip().lower()


def get_team_record(team_key: str) -> dict[str, Any] | None:
    _, _, records = _build_team_indexes()
    return records.get(team_key)


def get_team_records() -> dict[str, dict[str, Any]]:
    _, _, records = _build_team_indexes()
    return records


def get_team_aliases_by_league() -> dict[str, dict[str, set[str]]]:
    by_league, _, _ = _build_team_indexes()
    return by_league


def get_all_team_aliases() -> dict[str, set[str]]:
    _, all_aliases, _ = _build_team_indexes()
    return all_aliases
