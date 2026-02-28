"""
Feature Engineering and ML Pipeline
====================================

Builds feature matrices from market history and structured events,
then trains and runs ML models for line movement prediction.

Features:
---------
Time-series features (from market_history):
- price_velocity: Rate of probability change
- volatility: Rolling standard deviation of price
- provider_spread: Difference between providers
- time_to_game: Hours until game starts

Structured features (from structured_events):
- injury_severity: Aggregated injury severity for each team
- news_count: Number of recent news items
- position_importance: Weighted sum of affected player importance

Target variable:
- Δprob: Probability change in next X minutes
- Or: Binary classification (move > threshold)

Usage:
------
    from insights_generator.models.features import build_feature_matrix, train_model
    
    X, y, metadata = build_feature_matrix(conn)
    metrics = train_model(X, y, model_path)
"""

import json
import pickle
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

try:
    import lightgbm as lgb
    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False


# =============================================================================
# FEATURE EXTRACTION
# =============================================================================

def build_feature_matrix(
    conn: sqlite3.Connection,
    lookback_hours: int = 24,
    prediction_horizon_minutes: int = 30,
) -> tuple[np.ndarray, np.ndarray, list[dict]]:
    """
    Build feature matrix and target variable from database.
    
    Args:
        conn: Database connection
        lookback_hours: How many hours of history to use for features
        prediction_horizon_minutes: How far ahead to predict
        
    Returns:
        tuple: (X features array, y target array, metadata list)
    """
    if not PANDAS_AVAILABLE:
        raise ImportError("pandas is required for feature engineering")
    
    # Get market history with sufficient data
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
    
    query = """
        SELECT 
            mh.game_id,
            mh.market,
            mh.side,
            mh.line,
            mh.provider,
            mh.devigged_prob,
            mh.snapshot_time,
            g.commence_time,
            g.home_team,
            g.away_team
        FROM market_history mh
        JOIN games g ON mh.game_id = g.game_id
        WHERE mh.snapshot_time >= ?
        AND mh.devigged_prob IS NOT NULL
        ORDER BY mh.game_id, mh.market, mh.side, mh.provider, mh.snapshot_time
    """
    
    df = pd.read_sql_query(query, conn, params=(cutoff,))
    
    if df.empty:
        return np.array([]), np.array([]), []
    
    # Convert timestamps
    df["snapshot_time"] = pd.to_datetime(df["snapshot_time"])
    df["commence_time"] = pd.to_datetime(df["commence_time"])
    
    # Build features for each unique market position
    features_list = []
    targets_list = []
    metadata_list = []
    
    # Group by market position
    groups = df.groupby(["game_id", "market", "side", "provider"])
    
    for (game_id, market, side, provider), group in groups:
        group = group.sort_values("snapshot_time")
        
        if len(group) < 3:  # Need enough points for features
            continue
        
        # Calculate features at each point (except first and last few)
        for i in range(2, len(group) - 1):
            current = group.iloc[i]
            history = group.iloc[:i+1]
            future = group.iloc[i+1:]
            
            # Time-series features
            features = _calculate_timeseries_features(history, current)
            
            # Provider spread: max disagreement with other providers at this snapshot
            features["provider_spread"] = _calculate_provider_spread(
                df, game_id, market, side, provider, current["snapshot_time"],
            )
            
            # Add time to game
            if pd.notna(current["commence_time"]):
                hours_to_game = (current["commence_time"] - current["snapshot_time"]).total_seconds() / 3600
                features["time_to_game"] = max(0, hours_to_game)
            else:
                features["time_to_game"] = 168  # Default to 1 week
            
            # Get structured features (injury severity, news count)
            structured = _get_structured_features(
                conn, 
                game_id, 
                current["home_team"], 
                current["away_team"],
                current["snapshot_time"],
            )
            features.update(structured)
            
            # Merge AI scoring dimensions if available
            try:
                from insights_generator.scoring import get_score_features
                features.update(get_score_features(conn, game_id))
            except Exception:
                pass
            
            # Calculate target (future price movement)
            target = _calculate_target(
                current["devigged_prob"],
                future,
                prediction_horizon_minutes,
            )
            
            if target is not None:
                features_list.append(features)
                targets_list.append(target)
                metadata_list.append({
                    "game_id": game_id,
                    "market": market,
                    "side": side,
                    "provider": provider,
                    "snapshot_time": current["snapshot_time"].isoformat(),
                })
    
    if not features_list:
        return np.array([]), np.array([]), []
    
    # Convert to arrays
    feature_names = list(features_list[0].keys())
    X = np.array([[f.get(name, 0) for name in feature_names] for f in features_list])
    y = np.array(targets_list)
    
    # Store feature names in metadata
    for meta in metadata_list:
        meta["feature_names"] = feature_names
    
    return X, y, metadata_list


def _calculate_timeseries_features(
    history: "pd.DataFrame",
    current: "pd.Series",
) -> dict[str, float]:
    """
    Calculate time-series features from price history.
    
    Args:
        history: Historical prices up to current point
        current: Current price point
        
    Returns:
        dict: Feature dictionary
    """
    features = {}
    
    probs = history["devigged_prob"].values
    times = history["snapshot_time"].values
    
    # Price velocity (change per minute)
    if len(probs) >= 2:
        time_diff = (times[-1] - times[0]) / np.timedelta64(1, "m")
        if time_diff > 0:
            features["price_velocity"] = (probs[-1] - probs[0]) / time_diff
        else:
            features["price_velocity"] = 0
    else:
        features["price_velocity"] = 0
    
    # Volatility (standard deviation)
    if len(probs) >= 3:
        features["volatility"] = np.std(probs)
    else:
        features["volatility"] = 0
    
    # Recent momentum (last 3 points vs earlier)
    if len(probs) >= 5:
        recent_avg = np.mean(probs[-3:])
        earlier_avg = np.mean(probs[:-3])
        features["momentum"] = recent_avg - earlier_avg
    else:
        features["momentum"] = 0
    
    # Current price level
    features["current_prob"] = current["devigged_prob"]
    
    # Distance from 0.5 (uncertainty measure)
    features["uncertainty"] = abs(current["devigged_prob"] - 0.5)
    
    return features


def _calculate_provider_spread(
    df: "pd.DataFrame",
    game_id: str,
    market: str,
    side: str,
    provider: str,
    snapshot_time: "pd.Timestamp",
) -> float:
    """
    Compute max absolute spread between this provider and others for the same
    (game_id, market, side) at the closest snapshot to ``snapshot_time``.
    """
    same_market = df[
        (df["game_id"] == game_id)
        & (df["market"] == market)
        & (df["side"] == side)
        & (df["provider"] != provider)
    ]
    if same_market.empty:
        return 0.0

    # For each other provider, take the most recent snapshot <= current time
    latest = (
        same_market[same_market["snapshot_time"] <= snapshot_time]
        .sort_values("snapshot_time")
        .groupby("provider")
        .tail(1)
    )
    if latest.empty:
        return 0.0

    current_prob = df[
        (df["game_id"] == game_id)
        & (df["market"] == market)
        & (df["side"] == side)
        & (df["provider"] == provider)
        & (df["snapshot_time"] == snapshot_time)
    ]["devigged_prob"]

    if current_prob.empty:
        return 0.0

    return float((latest["devigged_prob"] - current_prob.iloc[0]).abs().max())


def _get_structured_features(
    conn: sqlite3.Connection,
    game_id: str,
    home_team: str,
    away_team: str,
    snapshot_time: datetime,
) -> dict[str, float]:
    """
    Get structured features from news/events for a game.
    
    Args:
        conn: Database connection
        game_id: Game ID
        home_team: Home team name
        away_team: Away team name
        snapshot_time: Current snapshot time
        
    Returns:
        dict: Structured feature dictionary
    """
    features = {
        "injury_severity_home": 0,
        "injury_severity_away": 0,
        "news_count": 0,
        "max_event_severity": 0,
    }
    
    try:
        # Get injury severity for home team
        cursor = conn.execute("""
            SELECT COALESCE(SUM(severity), 0) as total_severity
            FROM structured_events se
            JOIN news_headlines nh ON se.headline_id = nh.id
            WHERE se.event_type = 'injury'
            AND (LOWER(se.team) LIKE ? OR nh.game_id = ?)
            AND nh.published_at <= ?
            AND nh.published_at >= datetime(?, '-72 hours')
        """, (f"%{home_team.lower() if home_team else ''}%", game_id, 
              snapshot_time.isoformat(), snapshot_time.isoformat()))
        
        row = cursor.fetchone()
        if row:
            features["injury_severity_home"] = row[0] or 0
        
        # Get injury severity for away team
        cursor = conn.execute("""
            SELECT COALESCE(SUM(severity), 0) as total_severity
            FROM structured_events se
            JOIN news_headlines nh ON se.headline_id = nh.id
            WHERE se.event_type = 'injury'
            AND (LOWER(se.team) LIKE ? OR nh.game_id = ?)
            AND nh.published_at <= ?
            AND nh.published_at >= datetime(?, '-72 hours')
        """, (f"%{away_team.lower() if away_team else ''}%", game_id,
              snapshot_time.isoformat(), snapshot_time.isoformat()))
        
        row = cursor.fetchone()
        if row:
            features["injury_severity_away"] = row[0] or 0
        
        # Get recent news count for this game
        cursor = conn.execute("""
            SELECT COUNT(*) as news_count, MAX(relevance_score) as max_relevance
            FROM news_headlines
            WHERE game_id = ?
            AND published_at <= ?
            AND published_at >= datetime(?, '-24 hours')
        """, (game_id, snapshot_time.isoformat(), snapshot_time.isoformat()))
        
        row = cursor.fetchone()
        if row:
            features["news_count"] = row[0] or 0
        
        # Get max event severity
        cursor = conn.execute("""
            SELECT MAX(severity) as max_severity
            FROM structured_events se
            JOIN news_headlines nh ON se.headline_id = nh.id
            WHERE nh.game_id = ?
            AND nh.published_at <= ?
            AND nh.published_at >= datetime(?, '-24 hours')
        """, (game_id, snapshot_time.isoformat(), snapshot_time.isoformat()))
        
        row = cursor.fetchone()
        if row and row[0]:
            features["max_event_severity"] = row[0]
            
    except sqlite3.Error:
        pass  # Return default features on error
    
    return features


def _calculate_target(
    current_prob: float,
    future: "pd.DataFrame",
    horizon_minutes: int,
) -> float | None:
    """
    Calculate target variable (future price movement).
    
    Args:
        current_prob: Current probability
        future: Future price data
        horizon_minutes: How far ahead to look
        
    Returns:
        float: Price change, or None if insufficient data
    """
    if future.empty:
        return None
    
    # Find price at horizon
    horizon_time = future.iloc[0]["snapshot_time"] + timedelta(minutes=horizon_minutes)
    
    # Get closest point to horizon
    future_filtered = future[future["snapshot_time"] <= horizon_time]
    
    if future_filtered.empty:
        return None
    
    future_prob = future_filtered.iloc[-1]["devigged_prob"]
    
    return future_prob - current_prob


# =============================================================================
# MODEL TRAINING
# =============================================================================

def train_model(
    X: np.ndarray,
    y: np.ndarray,
    model_path: Path | str,
    model_type: str = "xgboost",
    test_size: float = 0.2,
) -> dict[str, float]:
    """
    Train an ML model on the feature matrix.
    
    Args:
        X: Feature matrix
        y: Target variable
        model_path: Where to save the trained model
        model_type: Model type ('xgboost', 'lightgbm', 'linear')
        test_size: Fraction of data for testing
        
    Returns:
        dict: Training metrics (mse, mae, r2)
    """
    if not SKLEARN_AVAILABLE:
        raise ImportError("scikit-learn is required for model training")
    
    if len(X) == 0 or len(y) == 0:
        raise ValueError("Empty feature matrix or target")
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=42
    )
    
    # Train model
    if model_type == "xgboost":
        if not XGBOOST_AVAILABLE:
            raise ImportError("xgboost is required for XGBoost models")
        
        model = xgb.XGBRegressor(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            random_state=42,
        )
        model.fit(X_train, y_train)
        
    elif model_type == "lightgbm":
        if not LIGHTGBM_AVAILABLE:
            raise ImportError("lightgbm is required for LightGBM models")
        
        model = lgb.LGBMRegressor(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            random_state=42,
        )
        model.fit(X_train, y_train)
        
    elif model_type == "linear":
        model = LinearRegression()
        model.fit(X_train, y_train)
        
    else:
        raise ValueError(f"Unknown model type: {model_type}")
    
    # Evaluate
    y_pred = model.predict(X_test)
    
    metrics = {
        "mse": mean_squared_error(y_test, y_pred),
        "mae": mean_absolute_error(y_test, y_pred),
        "r2": r2_score(y_test, y_pred),
        "train_samples": len(X_train),
        "test_samples": len(X_test),
    }
    
    # Save model
    model_path = Path(model_path)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(model_path, "wb") as f:
        pickle.dump({
            "model": model,
            "model_type": model_type,
            "metrics": metrics,
            "trained_at": datetime.now(timezone.utc).isoformat(),
        }, f)
    
    return metrics


def predict(
    conn: sqlite3.Connection,
    model_path: Path | str,
    store_predictions: bool = True,
) -> list[dict[str, Any]]:
    """
    Run predictions on current market data.
    
    Args:
        conn: Database connection
        model_path: Path to trained model
        store_predictions: Whether to store predictions in database
        
    Returns:
        list: List of prediction dictionaries
    """
    model_path = Path(model_path)
    
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")
    
    # Load model
    with open(model_path, "rb") as f:
        model_data = pickle.load(f)
    
    model = model_data["model"]
    model_type = model_data["model_type"]
    model_version = model_data.get("trained_at", "unknown")
    
    # Build features for current data
    X, _, metadata = build_feature_matrix(conn, lookback_hours=2)
    
    if len(X) == 0:
        return []
    
    # Make predictions
    y_pred = model.predict(X)
    
    predictions = []
    now = datetime.now(timezone.utc).isoformat()
    
    for i, (pred, meta) in enumerate(zip(y_pred, metadata)):
        # Calculate confidence based on prediction magnitude
        confidence = min(1.0, abs(pred) * 10)  # Scale to 0-1
        
        prediction = {
            "game_id": meta["game_id"],
            "market": meta["market"],
            "side": meta["side"],
            "provider": meta["provider"],
            "predicted_move": float(pred),
            "predicted_direction": "up" if pred > 0.01 else ("down" if pred < -0.01 else "stable"),
            "confidence": confidence,
            "features_json": json.dumps({
                name: float(X[i, j]) 
                for j, name in enumerate(meta.get("feature_names", []))
            }),
            "model_version": model_version,
            "model_type": model_type,
            "created_at": now,
        }
        
        predictions.append(prediction)
        
        if store_predictions:
            _store_prediction(conn, prediction)
    
    return predictions


def _store_prediction(
    conn: sqlite3.Connection,
    prediction: dict[str, Any],
) -> int | None:
    """
    Store a prediction in the ml_predictions table.
    
    Args:
        conn: Database connection
        prediction: Prediction dictionary
        
    Returns:
        int: ID of inserted prediction, or None if failed
    """
    try:
        cursor = conn.execute("""
            INSERT INTO ml_predictions (
                game_id, market, side, provider,
                predicted_move, predicted_direction, confidence,
                horizon_minutes, features_json,
                model_version, model_type, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            prediction["game_id"],
            prediction["market"],
            prediction["side"],
            prediction["provider"],
            prediction["predicted_move"],
            prediction["predicted_direction"],
            prediction["confidence"],
            30,  # Default horizon
            prediction["features_json"],
            prediction["model_version"],
            prediction["model_type"],
            prediction["created_at"],
        ))
        
        conn.commit()
        return cursor.lastrowid
        
    except sqlite3.Error as e:
        print(f"Warning: Failed to store prediction: {e}")
        return None


def evaluate_model(
    conn: sqlite3.Connection,
    hours_back: int = 24,
) -> dict[str, Any]:
    """
    Evaluate model performance on past predictions.
    
    Compares predictions to actual outcomes that have since occurred.
    
    Args:
        conn: Database connection
        hours_back: How many hours of predictions to evaluate
        
    Returns:
        dict: Evaluation metrics
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
    
    # Get predictions that can be evaluated
    query = """
        SELECT 
            p.*,
            mh.devigged_prob as actual_prob
        FROM ml_predictions p
        JOIN market_history mh ON 
            p.game_id = mh.game_id 
            AND p.market = mh.market 
            AND p.side = mh.side 
            AND p.provider = mh.provider
        WHERE p.created_at >= ?
        AND p.outcome_recorded_at IS NULL
        AND mh.snapshot_time > datetime(p.created_at, '+30 minutes')
        ORDER BY p.created_at
    """
    
    cursor = conn.execute(query, (cutoff,))
    rows = cursor.fetchall()
    
    if not rows:
        return {
            "predictions_evaluated": 0,
            "accuracy": None,
            "mae": None,
        }
    
    correct = 0
    total = 0
    abs_errors = []
    now = datetime.now(timezone.utc).isoformat()
    
    for row in rows:
        pred_move = row["predicted_move"]
        pred_direction = row["predicted_direction"]
        
        # Recover original probability from stored features
        original_prob = None
        if row["features_json"]:
            try:
                feats = json.loads(row["features_json"])
                original_prob = feats.get("current_prob")
            except (json.JSONDecodeError, TypeError):
                pass
        
        if original_prob is None:
            continue
        
        actual_move = row["actual_prob"] - original_prob
        actual_direction = (
            "up" if actual_move > 0.01
            else ("down" if actual_move < -0.01 else "stable")
        )
        
        if pred_direction == actual_direction:
            correct += 1
        
        abs_errors.append(abs(pred_move - actual_move))
        total += 1
        
        # Back-fill outcome columns
        try:
            conn.execute("""
                UPDATE ml_predictions
                SET actual_move = ?, actual_direction = ?,
                    outcome_recorded_at = ?,
                    prediction_correct = ?
                WHERE id = ?
            """, (
                actual_move,
                actual_direction,
                now,
                1 if pred_direction == actual_direction else 0,
                row["id"],
            ))
        except sqlite3.Error:
            pass
    
    conn.commit()
    
    return {
        "predictions_evaluated": total,
        "accuracy": correct / total if total > 0 else None,
        "mae": sum(abs_errors) / len(abs_errors) if abs_errors else None,
        "correct_predictions": correct,
    }
