#!/bin/bash
# =============================================================================
# START SCRIPT - Launch supervisord and all workers
# =============================================================================
# Usage: ./start.sh
# =============================================================================

set -e
cd "$(dirname "$0")"

echo "Starting continuous data collection system..."

# Create logs directory if needed
mkdir -p logs

# Set venv path (adjust if your venv is named differently)
if [ -d ".venv" ]; then
    export VENV=$(pwd)/.venv
elif [ -d "venv" ]; then
    export VENV=$(pwd)/venv
else
    echo "ERROR: No virtual environment found (.venv or venv)"
    echo "Create one with: python3 -m venv .venv"
    exit 1
fi

echo "Using Python from: $VENV/bin/python"

# Check if already running
if [ -f supervisord.pid ]; then
    PID=$(cat supervisord.pid)
    if ps -p $PID > /dev/null 2>&1; then
        echo "supervisord already running (PID $PID)"
        echo ""
        echo "Use these commands to manage:"
        echo "  supervisorctl -c supervisord.conf status"
        echo "  supervisorctl -c supervisord.conf restart all"
        echo "  ./stop.sh"
        exit 0
    else
        echo "Removing stale PID file..."
        rm -f supervisord.pid
    fi
fi

# Start supervisord
echo "Starting supervisord..."
supervisord -c supervisord.conf

# Wait a moment then show status
sleep 2
echo ""
echo "=== Worker Status ==="
supervisorctl -c supervisord.conf status

echo ""
echo "=== Commands ==="
echo "  View status:    supervisorctl -c supervisord.conf status"
echo "  Tail all logs:  supervisorctl -c supervisord.conf tail -f polymarket-worker"
echo "  Stop all:       ./stop.sh"
echo "  Restart all:    supervisorctl -c supervisord.conf restart all"
