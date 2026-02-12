#!/bin/bash
# Betting Automation Platform - Service Starter
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate venv if exists
[ -d ".venv" ] && source .venv/bin/activate

# Load .env
[ -f ".env" ] && export $(cat .env | grep -v '^#' | xargs)

mkdir -p logs

case "${1:-all}" in
    sportsbook)
        python3 services/sportsbook-worker/worker.py "${2:---daemon}"
        ;;
    openmarket)
        python3 services/openmarket-worker/worker.py "${2:---daemon}"
        ;;
    --once)
        echo "Running single poll for all workers..."
        python3 services/sportsbook-worker/worker.py --once
        python3 services/openmarket-worker/worker.py --once
        ;;
    all|*)
        echo "Starting all workers..."
        python3 services/sportsbook-worker/worker.py --daemon >> logs/sportsbook.log 2>&1 &
        echo "Sportsbook PID: $!"
        python3 services/openmarket-worker/worker.py --daemon >> logs/openmarket.log 2>&1 &
        echo "Openmarket PID: $!"
        echo "Done. View logs: tail -f logs/*.log"
        ;;
esac
