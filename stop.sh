#!/bin/bash
# =============================================================================
# STOP SCRIPT - Shutdown supervisord and all workers
# =============================================================================
# Usage: ./stop.sh
# =============================================================================

cd "$(dirname "$0")"

if [ ! -f supervisord.pid ]; then
    echo "supervisord is not running (no PID file found)"
    exit 0
fi

echo "Stopping supervisord and all workers..."
supervisorctl -c supervisord.conf shutdown

# Wait for clean shutdown
sleep 2

if [ -f supervisord.pid ]; then
    echo "Warning: PID file still exists, forcing cleanup..."
    rm -f supervisord.pid supervisor.sock
fi

echo "Stopped."
