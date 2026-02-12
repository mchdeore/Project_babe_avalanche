/**
 * PM2 Ecosystem Configuration
 * 
 * Usage:
 *   pm2 start ecosystem.config.js     # Start all services
 *   pm2 status                        # Check status
 *   pm2 logs                          # View logs
 *   pm2 monit                         # Real-time monitoring
 */

module.exports = {
  apps: [
    {
      name: 'sportsbook-worker',
      script: 'services/sportsbook-worker/worker.py',
      interpreter: 'python3',
      args: '--daemon',
      cwd: __dirname,
      autorestart: true,
      max_restarts: 10,
      restart_delay: 5000,
      cron_restart: '0 */6 * * *',
      max_memory_restart: '500M',
      env: {
        WORKER_TYPE: 'sportsbook',
        PYTHONUNBUFFERED: '1',
      },
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: 'logs/sportsbook-worker-error.log',
      out_file: 'logs/sportsbook-worker-out.log',
      merge_logs: true,
      watch: false,
    },
    {
      name: 'openmarket-worker',
      script: 'services/openmarket-worker/worker.py',
      interpreter: 'python3',
      args: '--daemon',
      cwd: __dirname,
      autorestart: true,
      max_restarts: 10,
      restart_delay: 3000,
      max_memory_restart: '500M',
      env: {
        WORKER_TYPE: 'openmarket',
        PYTHONUNBUFFERED: '1',
      },
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: 'logs/openmarket-worker-error.log',
      out_file: 'logs/openmarket-worker-out.log',
      merge_logs: true,
      watch: false,
    },
  ],
};
