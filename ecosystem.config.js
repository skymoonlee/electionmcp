// pm2 ecosystem config for electionmcp MCP server
// 실행: pm2 start ecosystem.config.js
//       pm2 logs electionmcp
//       pm2 restart electionmcp
module.exports = {
  apps: [
    {
      name: 'electionmcp',
      cwd: '/var/www/63mcp',
      script: '/var/www/63mcp/.venv/bin/python',
      args: '-m mcp_server.server',
      interpreter: 'none',  // python을 직접 실행 (node 인터프리터 사용 X)
      instances: 1,
      exec_mode: 'fork',
      max_memory_restart: '1G',
      autorestart: true,
      watch: false,
      env: {
        PYTHONPATH: '/var/www/63mcp/src',
        PYTHONUNBUFFERED: '1',
        // .env 파일에서 자동 로드되지만, pm2 환경에서는 명시 필요
        MCP_HOST: '0.0.0.0',
        MCP_PORT: '8780',
        HF_DATASET_REPO: 'skylee993393/korea-local-election-2026',
        LOCAL_PARQUET: '/var/www/63mcp/data/parquet/all.parquet',
        // FastMCP DNS rebinding 보호 — 리버스 프록시 통과하는 도메인 허용
        ALLOWED_HOSTS: 'mcp.electionmcp.kr',
      },
      error_file: '/var/www/63mcp/logs/err.log',
      out_file: '/var/www/63mcp/logs/out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
    },
  ],
};
