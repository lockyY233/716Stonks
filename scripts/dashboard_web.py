from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, render_template_string


HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>716Stonks Dashboard</title>
  <style>
    :root {
      --bg: #0f172a;
      --panel: #111827;
      --line: #1f2937;
      --text: #e5e7eb;
      --muted: #94a3b8;
      --up: #22c55e;
      --down: #ef4444;
      --accent: #38bdf8;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: ui-sans-serif, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
      background: radial-gradient(1200px 600px at 80% -10%, #1d4ed822, transparent), var(--bg);
      color: var(--text);
    }
    .wrap {
      max-width: 1200px;
      margin: 24px auto;
      padding: 0 16px;
    }
    .head {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      margin-bottom: 14px;
    }
    .title {
      font-size: 1.4rem;
      font-weight: 700;
      letter-spacing: .2px;
    }
    .meta {
      color: var(--muted);
      font-size: .92rem;
    }
    .panel {
      background: linear-gradient(180deg, #0b1220, #0a101c);
      border: 1px solid var(--line);
      border-radius: 12px;
      overflow: hidden;
      box-shadow: 0 10px 30px #00000033;
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    thead th {
      text-align: left;
      font-size: .84rem;
      color: var(--muted);
      font-weight: 600;
      letter-spacing: .3px;
      border-bottom: 1px solid var(--line);
      padding: 12px 10px;
      background: #0b1323;
      position: sticky;
      top: 0;
    }
    tbody td {
      padding: 11px 10px;
      border-bottom: 1px solid #111827;
      font-size: .95rem;
      vertical-align: middle;
    }
    tbody tr:hover { background: #0b1220; }
    .mono { font-variant-numeric: tabular-nums; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    .up { color: var(--up); font-weight: 600; }
    .down { color: var(--down); font-weight: 600; }
    .pill {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      border: 1px solid #1f2937;
      color: var(--accent);
      font-size: .75rem;
      background: #082f49;
    }
    .empty {
      color: var(--muted);
      text-align: center;
      padding: 24px;
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="head">
      <div class="title">716Stonks Live Dashboard</div>
      <div class="meta">
        Refresh: <span class="pill">2s</span>
        <span style="margin-left:10px;">Last update: <span id="last">-</span></span>
      </div>
    </div>

    <div class="panel">
      <table>
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Name</th>
            <th>Trend</th>
            <th>Current</th>
            <th>Base</th>
            <th>Change vs Base</th>
            <th>Slope</th>
            <th>Updated At (UTC)</th>
          </tr>
        </thead>
        <tbody id="rows">
          <tr><td colspan="7" class="empty">Loading...</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <script>
    function fmtMoney(v) { return "$" + Number(v).toFixed(2); }
    function fmtNum(v, d=4) { return Number(v).toFixed(d); }

    function render(data) {
      const tbody = document.getElementById("rows");
      const rows = data.stocks || [];
      if (!rows.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="empty">No companies found.</td></tr>';
        return;
      }
        tbody.innerHTML = rows.map(r => {
          const current = Number(r.current_price);
          const base = Number(r.base_price);
          const pct = base !== 0 ? ((current - base) / base) * 100 : 0;
          const cls = pct >= 0 ? "up" : "down";
          const sign = pct >= 0 ? "+" : "";
          const prices = Array.isArray(r.history_prices) ? r.history_prices : [];
          const spark = sparklineSvg(prices, 180, 40, pct >= 0 ? "#22c55e" : "#ef4444");
          return `
          <tr>
            <td class="mono"><strong>${r.symbol}</strong></td>
            <td>${r.name || ""}</td>
            <td>${spark}</td>
            <td class="mono">${fmtMoney(current)}</td>
            <td class="mono">${fmtMoney(base)}</td>
            <td class="mono ${cls}">${sign}${pct.toFixed(2)}%</td>
            <td class="mono">${fmtNum(r.slope)}</td>
            <td class="mono">${r.updated_at || "-"}</td>
          </tr>
        `;
      }).join("");
    }

    function sparklineSvg(values, width, height, stroke) {
      if (!values || values.length < 2) {
        return '<span class="muted">-</span>';
      }
      const min = Math.min(...values);
      const max = Math.max(...values);
      const pad = 2;
      const range = (max - min) || 1;
      const step = (width - pad * 2) / Math.max(1, values.length - 1);
      const points = values.map((v, i) => {
        const x = pad + i * step;
        const y = pad + (height - pad * 2) * (1 - (v - min) / range);
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      }).join(" ");
      return `
        <svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg">
          <polyline fill="none" stroke="${stroke}" stroke-width="2.2" points="${points}" />
        </svg>
      `;
    }

    async function tick() {
      try {
        const res = await fetch("/api/stocks", { cache: "no-store" });
        const data = await res.json();
        render(data);
        document.getElementById("last").textContent = data.server_time_utc || new Date().toISOString();
      } catch (_e) {}
    }

    tick();
    setInterval(tick, 2000);
  </script>
</body>
</html>
"""


def create_app() -> Flask:
    app = Flask(__name__)
    repo_root = Path(__file__).resolve().parents[1]
    db_path = Path(os.getenv("DASHBOARD_DB_PATH", str(repo_root / "data" / "stockbot.db")))

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @app.get("/")
    def dashboard():
        return render_template_string(HTML)

    @app.get("/api/stocks")
    def api_stocks():
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, name, current_price, base_price, slope, updated_at
                FROM companies
                ORDER BY symbol
                """
            ).fetchall()
            history_rows = conn.execute(
                """
                WITH ranked AS (
                    SELECT
                        symbol,
                        price,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY tick_index DESC) AS rn
                    FROM price_history
                )
                SELECT symbol, price, rn
                FROM ranked
                WHERE rn <= 40
                ORDER BY symbol, rn DESC
                """
            ).fetchall()
        history_map: dict[str, list[float]] = {}
        for h in history_rows:
            sym = str(h["symbol"])
            history_map.setdefault(sym, []).append(float(h["price"]))
        stocks = []
        for r in rows:
            row = dict(r)
            row["history_prices"] = history_map.get(str(r["symbol"]), [])
            stocks.append(row)
        return jsonify(
            {
                "server_time_utc": datetime.now(timezone.utc).isoformat(),
                "stocks": stocks,
            }
        )

    return app


def main() -> None:
    app = create_app()
    host = os.getenv("DASHBOARD_HOST", "127.0.0.1")
    port = int(os.getenv("DASHBOARD_PORT", "8082"))
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    main()
