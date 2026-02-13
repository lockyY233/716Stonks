from __future__ import annotations

import os
import secrets
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

from flask import Flask, g, jsonify, render_template_string, request

# Ensure project root is importable when running as a standalone script via systemd.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from stockbot.config.settings import DEFAULT_RANK, RANK_INCOME
from stockbot.services.perks import evaluate_user_perks

@dataclass(frozen=True)
class _CfgSpec:
    default: object
    cast: type
    description: str


APP_CONFIG_SPECS: dict[str, _CfgSpec] = {
    "START_BALANCE": _CfgSpec(5.0, float, "Starting cash for new users."),
    "TICK_INTERVAL": _CfgSpec(5, int, "Seconds between market ticks."),
    "TREND_MULTIPLIER": _CfgSpec(1e-2, float, "Multiplier used by trend-related math."),
    "DISPLAY_TIMEZONE": _CfgSpec("America/New_York", str, "Timezone used for display and market-close checks."),
    "MARKET_CLOSE_HOUR": _CfgSpec(21, int, "Local hour (0-23) used for daily market close logic."),
    "STONKERS_ROLE_NAME": _CfgSpec("📈💰📊Stonkers", str, "Role granted on registration and pinged on close updates."),
    "ANNOUNCEMENT_CHANNEL_ID": _CfgSpec(0, int, "Discord channel ID used for announcements; 0 means auto-pick."),
    "DRIFT_NOISE_FREQUENCY": _CfgSpec(0.7, float, "Normalized fast noise frequency [0,1]."),
    "DRIFT_NOISE_GAIN": _CfgSpec(0.8, float, "Fast noise gain multiplier."),
    "DRIFT_NOISE_LOW_FREQ_RATIO": _CfgSpec(0.08, float, "Low-band frequency ratio relative to fast frequency."),
    "DRIFT_NOISE_LOW_GAIN": _CfgSpec(3.0, float, "Low-band noise gain multiplier."),
    "TRADING_LIMITS": _CfgSpec(40, int, "Max shares traded per period; <=0 disables limits."),
    "TRADING_LIMITS_PERIOD": _CfgSpec(60, int, "Tick window length for trading-limit reset."),
    "TRADING_FEES": _CfgSpec(1.0, float, "Sell fee percentage applied to realized profit."),
    "COMMODITIES_LIMIT": _CfgSpec(5, int, "Max total commodity units a player can hold; <=0 disables."),
    "PAWN_SELL_RATE": _CfgSpec(75.0, float, "Pawn payout percentage when selling commodities to bank."),
}

AUTH_REQUIRED_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Web Admin Access Required</title>
  <style>
    body { margin:0; font-family: ui-sans-serif, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; background:#0f172a; color:#e5e7eb; }
    .wrap { min-height:100vh; display:grid; place-items:center; padding:20px; }
    .card { max-width:680px; width:100%; background:#0b1220; border:1px solid #1f2937; border-radius:12px; padding:18px; }
    .title { font-size:1.2rem; font-weight:700; margin-bottom:8px; }
    .muted { color:#94a3b8; }
    code { background:#111827; border:1px solid #374151; border-radius:6px; padding:2px 6px; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="title">Token expired or invalid</div>
      <div class="muted">
        If you are an admin, use <code>/webadmin</code> in Discord to generate a one-time access link.
      </div>
    </div>
  </div>
</body>
</html>
"""


MAIN_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>716Stonks Dashboard</title>
  <style>
    :root {
      --bg: #0f172a; --line: #1f2937; --text: #e5e7eb; --muted: #94a3b8;
      --up: #22c55e; --down: #ef4444; --btn:#1e293b; --btnH:#334155;
    }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: ui-sans-serif, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; background: radial-gradient(1200px 600px at 80% -10%, #1d4ed822, transparent), var(--bg); color: var(--text); }
    .wrap { max-width: 1400px; margin: 20px auto; padding: 0 14px; display: grid; grid-template-columns: 320px 1fr; gap: 14px; }
    .card { background: linear-gradient(180deg, #0b1220, #0a101c); border: 1px solid var(--line); border-radius: 12px; overflow: hidden; box-shadow: 0 10px 30px #00000033; }
    .head { padding: 14px 14px 8px 14px; border-bottom: 1px solid var(--line); display: flex; align-items: baseline; justify-content: space-between; gap: 10px; }
    .title { font-size: 1.1rem; font-weight: 700; }
    .muted { color: var(--muted); font-size: .85rem; }
    .side { padding: 10px; max-height: calc(100vh - 40px); overflow: auto; }
    .btnrow { display: grid; grid-template-columns: 1fr; gap: 8px; margin-bottom: 10px; }
    button, a.btn { background: var(--btn); border: 1px solid #334155; color: var(--text); padding: 9px 10px; border-radius: 8px; cursor: pointer; font-size: .86rem; text-decoration: none; text-align: center; }
    button:hover, a.btn:hover { background: var(--btnH); }
    button.active { border-color: #22c55e; }
    table { width: 100%; border-collapse: collapse; }
    thead th { text-align: left; font-size: .84rem; color: var(--muted); font-weight: 600; border-bottom: 1px solid var(--line); padding: 10px 8px; background: #0b1323; position: sticky; top: 0; }
    tbody td { padding: 9px 8px; border-bottom: 1px solid #111827; font-size: .92rem; }
    tbody tr:hover { background: #13203a; }
    tbody tr.clickable { cursor: pointer; }
    .thumb { width: 52px; height: 52px; border-radius: 8px; object-fit: cover; border: 1px solid #1f2937; background: #0b1323; display: block; }
    .mono { font-variant-numeric: tabular-nums; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    .up { color: var(--up); font-weight: 600; }
    .down { color: var(--down); font-weight: 600; }
    .empty { color: var(--muted); text-align: center; padding: 20px; }
    .tableWrap { overflow: auto; width: 100%; }
    .statsBar {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      padding: 10px 14px 12px 14px;
      border-bottom: 1px solid var(--line);
      background: #0a1324;
    }
    .stat {
      border: 1px solid #263246;
      border-radius: 8px;
      padding: 8px 10px;
      background: #0c1629;
    }
    .statLabel { color: var(--muted); font-size: .78rem; }
    .statValue { font-size: 1rem; font-weight: 700; margin-top: 2px; }
    [data-company-open]:hover {
      background: #1e3a8a66;
      color: #e0f2fe;
      text-decoration: underline;
      text-underline-offset: 2px;
    }
    @media (max-width: 980px) {
      .wrap { grid-template-columns: 1fr; }
      .side { max-height: none; }
      .head { flex-direction: column; align-items: flex-start; }
      thead th, tbody td { white-space: nowrap; }
      .statsBar { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="head">
        <div class="title">Views</div>
        <div class="muted">Tabs</div>
      </div>
      <div class="side">
        <div class="btnrow">
          <button id="showCompanies" class="active">Show Companies</button>
          <button id="showCommodities">Show Commodities</button>
          <button id="showPlayers">Show Players</button>
          <button id="showPerks">Show Perks</button>
          <button id="showFeedback">Feedback</button>
          <button id="showBankActions">Bank Actions</button>
          <button id="showActionHistory">Action History</button>
          <button id="showConfigs">Show App Configs</button>
          <button id="showServerSettings">Server Settings</button>
          <a class="btn" href="{{ db_access_url }}" target="_blank" rel="noopener">Open Database Access</a>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="head">
        <div class="title" id="tableTitle">Companies</div>
        <div class="muted">refresh 2s · last: <span id="last">-</span></div>
      </div>
      <div class="statsBar">
        <div class="stat">
          <div class="statLabel">Until Close</div>
          <div class="statValue" id="statUntilClose">-</div>
        </div>
        <div class="stat">
          <div class="statLabel">Until Next Reset</div>
          <div class="statValue" id="statUntilReset">-</div>
        </div>
        <div class="stat">
          <div class="statLabel">Companies</div>
          <div class="statValue" id="statCompanies">-</div>
        </div>
        <div class="stat">
          <div class="statLabel">Users</div>
          <div class="statValue" id="statUsers">-</div>
        </div>
      </div>
      <div class="tableWrap">
        <table>
          <thead id="thead"></thead>
          <tbody id="rows">
            <tr><td class="empty">Loading...</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    let currentTab = "companies";
    let pollTimer = null;
    let pollMs = 2000;
    let untilCloseSeconds = null;
    let untilResetSeconds = null;
    const ET_TIMEZONE = "America/New_York";
    const URL_AUTH_TOKEN = new URLSearchParams(window.location.search).get("token");
    function fmtMoney(v) { return "$" + Number(v).toFixed(2); }
    function fmtNum(v, d=4) { return Number(v).toFixed(d); }
    function fmtHMS(totalSeconds) {
      const s = Math.max(0, Number(totalSeconds) || 0);
      const h = Math.floor(s / 3600);
      const m = Math.floor((s % 3600) / 60);
      const sec = Math.floor(s % 60);
      return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(sec).padStart(2, "0")}`;
    }
    function esc(text) {
      return String(text).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }
    function fmtEtDate(value) {
      const dt = value ? new Date(value) : new Date();
      if (Number.isNaN(dt.getTime())) return String(value || "-");
      const parts = new Intl.DateTimeFormat("en-US", {
        timeZone: ET_TIMEZONE,
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      }).formatToParts(dt);
      const map = {};
      parts.forEach((p) => { map[p.type] = p.value; });
      return `${map.year}/${map.month}/${map.day} ${map.hour}:${map.minute}:${map.second}`;
    }
    function withAuthToken(url) {
      if (!URL_AUTH_TOKEN) return url;
      const sep = url.includes("?") ? "&" : "?";
      return `${url}${sep}token=${encodeURIComponent(URL_AUTH_TOKEN)}`;
    }
    function gotoWithAuth(url) {
      window.location.href = withAuthToken(url);
    }
    function setButtons() {
      document.querySelectorAll("button[id^='show']").forEach((btn) => btn.classList.remove("active"));
      if (currentTab === "companies") document.getElementById("showCompanies").classList.add("active");
      if (currentTab === "commodities") document.getElementById("showCommodities").classList.add("active");
      if (currentTab === "players") document.getElementById("showPlayers").classList.add("active");
      if (currentTab === "perks") document.getElementById("showPerks").classList.add("active");
      if (currentTab === "feedback") document.getElementById("showFeedback").classList.add("active");
      if (currentTab === "bankActions") document.getElementById("showBankActions").classList.add("active");
      if (currentTab === "actionHistory") document.getElementById("showActionHistory").classList.add("active");
      if (currentTab === "configs") document.getElementById("showConfigs").classList.add("active");
      if (currentTab === "serverSettings") document.getElementById("showServerSettings").classList.add("active");
    }
    function sparklineSvg(values, width, height, stroke) {
      if (!values || values.length < 2) return '<span class="muted">-</span>';
      const min = Math.min(...values), max = Math.max(...values);
      const pad = 2, range = (max - min) || 1, step = (width - pad * 2) / Math.max(1, values.length - 1);
      const points = values.map((v, i) => {
        const x = pad + i * step;
        const y = pad + (height - pad * 2) * (1 - (v - min) / range);
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      }).join(" ");
      return `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg"><polyline fill="none" stroke="${stroke}" stroke-width="2.2" points="${points}" /></svg>`;
    }
    function renderCompanies(rows) {
      document.getElementById("tableTitle").textContent = "Companies";
      document.getElementById("thead").innerHTML = `<tr>
        <th>Symbol</th><th>Name</th><th>Trend</th><th>Current</th><th>Base</th><th>Change vs Base</th><th>Slope</th><th>Action</th>
      </tr>`;
      const tbody = document.getElementById("rows");
      const controlRow = `<tr>
        <td colspan="8" style="padding:10px 8px;border-bottom:1px solid #1f2937;">
          <button id="addCompanyBtn" style="padding:6px 10px;background:#14532d;border-color:#22c55e;color:#dcfce7;">+ Add Company</button>
        </td>
      </tr>`;
      if (!rows.length) {
        tbody.innerHTML = controlRow + '<tr><td colspan="8" class="empty">No companies found.</td></tr>';
      } else {
        tbody.innerHTML = controlRow + rows.map(r => {
          const current = Number(r.current_price), base = Number(r.base_price);
          const pct = base !== 0 ? ((current - base) / base) * 100 : 0;
          const cls = pct >= 0 ? "up" : "down";
          const sign = pct >= 0 ? "+" : "";
          const spark = sparklineSvg(Array.isArray(r.history_prices) ? r.history_prices : [], 170, 36, pct >= 0 ? "#22c55e" : "#ef4444");
          return `<tr data-symbol="${esc(r.symbol)}">
            <td class="mono" data-company-open="${esc(r.symbol)}" style="cursor:pointer;"><strong>${esc(r.symbol)}</strong></td>
            <td data-company-open="${esc(r.symbol)}" style="cursor:pointer;">${esc(r.name || "")}</td>
            <td data-company-open="${esc(r.symbol)}" style="cursor:pointer;">${spark}</td>
            <td class="mono">${fmtMoney(current)}</td>
            <td><input class="mono" data-company-base="${esc(r.symbol)}" type="number" step="0.01" value="${Number(base).toFixed(2)}" style="width:110px;padding:6px 8px;border-radius:6px;border:1px solid #334155;background:#0b1323;color:#e5e7eb;" /></td>
            <td class="mono ${cls}">${sign}${pct.toFixed(2)}%</td>
            <td><input class="mono" data-company-slope="${esc(r.symbol)}" type="number" step="0.0001" value="${fmtNum(r.slope)}" style="width:110px;padding:6px 8px;border-radius:6px;border:1px solid #334155;background:#0b1323;color:#e5e7eb;" /></td>
            <td>
              <button data-company-save="${esc(r.symbol)}" style="padding:6px 10px;">Save</button>
            </td>
          </tr>`;
        }).join("");
      }
      const addBtn = document.getElementById("addCompanyBtn");
      if (addBtn) {
        addBtn.addEventListener("click", async () => {
          const symbol = (window.prompt("Symbol (e.g. ATEST):", "") || "").trim().toUpperCase();
          if (!symbol) return;
          const name = (window.prompt("Company name:", symbol) || "").trim();
          if (!name) return;
          const baseRaw = window.prompt("Base price:", "1.00");
          if (baseRaw === null) return;
          const slopeRaw = window.prompt("Slope:", "0.0000");
          if (slopeRaw === null) return;
          const base = Number(baseRaw);
          const slope = Number(slopeRaw);
          if (!Number.isFinite(base) || base <= 0 || !Number.isFinite(slope)) {
            alert("Invalid base/slope.");
            return;
          }
          addBtn.disabled = true;
          const old = addBtn.textContent;
          addBtn.textContent = "Adding...";
          try {
            const res = await fetch(withAuthToken("/api/company"), {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ symbol, name, base_price: base, slope }),
            });
            if (!res.ok) {
              const err = await res.json().catch(() => ({ error: "Create failed" }));
              alert(err.error || "Create failed");
            } else {
              await loadCompanies();
            }
          } finally {
            addBtn.disabled = false;
            addBtn.textContent = old;
          }
        });
      }
      document.querySelectorAll("[data-company-open]").forEach((el) => {
        el.addEventListener("click", () => {
          const sym = el.getAttribute("data-company-open");
          gotoWithAuth(`/company/${encodeURIComponent(sym || "")}`);
        });
      });
      document.querySelectorAll("button[data-company-save]").forEach((btn) => {
        btn.addEventListener("click", async () => {
          const sym = btn.getAttribute("data-company-save");
          if (!sym) return;
          const baseInput = document.querySelector(`input[data-company-base="${CSS.escape(sym)}"]`);
          const slopeInput = document.querySelector(`input[data-company-slope="${CSS.escape(sym)}"]`);
          if (!baseInput || !slopeInput) return;
          const payload = {
            base_price: Number(baseInput.value),
            slope: Number(slopeInput.value),
          };
          btn.disabled = true;
          const originalText = btn.textContent;
          btn.textContent = "Saving...";
          try {
            const res = await fetch(withAuthToken(`/api/company/${encodeURIComponent(sym)}/update`), {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            });
            if (!res.ok) {
              const err = await res.json().catch(()=>({error:"Failed"}));
              btn.textContent = err.error || "Failed";
            } else {
              btn.textContent = "Saved";
              setTimeout(() => { btn.textContent = originalText; }, 800);
              await loadCompanies();
            }
          } catch (_e) {
            btn.textContent = "Failed";
          } finally {
            btn.disabled = false;
          }
        });
      });
    }
    function renderCommodities(rows) {
      document.getElementById("tableTitle").textContent = "Commodities";
      document.getElementById("thead").innerHTML = `<tr>
        <th>Image</th><th>Name</th><th>Price</th><th>Rarity</th><th>Tags</th><th>Description</th>
      </tr>`;
      const tbody = document.getElementById("rows");
      const controlRow = `<tr>
        <td colspan="6" style="padding:10px 8px;border-bottom:1px solid #1f2937;">
          <button id="addCommodityBtn" style="padding:6px 10px;background:#14532d;border-color:#22c55e;color:#dcfce7;">+ Add Commodity</button>
        </td>
      </tr>`;
      if (!rows.length) {
        tbody.innerHTML = controlRow + '<tr><td colspan="6" class="empty">No commodities found.</td></tr>';
      } else {
        const fallback = "data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='52' height='52'><rect width='100%' height='100%' fill='%230b1323'/><text x='50%' y='50%' fill='%2394a3b8' font-size='9' text-anchor='middle' dominant-baseline='middle'>No Img</text></svg>";
        tbody.innerHTML = controlRow + rows.map((r) => `<tr class="clickable" data-commodity="${esc(r.name)}">
          <td><img class="thumb" src="${esc(r.image_url || fallback)}" alt="${esc(r.name)}" onerror="this.onerror=null;this.src='${fallback}'" /></td>
          <td><strong>${esc(r.name)}</strong></td>
          <td class="mono">${fmtMoney(r.price)}</td>
          <td>${esc((r.rarity || "common").toUpperCase())}</td>
          <td>${esc(String((r.tags || []).join(", ")))}</td>
          <td>${esc(r.description || "")}</td>
        </tr>`).join("");
      }
      const addBtn = document.getElementById("addCommodityBtn");
      if (addBtn) {
        addBtn.addEventListener("click", async () => {
          const name = (window.prompt("Commodity name:", "") || "").trim();
          if (!name) return;
          const priceRaw = window.prompt("Price:", "1.00");
          if (priceRaw === null) return;
          const rarity = (window.prompt("Rarity (common/uncommon/rare/legendary/exotic):", "common") || "common").trim().toLowerCase();
          const image_url = (window.prompt("Image URL (optional):", "") || "").trim();
          const tags = (window.prompt("Tags (comma-separated, optional):", "") || "").trim();
          const description = (window.prompt("Description (optional):", "") || "").trim();
          const price = Number(priceRaw);
          if (!Number.isFinite(price) || price <= 0) {
            alert("Invalid price.");
            return;
          }
          addBtn.disabled = true;
          const old = addBtn.textContent;
          addBtn.textContent = "Adding...";
          try {
            const res = await fetch(withAuthToken("/api/commodity"), {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ name, price, rarity, image_url, tags, description }),
            });
            if (!res.ok) {
              const err = await res.json().catch(() => ({ error: "Create failed" }));
              alert(err.error || "Create failed");
            } else {
              await loadCommodities();
            }
          } finally {
            addBtn.disabled = false;
            addBtn.textContent = old;
          }
        });
      }
      document.querySelectorAll("tr[data-commodity]").forEach((tr) => {
        tr.addEventListener("click", () => {
          const name = tr.getAttribute("data-commodity");
          gotoWithAuth(`/commodity/${encodeURIComponent(name)}`);
        });
      });
    }
    function renderPlayers(rows) {
      document.getElementById("tableTitle").textContent = "Players";
      document.getElementById("thead").innerHTML = `<tr>
        <th>User</th><th>Rank</th><th>Bank</th><th>Networth</th><th>Owe</th><th>Trading Limit</th>
      </tr>`;
      const tbody = document.getElementById("rows");
      if (!rows.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="empty">No players found.</td></tr>';
        return;
      }
      tbody.innerHTML = rows.map((r) => `<tr class="clickable" data-user="${esc(r.user_id)}">
        <td>${esc(r.display_name || ("User " + r.user_id))}<div class="muted mono">${esc(r.user_id)}</div></td>
        <td>${esc(r.rank || "-")}</td>
        <td class="mono">${fmtMoney(r.bank)}</td>
        <td class="mono">${fmtMoney(r.networth)}</td>
        <td class="mono">${fmtMoney(r.owe || 0)}</td>
        <td class="mono">${
          r.trade_limit_enabled
            ? `${Number(r.trade_limit_remaining || 0)}/${Number(r.trade_limit_limit || 0)}`
            : "Disabled"
        }</td>
      </tr>`).join("");
      document.querySelectorAll("tr[data-user]").forEach((tr) => {
        tr.addEventListener("click", () => {
          const uid = tr.getAttribute("data-user");
          gotoWithAuth(`/player/${encodeURIComponent(uid)}`);
        });
      });
    }
    async function renderPerks(rows) {
      document.getElementById("tableTitle").textContent = "Perks";
      document.getElementById("thead").innerHTML = `<tr>
        <th>Name</th><th>Enabled</th><th>Priority</th><th>Stack Mode</th><th>Max Stacks</th><th>Requirements</th><th>Effects</th><th>Description</th>
      </tr>`;
      const tbody = document.getElementById("rows");
      const controlRow = `<tr>
        <td colspan="8" style="padding:10px 8px;border-bottom:1px solid #1f2937;">
          <button id="addPerkBtn" style="padding:6px 10px;background:#14532d;border-color:#22c55e;color:#dcfce7;">+ Add Perk</button>
          <span style="margin-left:10px;" class="muted">Preview user:</span>
          <select id="perkPreviewUser" style="padding:6px 8px;border-radius:6px;border:1px solid #334155;background:#0b1323;color:#e5e7eb;min-width:220px;"></select>
          <button id="perkPreviewRefresh" style="padding:6px 10px;">Refresh Preview</button>
          <div id="perkPreviewBox" class="muted" style="margin-top:8px;"></div>
        </td>
      </tr>`;
      if (!rows.length) {
        tbody.innerHTML = controlRow + '<tr><td colspan="8" class="empty">No perks found.</td></tr>';
      } else {
        tbody.innerHTML = controlRow + rows.map((r) => `<tr class="clickable" data-perk-id="${esc(String(r.id || ""))}">
          <td><strong>${esc(r.name || "")}</strong></td>
          <td>${Number(r.enabled || 0) ? "Yes" : "No"}</td>
          <td class="mono">${esc(String(r.priority ?? 100))}</td>
          <td>${esc(String(r.stack_mode || "add"))}</td>
          <td class="mono">${esc(String(r.max_stacks ?? 1))}</td>
          <td class="mono">${esc(String(r.requirements_count ?? 0))}</td>
          <td class="mono">${esc(String(r.effects_count ?? 0))}</td>
          <td>${esc(String(r.description || ""))}</td>
        </tr>`).join("");
      }
      await loadPerkPreviewUsers();
      bindPerkPreviewRefresh();
      const addBtn = document.getElementById("addPerkBtn");
      if (addBtn) {
        addBtn.addEventListener("click", async () => {
          const name = (window.prompt("Perk name:", "") || "").trim();
          if (!name) return;
          const description = (window.prompt("Description (optional):", "") || "").trim();
          addBtn.disabled = true;
          const old = addBtn.textContent;
          addBtn.textContent = "Adding...";
          try {
            const res = await fetch(withAuthToken("/api/perk"), {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ name, description }),
            });
            if (!res.ok) {
              const err = await res.json().catch(() => ({ error: "Create failed" }));
              alert(err.error || "Create failed");
            } else {
              const data = await res.json();
              const id = Number(data.id || 0);
              if (id > 0) {
                gotoWithAuth(`/perk/${id}`);
                return;
              }
              await loadPerks();
            }
          } finally {
            addBtn.disabled = false;
            addBtn.textContent = old;
          }
        });
      }
      document.querySelectorAll("tr[data-perk-id]").forEach((tr) => {
        tr.addEventListener("click", () => {
          const id = tr.getAttribute("data-perk-id");
          if (!id) return;
          gotoWithAuth(`/perk/${encodeURIComponent(id)}`);
        });
      });
    }
    async function loadPerkPreviewUsers() {
      const select = document.getElementById("perkPreviewUser");
      if (!select) return;
      try {
        const res = await fetch(withAuthToken("/api/players"), { cache: "no-store" });
        if (!res.ok) {
          select.innerHTML = '<option value="">(No users)</option>';
          return;
        }
        const data = await res.json();
        const players = Array.isArray(data.players) ? data.players : [];
        if (!players.length) {
          select.innerHTML = '<option value="">(No users)</option>';
          const box = document.getElementById("perkPreviewBox");
          if (box) box.textContent = "No players available for preview.";
          return;
        }
        const old = String(select.value || "");
        select.innerHTML = players.map((p) => {
          const uid = String(p.user_id || "");
          const name = String(p.display_name || `User ${uid}`);
          return `<option value="${esc(uid)}">${esc(name)} (${esc(uid)})</option>`;
        }).join("");
        if (old && [...select.options].some((o) => o.value === old)) {
          select.value = old;
        }
        await loadPerkPreview();
      } catch (_e) {
        select.innerHTML = '<option value="">(Failed to load)</option>';
      }
    }
    function bindPerkPreviewRefresh() {
      const btn = document.getElementById("perkPreviewRefresh");
      const select = document.getElementById("perkPreviewUser");
      if (!btn || !select) return;
      btn.onclick = async () => { await loadPerkPreview(); };
      select.onchange = async () => { await loadPerkPreview(); };
    }
    async function loadPerkPreview() {
      const select = document.getElementById("perkPreviewUser");
      const box = document.getElementById("perkPreviewBox");
      if (!select || !box) return;
      const userId = String(select.value || "");
      if (!userId) {
        box.textContent = "Select a player to preview.";
        return;
      }
      box.textContent = "Loading preview...";
      try {
        const res = await fetch(withAuthToken(`/api/perk-preview?user_id=${encodeURIComponent(userId)}`), { cache: "no-store" });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          box.textContent = String(data.error || "Failed to load preview.");
          return;
        }
        const matched = Array.isArray(data.matched_perks) ? data.matched_perks : [];
        const header = `Rank: ${data.rank} · Income $${Number(data.base_income || 0).toFixed(2)} -> $${Number(data.final_income || 0).toFixed(2)} · Limits ${Number(data.base_trade_limits || 0).toFixed(0)} -> ${Number(data.final_trade_limits || 0).toFixed(0)} · Networth $${Number(data.base_networth || 0).toFixed(2)} -> $${Number(data.final_networth || 0).toFixed(2)}`;
        if (!matched.length) {
          box.textContent = `${header} · No triggered perks.`;
          return;
        }
        const lines = matched.map((m) => {
          const disp = String(m.display || "").trim();
          if (disp) return `${m.name}(x${m.stacks}) (${disp})`;
          return `${m.name}(x${m.stacks}) (income ${Number(m.add || 0).toFixed(2)}, x${Number(m.mul || 1).toFixed(2)})`;
        });
        box.textContent = `${header} · Triggered: ${lines.join(" | ")}`;
      } catch (_e) {
        box.textContent = "Failed to load preview.";
      }
    }
    function renderActionHistory(rows) {
      document.getElementById("tableTitle").textContent = "Action History";
      document.getElementById("thead").innerHTML = `<tr>
        <th>When (UTC)</th><th>User</th><th>Action</th><th>Target</th><th>Qty</th><th>Unit</th><th>Total</th><th>Details</th>
      </tr>`;
      const tbody = document.getElementById("rows");
      const controlRow = `<tr>
        <td colspan="8" style="padding:10px 8px;border-bottom:1px solid #1f2937;">
          <button id="purgeActionHistoryBtn" style="padding:6px 10px;background:#7f1d1d;border-color:#ef4444;color:#fee2e2;">Purge All History</button>
        </td>
      </tr>`;
      if (!rows.length) {
        tbody.innerHTML = controlRow + '<tr><td colspan="8" class="empty">No player actions recorded yet.</td></tr>';
      } else {
        tbody.innerHTML = controlRow + rows.map((r) => `<tr>
        <td class="mono">${esc(fmtEtDate(r.created_at || ""))}</td>
        <td>${esc(r.display_name || ("User " + r.user_id))}<div class="muted mono">${esc(String(r.user_id || ""))}</div></td>
        <td>${esc(String(r.action_type || "").toUpperCase())}</td>
        <td>${esc(String(r.target_type || "").toUpperCase())}: <span class="mono">${esc(r.target_symbol || "-")}</span></td>
        <td class="mono">${fmtNum(r.quantity || 0, 2)}</td>
        <td class="mono">${fmtMoney(r.unit_price || 0)}</td>
        <td class="mono">${fmtMoney(r.total_amount || 0)}</td>
        <td>${esc(r.details || "")}</td>
      </tr>`).join("");
      }
      const purgeBtn = document.getElementById("purgeActionHistoryBtn");
      if (purgeBtn) {
        purgeBtn.addEventListener("click", async () => {
          const ok = window.confirm("Purge all action history? This cannot be undone.");
          if (!ok) return;
          purgeBtn.disabled = true;
          const old = purgeBtn.textContent;
          purgeBtn.textContent = "Purging...";
          try {
            const res = await fetch(withAuthToken("/api/action-history"), { method: "DELETE" });
            if (!res.ok) {
              const err = await res.json().catch(() => ({ error: "Failed" }));
              purgeBtn.textContent = err.error || "Failed";
              purgeBtn.disabled = false;
              return;
            }
            await loadActionHistory();
          } catch (_e) {
            purgeBtn.textContent = "Failed";
            purgeBtn.disabled = false;
            return;
          }
          purgeBtn.textContent = old;
          purgeBtn.disabled = false;
        });
      }
    }
    function renderFeedback(rows) {
      document.getElementById("tableTitle").textContent = "Feedback";
      document.getElementById("thead").innerHTML = `<tr>
        <th>ID</th><th>When (ET)</th><th>Message</th><th>Action</th>
      </tr>`;
      const tbody = document.getElementById("rows");
      if (!rows.length) {
        tbody.innerHTML = '<tr><td colspan="4" class="empty">No feedback yet.</td></tr>';
        return;
      }
      tbody.innerHTML = rows.map((r) => `<tr data-feedback-id="${esc(String(r.id || ""))}">
        <td class="mono">${esc(String(r.id || ""))}</td>
        <td class="mono">${esc(fmtEtDate(r.created_at || ""))}</td>
        <td>${esc(r.message || "")}</td>
        <td><button data-feedback-delete="${esc(String(r.id || ""))}" style="padding:6px 10px;background:#7f1d1d;border-color:#ef4444;color:#fee2e2;">Delete</button></td>
      </tr>`).join("");
      document.querySelectorAll("button[data-feedback-delete]").forEach((btn) => {
        btn.addEventListener("click", async () => {
          const id = btn.getAttribute("data-feedback-delete");
          if (!id) return;
          btn.disabled = true;
          const original = btn.textContent;
          btn.textContent = "Deleting...";
          try {
            const res = await fetch(withAuthToken(`/api/feedback/${encodeURIComponent(id)}`), { method: "DELETE" });
            if (!res.ok) {
              const err = await res.json().catch(() => ({ error: "Failed" }));
              btn.textContent = err.error || "Failed";
              btn.disabled = false;
              return;
            }
            const tr = document.querySelector(`tr[data-feedback-id="${CSS.escape(id)}"]`);
            if (tr) tr.remove();
            if (!document.querySelector("tr[data-feedback-id]")) {
              document.getElementById("rows").innerHTML = '<tr><td colspan="4" class="empty">No feedback yet.</td></tr>';
            }
          } catch (_e) {
            btn.textContent = "Failed";
            btn.disabled = false;
            return;
          }
          btn.textContent = original;
          btn.disabled = false;
        });
      });
    }
    function renderBankActions(rows) {
      document.getElementById("tableTitle").textContent = "Bank Actions";
      document.getElementById("thead").innerHTML = `<tr>
        <th>ID</th><th>Created (UTC)</th><th>User</th><th>Type</th><th>Amount</th><th>Reason</th><th>Decision</th>
      </tr>`;
      const tbody = document.getElementById("rows");
      if (!rows.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="empty">No pending bank requests.</td></tr>';
        return;
      }
      tbody.innerHTML = rows.map((r) => `<tr data-bank-request="${esc(String(r.id))}">
        <td class="mono">${esc(String(r.id))}</td>
        <td class="mono">${esc(r.created_at || "-")}</td>
        <td>${esc(r.display_name || ("User " + r.user_id))}<div class="muted mono">${esc(String(r.user_id || ""))}</div></td>
        <td>${esc(String(r.request_type || "").toUpperCase())}</td>
        <td class="mono">${fmtMoney(r.amount || 0)}</td>
        <td>${esc(r.reason || "-")}</td>
        <td>
          <input data-bank-reason="${esc(String(r.id))}" placeholder="Optional reason..." style="width:100%;padding:6px 8px;margin-bottom:6px;border-radius:6px;border:1px solid #334155;background:#0b1323;color:#e5e7eb;" />
          <div style="display:flex;gap:6px;flex-wrap:wrap;">
            <button data-bank-approve="${esc(String(r.id))}" style="padding:6px 10px;background:#166534;border-color:#22c55e;">Approve</button>
            <button data-bank-deny="${esc(String(r.id))}" style="padding:6px 10px;background:#7f1d1d;border-color:#ef4444;">Deny</button>
          </div>
        </td>
      </tr>`).join("");

      document.querySelectorAll("button[data-bank-approve],button[data-bank-deny]").forEach((btn) => {
        btn.addEventListener("click", async () => {
          const id = btn.getAttribute("data-bank-approve") || btn.getAttribute("data-bank-deny");
          if (!id) return;
          const isApprove = btn.hasAttribute("data-bank-approve");
          const reasonInput = document.querySelector(`input[data-bank-reason="${CSS.escape(id)}"]`);
          const reason = reasonInput ? String(reasonInput.value || "").trim() : "";
          const url = isApprove
            ? `/api/bank-requests/${encodeURIComponent(id)}/approve`
            : `/api/bank-requests/${encodeURIComponent(id)}/deny`;
          btn.disabled = true;
          const original = btn.textContent;
          btn.textContent = isApprove ? "Approving..." : "Denying...";
          try {
            const res = await fetch(withAuthToken(url), {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ reason }),
            });
            if (!res.ok) {
              const err = await res.json().catch(() => ({ error: "Failed" }));
              btn.textContent = err.error || "Failed";
            } else {
              const tr = document.querySelector(`tr[data-bank-request="${CSS.escape(id)}"]`);
              if (tr) tr.remove();
              if (!document.querySelector("tr[data-bank-request]")) {
                document.getElementById("rows").innerHTML = '<tr><td colspan="7" class="empty">No pending bank requests.</td></tr>';
              }
            }
          } catch (_e) {
            btn.textContent = "Failed";
          } finally {
            btn.disabled = false;
            if (btn.textContent !== "Failed") btn.textContent = original;
          }
        });
      });
    }
    function renderConfigs(rows) {
      document.getElementById("tableTitle").textContent = "App Configs";
      document.getElementById("thead").innerHTML = `<tr>
        <th>Name</th><th>Value</th><th>Default</th><th>Type</th><th>Description</th><th>Action</th>
      </tr>`;
      const tbody = document.getElementById("rows");
      if (!rows.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="empty">No app configs found.</td></tr>';
        return;
      }
      tbody.innerHTML = rows.map((r) => `<tr data-config="${esc(r.name)}">
        <td class="mono"><strong>${esc(r.name)}</strong></td>
        <td><input class="cfg-value mono" data-config-input="${esc(r.name)}" value="${esc(String(r.value))}" style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid #334155;background:#0b1323;color:#e5e7eb;" /></td>
        <td class="mono">${esc(String(r.default))}</td>
        <td>${esc(r.type || "-")}</td>
        <td>${esc(r.description || "")}</td>
        <td><button class="cfg-save" data-config-save="${esc(r.name)}" style="padding:6px 10px;">Save</button></td>
      </tr>`).join("");
      document.querySelectorAll("button[data-config-save]").forEach((btn) => {
        btn.addEventListener("click", async () => {
          const name = btn.getAttribute("data-config-save");
          const input = document.querySelector(`input[data-config-input="${CSS.escape(name)}"]`);
          if (!input) return;
          const rawValue = input.value;
          btn.disabled = true;
          const originalText = btn.textContent;
          btn.textContent = "Saving...";
          try {
            const res = await fetch(withAuthToken(`/api/app-config/${encodeURIComponent(name)}/update`), {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ value: rawValue }),
            });
            if (!res.ok) {
              const err = await res.json().catch(()=>({error:"Save failed"}));
              btn.textContent = err.error || "Failed";
            } else {
              const data = await res.json();
              input.value = String(data.value);
              btn.textContent = "Saved";
              setTimeout(() => { btn.textContent = originalText; }, 800);
            }
          } catch (_e) {
            btn.textContent = "Failed";
          } finally {
            btn.disabled = false;
          }
        });
      });
    }
    function renderServerSettings() {
      document.getElementById("tableTitle").textContent = "Server Settings";
      document.getElementById("thead").innerHTML = `<tr>
        <th>Action</th><th>Description</th><th>Run</th><th>Status</th>
      </tr>`;
      const tbody = document.getElementById("rows");
      tbody.innerHTML = `<tr>
        <td><strong>Create Database Backup</strong></td>
        <td>Create a point-in-time copy of the database now.</td>
        <td><button id="runBackupNow" style="padding:6px 10px;">Run</button></td>
        <td id="backupStatus" class="mono">-</td>
      </tr>`;
      const btn = document.getElementById("runBackupNow");
      const status = document.getElementById("backupStatus");
      btn.addEventListener("click", async () => {
        btn.disabled = true;
        status.textContent = "Running...";
        try {
          const res = await fetch(withAuthToken("/api/server-actions/backup"), { method: "POST" });
          const data = await res.json().catch(() => ({}));
          if (!res.ok) {
            status.textContent = data.error || "Failed";
          } else {
            status.textContent = `OK: ${String(data.backup_path || "")}`;
          }
        } catch (_e) {
          status.textContent = "Failed";
        } finally {
          btn.disabled = false;
        }
      });

      loadBackupsIntoServerSettings();
    }
    async function loadBackupsIntoServerSettings() {
      const tbody = document.getElementById("rows");
      try {
        const res = await fetch(withAuthToken("/api/server-actions/backups"), { cache: "no-store" });
        if (!res.ok) return;
        const data = await res.json();
        const backups = Array.isArray(data.backups) ? data.backups : [];
        if (!backups.length) {
          tbody.insertAdjacentHTML("beforeend", `<tr><td colspan="4" class="empty">No backups found.</td></tr>`);
          return;
        }
        backups.forEach((b) => {
          const row = `<tr data-backup="${esc(b.name)}">
            <td><strong>Delete Backup</strong></td>
            <td class="mono">${esc(b.name)}<br><span class="muted">${esc(String(b.size_human || ""))}</span></td>
            <td><button data-delete-backup="${esc(b.name)}" style="padding:6px 10px;background:#7f1d1d;border-color:#b91c1c;">Delete</button></td>
            <td class="mono" data-backup-status="${esc(b.name)}">-</td>
          </tr>`;
          tbody.insertAdjacentHTML("beforeend", row);
        });
        document.querySelectorAll("button[data-delete-backup]").forEach((btn) => {
          btn.addEventListener("click", async () => {
            const name = btn.getAttribute("data-delete-backup");
            if (!name) return;
            const status = document.querySelector(`[data-backup-status="${CSS.escape(name)}"]`);
            btn.disabled = true;
            if (status) status.textContent = "Deleting...";
            try {
              const delRes = await fetch(withAuthToken(`/api/server-actions/backups/${encodeURIComponent(name)}`), { method: "DELETE" });
              const delData = await delRes.json().catch(() => ({}));
              if (!delRes.ok) {
                if (status) status.textContent = delData.error || "Failed";
              } else {
                const tr = document.querySelector(`tr[data-backup="${CSS.escape(name)}"]`);
                if (tr) tr.remove();
              }
            } catch (_e) {
              if (status) status.textContent = "Failed";
            } finally {
              btn.disabled = false;
            }
          });
        });
      } catch (_e) {
        tbody.insertAdjacentHTML("beforeend", `<tr><td colspan="4" class="empty">Failed to load backups.</td></tr>`);
      }
    }
    async function loadCompanies() {
      const res = await fetch(withAuthToken("/api/stocks"), { cache: "no-store" });
      const data = await res.json();
      renderCompanies(data.stocks || []);
      document.getElementById("last").textContent = fmtEtDate(data.server_time_utc || "");
    }
    async function loadCommodities() {
      const res = await fetch(withAuthToken("/api/commodities"), { cache: "no-store" });
      const data = await res.json();
      renderCommodities(data.commodities || []);
      document.getElementById("last").textContent = fmtEtDate();
    }
    async function loadPlayers() {
      const res = await fetch(withAuthToken("/api/players"), { cache: "no-store" });
      const data = await res.json();
      renderPlayers(data.players || []);
      document.getElementById("last").textContent = fmtEtDate();
    }
    async function loadPerks() {
      const res = await fetch(withAuthToken("/api/perks"), { cache: "no-store" });
      const data = await res.json();
      await renderPerks(data.perks || []);
      document.getElementById("last").textContent = fmtEtDate();
    }
    async function loadActionHistory() {
      const res = await fetch(withAuthToken("/api/action-history"), { cache: "no-store" });
      const data = await res.json();
      renderActionHistory(data.actions || []);
      document.getElementById("last").textContent = fmtEtDate();
    }
    async function loadFeedback() {
      const res = await fetch(withAuthToken("/api/feedback"), { cache: "no-store" });
      const data = await res.json();
      renderFeedback(data.feedback || []);
      document.getElementById("last").textContent = fmtEtDate();
    }
    async function loadBankActions() {
      const res = await fetch(withAuthToken("/api/bank-requests?status=pending"), { cache: "no-store" });
      const data = await res.json();
      renderBankActions(data.requests || []);
      document.getElementById("last").textContent = fmtEtDate();
    }
    async function loadHeaderStats() {
      try {
        const res = await fetch(withAuthToken("/api/dashboard-stats"), { cache: "no-store" });
        if (!res.ok) return;
        const data = await res.json();
        if (Number.isFinite(Number(data.seconds_until_close))) {
          untilCloseSeconds = Math.max(0, Number(data.seconds_until_close));
          document.getElementById("statUntilClose").textContent = fmtHMS(untilCloseSeconds);
        } else {
          document.getElementById("statUntilClose").textContent = data.until_close || "-";
        }
        if (Number.isFinite(Number(data.seconds_until_reset))) {
          untilResetSeconds = Math.max(0, Number(data.seconds_until_reset));
          document.getElementById("statUntilReset").textContent = `${(untilResetSeconds / 60).toFixed(2)} min`;
        } else {
          document.getElementById("statUntilReset").textContent = data.until_reset || "-";
        }
        document.getElementById("statCompanies").textContent = String(data.company_count ?? "-");
        document.getElementById("statUsers").textContent = String(data.user_count ?? "-");
      } catch (_e) {}
    }
    function startHeaderTicker() {
      setInterval(() => {
        if (!Number.isFinite(untilCloseSeconds)) return;
        untilCloseSeconds = Math.max(0, untilCloseSeconds - 1);
        document.getElementById("statUntilClose").textContent = fmtHMS(untilCloseSeconds);
      }, 1000);
      setInterval(() => {
        if (!Number.isFinite(untilResetSeconds)) return;
        untilResetSeconds = Math.max(0, untilResetSeconds - 1);
        document.getElementById("statUntilReset").textContent = `${(untilResetSeconds / 60).toFixed(2)} min`;
      }, 1000);
    }
    async function loadConfigs() {
      const res = await fetch(withAuthToken("/api/app-configs"), { cache: "no-store" });
      const data = await res.json();
      renderConfigs(data.configs || []);
      document.getElementById("last").textContent = fmtEtDate();
    }
    async function tick() {
      const active = document.activeElement;
      const editing =
        active &&
        (active.tagName === "INPUT" || active.tagName === "TEXTAREA" || active.tagName === "SELECT");
      if (editing) return;
      try {
        await loadHeaderStats();
        if (currentTab === "companies") await loadCompanies();
        else if (currentTab === "commodities") await loadCommodities();
        else if (currentTab === "players") await loadPlayers();
        else if (currentTab === "perks") await loadPerks();
        else if (currentTab === "feedback") await loadFeedback();
        else if (currentTab === "bankActions") await loadBankActions();
        else if (currentTab === "actionHistory") await loadActionHistory();
        else if (currentTab === "configs") await loadConfigs();
        else return;
      } catch (_e) {}
    }
    async function syncPollInterval() {
      try {
        const res = await fetch(withAuthToken("/api/app-config/TICK_INTERVAL"), { cache: "no-store" });
        if (!res.ok) return;
        const data = await res.json();
        const seconds = Number(data?.config?.value);
        if (!Number.isFinite(seconds) || seconds <= 0) return;
        const nextMs = Math.max(1000, Math.round(seconds * 1000));
        if (nextMs === pollMs && pollTimer) return;
        pollMs = nextMs;
        if (pollTimer) clearInterval(pollTimer);
        pollTimer = setInterval(tick, pollMs);
      } catch (_e) {}
    }
    document.getElementById("showCompanies").addEventListener("click", () => { currentTab = "companies"; setButtons(); tick(); });
    document.getElementById("showCommodities").addEventListener("click", () => { currentTab = "commodities"; setButtons(); tick(); });
    document.getElementById("showPlayers").addEventListener("click", () => { currentTab = "players"; setButtons(); tick(); });
    document.getElementById("showPerks").addEventListener("click", () => { currentTab = "perks"; setButtons(); tick(); });
    document.getElementById("showFeedback").addEventListener("click", () => { currentTab = "feedback"; setButtons(); tick(); });
    document.getElementById("showBankActions").addEventListener("click", () => { currentTab = "bankActions"; setButtons(); tick(); });
    document.getElementById("showActionHistory").addEventListener("click", () => { currentTab = "actionHistory"; setButtons(); tick(); });
    document.getElementById("showConfigs").addEventListener("click", () => { currentTab = "configs"; setButtons(); tick(); });
    document.getElementById("showServerSettings").addEventListener("click", () => { currentTab = "serverSettings"; setButtons(); renderServerSettings(); });
    setButtons();
    tick();
    syncPollInterval();
    startHeaderTicker();
    setInterval(syncPollInterval, 30000);
  </script>
</body>
</html>
"""


DETAIL_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{{ symbol }} · 716Stonks</title>
  <style>
    :root {
      --bg: #0f172a; --panel: #111827; --line: #1f2937; --text: #e5e7eb; --muted: #94a3b8; --btn:#334155; --btnH:#475569;
    }
    body { margin:0; background:var(--bg); color:var(--text); font-family:ui-sans-serif, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; }
    .wrap { max-width: 1100px; margin: 20px auto; padding: 0 14px; display: grid; grid-template-columns: 2fr 1fr; gap: 14px; }
    .card { background: linear-gradient(180deg,#0b1220,#0a101c); border: 1px solid var(--line); border-radius: 12px; padding: 12px; }
    .title { font-size: 1.2rem; font-weight: 700; margin-bottom: 8px; }
    .muted { color: var(--muted); font-size: .85rem; }
    .chartWrap { height: 380px; border: 1px solid var(--line); border-radius: 10px; padding: 10px; background: #0b1323; }
    #chart { width: 100%; height: 100%; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 8px; }
    .row.one { grid-template-columns: 1fr; }
    label { display:block; font-size:.82rem; color: var(--muted); margin-bottom: 4px; }
    input, textarea { width:100%; padding:8px; border-radius:8px; border:1px solid #334155; background:#0b1323; color:var(--text); }
    textarea { min-height: 96px; resize: vertical; }
    button { padding:9px 12px; border:1px solid #334155; border-radius:8px; background:var(--btn); color:var(--text); cursor:pointer; }
    button:hover { background: var(--btnH); }
    .top { display:flex; justify-content:space-between; align-items:center; margin-bottom:10px; }
    a { color:#7dd3fc; text-decoration:none; font-size:.9rem; }
    @media (max-width: 1024px) {
      .wrap { grid-template-columns: 1fr; }
    }
    @media (max-width: 700px) {
      .row { grid-template-columns: 1fr; }
      .chartWrap { height: 290px; }
      .top { flex-direction: column; align-items: flex-start; gap: 6px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="top">
        <div class="title">{{ symbol }} · Live Graph</div>
        <a id="backLink" href="/">Back to Dashboard</a>
      </div>
      <div class="muted">Realtime refresh: 2s · Last: <span id="last">-</span></div>
      <div style="margin:8px 0 10px 0;"><button id="viewToggle">Showing: Last 80</button></div>
      <div class="chartWrap"><svg id="chart" viewBox="0 0 900 360"></svg></div>
    </div>
    <div class="card">
      <div class="title">Edit Parameters</div>
      <div class="row">
        <div><label>Name</label><input id="name" /></div>
        <div><label>Current Price</label><input id="current_price" type="number" step="0.01" /></div>
      </div>
      <div class="row">
        <div><label>Base Price</label><input id="base_price" type="number" step="0.01" /></div>
        <div><label>Slope</label><input id="slope" type="number" step="0.0001" /></div>
      </div>
      <div class="row">
        <div><label>Drift (%)</label><input id="drift" type="number" step="0.001" /></div>
        <div><label>Liquidity</label><input id="liquidity" type="number" step="0.01" /></div>
      </div>
      <div class="row">
        <div><label>Impact Power</label><input id="impact_power" type="number" step="0.001" /></div>
        <div><label>Founded Year</label><input id="founded_year" type="number" step="1" /></div>
      </div>
      <div class="row">
        <div><label>Location</label><input id="location" /></div>
        <div><label>Industry</label><input id="industry" /></div>
      </div>
      <div class="row one">
        <div><label>Evaluation</label><textarea id="evaluation"></textarea></div>
      </div>
      <div class="row one">
        <div><label>Description</label><textarea id="description"></textarea></div>
      </div>
      <button id="saveBtn">Save</button>
      <button id="deleteBtn" style="margin-left:8px;background:#7f1d1d;border-color:#ef4444;color:#fee2e2;">Delete Company</button>
      <div class="muted" id="msg" style="margin-top:8px;"></div>
    </div>
  </div>

  <script>
    const SYMBOL = {{ symbol|tojson }};
    const URL_AUTH_TOKEN = new URLSearchParams(window.location.search).get("token");
    const ET_TIMEZONE = "America/New_York";
    const RECENT_COUNT = 80;
    let showRecent = true;
    let historyAll = [];
    function el(id){ return document.getElementById(id); }
    function withAuthToken(url) {
      if (!URL_AUTH_TOKEN) return url;
      const sep = url.includes("?") ? "&" : "?";
      return `${url}${sep}token=${encodeURIComponent(URL_AUTH_TOKEN)}`;
    }
    function wireBackLink() {
      const back = el("backLink");
      if (back) back.href = withAuthToken("/");
    }
    function fmtEtDate(value) {
      const dt = value ? new Date(value) : new Date();
      if (Number.isNaN(dt.getTime())) return String(value || "-");
      const parts = new Intl.DateTimeFormat("en-US", {
        timeZone: ET_TIMEZONE,
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      }).formatToParts(dt);
      const map = {};
      parts.forEach((p) => { map[p.type] = p.value; });
      return `${map.year}/${map.month}/${map.day} ${map.hour}:${map.minute}:${map.second}`;
    }

    function drawLine(values) {
      const svg = el("chart");
      const w = 900, h = 360;
      const left = 60, right = 18, top = 16, bottom = 42;
      const pw = w - left - right, ph = h - top - bottom;
      if (!values || values.length < 2) {
        svg.innerHTML = "";
        return;
      }
      const min = Math.min(...values), max = Math.max(...values), range = (max-min)||1;
      const step = pw / Math.max(1, values.length - 1);
      const pts = values.map((v,i)=>{
        const x = left + i * step;
        const y = top + ph * (1 - ((v-min)/range));
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      }).join(" ");
      const up = values[values.length-1] >= values[0];
      const hGrid = Array.from({length: 6}, (_, i) => {
        const y = top + (ph * i / 5);
        return `<line x1="${left}" y1="${y}" x2="${w-right}" y2="${y}" stroke="#1f2937" stroke-width="1" />`;
      }).join("");
      const vGrid = Array.from({length: 6}, (_, i) => {
        const x = left + (pw * i / 5);
        return `<line x1="${x}" y1="${top}" x2="${x}" y2="${h-bottom}" stroke="#1f2937" stroke-width="1" />`;
      }).join("");
      svg.innerHTML = `
        ${hGrid}
        ${vGrid}
        <line x1="${left}" y1="${h-bottom}" x2="${w-right}" y2="${h-bottom}" stroke="#475569" stroke-width="1.2" />
        <line x1="${left}" y1="${top}" x2="${left}" y2="${h-bottom}" stroke="#475569" stroke-width="1.2" />
        <text x="${left}" y="${top-3}" fill="#94a3b8" font-size="11">${max.toFixed(2)}</text>
        <text x="${left}" y="${h-bottom+14}" fill="#94a3b8" font-size="11">${min.toFixed(2)}</text>
        <text x="${left}" y="${h-8}" fill="#94a3b8" font-size="11">0</text>
        <text x="${w-right-24}" y="${h-8}" fill="#94a3b8" font-size="11">${values.length-1}</text>
        <text x="${w/2-30}" y="${h-8}" fill="#94a3b8" font-size="12">X: Tick Index</text>
        <text x="${left+8}" y="${top+14}" fill="#94a3b8" font-size="12">Y: Price ($)</text>
        <polyline fill="none" stroke="${up ? '#22c55e' : '#ef4444'}" stroke-width="3" points="${pts}" />
      `;
    }

    function fillForm(c) {
      el("name").value = c.name || "";
      el("current_price").value = Number(c.current_price).toFixed(2);
      el("base_price").value = Number(c.base_price).toFixed(2);
      el("slope").value = Number(c.slope).toFixed(4);
      el("drift").value = Number(c.drift).toFixed(3);
      el("liquidity").value = Number(c.liquidity).toFixed(2);
      el("impact_power").value = Number(c.impact_power).toFixed(3);
      el("founded_year").value = Number(c.founded_year || 2000).toFixed(0);
      el("location").value = c.location || "";
      el("industry").value = c.industry || "";
      el("evaluation").value = c.evaluation || "";
      el("description").value = c.description || "";
    }

    function refreshChart() {
      const values = showRecent ? historyAll.slice(-RECENT_COUNT) : historyAll;
      drawLine(values);
      el("viewToggle").textContent = showRecent ? `Showing: Last ${RECENT_COUNT}` : "Showing: All History";
    }

    async function load() {
      const res = await fetch(withAuthToken(`/api/company/${encodeURIComponent(SYMBOL)}`), { cache: "no-store" });
      if (!res.ok) return;
      const data = await res.json();
      if (!data.company) return;
      fillForm(data.company);
      historyAll = Array.isArray(data.history_prices) ? data.history_prices : [];
      refreshChart();
      el("last").textContent = fmtEtDate(data.server_time_utc || "");
    }

    async function save() {
      const payload = {
        name: el("name").value,
        current_price: Number(el("current_price").value),
        base_price: Number(el("base_price").value),
        slope: Number(el("slope").value),
        drift: Number(el("drift").value),
        liquidity: Number(el("liquidity").value),
        impact_power: Number(el("impact_power").value),
        founded_year: Number(el("founded_year").value),
        location: el("location").value,
        industry: el("industry").value,
        evaluation: el("evaluation").value,
        description: el("description").value,
      };
      const res = await fetch(withAuthToken(`/api/company/${encodeURIComponent(SYMBOL)}/update`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (res.ok) {
        el("msg").textContent = "Saved.";
        await load();
      } else {
        const err = await res.json().catch(()=>({error:"Failed"}));
        el("msg").textContent = err.error || "Save failed.";
      }
    }

    async function removeCompany() {
      const ok = window.confirm(`Delete company ${SYMBOL}? This cannot be undone.`);
      if (!ok) return;
      const btn = el("deleteBtn");
      const old = btn.textContent;
      btn.disabled = true;
      btn.textContent = "Deleting...";
      try {
        const res = await fetch(withAuthToken(`/api/company/${encodeURIComponent(SYMBOL)}`), {
          method: "DELETE",
        });
        if (!res.ok) {
          const err = await res.json().catch(()=>({error:"Delete failed"}));
          el("msg").textContent = err.error || "Delete failed.";
          btn.disabled = false;
          btn.textContent = old;
          return;
        }
        window.location.href = withAuthToken("/");
      } catch (_e) {
        el("msg").textContent = "Delete failed.";
        btn.disabled = false;
        btn.textContent = old;
      }
    }

    el("saveBtn").addEventListener("click", save);
    el("deleteBtn").addEventListener("click", removeCompany);
    el("viewToggle").addEventListener("click", () => {
      showRecent = !showRecent;
      refreshChart();
    });
    wireBackLink();
    load();
    setInterval(load, 2000);
  </script>
</body>
</html>
"""

COMMODITY_DETAIL_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{{ name }} · Commodity</title>
  <style>
    :root { --bg:#0f172a; --line:#1f2937; --text:#e5e7eb; --muted:#94a3b8; --btn:#334155; --btnH:#475569; }
    body { margin:0; background:var(--bg); color:var(--text); font-family:ui-sans-serif, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; }
    .wrap { max-width: 920px; margin: 20px auto; padding: 0 14px; }
    .card { background: linear-gradient(180deg,#0b1220,#0a101c); border: 1px solid var(--line); border-radius: 12px; padding: 12px; }
    .top { display:flex; justify-content:space-between; align-items:center; margin-bottom:10px; }
    .title { font-size:1.2rem; font-weight:700; }
    .row { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:8px; }
    .row.one { grid-template-columns:1fr; }
    label { display:block; font-size:.82rem; color:var(--muted); margin-bottom:4px; }
    input, textarea { width:100%; padding:8px; border-radius:8px; border:1px solid #334155; background:#0b1323; color:var(--text); }
    textarea { min-height: 110px; resize: vertical; }
    button { padding:9px 12px; border:1px solid #334155; border-radius:8px; background:var(--btn); color:var(--text); cursor:pointer; }
    button:hover { background: var(--btnH); }
    .muted { color:var(--muted); font-size:.85rem; }
    .preview { width: 300px; height: 300px; max-width: 100%; border-radius: 10px; object-fit: cover; border: 1px solid #1f2937; background: #0b1323; display: block; }
    a { color:#7dd3fc; text-decoration:none; font-size:.9rem; }
    @media (max-width: 760px) {
      .row { grid-template-columns: 1fr; }
      .top { flex-direction: column; align-items: flex-start; gap: 6px; }
      .preview { width: 100%; height: auto; aspect-ratio: 1 / 1; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="top">
        <div class="title">Commodity · {{ name }}</div>
        <a id="backLink" href="/">Back to Dashboard</a>
      </div>
      <div class="row">
        <div><label>Name</label><input id="name" /></div>
        <div><label>Price</label><input id="price" type="number" step="0.01" /></div>
      </div>
      <div class="row">
        <div><label>Rarity</label><input id="rarity" /></div>
        <div><label>Image URL</label><input id="image_url" /></div>
      </div>
      <div class="row one">
        <div><label>Tags (comma-separated)</label><input id="tags" /></div>
      </div>
      <div class="row">
        <div><label>Thumbnail Preview</label><img id="preview" class="preview" alt="Commodity thumbnail" /></div>
        <div></div>
      </div>
      <div class="row one">
        <div><label>Description</label><textarea id="description"></textarea></div>
      </div>
      <button id="saveBtn">Save</button>
      <div id="msg" class="muted" style="margin-top:8px;"></div>
    </div>
  </div>
  <script>
    const COMMODITY = {{ name|tojson }};
    const URL_AUTH_TOKEN = new URLSearchParams(window.location.search).get("token");
    const FALLBACK = "data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='300' height='300'><rect width='100%' height='100%' fill='%230b1323'/><text x='50%' y='50%' fill='%2394a3b8' font-size='22' text-anchor='middle' dominant-baseline='middle'>No Img</text></svg>";
    function el(id){ return document.getElementById(id); }
    function withAuthToken(url) {
      if (!URL_AUTH_TOKEN) return url;
      const sep = url.includes("?") ? "&" : "?";
      return `${url}${sep}token=${encodeURIComponent(URL_AUTH_TOKEN)}`;
    }
    function wireBackLink() {
      const back = el("backLink");
      if (back) back.href = withAuthToken("/");
    }
    function syncPreview() {
      const img = el("preview");
      img.onerror = () => { img.onerror = null; img.src = FALLBACK; };
      img.src = el("image_url").value || FALLBACK;
    }
    async function load() {
      const res = await fetch(withAuthToken(`/api/commodity/${encodeURIComponent(COMMODITY)}`), { cache: "no-store" });
      if (!res.ok) return;
      const data = await res.json();
      const c = data.commodity;
      el("name").value = c.name || "";
      el("price").value = Number(c.price).toFixed(2);
      el("rarity").value = c.rarity || "common";
      el("image_url").value = c.image_url || "";
      el("tags").value = String((c.tags || []).join(", "));
      el("description").value = c.description || "";
      syncPreview();
    }
    async function save() {
      const payload = {
        name: el("name").value,
        price: Number(el("price").value),
        rarity: el("rarity").value,
        image_url: el("image_url").value,
        tags: el("tags").value,
        description: el("description").value,
      };
      const res = await fetch(withAuthToken(`/api/commodity/${encodeURIComponent(COMMODITY)}/update`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (res.ok) {
        el("msg").textContent = "Saved.";
      } else {
        const err = await res.json().catch(()=>({error:"Failed"}));
        el("msg").textContent = err.error || "Save failed.";
      }
      await load();
    }
    el("saveBtn").addEventListener("click", save);
    el("image_url").addEventListener("input", syncPreview);
    wireBackLink();
    load();
  </script>
</body>
</html>
"""

PLAYER_DETAIL_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Player {{ user_id }}</title>
  <style>
    :root { --bg:#0f172a; --line:#1f2937; --text:#e5e7eb; --muted:#94a3b8; --btn:#334155; --btnH:#475569; }
    body { margin:0; background:var(--bg); color:var(--text); font-family:ui-sans-serif, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; }
    .wrap { max-width: 920px; margin: 20px auto; padding: 0 14px; }
    .card { background: linear-gradient(180deg,#0b1220,#0a101c); border: 1px solid var(--line); border-radius: 12px; padding: 12px; }
    .top { display:flex; justify-content:space-between; align-items:center; margin-bottom:10px; }
    .title { font-size:1.2rem; font-weight:700; }
    .row { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:8px; }
    label { display:block; font-size:.82rem; color:var(--muted); margin-bottom:4px; }
    input { width:100%; padding:8px; border-radius:8px; border:1px solid #334155; background:#0b1323; color:var(--text); }
    button { padding:9px 12px; border:1px solid #334155; border-radius:8px; background:var(--btn); color:var(--text); cursor:pointer; }
    button:hover { background: var(--btnH); }
    .muted { color:var(--muted); font-size:.85rem; }
    a { color:#7dd3fc; text-decoration:none; font-size:.9rem; }
    @media (max-width: 760px) {
      .row { grid-template-columns: 1fr; }
      .top { flex-direction: column; align-items: flex-start; gap: 6px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="top">
        <div class="title">Player · {{ user_id }}</div>
        <a id="backLink" href="/">Back to Dashboard</a>
      </div>
      <div class="row">
        <div><label>Display Name (read-only)</label><input id="display_name" readonly /></div>
        <div><label>Joined At (read-only)</label><input id="joined_at" readonly /></div>
      </div>
      <div class="row">
        <div><label>Bank</label><input id="bank" type="number" step="0.01" /></div>
        <div><label>Networth</label><input id="networth" type="number" step="0.01" /></div>
      </div>
      <div class="row">
        <div><label>Owe</label><input id="owe" type="number" step="0.01" /></div>
        <div><label>Rank</label><input id="rank" /></div>
      </div>
      <div class="row">
        <div><label>Current Trade Limit (read-only)</label><input id="trade_limit_status" readonly /></div>
        <div><label>Trade Usage Action</label><button id="resetTradeBtn" type="button" style="background:#7f1d1d;border-color:#ef4444;color:#fee2e2;">Reset Trade Usage</button></div>
      </div>
      <button id="saveBtn">Save</button>
      <div id="msg" class="muted" style="margin-top:8px;"></div>
    </div>
  </div>
  <script>
    const USER_ID = {{ user_id|tojson }};
    function el(id){ return document.getElementById(id); }
    const URL_AUTH_TOKEN = new URLSearchParams(window.location.search).get("token");
    function withAuthToken(url) {
      if (!URL_AUTH_TOKEN) return url;
      const sep = url.includes("?") ? "&" : "?";
      return `${url}${sep}token=${encodeURIComponent(URL_AUTH_TOKEN)}`;
    }
    function wireBackLink() {
      const back = el("backLink");
      if (back) back.href = withAuthToken("/");
    }
    async function load() {
      const res = await fetch(withAuthToken(`/api/player/${encodeURIComponent(USER_ID)}`), { cache: "no-store" });
      if (!res.ok) return;
      const data = await res.json();
      const p = data.player;
      el("display_name").value = p.display_name || "";
      el("joined_at").value = p.joined_at || "";
      el("bank").value = Number(p.bank).toFixed(2);
      el("networth").value = Number(p.networth).toFixed(2);
      el("owe").value = Number(p.owe || 0).toFixed(2);
      el("rank").value = p.rank || "";
      if (p.trade_limit_enabled) {
        el("trade_limit_status").value =
          `${Number(p.trade_limit_remaining || 0)}/${Number(p.trade_limit_limit || 0)} shares remaining `
          + `(${Number(p.trade_limit_used || 0)} used, ${Number(p.trade_limit_window_minutes || 0).toFixed(2)} min window)`;
      } else {
        el("trade_limit_status").value = "Disabled";
      }
    }
    async function save() {
      const payload = {
        bank: Number(el("bank").value),
        networth: Number(el("networth").value),
        owe: Number(el("owe").value),
        rank: el("rank").value,
      };
      const res = await fetch(withAuthToken(`/api/player/${encodeURIComponent(USER_ID)}/update`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (res.ok) {
        el("msg").textContent = "Saved.";
      } else {
        const err = await res.json().catch(()=>({error:"Failed"}));
        el("msg").textContent = err.error || "Save failed.";
      }
      await load();
    }
    async function resetTradeUsage() {
      const btn = el("resetTradeBtn");
      const original = btn.textContent;
      btn.disabled = true;
      btn.textContent = "Resetting...";
      try {
        const res = await fetch(withAuthToken(`/api/player/${encodeURIComponent(USER_ID)}/reset-trade-usage`), {
          method: "POST",
        });
        if (res.ok) {
          el("msg").textContent = "Trade usage reset.";
        } else {
          const err = await res.json().catch(()=>({error:"Failed"}));
          el("msg").textContent = err.error || "Reset failed.";
        }
      } finally {
        btn.disabled = false;
        btn.textContent = original;
      }
      await load();
    }
    el("saveBtn").addEventListener("click", save);
    el("resetTradeBtn").addEventListener("click", resetTradeUsage);
    wireBackLink();
    load();
  </script>
</body>
</html>
"""

APP_CONFIG_DETAIL_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{{ config_name }} · App Config</title>
  <style>
    :root { --bg:#0f172a; --line:#1f2937; --text:#e5e7eb; --muted:#94a3b8; --btn:#334155; --btnH:#475569; }
    body { margin:0; background:var(--bg); color:var(--text); font-family:ui-sans-serif, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; }
    .wrap { max-width: 920px; margin: 20px auto; padding: 0 14px; }
    .card { background: linear-gradient(180deg,#0b1220,#0a101c); border: 1px solid var(--line); border-radius: 12px; padding: 12px; }
    .top { display:flex; justify-content:space-between; align-items:center; margin-bottom:10px; }
    .title { font-size:1.2rem; font-weight:700; }
    label { display:block; font-size:.82rem; color:var(--muted); margin-bottom:4px; }
    input, textarea { width:100%; padding:8px; border-radius:8px; border:1px solid #334155; background:#0b1323; color:var(--text); }
    textarea { min-height: 80px; resize: vertical; }
    button { padding:9px 12px; border:1px solid #334155; border-radius:8px; background:var(--btn); color:var(--text); cursor:pointer; margin-top: 10px; }
    button:hover { background: var(--btnH); }
    .muted { color:var(--muted); font-size:.85rem; margin-top:8px; }
    a { color:#7dd3fc; text-decoration:none; font-size:.9rem; }
    @media (max-width: 760px) {
      .top { flex-direction: column; align-items: flex-start; gap: 6px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="top">
        <div class="title">App Config · {{ config_name }}</div>
        <a id="backLink" href="/">Back to Dashboard</a>
      </div>
      <label>Name (read-only)</label>
      <input id="name" readonly />
      <label style="margin-top:8px;">Type (read-only)</label>
      <input id="type" readonly />
      <label style="margin-top:8px;">Default (read-only)</label>
      <input id="default" readonly />
      <label style="margin-top:8px;">Description (read-only)</label>
      <textarea id="description" readonly></textarea>
      <label style="margin-top:8px;">Value</label>
      <input id="value" />
      <button id="saveBtn">Save</button>
      <div id="msg" class="muted"></div>
    </div>
  </div>
  <script>
    const CONFIG_NAME = {{ config_name|tojson }};
    const URL_AUTH_TOKEN = new URLSearchParams(window.location.search).get("token");
    function el(id){ return document.getElementById(id); }
    function withAuthToken(url) {
      if (!URL_AUTH_TOKEN) return url;
      const sep = url.includes("?") ? "&" : "?";
      return `${url}${sep}token=${encodeURIComponent(URL_AUTH_TOKEN)}`;
    }
    function wireBackLink() {
      const back = el("backLink");
      if (back) back.href = withAuthToken("/");
    }
    async function load() {
      const res = await fetch(withAuthToken(`/api/app-config/${encodeURIComponent(CONFIG_NAME)}`), { cache: "no-store" });
      if (!res.ok) return;
      const data = await res.json();
      const c = data.config;
      el("name").value = c.name;
      el("type").value = c.type || "";
      el("default").value = String(c.default);
      el("description").value = c.description || "";
      el("value").value = String(c.value);
    }
    async function save() {
      const res = await fetch(withAuthToken(`/api/app-config/${encodeURIComponent(CONFIG_NAME)}/update`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: el("value").value }),
      });
      if (res.ok) {
        el("msg").textContent = "Saved.";
      } else {
        const err = await res.json().catch(()=>({error:"Failed"}));
        el("msg").textContent = err.error || "Save failed.";
      }
      await load();
    }
    el("saveBtn").addEventListener("click", save);
    wireBackLink();
    load();
  </script>
</body>
</html>
"""

PERK_DETAIL_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Perk {{ perk_id }} · 716Stonks</title>
  <style>
    :root { --bg:#0f172a; --line:#1f2937; --text:#e5e7eb; --muted:#94a3b8; --btn:#334155; --btnH:#475569; }
    body { margin:0; background:var(--bg); color:var(--text); font-family:ui-sans-serif, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; }
    .wrap { max-width: 1100px; margin: 20px auto; padding: 0 14px; display:grid; grid-template-columns: 1fr; gap: 12px; }
    .card { background: linear-gradient(180deg,#0b1220,#0a101c); border: 1px solid var(--line); border-radius: 12px; padding: 12px; }
    .top { display:flex; justify-content:space-between; align-items:center; margin-bottom:10px; }
    .title { font-size:1.2rem; font-weight:700; }
    .row { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:8px; }
    .row.one { grid-template-columns:1fr; }
    label { display:block; font-size:.82rem; color:var(--muted); margin-bottom:4px; }
    input, textarea, select { width:100%; padding:8px; border-radius:8px; border:1px solid #334155; background:#0b1323; color:var(--text); }
    textarea { min-height: 86px; resize: vertical; }
    button { padding:8px 11px; border:1px solid #334155; border-radius:8px; background:var(--btn); color:var(--text); cursor:pointer; }
    button:hover { background: var(--btnH); }
    table { width:100%; border-collapse: collapse; }
    th, td { border-bottom: 1px solid #1f2937; padding: 7px 6px; text-align: left; }
    th { color: var(--muted); font-size:.84rem; }
    .muted { color:var(--muted); font-size:.85rem; }
    a { color:#7dd3fc; text-decoration:none; font-size:.9rem; }
    @media (max-width: 760px) {
      .row { grid-template-columns: 1fr; }
      .top { flex-direction: column; align-items: flex-start; gap: 6px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="top">
        <div class="title">Perk Editor · #{{ perk_id }}</div>
        <a id="backLink" href="/">Back to Dashboard</a>
      </div>
      <div class="row">
        <div><label>Name</label><input id="name" /></div>
        <div><label>Priority</label><input id="priority" type="number" step="1" /></div>
      </div>
      <div class="row">
        <div><label>Enabled (1/0)</label><input id="enabled" type="number" step="1" min="0" max="1" /></div>
        <div><label>Stack Mode</label><select id="stack_mode"><option value="add">add</option><option value="override">override</option><option value="max_only">max_only</option></select></div>
      </div>
      <div class="row">
        <div><label>Max Stacks</label><input id="max_stacks" type="number" step="1" min="1" /></div>
        <div></div>
      </div>
      <div class="row one">
        <div><label>Description</label><textarea id="description"></textarea></div>
      </div>
      <button id="savePerkBtn">Save Perk</button>
      <button id="deletePerkBtn" style="margin-left:8px;background:#7f1d1d;border-color:#ef4444;color:#fee2e2;">Delete Perk</button>
      <div id="msg" class="muted" style="margin-top:8px;"></div>
    </div>

    <div class="card">
      <div class="top"><div class="title">Requirements</div></div>
      <table>
        <thead><tr><th>ID</th><th>Group</th><th>Type</th><th>Commodity</th><th>Op</th><th>Value</th><th>Action</th></tr></thead>
        <tbody id="reqRows"><tr><td colspan="7" class="muted">Loading...</td></tr></tbody>
      </table>
      <div class="row" style="margin-top:10px;">
        <div><label>Group</label><input id="new_req_group" type="number" step="1" value="1" /></div>
        <div><label>Type</label><select id="new_req_type"><option value="commodity_qty">commodity_qty</option><option value="tag_qty">tag_qty</option></select></div>
      </div>
      <div class="row">
        <div><label>Commodity Name</label><select id="new_req_commodity"></select></div>
        <div><label>Operator</label><select id="new_req_operator"><option>>=</option><option>></option><option><=</option><option><</option><option>==</option><option>!=</option></select></div>
      </div>
      <div class="row">
        <div><label>Value (quantity)</label><input id="new_req_value" type="number" step="1" min="0" value="1" /></div>
        <div><button id="addReqBtn" style="margin-top:25px;">+ Add Requirement</button></div>
      </div>
    </div>

    <div class="card">
      <div class="top"><div class="title">Effects</div></div>
      <table>
        <thead><tr><th>ID</th><th>Target</th><th>Mode</th><th>Value</th><th>Scale Src</th><th>Scale Key</th><th>Scale Factor</th><th>Cap</th><th>Action</th></tr></thead>
        <tbody id="effectRows"><tr><td colspan="9" class="muted">Loading...</td></tr></tbody>
      </table>
      <div class="row" style="margin-top:10px;">
        <div><label>Target Stat</label><select id="new_eff_target"><option value="income">income</option><option value="trade_limits">trade_limits</option><option value="networth">networth</option></select></div>
        <div><label>Value Mode</label><select id="new_eff_mode"><option value="flat">flat</option><option value="multiplier">multiplier</option><option value="per_item">per_item</option></select></div>
      </div>
      <div class="row">
        <div><label>Value</label><input id="new_eff_value" type="number" step="0.01" value="0" /></div>
        <div><label>Scale Source</label><select id="new_eff_source"><option value="none">none</option><option value="commodity_qty">commodity_qty</option></select></div>
      </div>
      <div class="row">
        <div><label>Scale Key (commodity name)</label><select id="new_eff_key"></select></div>
        <div><label>Scale Factor</label><input id="new_eff_factor" type="number" step="0.01" value="0" /></div>
      </div>
      <div class="row">
        <div><label>Cap (0=none)</label><input id="new_eff_cap" type="number" step="0.01" value="0" /></div>
        <div><button id="addEffBtn" style="margin-top:25px;">+ Add Effect</button></div>
      </div>
    </div>
  </div>

  <script>
    const PERK_ID = Number({{ perk_id|tojson }});
    const URL_AUTH_TOKEN = new URLSearchParams(window.location.search).get("token");
    let commodityNames = [];
    let commodityTags = [];
    function el(id){ return document.getElementById(id); }
    function esc(text) {
      return String(text).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }
    function withAuthToken(url) {
      if (!URL_AUTH_TOKEN) return url;
      const sep = url.includes("?") ? "&" : "?";
      return `${url}${sep}token=${encodeURIComponent(URL_AUTH_TOKEN)}`;
    }
    function wireBackLink() {
      const back = el("backLink");
      if (back) back.href = withAuthToken("/");
    }
    function commoditySelectOptions(selectedName) {
      const selected = String(selectedName || "");
      const names = [...commodityNames];
      if (selected && !names.some((n) => n.toLowerCase() === selected.toLowerCase())) {
        names.push(selected);
      }
      names.sort((a, b) => a.localeCompare(b));
      if (!names.length) return '<option value="">(No commodities)</option>';
      return names.map((name) => {
        const isSel = String(name) === selected ? " selected" : "";
        return `<option value="${esc(name)}"${isSel}>${esc(name)}</option>`;
      }).join("");
    }
    function scaleKeySelectOptions(selectedName) {
      return commoditySelectOptions(selectedName);
    }
    function requirementKeyOptions(reqType, selectedName) {
      const selected = String(selectedName || "");
      const fromType = String(reqType || "commodity_qty");
      const values = fromType === "tag_qty" ? [...commodityTags] : [...commodityNames];
      if (selected && !values.some((n) => n.toLowerCase() === selected.toLowerCase())) {
        values.push(selected);
      }
      values.sort((a, b) => a.localeCompare(b));
      if (!values.length) return '<option value="">(None)</option>';
      return values.map((v) => `<option value="${esc(v)}"${String(v) === selected ? " selected" : ""}>${esc(v)}</option>`).join("");
    }
    async function loadCommodityNames() {
      const res = await fetch(withAuthToken("/api/commodities"), { cache: "no-store" });
      if (!res.ok) return;
      const data = await res.json();
      const rows = Array.isArray(data.commodities) ? data.commodities : [];
      commodityNames = rows
        .map((r) => String(r.name || "").trim())
        .filter((n) => n.length > 0);
      commodityTags = Array.from(new Set(
        rows
          .flatMap((r) => Array.isArray(r.tags) ? r.tags : [])
          .map((t) => String(t || "").trim())
          .filter((t) => t.length > 0)
      ));
      const select = el("new_req_commodity");
      if (select) {
        const current = String(select.value || "");
        const reqType = String(el("new_req_type")?.value || "commodity_qty");
        select.innerHTML = requirementKeyOptions(reqType, current);
      }
      const effSelect = el("new_eff_key");
      if (effSelect) {
        const current = String(effSelect.value || "");
        effSelect.innerHTML = scaleKeySelectOptions(current);
      }
    }
    async function load() {
      await loadCommodityNames();
      const res = await fetch(withAuthToken(`/api/perk/${encodeURIComponent(PERK_ID)}`), { cache: "no-store" });
      if (!res.ok) {
        el("msg").textContent = "Perk not found.";
        return;
      }
      const data = await res.json();
      const p = data.perk || {};
      el("name").value = p.name || "";
      el("description").value = p.description || "";
      el("enabled").value = Number(p.enabled || 0);
      el("priority").value = Number(p.priority || 100);
      el("stack_mode").value = p.stack_mode || "add";
      el("max_stacks").value = Number(p.max_stacks || 1);
      renderRequirements(Array.isArray(data.requirements) ? data.requirements : []);
      renderEffects(Array.isArray(data.effects) ? data.effects : []);
      const newSelect = el("new_req_commodity");
      if (newSelect && !newSelect.value && commodityNames.length) {
        newSelect.value = commodityNames[0];
      }
      const newEffSelect = el("new_eff_key");
      if (newEffSelect && !newEffSelect.value && commodityNames.length) {
        newEffSelect.value = commodityNames[0];
      }
    }
    function renderRequirements(rows) {
      const tbody = el("reqRows");
      if (!rows.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="muted">No requirements yet.</td></tr>';
        return;
      }
      tbody.innerHTML = rows.map((r) => `<tr data-req-id="${esc(String(r.id))}">
        <td class="mono">${esc(String(r.id))}</td>
        <td><input data-req-group="${esc(String(r.id))}" type="number" step="1" value="${esc(String(r.group_id || 1))}" /></td>
        <td><select data-req-type="${esc(String(r.id))}"><option value="commodity_qty"${String(r.req_type || "commodity_qty") === "commodity_qty" ? " selected" : ""}>commodity_qty</option><option value="tag_qty"${String(r.req_type || "") === "tag_qty" ? " selected" : ""}>tag_qty</option></select></td>
        <td><select data-req-commodity="${esc(String(r.id))}">${requirementKeyOptions(String(r.req_type || "commodity_qty"), String(r.commodity_name || ""))}</select></td>
        <td><input data-req-operator="${esc(String(r.id))}" value="${esc(String(r.operator || ">="))}" /></td>
        <td><input data-req-value="${esc(String(r.id))}" type="number" step="1" min="0" value="${esc(String(Number(r.value || 1).toFixed(0)))}" /></td>
        <td><button data-req-save="${esc(String(r.id))}">Save</button> <button data-req-del="${esc(String(r.id))}" style="background:#7f1d1d;border-color:#ef4444;color:#fee2e2;">Delete</button></td>
      </tr>`).join("");
      document.querySelectorAll("select[data-req-type]").forEach((sel) => {
        sel.addEventListener("change", () => {
          const reqId = sel.getAttribute("data-req-type");
          if (!reqId) return;
          const keySel = document.querySelector(`select[data-req-commodity="${CSS.escape(reqId)}"]`);
          if (!keySel) return;
          const current = String(keySel.value || "");
          keySel.innerHTML = requirementKeyOptions(String(sel.value || "commodity_qty"), current);
        });
      });
      document.querySelectorAll("button[data-req-save]").forEach((btn) => {
        btn.addEventListener("click", () => updateRequirement(btn.getAttribute("data-req-save")));
      });
      document.querySelectorAll("button[data-req-del]").forEach((btn) => {
        btn.addEventListener("click", () => deleteRequirement(btn.getAttribute("data-req-del")));
      });
    }
    function renderEffects(rows) {
      const tbody = el("effectRows");
      if (!rows.length) {
        tbody.innerHTML = '<tr><td colspan="9" class="muted">No effects yet.</td></tr>';
        return;
      }
      tbody.innerHTML = rows.map((r) => `<tr data-eff-id="${esc(String(r.id))}">
        <td class="mono">${esc(String(r.id))}</td>
        <td><input data-eff-target="${esc(String(r.id))}" value="${esc(String(r.target_stat || "income"))}" /></td>
        <td><input data-eff-mode="${esc(String(r.id))}" value="${esc(String(r.value_mode || "flat"))}" /></td>
        <td><input data-eff-value="${esc(String(r.id))}" type="number" step="0.01" value="${esc(String(r.value || 0))}" /></td>
        <td><input data-eff-source="${esc(String(r.id))}" value="${esc(String(r.scale_source || "none"))}" /></td>
        <td><select data-eff-key="${esc(String(r.id))}">${scaleKeySelectOptions(String(r.scale_key || ""))}</select></td>
        <td><input data-eff-factor="${esc(String(r.id))}" type="number" step="0.01" value="${esc(String(r.scale_factor || 0))}" /></td>
        <td><input data-eff-cap="${esc(String(r.id))}" type="number" step="0.01" value="${esc(String(r.cap || 0))}" /></td>
        <td><button data-eff-save="${esc(String(r.id))}">Save</button> <button data-eff-del="${esc(String(r.id))}" style="background:#7f1d1d;border-color:#ef4444;color:#fee2e2;">Delete</button></td>
      </tr>`).join("");
      document.querySelectorAll("button[data-eff-save]").forEach((btn) => {
        btn.addEventListener("click", () => updateEffect(btn.getAttribute("data-eff-save")));
      });
      document.querySelectorAll("button[data-eff-del]").forEach((btn) => {
        btn.addEventListener("click", () => deleteEffect(btn.getAttribute("data-eff-del")));
      });
    }
    async function savePerk() {
      const payload = {
        name: el("name").value.trim(),
        description: el("description").value,
        enabled: Number(el("enabled").value),
        priority: Number(el("priority").value),
        stack_mode: el("stack_mode").value,
        max_stacks: Number(el("max_stacks").value),
      };
      const res = await fetch(withAuthToken(`/api/perk/${encodeURIComponent(PERK_ID)}/update`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Save failed" }));
        el("msg").textContent = err.error || "Save failed.";
        return;
      }
      el("msg").textContent = "Perk saved.";
      await load();
    }
    async function deletePerk() {
      if (!window.confirm("Delete this perk?")) return;
      const res = await fetch(withAuthToken(`/api/perk/${encodeURIComponent(PERK_ID)}`), { method: "DELETE" });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Delete failed" }));
        el("msg").textContent = err.error || "Delete failed.";
        return;
      }
      window.location.href = withAuthToken("/");
    }
    async function addRequirement() {
      const payload = {
        group_id: Number(el("new_req_group").value),
        req_type: el("new_req_type").value,
        commodity_name: el("new_req_commodity").value.trim(),
        operator: el("new_req_operator").value,
        value: Number(el("new_req_value").value),
      };
      const res = await fetch(withAuthToken(`/api/perk/${encodeURIComponent(PERK_ID)}/requirements`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Add failed" }));
        el("msg").textContent = err.error || "Add requirement failed.";
        return;
      }
      await load();
    }
    async function updateRequirement(reqId) {
      if (!reqId) return;
      const payload = {
        group_id: Number(document.querySelector(`input[data-req-group="${CSS.escape(reqId)}"]`).value),
        req_type: document.querySelector(`input[data-req-type="${CSS.escape(reqId)}"]`).value,
        commodity_name: document.querySelector(`select[data-req-commodity="${CSS.escape(reqId)}"]`).value,
        operator: document.querySelector(`input[data-req-operator="${CSS.escape(reqId)}"]`).value,
        value: Number(document.querySelector(`input[data-req-value="${CSS.escape(reqId)}"]`).value),
      };
      const res = await fetch(withAuthToken(`/api/perk/${encodeURIComponent(PERK_ID)}/requirements/${encodeURIComponent(reqId)}/update`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Update failed" }));
        el("msg").textContent = err.error || "Update requirement failed.";
        return;
      }
      await load();
    }
    async function deleteRequirement(reqId) {
      if (!reqId || !window.confirm("Delete this requirement?")) return;
      const res = await fetch(withAuthToken(`/api/perk/${encodeURIComponent(PERK_ID)}/requirements/${encodeURIComponent(reqId)}`), { method: "DELETE" });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Delete failed" }));
        el("msg").textContent = err.error || "Delete requirement failed.";
        return;
      }
      await load();
    }
    async function addEffect() {
      const payload = {
        target_stat: el("new_eff_target").value,
        value_mode: el("new_eff_mode").value,
        value: Number(el("new_eff_value").value),
        scale_source: el("new_eff_source").value,
        scale_key: el("new_eff_key").value.trim(),
        scale_factor: Number(el("new_eff_factor").value),
        cap: Number(el("new_eff_cap").value),
      };
      const res = await fetch(withAuthToken(`/api/perk/${encodeURIComponent(PERK_ID)}/effects`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Add failed" }));
        el("msg").textContent = err.error || "Add effect failed.";
        return;
      }
      await load();
    }
    async function updateEffect(effectId) {
      if (!effectId) return;
      const payload = {
        target_stat: document.querySelector(`input[data-eff-target="${CSS.escape(effectId)}"]`).value,
        value_mode: document.querySelector(`input[data-eff-mode="${CSS.escape(effectId)}"]`).value,
        value: Number(document.querySelector(`input[data-eff-value="${CSS.escape(effectId)}"]`).value),
        scale_source: document.querySelector(`input[data-eff-source="${CSS.escape(effectId)}"]`).value,
        scale_key: document.querySelector(`select[data-eff-key="${CSS.escape(effectId)}"]`).value,
        scale_factor: Number(document.querySelector(`input[data-eff-factor="${CSS.escape(effectId)}"]`).value),
        cap: Number(document.querySelector(`input[data-eff-cap="${CSS.escape(effectId)}"]`).value),
      };
      const res = await fetch(withAuthToken(`/api/perk/${encodeURIComponent(PERK_ID)}/effects/${encodeURIComponent(effectId)}/update`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Update failed" }));
        el("msg").textContent = err.error || "Update effect failed.";
        return;
      }
      await load();
    }
    async function deleteEffect(effectId) {
      if (!effectId || !window.confirm("Delete this effect?")) return;
      const res = await fetch(withAuthToken(`/api/perk/${encodeURIComponent(PERK_ID)}/effects/${encodeURIComponent(effectId)}`), { method: "DELETE" });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Delete failed" }));
        el("msg").textContent = err.error || "Delete effect failed.";
        return;
      }
      await load();
    }
    el("savePerkBtn").addEventListener("click", savePerk);
    el("deletePerkBtn").addEventListener("click", deletePerk);
    el("addReqBtn").addEventListener("click", addRequirement);
    el("addEffBtn").addEventListener("click", addEffect);
    const newReqType = el("new_req_type");
    if (newReqType) {
      newReqType.addEventListener("change", () => {
        const sel = el("new_req_commodity");
        if (!sel) return;
        sel.innerHTML = requirementKeyOptions(String(newReqType.value || "commodity_qty"), String(sel.value || ""));
      });
    }
    wireBackLink();
    load();
  </script>
</body>
</html>
"""


def create_app() -> Flask:
    app = Flask(__name__)
    repo_root = Path(__file__).resolve().parents[1]
    db_path = Path(os.getenv("DASHBOARD_DB_PATH", str(repo_root / "data" / "stockbot.db")))
    auth_cookie = "webadmin_session"
    session_ttl_seconds = max(300, int(os.getenv("WEBADMIN_SESSION_TTL_SECONDS", "43200")))
    godtoken_env = (os.getenv("WEBADMIN_GODTOKEN") or os.getenv("GODTOKEN") or "GODTOKEN").strip()

    def _connect() -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _state_get(key: str) -> str | None:
        with _connect() as conn:
            row = conn.execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
            return None if row is None else str(row["value"])

    def _state_set(key: str, value: str) -> None:
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO app_state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def _state_delete(key: str) -> None:
        with _connect() as conn:
            conn.execute("DELETE FROM app_state WHERE key = ?", (key,))

    def _cleanup_expired_auth_entries() -> None:
        now_epoch = int(time.time())
        with _connect() as conn:
            conn.execute(
                """
                DELETE FROM app_state
                WHERE (key LIKE 'webadmin:token:%' OR key LIKE 'webadmin:session:%')
                  AND CAST(value AS INTEGER) < ?
                """,
                (now_epoch,),
            )

    def _consume_one_time_token(token: str) -> bool:
        key = f"webadmin:token:{token}"
        now_epoch = int(time.time())
        with _connect() as conn:
            row = conn.execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
            if row is None:
                return False
            try:
                expires_at = int(str(row["value"]))
            except ValueError:
                conn.execute("DELETE FROM app_state WHERE key = ?", (key,))
                return False
            if expires_at < now_epoch:
                conn.execute("DELETE FROM app_state WHERE key = ?", (key,))
                return False
            conn.execute("DELETE FROM app_state WHERE key = ?", (key,))
            conn.execute(
                """
                INSERT INTO app_state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (f"webadmin:grace:{token}", str(now_epoch + 120)),
            )
            return True

    def _token_in_grace(token: str) -> bool:
        raw = _state_get(f"webadmin:grace:{token}")
        if raw is None:
            return False
        try:
            return int(raw) >= int(time.time())
        except ValueError:
            return False

    def _create_session() -> str:
        sid = secrets.token_urlsafe(32)
        expires_at = int(time.time()) + session_ttl_seconds
        _state_set(f"webadmin:session:{sid}", str(expires_at))
        return sid

    def _session_valid(session_id: str | None) -> bool:
        if not session_id:
            return False
        raw = _state_get(f"webadmin:session:{session_id}")
        if raw is None:
            return False
        try:
            return int(raw) >= int(time.time())
        except ValueError:
            return False

    def _config_key(name: str) -> str:
        return f"config:{name}"

    def _normalize_config(name: str, value):
        if name in {"START_BALANCE", "DRIFT_NOISE_FREQUENCY", "DRIFT_NOISE_GAIN", "DRIFT_NOISE_LOW_FREQ_RATIO", "DRIFT_NOISE_LOW_GAIN", "TRADING_FEES", "TREND_MULTIPLIER", "PAWN_SELL_RATE"}:
            number = float(value)
            if name == "DRIFT_NOISE_FREQUENCY":
                return max(0.0, min(1.0, number))
            if name == "PAWN_SELL_RATE":
                return max(0.0, min(100.0, number))
            if name in {"START_BALANCE", "DRIFT_NOISE_GAIN", "DRIFT_NOISE_LOW_FREQ_RATIO", "DRIFT_NOISE_LOW_GAIN", "TRADING_FEES"}:
                return max(0.0, number)
            return number
        if name in {"TICK_INTERVAL", "MARKET_CLOSE_HOUR", "TRADING_LIMITS", "TRADING_LIMITS_PERIOD", "ANNOUNCEMENT_CHANNEL_ID", "COMMODITIES_LIMIT"}:
            if name == "ANNOUNCEMENT_CHANNEL_ID":
                text = str(value).strip()
                if text == "":
                    return 0
                if text.startswith("+"):
                    text = text[1:]
                if not text.isdigit():
                    raise ValueError("ANNOUNCEMENT_CHANNEL_ID must be a positive integer channel ID.")
                return max(0, int(text))

            number = int(float(value))
            if name == "TICK_INTERVAL":
                return max(1, number)
            if name == "MARKET_CLOSE_HOUR":
                return max(0, min(23, number))
            if name == "TRADING_LIMITS_PERIOD":
                return max(1, number)
            if name == "COMMODITIES_LIMIT":
                return max(0, number)
            return number
        text = str(value).strip()
        return text or str(APP_CONFIG_SPECS[name].default)

    def _ensure_config_defaults() -> None:
        with _connect() as conn:
            for name, spec in APP_CONFIG_SPECS.items():
                row = conn.execute(
                    "SELECT value FROM app_state WHERE key = ?",
                    (_config_key(name),),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO app_state (key, value)
                        VALUES (?, ?)
                        """,
                        (_config_key(name), str(_normalize_config(name, spec.default))),
                    )

    def _ensure_perk_tables() -> None:
        with _connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS perks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    priority INTEGER NOT NULL DEFAULT 100,
                    stack_mode TEXT NOT NULL DEFAULT 'add',
                    max_stacks INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS perk_requirements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    perk_id INTEGER NOT NULL,
                    group_id INTEGER NOT NULL DEFAULT 1,
                    req_type TEXT NOT NULL DEFAULT 'commodity_qty',
                    commodity_name TEXT NOT NULL DEFAULT '',
                    operator TEXT NOT NULL DEFAULT '>=',
                    value INTEGER NOT NULL DEFAULT 1,
                    FOREIGN KEY (perk_id)
                        REFERENCES perks (id)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS perk_effects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    perk_id INTEGER NOT NULL,
                    effect_type TEXT NOT NULL DEFAULT 'stat_mod',
                    target_stat TEXT NOT NULL DEFAULT 'income',
                    value_mode TEXT NOT NULL DEFAULT 'flat',
                    value REAL NOT NULL DEFAULT 0,
                    scale_source TEXT NOT NULL DEFAULT 'none',
                    scale_key TEXT NOT NULL DEFAULT '',
                    scale_factor REAL NOT NULL DEFAULT 0,
                    cap REAL NOT NULL DEFAULT 0,
                    FOREIGN KEY (perk_id)
                        REFERENCES perks (id)
                        ON DELETE CASCADE
                );
                """
            )
            perk_cols = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(perks);").fetchall()
            }
            if "income_multiplier" in perk_cols:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS perks_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        guild_id INTEGER NOT NULL,
                        name TEXT NOT NULL,
                        description TEXT NOT NULL DEFAULT '',
                        enabled INTEGER NOT NULL DEFAULT 1,
                        priority INTEGER NOT NULL DEFAULT 100,
                        stack_mode TEXT NOT NULL DEFAULT 'add',
                        max_stacks INTEGER NOT NULL DEFAULT 1
                    );
                    INSERT INTO perks_new (id, guild_id, name, description, enabled, priority, stack_mode, max_stacks)
                    SELECT id, guild_id, name, description, enabled, 100, 'add', 1
                    FROM perks;
                    DROP TABLE perks;
                    ALTER TABLE perks_new RENAME TO perks;
                    """
                )
            else:
                if "priority" not in perk_cols:
                    conn.execute("ALTER TABLE perks ADD COLUMN priority INTEGER NOT NULL DEFAULT 100;")
                if "stack_mode" not in perk_cols:
                    conn.execute("ALTER TABLE perks ADD COLUMN stack_mode TEXT NOT NULL DEFAULT 'add';")
                if "max_stacks" not in perk_cols:
                    conn.execute("ALTER TABLE perks ADD COLUMN max_stacks INTEGER NOT NULL DEFAULT 1;")

            req_cols = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(perk_requirements);").fetchall()
            }
            if req_cols and "required_qty" in req_cols:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS perk_requirements_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        perk_id INTEGER NOT NULL,
                        group_id INTEGER NOT NULL DEFAULT 1,
                        req_type TEXT NOT NULL DEFAULT 'commodity_qty',
                        commodity_name TEXT NOT NULL DEFAULT '',
                        operator TEXT NOT NULL DEFAULT '>=',
                        value INTEGER NOT NULL DEFAULT 1,
                        FOREIGN KEY (perk_id)
                            REFERENCES perks (id)
                            ON DELETE CASCADE
                    );
                    INSERT INTO perk_requirements_new (id, perk_id, group_id, req_type, commodity_name, operator, value)
                    SELECT id, perk_id, 1, 'commodity_qty', commodity_name, '>=', required_qty
                    FROM perk_requirements;
                    DROP TABLE perk_requirements;
                    ALTER TABLE perk_requirements_new RENAME TO perk_requirements;
                    """
                )

    def _ensure_commodity_tags_table() -> None:
        with _connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS commodity_tags (
                    guild_id INTEGER NOT NULL,
                    commodity_name TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    PRIMARY KEY (guild_id, commodity_name, tag),
                    FOREIGN KEY (guild_id, commodity_name)
                        REFERENCES commodities (guild_id, name)
                        ON DELETE CASCADE
                );
                """
            )

    def _parse_tags(raw: object) -> list[str]:
        text = str(raw or "")
        parts = [p.strip().lower() for p in text.split(",")]
        tags: list[str] = []
        for tag in parts:
            if not tag:
                continue
            if tag not in tags:
                tags.append(tag)
        return tags

    def _sanitize_stack_mode(raw: object) -> str:
        mode = str(raw or "add").strip().lower()
        if mode not in {"add", "override", "max_only"}:
            return "add"
        return mode

    def _sanitize_operator(raw: object) -> str:
        op = str(raw or ">=").strip()
        if op not in {">", ">=", "<", "<=", "==", "!="}:
            return ">="
        return op

    def _get_config(name: str):
        spec = APP_CONFIG_SPECS[name]
        with _connect() as conn:
            row = conn.execute(
                "SELECT value FROM app_state WHERE key = ?",
                (_config_key(name),),
            ).fetchone()
        raw = spec.default if row is None else row["value"]
        return _normalize_config(name, raw)

    def _set_config(name: str, value):
        normalized = _normalize_config(name, value)
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO app_state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (_config_key(name), str(normalized)),
            )
        return normalized

    def _config_value_for_json(name: str, value):
        # Discord snowflakes exceed JS safe integer range; keep them as strings in JSON.
        if name == "ANNOUNCEMENT_CHANNEL_ID":
            return str(value)
        return value

    def _create_database_backup_now(prefix: str = "manual") -> str:
        backup_dir = db_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        backup_path = backup_dir / f"{prefix}_{stamp}.db"
        with _connect() as source_conn, sqlite3.connect(backup_path) as backup_conn:
            source_conn.backup(backup_conn)
        return str(backup_path)

    def _backup_dir() -> Path:
        backup_dir = db_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        return backup_dir

    def _pick_default_guild_id(conn: sqlite3.Connection) -> int:
        for table in ("companies", "users", "commodities"):
            row = conn.execute(
                f"SELECT guild_id FROM {table} ORDER BY guild_id DESC LIMIT 1"
            ).fetchone()
            if row is not None:
                return int(row["guild_id"])
        return 0

    def _human_size(size: int) -> str:
        units = ["B", "KB", "MB", "GB", "TB"]
        value = float(size)
        for unit in units:
            if value < 1024.0 or unit == units[-1]:
                return f"{value:.1f}{unit}"
            value /= 1024.0
        return f"{size}B"

    def _until_close_text() -> str:
        close_hour = int(_get_config("MARKET_CLOSE_HOUR"))
        tz_name = str(_get_config("DISPLAY_TIMEZONE"))
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = timezone.utc

        now_local = datetime.now(timezone.utc).astimezone(tz)
        close_local = now_local.replace(hour=close_hour, minute=0, second=0, microsecond=0)
        if now_local >= close_local:
            close_local = close_local + timedelta(days=1)
        delta = close_local - now_local
        total_seconds = max(0, int(delta.total_seconds()))
        hours, rem = divmod(total_seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def _until_close_seconds() -> int:
        close_hour = int(_get_config("MARKET_CLOSE_HOUR"))
        tz_name = str(_get_config("DISPLAY_TIMEZONE"))
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = timezone.utc
        now_local = datetime.now(timezone.utc).astimezone(tz)
        close_local = now_local.replace(hour=close_hour, minute=0, second=0, microsecond=0)
        if now_local >= close_local:
            close_local = close_local + timedelta(days=1)
        return max(0, int((close_local - now_local).total_seconds()))

    def _until_reset_seconds() -> int:
        period = max(1, int(_get_config("TRADING_LIMITS_PERIOD")))
        tick_interval = max(1, int(_get_config("TICK_INTERVAL")))
        tick_raw = _state_get("last_tick")
        try:
            tick = max(0, int(tick_raw)) if tick_raw is not None else 0
        except ValueError:
            tick = 0
        ticks_remaining = period - (tick % period)
        if ticks_remaining <= 0:
            ticks_remaining = period
        return ticks_remaining * tick_interval

    _ensure_config_defaults()
    _ensure_perk_tables()
    _ensure_commodity_tags_table()

    @app.before_request
    def _auth_gate():
        _cleanup_expired_auth_entries()
        g._new_webadmin_sid = None

        token = (request.args.get("token") or "").strip()
        godtoken_state = (_state_get("webadmin:godtoken") or "").strip()
        godtoken_ok = bool(token) and (
            token == godtoken_env or (godtoken_state and token == godtoken_state)
        )
        if token:
            if godtoken_ok or _token_in_grace(token) or _consume_one_time_token(token):
                sid = _create_session()
                g._new_webadmin_sid = sid
                return None
            return render_template_string(AUTH_REQUIRED_HTML), 401

        # Strict mode: token is required on every request.
        return render_template_string(AUTH_REQUIRED_HTML), 401

    @app.after_request
    def _set_session_cookie(resp):
        sid = getattr(g, "_new_webadmin_sid", None)
        if sid:
            resp.set_cookie(
                auth_cookie,
                sid,
                max_age=session_ttl_seconds,
                httponly=True,
                samesite="Lax",
                secure=(request.scheme == "https"),
            )
        return resp

    def _history_for(
        conn: sqlite3.Connection,
        symbol: str,
        limit: int | None = 60,
    ) -> list[float]:
        if limit is None:
            rows = conn.execute(
                """
                SELECT price
                FROM price_history
                WHERE symbol = ? COLLATE NOCASE
                ORDER BY tick_index DESC
                """,
                (symbol,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT price
                FROM price_history
                WHERE symbol = ? COLLATE NOCASE
                ORDER BY tick_index DESC
                LIMIT ?
                """,
                (symbol, max(2, limit)),
            ).fetchall()
        return [float(r["price"]) for r in rows[::-1]]

    @app.get("/")
    def dashboard():
        db_access_url = os.getenv("DASHBOARD_DB_ACCESS_URL")
        if not db_access_url:
            host = request.host.split(":", 1)[0]
            db_access_url = f"http://{host}:8081"
        return render_template_string(MAIN_HTML, db_access_url=db_access_url)

    @app.get("/company/<symbol>")
    def company_page(symbol: str):
        return render_template_string(DETAIL_HTML, symbol=symbol.upper())

    @app.get("/commodity/<name>")
    def commodity_page(name: str):
        return render_template_string(COMMODITY_DETAIL_HTML, name=name)

    @app.get("/player/<int:user_id>")
    def player_page(user_id: int):
        return render_template_string(PLAYER_DETAIL_HTML, user_id=str(user_id))

    @app.get("/app-config/<config_name>")
    def app_config_page(config_name: str):
        return render_template_string(APP_CONFIG_DETAIL_HTML, config_name=config_name)

    @app.get("/perk/<int:perk_id>")
    def perk_page(perk_id: int):
        return render_template_string(PERK_DETAIL_HTML, perk_id=perk_id)

    @app.get("/api/stocks")
    def api_stocks():
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, name, current_price, base_price, slope, drift, liquidity, impact_power, updated_at
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
        stocks: list[dict] = []
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

    @app.get("/api/dashboard-stats")
    def api_dashboard_stats():
        with _connect() as conn:
            companies_row = conn.execute("SELECT COUNT(*) AS c FROM companies").fetchone()
            users_row = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
        company_count = int(companies_row["c"]) if companies_row is not None else 0
        user_count = int(users_row["c"]) if users_row is not None else 0
        return jsonify(
            {
                "until_close": _until_close_text(),
                "seconds_until_close": _until_close_seconds(),
                "until_reset": f"{_until_reset_seconds() / 60.0:.2f} min",
                "seconds_until_reset": _until_reset_seconds(),
                "company_count": company_count,
                "user_count": user_count,
            }
        )

    @app.get("/api/company/<symbol>")
    def api_company(symbol: str):
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT symbol, name, location, industry, founded_year, description, evaluation,
                       current_price, base_price, slope, drift, liquidity, impact_power,
                       pending_buy, pending_sell, starting_tick, last_tick, updated_at
                FROM companies
                WHERE symbol = ? COLLATE NOCASE
                """,
                (symbol,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Company not found"}), 404
            history = _history_for(conn, symbol, limit=None)
        return jsonify(
            {
                "server_time_utc": datetime.now(timezone.utc).isoformat(),
                "company": dict(row),
                "history_prices": history,
            }
        )

    @app.get("/api/commodities")
    def api_commodities():
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT name, price, rarity, image_url, description
                FROM commodities
                ORDER BY name
                """
            ).fetchall()
            tag_rows = conn.execute(
                """
                SELECT commodity_name, tag
                FROM commodity_tags
                ORDER BY commodity_name, tag
                """
            ).fetchall()
        tag_map: dict[str, list[str]] = {}
        for row in tag_rows:
            cname = str(row["commodity_name"])
            tag_map.setdefault(cname.lower(), []).append(str(row["tag"]))
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["tags"] = tag_map.get(str(item.get("name", "")).lower(), [])
            out.append(item)
        return jsonify({"commodities": out})

    @app.get("/api/commodity/<name>")
    def api_commodity(name: str):
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT name, price, rarity, image_url, description
                FROM commodities
                WHERE name = ? COLLATE NOCASE
                """,
                (name,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Commodity not found"}), 404
            tag_rows = conn.execute(
                """
                SELECT tag
                FROM commodity_tags
                WHERE commodity_name = ? COLLATE NOCASE
                ORDER BY tag
                """,
                (name,),
            ).fetchall()
        item = dict(row)
        item["tags"] = [str(r["tag"]) for r in tag_rows]
        return jsonify({"commodity": item})

    @app.get("/api/players")
    def api_players():
        limit = int(_get_config("TRADING_LIMITS"))
        period = max(1, int(_get_config("TRADING_LIMITS_PERIOD")))
        tick_raw = _state_get("last_tick")
        try:
            tick = max(0, int(tick_raw)) if tick_raw is not None else 0
        except ValueError:
            tick = 0
        bucket = tick // period

        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT guild_id, user_id, display_name, rank, bank, networth, owe
                FROM users
                ORDER BY networth DESC, bank DESC, user_id ASC
                """
            ).fetchall()
        players: list[dict] = []
        for r in rows:
            row = dict(r)
            row["user_id"] = str(row["user_id"])
            guild_id = int(row.get("guild_id", 0))
            user_id = row["user_id"]
            if limit > 0:
                used_raw = _state_get(f"trade_used:{guild_id}:{user_id}:{bucket}")
                try:
                    used = int(float(used_raw)) if used_raw is not None else 0
                except ValueError:
                    used = 0
                row["trade_limit_enabled"] = True
                row["trade_limit_limit"] = limit
                row["trade_limit_remaining"] = max(0, limit - used)
            else:
                row["trade_limit_enabled"] = False
                row["trade_limit_limit"] = 0
                row["trade_limit_remaining"] = 0
            players.append(row)
        return jsonify({"players": players})

    @app.get("/api/perks")
    def api_perks():
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    p.id,
                    p.guild_id,
                    p.name,
                    p.description,
                    p.enabled,
                    p.priority,
                    p.stack_mode,
                    p.max_stacks,
                    (
                        SELECT COUNT(*)
                        FROM perk_requirements pr
                        WHERE pr.perk_id = p.id
                    ) AS requirements_count,
                    (
                        SELECT COUNT(*)
                        FROM perk_effects pe
                        WHERE pe.perk_id = p.id
                    ) AS effects_count
                FROM perks p
                ORDER BY p.priority ASC, p.id ASC
                """
            ).fetchall()
        return jsonify({"perks": [dict(r) for r in rows]})

    @app.get("/api/perk/<int:perk_id>")
    def api_perk(perk_id: int):
        with _connect() as conn:
            perk_row = conn.execute(
                """
                SELECT id, guild_id, name, description, enabled, priority, stack_mode, max_stacks
                FROM perks
                WHERE id = ?
                """,
                (perk_id,),
            ).fetchone()
            if perk_row is None:
                return jsonify({"error": "Perk not found"}), 404
            req_rows = conn.execute(
                """
                SELECT id, perk_id, group_id, req_type, commodity_name, operator, value
                FROM perk_requirements
                WHERE perk_id = ?
                ORDER BY group_id ASC, id ASC
                """,
                (perk_id,),
            ).fetchall()
            effect_rows = conn.execute(
                """
                SELECT id, perk_id, effect_type, target_stat, value_mode, value, scale_source, scale_key, scale_factor, cap
                FROM perk_effects
                WHERE perk_id = ?
                ORDER BY id ASC
                """,
                (perk_id,),
            ).fetchall()
        return jsonify(
            {
                "perk": dict(perk_row),
                "requirements": [dict(r) for r in req_rows],
                "effects": [dict(r) for r in effect_rows],
            }
        )

    @app.get("/api/action-history")
    def api_action_history():
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    ah.created_at,
                    ah.user_id,
                    COALESCE(u.display_name, '') AS display_name,
                    ah.action_type,
                    ah.target_type,
                    ah.target_symbol,
                    ah.quantity,
                    ah.unit_price,
                    ah.total_amount,
                    ah.details
                FROM action_history ah
                LEFT JOIN users u
                  ON u.guild_id = ah.guild_id
                 AND u.user_id = ah.user_id
                ORDER BY ah.id DESC
                LIMIT 400
                """
            ).fetchall()
        return jsonify({"actions": [dict(r) for r in rows]})

    @app.delete("/api/action-history")
    def api_action_history_delete():
        with _connect() as conn:
            cur = conn.execute("DELETE FROM action_history")
        return jsonify({"ok": True, "deleted": int(cur.rowcount)})

    @app.get("/api/feedback")
    def api_feedback():
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT id, guild_id, message, created_at
                FROM feedback
                ORDER BY id DESC
                LIMIT 500
                """
            ).fetchall()
        return jsonify({"feedback": [dict(r) for r in rows]})

    @app.delete("/api/feedback/<int:feedback_id>")
    def api_feedback_delete(feedback_id: int):
        with _connect() as conn:
            cur = conn.execute(
                """
                DELETE FROM feedback
                WHERE id = ?
                """,
                (feedback_id,),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Feedback not found"}), 404
        return jsonify({"ok": True, "deleted": feedback_id})

    @app.get("/api/player/<int:user_id>")
    def api_player(user_id: int):
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT guild_id, user_id, display_name, joined_at, rank, bank, networth, owe
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Player not found"}), 404
        player = dict(row)
        guild_id = int(player.get("guild_id", 0))
        limit = int(_get_config("TRADING_LIMITS"))
        period = int(_get_config("TRADING_LIMITS_PERIOD"))
        tick_interval = max(1, int(_get_config("TICK_INTERVAL")))
        tick_raw = _state_get("last_tick")
        try:
            tick = max(0, int(tick_raw)) if tick_raw is not None else 0
        except ValueError:
            tick = 0

        if limit > 0 and period > 0:
            bucket = tick // period
            used_raw = _state_get(f"trade_used:{guild_id}:{user_id}:{bucket}")
            try:
                used = int(float(used_raw)) if used_raw is not None else 0
            except ValueError:
                used = 0
            remaining = max(0, limit - used)
            player["trade_limit_enabled"] = True
            player["trade_limit_limit"] = limit
            player["trade_limit_used"] = used
            player["trade_limit_remaining"] = remaining
            player["trade_limit_window_minutes"] = (period * tick_interval) / 60.0
        else:
            player["trade_limit_enabled"] = False
            player["trade_limit_limit"] = limit
            player["trade_limit_used"] = 0
            player["trade_limit_remaining"] = 0
            player["trade_limit_window_minutes"] = 0.0

        return jsonify({"player": player})

    @app.get("/api/perk-preview")
    def api_perk_preview():
        user_raw = (request.args.get("user_id") or "").strip()
        if not user_raw:
            return jsonify({"error": "user_id is required"}), 400
        try:
            user_id = int(user_raw)
        except ValueError:
            return jsonify({"error": "Invalid user_id"}), 400

        with _connect() as conn:
            row = conn.execute(
                """
                SELECT guild_id, user_id, display_name, rank
                FROM users
                WHERE user_id = ?
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Player not found"}), 404
            guild_id = int(row["guild_id"])
            rank = str(row["rank"] or DEFAULT_RANK)
            display_name = str(row["display_name"] or f"User {user_id}")

        lookup = {k.lower(): float(v) for k, v in RANK_INCOME.items()}
        base_income = lookup.get(rank.lower(), float(RANK_INCOME.get(DEFAULT_RANK, 0.0)))
        base_trade_limits = int(_get_config("TRADING_LIMITS"))
        result = evaluate_user_perks(
            guild_id=guild_id,
            user_id=user_id,
            base_income=base_income,
            base_trade_limits=base_trade_limits,
            base_networth=None,
        )
        matched = list(result.get("matched_perks", []))
        return jsonify(
            {
                "user_id": str(user_id),
                "display_name": display_name,
                "rank": rank,
                "base_income": float(result["base"]["income"]),
                "final_income": float(result["final"]["income"]),
                "base_trade_limits": float(result["base"]["trade_limits"]),
                "final_trade_limits": float(result["final"]["trade_limits"]),
                "base_networth": float(result["base"]["networth"]),
                "final_networth": float(result["final"]["networth"]),
                "matched_perks": matched,
            }
        )

    @app.get("/api/app-configs")
    def api_app_configs():
        configs = []
        for name, spec in APP_CONFIG_SPECS.items():
            value = _get_config(name)
            configs.append(
                {
                    "name": name,
                    "value": _config_value_for_json(name, value),
                    "default": _normalize_config(name, spec.default),
                    "type": spec.cast.__name__,
                    "description": spec.description,
                }
            )
        return jsonify({"configs": configs})

    @app.get("/api/app-config/<config_name>")
    def api_app_config(config_name: str):
        if config_name not in APP_CONFIG_SPECS:
            return jsonify({"error": "Unknown app config"}), 404
        spec = APP_CONFIG_SPECS[config_name]
        value = _get_config(config_name)
        return jsonify(
            {
                "config": {
                    "name": config_name,
                    "value": _config_value_for_json(config_name, value),
                    "default": _normalize_config(config_name, spec.default),
                    "type": spec.cast.__name__,
                    "description": spec.description,
                }
            }
        )

    @app.post("/api/company/<symbol>/adjust")
    def api_company_adjust(symbol: str):
        data = request.get_json(silent=True) or {}
        field = str(data.get("field", "")).strip()
        if field not in {"base_price", "slope"}:
            return jsonify({"error": "Only base_price and slope are adjustable here."}), 400
        try:
            delta = float(data.get("delta", 0))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid delta"}), 400

        with _connect() as conn:
            row = conn.execute(
                f"SELECT {field} FROM companies WHERE symbol = ? COLLATE NOCASE",
                (symbol,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Company not found"}), 404
            current = float(row[field])
            next_value = current + delta
            if field == "base_price":
                next_value = max(0.01, next_value)
            conn.execute(
                f"UPDATE companies SET {field} = ?, updated_at = ? WHERE symbol = ? COLLATE NOCASE",
                (next_value, datetime.now(timezone.utc).isoformat(), symbol),
            )
        return jsonify({"ok": True, field: next_value})

    @app.post("/api/company")
    def api_company_create():
        data = request.get_json(silent=True) or {}
        symbol = str(data.get("symbol", "")).strip().upper()
        name = str(data.get("name", "")).strip()
        if not symbol or not name:
            return jsonify({"error": "symbol and name are required"}), 400
        try:
            base_price = max(0.01, float(data.get("base_price", 1.0)))
            slope = float(data.get("slope", 0.0))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid base_price/slope"}), 400

        now_iso = datetime.now(timezone.utc).isoformat()
        tick_raw = _state_get("last_tick")
        try:
            tick = max(0, int(tick_raw)) if tick_raw is not None else 0
        except ValueError:
            tick = 0

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            existing = conn.execute(
                """
                SELECT 1
                FROM companies
                WHERE guild_id = ? AND symbol = ? COLLATE NOCASE
                """,
                (guild_id, symbol),
            ).fetchone()
            if existing is not None:
                return jsonify({"error": f"Company `{symbol}` already exists"}), 409
            conn.execute(
                """
                INSERT INTO companies (
                    guild_id, symbol, name, location, industry, founded_year, description, evaluation,
                    base_price, slope, drift, liquidity, impact_power, pending_buy, pending_sell,
                    starting_tick, current_price, last_tick, updated_at
                )
                VALUES (?, ?, ?, '', '', 2000, '', '', ?, ?, 0.0, 100.0, 1.0, 0.0, 0.0, ?, ?, ?, ?)
                """,
                (guild_id, symbol, name, base_price, slope, tick, base_price, tick, now_iso),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO price_history (guild_id, symbol, tick_index, ts, price)
                VALUES (?, ?, ?, ?, ?)
                """,
                (guild_id, symbol, tick, now_iso, base_price),
            )
        return jsonify({"ok": True, "symbol": symbol})

    @app.post("/api/company/<symbol>/update")
    def api_company_update(symbol: str):
        data = request.get_json(silent=True) or {}
        allowed = {
            "name": str,
            "location": str,
            "industry": str,
            "founded_year": int,
            "description": str,
            "evaluation": str,
            "current_price": float,
            "base_price": float,
            "slope": float,
            "drift": float,
            "liquidity": float,
            "impact_power": float,
            "pending_buy": float,
            "pending_sell": float,
            "starting_tick": int,
            "last_tick": int,
        }
        updates: dict[str, object] = {}
        for key, caster in allowed.items():
            if key not in data:
                continue
            raw = data.get(key)
            try:
                value = caster(raw) if caster is not str else str(raw)
            except (TypeError, ValueError):
                return jsonify({"error": f"Invalid value for {key}"}), 400
            updates[key] = value

        if not updates:
            return jsonify({"error": "No valid fields provided"}), 400

        if "base_price" in updates:
            updates["base_price"] = max(0.01, float(updates["base_price"]))
        if "current_price" in updates:
            updates["current_price"] = max(0.01, float(updates["current_price"]))
        if "liquidity" in updates:
            updates["liquidity"] = max(1.0, float(updates["liquidity"]))
        if "impact_power" in updates:
            updates["impact_power"] = max(0.1, float(updates["impact_power"]))
        if "founded_year" in updates:
            updates["founded_year"] = max(0, int(updates["founded_year"]))
        if "starting_tick" in updates:
            updates["starting_tick"] = max(0, int(updates["starting_tick"]))
        if "last_tick" in updates:
            updates["last_tick"] = max(0, int(updates["last_tick"]))

        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())
        values.extend([datetime.now(timezone.utc).isoformat(), symbol])

        with _connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM companies WHERE symbol = ? COLLATE NOCASE",
                (symbol,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Company not found"}), 404
            conn.execute(
                f"UPDATE companies SET {set_clause}, updated_at = ? WHERE symbol = ? COLLATE NOCASE",
                values,
            )
        return jsonify({"ok": True})

    @app.delete("/api/company/<symbol>")
    def api_company_delete(symbol: str):
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT guild_id, symbol
                FROM companies
                WHERE symbol = ? COLLATE NOCASE
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Company not found"}), 404
            guild_id = int(row["guild_id"])
            real_symbol = str(row["symbol"])

            conn.execute(
                """
                DELETE FROM companies
                WHERE guild_id = ? AND symbol = ? COLLATE NOCASE
                """,
                (guild_id, real_symbol),
            )
            conn.execute(
                """
                DELETE FROM price_history
                WHERE guild_id = ? AND symbol = ? COLLATE NOCASE
                """,
                (guild_id, real_symbol),
            )
            conn.execute(
                """
                DELETE FROM daily_close
                WHERE guild_id = ? AND symbol = ? COLLATE NOCASE
                """,
                (guild_id, real_symbol),
            )
            conn.execute(
                """
                DELETE FROM holdings
                WHERE guild_id = ? AND symbol = ? COLLATE NOCASE
                """,
                (guild_id, real_symbol),
            )
        return jsonify({"ok": True, "deleted": real_symbol})

    @app.post("/api/commodity/<name>/update")
    def api_commodity_update(name: str):
        data = request.get_json(silent=True) or {}
        allowed = {
            "name": str,
            "price": float,
            "rarity": str,
            "image_url": str,
            "description": str,
        }
        updates: dict[str, object] = {}
        for key, caster in allowed.items():
            if key not in data:
                continue
            raw = data.get(key)
            try:
                value = caster(raw) if caster is not str else str(raw)
            except (TypeError, ValueError):
                return jsonify({"error": f"Invalid value for {key}"}), 400
            updates[key] = value
        tags_in_payload = "tags" in data
        if not updates and not tags_in_payload:
            return jsonify({"error": "No valid fields provided"}), 400
        if "price" in updates:
            updates["price"] = max(0.01, float(updates["price"]))
        tags = _parse_tags(data.get("tags", ""))

        with _connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM commodities WHERE name = ? COLLATE NOCASE",
                (name,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Commodity not found"}), 404
            guild_row = conn.execute(
                """
                SELECT guild_id, name
                FROM commodities
                WHERE name = ? COLLATE NOCASE
                """,
                (name,),
            ).fetchone()
            if guild_row is None:
                return jsonify({"error": "Commodity not found"}), 404
            guild_id = int(guild_row["guild_id"])
            old_name = str(guild_row["name"])
            next_name = str(updates.get("name", old_name))
            if updates:
                set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
                values = list(updates.values())
                values.append(name)
                conn.execute(
                    f"UPDATE commodities SET {set_clause} WHERE name = ? COLLATE NOCASE",
                    values,
                )
            conn.execute(
                """
                DELETE FROM commodity_tags
                WHERE guild_id = ? AND commodity_name = ? COLLATE NOCASE
                """,
                (guild_id, old_name),
            )
            for tag in tags:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO commodity_tags (guild_id, commodity_name, tag)
                    VALUES (?, ?, ?)
                    """,
                    (guild_id, next_name, tag),
                )
        return jsonify({"ok": True})

    @app.post("/api/commodity")
    def api_commodity_create():
        data = request.get_json(silent=True) or {}
        name = str(data.get("name", "")).strip()
        if not name:
            return jsonify({"error": "name is required"}), 400
        try:
            price = max(0.01, float(data.get("price", 1.0)))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid price"}), 400
        rarity = str(data.get("rarity", "common")).strip().lower() or "common"
        image_url = str(data.get("image_url", "")).strip()
        description = str(data.get("description", "")).strip()
        tags = _parse_tags(data.get("tags", ""))

        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            existing = conn.execute(
                """
                SELECT 1
                FROM commodities
                WHERE guild_id = ? AND name = ? COLLATE NOCASE
                """,
                (guild_id, name),
            ).fetchone()
            if existing is not None:
                return jsonify({"error": f"Commodity `{name}` already exists"}), 409
            conn.execute(
                """
                INSERT INTO commodities (guild_id, name, price, rarity, image_url, description)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (guild_id, name, price, rarity, image_url, description),
            )
            for tag in tags:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO commodity_tags (guild_id, commodity_name, tag)
                    VALUES (?, ?, ?)
                    """,
                    (guild_id, name, tag),
                )
        return jsonify({"ok": True, "name": name})

    @app.post("/api/player/<int:user_id>/update")
    def api_player_update(user_id: int):
        data = request.get_json(silent=True) or {}
        updates: dict[str, object] = {}
        if "bank" in data:
            try:
                updates["bank"] = max(0.0, float(data.get("bank")))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid value for bank"}), 400
        if "networth" in data:
            try:
                updates["networth"] = max(0.0, float(data.get("networth")))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid value for networth"}), 400
        if "owe" in data:
            try:
                updates["owe"] = max(0.0, float(data.get("owe")))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid value for owe"}), 400
        if "rank" in data:
            updates["rank"] = str(data.get("rank", "")).strip()
        if not updates:
            return jsonify({"error": "No valid fields provided"}), 400

        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())
        values.append(user_id)
        with _connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Player not found"}), 404
            conn.execute(
                f"UPDATE users SET {set_clause} WHERE user_id = ?",
                values,
            )
        return jsonify({"ok": True})

    @app.post("/api/player/<int:user_id>/reset-trade-usage")
    def api_player_reset_trade_usage(user_id: int):
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT guild_id
                FROM users
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
            if row is None:
                return jsonify({"error": "Player not found"}), 404
            guild_id = int(row["guild_id"])
            cur = conn.execute(
                """
                DELETE FROM app_state
                WHERE key LIKE ?
                """,
                (f"trade_used:{guild_id}:{user_id}:%",),
            )
        return jsonify({"ok": True, "cleared": int(cur.rowcount)})

    @app.post("/api/perk")
    def api_perk_create():
        data = request.get_json(silent=True) or {}
        name = str(data.get("name", "")).strip()
        if not name:
            return jsonify({"error": "name is required"}), 400
        description = str(data.get("description", "")).strip()
        with _connect() as conn:
            guild_id = _pick_default_guild_id(conn)
            cur = conn.execute(
                """
                INSERT INTO perks (guild_id, name, description, enabled, priority, stack_mode, max_stacks)
                VALUES (?, ?, ?, 1, 100, 'add', 1)
                """,
                (guild_id, name, description),
            )
            perk_id = int(cur.lastrowid)
        return jsonify({"ok": True, "id": perk_id})

    @app.post("/api/perk/<int:perk_id>/update")
    def api_perk_update(perk_id: int):
        data = request.get_json(silent=True) or {}
        updates: dict[str, object] = {}
        if "name" in data:
            name = str(data.get("name", "")).strip()
            if not name:
                return jsonify({"error": "name cannot be empty"}), 400
            updates["name"] = name
        if "description" in data:
            updates["description"] = str(data.get("description", ""))
        if "enabled" in data:
            try:
                updates["enabled"] = 1 if int(data.get("enabled")) != 0 else 0
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid enabled value"}), 400
        if "priority" in data:
            try:
                updates["priority"] = int(data.get("priority"))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid priority"}), 400
        if "stack_mode" in data:
            updates["stack_mode"] = _sanitize_stack_mode(data.get("stack_mode"))
        if "max_stacks" in data:
            try:
                updates["max_stacks"] = max(1, int(data.get("max_stacks")))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid max_stacks"}), 400
        if not updates:
            return jsonify({"error": "No valid fields provided"}), 400

        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())
        values.append(perk_id)
        with _connect() as conn:
            cur = conn.execute(
                f"UPDATE perks SET {set_clause} WHERE id = ?",
                values,
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Perk not found"}), 404
        return jsonify({"ok": True})

    @app.delete("/api/perk/<int:perk_id>")
    def api_perk_delete(perk_id: int):
        with _connect() as conn:
            cur = conn.execute(
                "DELETE FROM perks WHERE id = ?",
                (perk_id,),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Perk not found"}), 404
        return jsonify({"ok": True})

    @app.post("/api/perk/<int:perk_id>/requirements")
    def api_perk_requirement_create(perk_id: int):
        data = request.get_json(silent=True) or {}
        try:
            group_id = max(1, int(data.get("group_id", 1)))
            value = max(0, int(float(data.get("value", 1))))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid group/value"}), 400
        req_type = str(data.get("req_type", "commodity_qty")).strip().lower() or "commodity_qty"
        commodity_name = str(data.get("commodity_name", "")).strip()
        operator = _sanitize_operator(data.get("operator", ">="))
        if req_type not in {"commodity_qty", "tag_qty"}:
            return jsonify({"error": "Unsupported req_type"}), 400
        if not commodity_name:
            return jsonify({"error": "commodity_name/tag is required"}), 400
        with _connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM perks WHERE id = ?",
                (perk_id,),
            ).fetchone()
            if exists is None:
                return jsonify({"error": "Perk not found"}), 404
            cur = conn.execute(
                """
                INSERT INTO perk_requirements (perk_id, group_id, req_type, commodity_name, operator, value)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (perk_id, group_id, req_type, commodity_name, operator, value),
            )
        return jsonify({"ok": True, "id": int(cur.lastrowid)})

    @app.post("/api/perk/<int:perk_id>/requirements/<int:req_id>/update")
    def api_perk_requirement_update(perk_id: int, req_id: int):
        data = request.get_json(silent=True) or {}
        updates: dict[str, object] = {}
        if "group_id" in data:
            try:
                updates["group_id"] = max(1, int(data.get("group_id")))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid group_id"}), 400
        if "req_type" in data:
            req_type = str(data.get("req_type", "")).strip().lower()
            if req_type not in {"commodity_qty", "tag_qty"}:
                return jsonify({"error": "Unsupported req_type"}), 400
            updates["req_type"] = req_type
        if "commodity_name" in data:
            updates["commodity_name"] = str(data.get("commodity_name", "")).strip()
        if "operator" in data:
            updates["operator"] = _sanitize_operator(data.get("operator"))
        if "value" in data:
            try:
                updates["value"] = max(0, int(float(data.get("value"))))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid value"}), 400
        if not updates:
            return jsonify({"error": "No valid fields provided"}), 400
        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())
        values.extend([req_id, perk_id])
        with _connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE perk_requirements
                SET {set_clause}
                WHERE id = ? AND perk_id = ?
                """,
                values,
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Requirement not found"}), 404
        return jsonify({"ok": True})

    @app.delete("/api/perk/<int:perk_id>/requirements/<int:req_id>")
    def api_perk_requirement_delete(perk_id: int, req_id: int):
        with _connect() as conn:
            cur = conn.execute(
                """
                DELETE FROM perk_requirements
                WHERE id = ? AND perk_id = ?
                """,
                (req_id, perk_id),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Requirement not found"}), 404
        return jsonify({"ok": True})

    @app.post("/api/perk/<int:perk_id>/effects")
    def api_perk_effect_create(perk_id: int):
        data = request.get_json(silent=True) or {}
        target_stat = str(data.get("target_stat", "income")).strip().lower() or "income"
        value_mode = str(data.get("value_mode", "flat")).strip().lower() or "flat"
        scale_source = str(data.get("scale_source", "none")).strip().lower() or "none"
        scale_key = str(data.get("scale_key", "")).strip()
        try:
            value = float(data.get("value", 0))
            scale_factor = float(data.get("scale_factor", 0))
            cap = float(data.get("cap", 0))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid numeric effect fields"}), 400

        with _connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM perks WHERE id = ?",
                (perk_id,),
            ).fetchone()
            if exists is None:
                return jsonify({"error": "Perk not found"}), 404
            cur = conn.execute(
                """
                INSERT INTO perk_effects (
                    perk_id, effect_type, target_stat, value_mode, value,
                    scale_source, scale_key, scale_factor, cap
                )
                VALUES (?, 'stat_mod', ?, ?, ?, ?, ?, ?, ?)
                """,
                (perk_id, target_stat, value_mode, value, scale_source, scale_key, scale_factor, cap),
            )
        return jsonify({"ok": True, "id": int(cur.lastrowid)})

    @app.post("/api/perk/<int:perk_id>/effects/<int:effect_id>/update")
    def api_perk_effect_update(perk_id: int, effect_id: int):
        data = request.get_json(silent=True) or {}
        updates: dict[str, object] = {}
        for text_key in {"target_stat", "value_mode", "scale_source", "scale_key"}:
            if text_key in data:
                updates[text_key] = str(data.get(text_key, "")).strip().lower()
        for num_key in {"value", "scale_factor", "cap"}:
            if num_key in data:
                try:
                    updates[num_key] = float(data.get(num_key))
                except (TypeError, ValueError):
                    return jsonify({"error": f"Invalid {num_key}"}), 400
        if not updates:
            return jsonify({"error": "No valid fields provided"}), 400
        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())
        values.extend([effect_id, perk_id])
        with _connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE perk_effects
                SET {set_clause}
                WHERE id = ? AND perk_id = ?
                """,
                values,
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Effect not found"}), 404
        return jsonify({"ok": True})

    @app.delete("/api/perk/<int:perk_id>/effects/<int:effect_id>")
    def api_perk_effect_delete(perk_id: int, effect_id: int):
        with _connect() as conn:
            cur = conn.execute(
                """
                DELETE FROM perk_effects
                WHERE id = ? AND perk_id = ?
                """,
                (effect_id, perk_id),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Effect not found"}), 404
        return jsonify({"ok": True})

    @app.get("/api/bank-requests")
    def api_bank_requests():
        status = (request.args.get("status") or "pending").strip().lower()
        if status not in {"pending", "approved", "denied", "all"}:
            return jsonify({"error": "Invalid status"}), 400
        limit_raw = request.args.get("limit")
        try:
            limit = max(1, min(1000, int(limit_raw))) if limit_raw is not None else 300
        except ValueError:
            return jsonify({"error": "Invalid limit"}), 400

        where_status = "" if status == "all" else "WHERE br.status = ?"
        args: tuple[object, ...] = () if status == "all" else (status,)
        with _connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    br.id,
                    br.guild_id,
                    br.user_id,
                    COALESCE(u.display_name, '') AS display_name,
                    br.request_type,
                    br.amount,
                    br.reason,
                    br.status,
                    br.decision_reason,
                    br.reviewed_by,
                    br.created_at,
                    br.reviewed_at,
                    br.processed_at
                FROM bank_requests br
                LEFT JOIN users u
                  ON u.guild_id = br.guild_id
                 AND u.user_id = br.user_id
                {where_status}
                ORDER BY br.id DESC
                LIMIT ?
                """,
                (*args, limit),
            ).fetchall()
        return jsonify({"requests": [dict(r) for r in rows]})

    @app.post("/api/bank-requests/<int:request_id>/approve")
    def api_bank_request_approve(request_id: int):
        data = request.get_json(silent=True) or {}
        reason = str(data.get("reason", "")).strip()
        now = datetime.now(timezone.utc).isoformat()
        with _connect() as conn:
            cur = conn.execute(
                """
                UPDATE bank_requests
                SET status = 'approved',
                    decision_reason = ?,
                    reviewed_by = 0,
                    reviewed_at = ?
                WHERE id = ? AND status = 'pending'
                """,
                (reason, now, request_id),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Request not found or already reviewed"}), 404
        return jsonify({"ok": True})

    @app.post("/api/bank-requests/<int:request_id>/deny")
    def api_bank_request_deny(request_id: int):
        data = request.get_json(silent=True) or {}
        reason = str(data.get("reason", "")).strip()
        now = datetime.now(timezone.utc).isoformat()
        with _connect() as conn:
            cur = conn.execute(
                """
                UPDATE bank_requests
                SET status = 'denied',
                    decision_reason = ?,
                    reviewed_by = 0,
                    reviewed_at = ?
                WHERE id = ? AND status = 'pending'
                """,
                (reason, now, request_id),
            )
            if cur.rowcount <= 0:
                return jsonify({"error": "Request not found or already reviewed"}), 404
        return jsonify({"ok": True})

    @app.post("/api/app-config/<config_name>/update")
    def api_app_config_update(config_name: str):
        if config_name not in APP_CONFIG_SPECS:
            return jsonify({"error": "Unknown app config"}), 404
        data = request.get_json(silent=True) or {}
        if "value" not in data:
            return jsonify({"error": "Missing value"}), 400
        try:
            value = _set_config(config_name, data.get("value"))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid value"}), 400
        return jsonify({"ok": True, "value": _config_value_for_json(config_name, value)})

    @app.post("/api/server-actions/backup")
    def api_server_action_backup():
        try:
            backup_path = _create_database_backup_now(prefix="manual")
        except Exception as exc:
            return jsonify({"error": f"Backup failed: {exc}"}), 500
        return jsonify({"ok": True, "backup_path": backup_path})

    @app.get("/api/server-actions/backups")
    def api_server_action_backups():
        backup_dir = _backup_dir()
        files = []
        for path in sorted(backup_dir.glob("*.db"), key=lambda p: p.stat().st_mtime, reverse=True):
            try:
                stat = path.stat()
            except OSError:
                continue
            files.append(
                {
                    "name": path.name,
                    "size_bytes": int(stat.st_size),
                    "size_human": _human_size(int(stat.st_size)),
                    "mtime_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
                }
            )
        return jsonify({"backups": files})

    @app.delete("/api/server-actions/backups/<backup_name>")
    def api_server_action_delete_backup(backup_name: str):
        if "/" in backup_name or "\\" in backup_name:
            return jsonify({"error": "Invalid backup name"}), 400
        if not backup_name.endswith(".db"):
            return jsonify({"error": "Only .db backup files are deletable"}), 400

        backup_dir = _backup_dir().resolve()
        target = (backup_dir / backup_name).resolve()
        if target.parent != backup_dir:
            return jsonify({"error": "Invalid backup path"}), 400
        if not target.exists():
            return jsonify({"error": "Backup not found"}), 404

        try:
            target.unlink()
        except OSError as exc:
            return jsonify({"error": f"Delete failed: {exc}"}), 500
        return jsonify({"ok": True, "deleted": backup_name})

    return app


def main() -> None:
    app = create_app()
    host = os.getenv("DASHBOARD_HOST", "127.0.0.1")
    port = int(os.getenv("DASHBOARD_PORT", "8082"))
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    main()
