import { useEffect, useMemo, useState } from "react";

const RAW_API_BASE = String(import.meta.env.VITE_API_BASE || "").trim();
const REFRESH_MS = 5000;
const DISCORD_CLIENT_ID = String(import.meta.env.VITE_DISCORD_CLIENT_ID || "").trim();
const DISCORD_REDIRECT_URI = String(import.meta.env.VITE_DISCORD_REDIRECT_URI || "").trim();

function normalizeBase(value) {
  return String(value || "").replace(/\/+$/, "");
}

function buildApiBases() {
  const envBase = normalizeBase(RAW_API_BASE);
  const isDiscordActivityHost =
    typeof window !== "undefined" && window.location.hostname.endsWith(".discordsays.com");
  // Inside Discord Activity webview, CSP blocks absolute external fetch targets.
  // Use proxy-relative /api only.
  if (isDiscordActivityHost) return [""];
  if (!envBase) return [""];
  return [envBase];
}
function inferDashboardBase() {
  if (typeof window === "undefined") return "";
  const { protocol, hostname } = window.location;
  if (!hostname.includes("-activity.")) return "";
  const dashboardHost = hostname.replace("-activity.", ".");
  return normalizeBase(`${protocol}//${dashboardHost}`);
}
const API_BASES = (() => {
  const inferred = inferDashboardBase();
  const bases = buildApiBases();
  // On activity host (e.g. 716stonks-activity.*), /api usually 404s.
  // Prefer inferred dashboard origin first to avoid noisy 404 requests.
  if (inferred) {
    const filtered = bases.filter((b) => b !== inferred && b !== "");
    return [inferred, ...filtered, ""];
  }
  return bases;
})();

function money(v) {
  return `$${Number(v || 0).toFixed(2)}`;
}

function changePct(current, base) {
  const c = Number(current || 0);
  const b = Number(base || 0);
  if (!Number.isFinite(c) || !Number.isFinite(b) || b === 0) return 0;
  return ((c - b) / b) * 100;
}

async function fetchJson(url) {
  const res = await fetch(url, { cache: "no-store" });
  const contentType = String(res.headers.get("content-type") || "").toLowerCase();
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}: ${body.slice(0, 180)}`);
  }
  if (!contentType.includes("application/json")) {
    const body = await res.text().catch(() => "");
    throw new Error(`Expected JSON but got '${contentType || "unknown"}': ${body.slice(0, 180)}`);
  }
  return res.json();
}

async function fetchJsonWithFallback(path) {
  let lastErr = null;
  for (const base of API_BASES) {
    const url = `${base}${path}`;
    try {
      return await fetchJson(url);
    } catch (e) {
      lastErr = e;
    }
  }
  const isDiscordActivityHost =
    typeof window !== "undefined" && window.location.hostname.endsWith(".discordsays.com");
  if (isDiscordActivityHost) {
    const detail = String(lastErr?.message || "").trim();
    throw new Error(
      detail
        ? `Activity API proxy failed (${detail}). Check Discord URL mapping: /api -> 716stonks.cfm1.uk/api`
        : "Activity API proxy is not configured. Add Discord URL mapping: /api -> 716stonks.cfm1.uk/api"
    );
  }
  throw lastErr || new Error("Failed to load data");
}

async function postJsonWithFallback(path, body) {
  let lastErr = null;
  for (const base of API_BASES) {
    const url = `${base}${path}`;
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body || {}),
      });
      const contentType = String(res.headers.get("content-type") || "").toLowerCase();
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`${res.status} ${res.statusText}: ${text.slice(0, 180)}`);
      }
      if (!contentType.includes("application/json")) {
        const text = await res.text().catch(() => "");
        throw new Error(`Expected JSON but got '${contentType || "unknown"}': ${text.slice(0, 180)}`);
      }
      return await res.json();
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error("Failed to post data");
}

function pickLaunchName() {
  if (typeof window === "undefined") return "";
  const params = new URLSearchParams(window.location.search);
  return (
    params.get("username") ||
    params.get("global_name") ||
    params.get("display_name") ||
    params.get("user_name") ||
    ""
  ).trim();
}

function pickLaunchUserId() {
  if (typeof window === "undefined") return "";
  const params = new URLSearchParams(window.location.search);
  const raw = String(params.get("user_id") || params.get("id") || "").trim();
  if (!raw) return "";
  const normalized = raw.startsWith("+") ? raw.slice(1) : raw;
  return /^\d+$/.test(normalized) ? normalized : "";
}

function discordAvatarUrl(userId, avatarHash) {
  const uid = String(userId || "").trim();
  const av = String(avatarHash || "").trim();
  if (!uid || !av) return "";
  return `https://cdn.discordapp.com/avatars/${uid}/${av}.png?size=128`;
}

async function detectViewerIdentity() {
  const launchName = pickLaunchName();
  const launchId = pickLaunchUserId();
  if (launchName) return { id: launchId, name: launchName, avatar_url: "", reason: "query-param" };
  if (typeof window === "undefined") return { name: "", reason: "no-window" };
  if (!window.location.hostname.endsWith(".discordsays.com")) {
    return { name: "", reason: "not-discord-activity-host" };
  }
  if (!DISCORD_CLIENT_ID) return { name: "", reason: "missing-client-id" };
  try {
    const { DiscordSDK } = await import("@discord/embedded-app-sdk");
    const sdk = new DiscordSDK(DISCORD_CLIENT_ID);
    await sdk.ready();
    const isActivityHost = window.location.hostname.endsWith(".discordsays.com");
    const redirectUri = isActivityHost ? "" : (DISCORD_REDIRECT_URI || window.location.origin);
    const authPayload = {
      client_id: DISCORD_CLIENT_ID,
      response_type: "code",
      state: "activity-auth",
      prompt: "none",
      scope: ["identify"],
    };
    if (redirectUri) authPayload.redirect_uri = redirectUri;
    const auth = await sdk.commands.authorize(authPayload);
    const code = String(auth?.code || "").trim();
    if (!code) return { name: "", reason: "sdk-authorize-no-code" };
    const query = new URLSearchParams();
    query.set("code", code);
    if (redirectUri) query.set("redirect_uri", redirectUri);
    const token = await fetchJsonWithFallback(`/api/activity/oauth/token?${query.toString()}`);
    const accessToken = String(token?.access_token || "").trim();
    if (!accessToken) return { name: "", reason: "oauth-no-access-token" };
    const me = await sdk.commands.authenticate({ access_token: accessToken });
    const user = me?.user || {};
    const userId = String(user.id || "").trim();
    const name =
      String(user.global_name || "").trim() ||
      String(user.display_name || "").trim() ||
      String(user.username || "").trim();
    if (name) {
      return {
        id: userId,
        name,
        avatar_url: discordAvatarUrl(userId, user.avatar),
        reason: "sdk-authenticated",
      };
    }
    return { name: "", reason: "sdk-auth-user-empty" };
  } catch (err) {
    const detail = String(err?.message || err || "").trim();
    return {
      name: "",
      reason: detail ? `sdk-init-failed: ${detail}` : "sdk-init-failed",
    };
  }
}

export default function App() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [viewerName, setViewerName] = useState("Guest");
  const [viewerId, setViewerId] = useState("");
  const [viewerAvatar, setViewerAvatar] = useState("");
  const [viewerDebug, setViewerDebug] = useState("");
  const [statusData, setStatusData] = useState(null);
  const [stocks, setStocks] = useState([]);
  const [stats, setStats] = useState(null);
  const [lastUpdated, setLastUpdated] = useState("-");

  const sorted = useMemo(() => {
    return [...stocks].sort((a, b) => String(a.symbol || "").localeCompare(String(b.symbol || "")));
  }, [stocks]);

  useEffect(() => {
    let timer = null;
    let mounted = true;

    async function load() {
      try {
        const [stocksJson, statsJson] = await Promise.all([
          fetchJsonWithFallback("/api/stocks"),
          fetchJsonWithFallback("/api/dashboard-stats"),
        ]);
        if (!mounted) return;

        setStocks(Array.isArray(stocksJson.stocks) ? stocksJson.stocks : []);
        setStats(statsJson || null);
        if (viewerId) {
          try {
            const statusJson = await fetchJsonWithFallback(`/api/activity/status?user_id=${encodeURIComponent(viewerId)}`);
            if (!mounted) return;
            setStatusData(statusJson || null);
          } catch {
            if (!mounted) return;
            setStatusData(null);
          }
        } else {
          setStatusData(null);
        }
        setLastUpdated(new Date().toLocaleTimeString());
        setError("");
      } catch (e) {
        if (!mounted) return;
        setError(String(e?.message || e || "Failed to load data"));
      } finally {
        if (mounted) setLoading(false);
      }
    }

    load();
    timer = setInterval(load, REFRESH_MS);
    return () => {
      mounted = false;
      if (timer) clearInterval(timer);
    };
  }, [viewerId]);

  useEffect(() => {
    let mounted = true;
    detectViewerIdentity()
      .then((result) => {
        if (!mounted) return;
        const nextName = String(result?.name || "").trim();
        const nextId = String(result?.id || "").trim();
        const nextAvatar = String(result?.avatar_url || "").trim();
        if (nextName) {
          setViewerName(nextName);
          setViewerId(nextId);
          setViewerAvatar(nextAvatar);
          setViewerDebug(String(result?.reason || ""));
        } else {
          setViewerName("Guest");
          setViewerId("");
          setViewerAvatar("");
          setViewerDebug(String(result?.reason || "unknown"));
        }
      })
      .catch(() => {});
    return () => {
      mounted = false;
    };
  }, []);

  return (
    <main className="layout">
      <aside className="sidebar panel">
        <div className="panelHead">
          {viewerAvatar ? <img className="avatar" src={viewerAvatar} alt="avatar" /> : null}
          <h1>716Stonks</h1>
          <p>{viewerName}</p>
          {viewerDebug ? <p className="muted">Identity: {viewerDebug}</p> : null}
        </div>
        <div className="menu">
          <button className="menuBtn active" type="button">Companies</button>
          <button className="menuBtn" type="button" disabled>Ranking (Soon)</button>
          <button className="menuBtn" type="button" disabled>Shop (Soon)</button>
          <button className="menuBtn" type="button" disabled>Status (Soon)</button>
        </div>
      </aside>

      <section className="content panel">
        <header className="contentHead">
          <div>
            <h2>Companies</h2>
            <p>Live market snapshot</p>
          </div>
          <div className="stamp">Updated: {lastUpdated}</div>
        </header>

        <section className="stats">
          <div className="stat">
            <span>Companies</span>
            <strong>{stats?.companies_count ?? "-"}</strong>
          </div>
          <div className="stat">
            <span>Players</span>
            <strong>{stats?.users_count ?? "-"}</strong>
          </div>
          <div className="stat">
            <span>Until Close</span>
            <strong>{stats?.until_close_hms ?? "-"}</strong>
          </div>
          <div className="stat">
            <span>Ticking</span>
            <strong>{stats?.ticks_paused ? "Paused" : "Running"}</strong>
          </div>
        </section>

        {statusData?.user ? (
          <section className="statusCard">
            <h3>Activity Status</h3>
            <div className="statusGrid">
              <div><span>Name</span><strong>{statusData.user.display_name}</strong></div>
              <div><span>Rank</span><strong>{statusData.user.rank}</strong></div>
              <div><span>Balance</span><strong>{money(statusData.user.bank)}</strong></div>
              <div><span>Networth</span><strong>{money(statusData.user.networth)}</strong></div>
              <div><span>Owe</span><strong>{money(statusData.user.owe)}</strong></div>
              <div><span>Joined</span><strong>{String(statusData.user.joined_at || "").slice(0, 10) || "-"}</strong></div>
            </div>
            <div className="statusLine">
              <span>Trading Limit</span>
              <strong>
                {statusData.trade_limit?.enabled
                  ? `${statusData.trade_limit.remaining}/${statusData.trade_limit.limit} remaining (${Number(statusData.trade_limit.period_minutes || 0).toFixed(2)} min window)`
                  : "Disabled"}
              </strong>
            </div>
            <div className="statusLine">
              <span>Commodities</span>
              <strong>{statusData.commodities_used ?? 0}/{statusData.commodities_limit ?? 0}</strong>
            </div>
            <div className="statusLists">
              <div>
                <h4>Stocks</h4>
                {Array.isArray(statusData.stocks) && statusData.stocks.length
                  ? statusData.stocks.map((r) => (
                    <div key={r.symbol} className="rowItem">{r.symbol}: {r.shares}</div>
                  ))
                  : <div className="rowItem muted">None</div>}
              </div>
              <div>
                <h4>Commodities</h4>
                {Array.isArray(statusData.commodities) && statusData.commodities.length
                  ? statusData.commodities.map((r) => (
                    <div key={r.name} className="rowItem">{r.name}: {r.quantity} ({money(r.value)})</div>
                  ))
                  : <div className="rowItem muted">None</div>}
              </div>
              <div>
                <h4>Perks</h4>
                {Array.isArray(statusData.perks) && statusData.perks.length
                  ? statusData.perks.map((r, idx) => (
                    <div key={`${r.name}-${idx}`} className="rowItem">
                      {r.name} {r.stacks > 1 ? `(x${r.stacks})` : ""}{r.display ? ` · ${r.display}` : ""}
                    </div>
                  ))
                  : <div className="rowItem muted">None</div>}
              </div>
            </div>
          </section>
        ) : null}

        {loading ? <div className="empty">Loading...</div> : null}
        {error ? <div className="error">Error: {error}</div> : null}

        {!loading && !error ? (
          <div className="tableWrap">
            <table>
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>Name</th>
                  <th>Current</th>
                  <th>Change vs Base</th>
                </tr>
              </thead>
              <tbody>
                {sorted.length === 0 ? (
                  <tr>
                    <td colSpan="4" className="empty">No companies found.</td>
                  </tr>
                ) : (
                  sorted.map((row) => {
                    const pct = changePct(row.current_price, row.base_price);
                    const up = pct >= 0;
                    return (
                      <tr key={row.symbol}>
                        <td className="mono">{row.symbol}</td>
                        <td>{row.name}</td>
                        <td className="mono">{money(row.current_price)}</td>
                        <td className={`mono ${up ? "up" : "down"}`}>
                          {up ? "+" : ""}
                          {pct.toFixed(2)}%
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        ) : null}
      </section>
    </main>
  );
}
