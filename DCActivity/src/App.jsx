import { useEffect, useMemo, useRef, useState } from "react";

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

function moneySigned(v) {
  const n = Number(v || 0);
  const sign = n > 0 ? "+" : "";
  return `${sign}$${n.toFixed(2)}`;
}

function changePct(current, reference) {
  const c = Number(current || 0);
  const r = Number(reference || 0);
  if (!Number.isFinite(c) || !Number.isFinite(r) || r === 0) return 0;
  return ((c - r) / r) * 100;
}

function shopImageSrc(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  // Always go through Activity API proxy so Discord Activity host
  // does not need direct /media route mappings.
  return `/api/activity/image-proxy?url=${encodeURIComponent(raw)}`;
}

function rarityClass(rarity) {
  const key = String(rarity || "common").trim().toLowerCase();
  if (["common", "uncommon", "rare", "legendary", "exotic"].includes(key)) {
    return `rarity-${key}`;
  }
  return "rarity-common";
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

function defaultDiscordAvatarUrl(userId) {
  const raw = String(userId || "").trim();
  if (!raw) return "https://cdn.discordapp.com/embed/avatars/0.png";
  try {
    const idx = Number((BigInt(raw) >> 22n) % 6n);
    return `https://cdn.discordapp.com/embed/avatars/${Math.max(0, Math.min(5, idx))}.png`;
  } catch {
    return "https://cdn.discordapp.com/embed/avatars/0.png";
  }
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
  const [activeTab, setActiveTab] = useState("companies");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [identityLoading, setIdentityLoading] = useState(true);
  const [viewerName, setViewerName] = useState("");
  const [viewerId, setViewerId] = useState("");
  const [viewerAvatar, setViewerAvatar] = useState("");
  const [statusError, setStatusError] = useState("");
  const [statusData, setStatusData] = useState(null);
  const [stocks, setStocks] = useState([]);
  const [shopItems, setShopItems] = useState([]);
  const [shopBucket, setShopBucket] = useState(null);
  const [shopBusyByName, setShopBusyByName] = useState({});
  const [historyRows, setHistoryRows] = useState([]);
  const [historyTotal, setHistoryTotal] = useState(0);
  const [historyPage, setHistoryPage] = useState(0);
  const [historyLimit] = useState(20);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState("");
  const [tradeQtyBySymbol, setTradeQtyBySymbol] = useState({});
  const [tradeBusyBySymbol, setTradeBusyBySymbol] = useState({});
  const [tradeMsg, setTradeMsg] = useState("");
  const [stats, setStats] = useState(null);
  const [lastUpdated, setLastUpdated] = useState("-");
  const [dualStatus, setDualStatus] = useState(null);
  const [dualError, setDualError] = useState("");
  const [dualCreateBet, setDualCreateBet] = useState("50");
  const [dualGuess, setDualGuess] = useState("");
  const [dualBusy, setDualBusy] = useState(false);
  const dualAutoActionAtRef = useRef(0);

  const sorted = useMemo(() => {
    return [...stocks].sort((a, b) => String(a.symbol || "").localeCompare(String(b.symbol || "")));
  }, [stocks]);

  async function refreshStatusAndDual() {
    if (!viewerId) {
      setStatusData(null);
      setStatusError("");
      setDualStatus(null);
      setDualError("");
      return;
    }
    const [statusRes, dualRes] = await Promise.allSettled([
      fetchJsonWithFallback(`/api/activity/status?user_id=${encodeURIComponent(viewerId)}`),
      fetchJsonWithFallback(`/api/activity/dual/status?user_id=${encodeURIComponent(viewerId)}`),
    ]);
    if (statusRes.status === "fulfilled") {
      setStatusData(statusRes.value || null);
      setStatusError("");
    } else {
      setStatusData(null);
      setStatusError(String(statusRes.reason?.message || statusRes.reason || "Failed to load status"));
    }
    if (dualRes.status === "fulfilled") {
      setDualStatus(dualRes.value || null);
      setDualError("");
    } else {
      setDualStatus(null);
      setDualError(String(dualRes.reason?.message || dualRes.reason || "Failed to load DUAL status"));
    }
  }

  useEffect(() => {
    let timer = null;
    let mounted = true;

    async function load() {
      try {
        const [stocksJson, statsJson] = await Promise.all([
          fetchJsonWithFallback(`/api/stocks${viewerId ? `?user_id=${encodeURIComponent(viewerId)}` : ""}`),
          fetchJsonWithFallback("/api/dashboard-stats"),
        ]);
        if (!mounted) return;

        setStocks(Array.isArray(stocksJson.stocks) ? stocksJson.stocks : []);
        setStats(statsJson || null);
        try {
          const shopJson = await fetchJsonWithFallback("/api/shop");
          if (!mounted) return;
          setShopItems(Array.isArray(shopJson?.items) ? shopJson.items : []);
          setShopBucket(shopJson?.bucket ?? null);
        } catch {
          if (!mounted) return;
          setShopItems([]);
          setShopBucket(null);
        }
        if (viewerId) {
          const [statusResult, dualResult] = await Promise.allSettled([
            fetchJsonWithFallback(`/api/activity/status?user_id=${encodeURIComponent(viewerId)}`),
            fetchJsonWithFallback(`/api/activity/dual/status?user_id=${encodeURIComponent(viewerId)}`),
          ]);
          if (!mounted) return;
          if (statusResult.status === "fulfilled") {
            setStatusData(statusResult.value || null);
            setStatusError("");
          } else {
            setStatusData(null);
            setStatusError(String(statusResult.reason?.message || statusResult.reason || "Failed to load status"));
          }
          if (dualResult.status === "fulfilled") {
            setDualStatus(dualResult.value || null);
            setDualError("");
          } else {
            setDualStatus(null);
            setDualError(String(dualResult.reason?.message || dualResult.reason || "Failed to load DUAL status"));
          }
        } else {
          setStatusData(null);
          setStatusError("");
          setDualStatus(null);
          setDualError("");
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
    async function loadHistory() {
      if (!viewerId) {
        setHistoryRows([]);
        setHistoryTotal(0);
        setHistoryError("");
        return;
      }
      setHistoryLoading(true);
      try {
        const offset = historyPage * historyLimit;
        const json = await fetchJsonWithFallback(
          `/api/activity/history?user_id=${encodeURIComponent(viewerId)}&limit=${historyLimit}&offset=${offset}`
        );
        if (!mounted) return;
        setHistoryRows(Array.isArray(json?.rows) ? json.rows : []);
        setHistoryTotal(Number(json?.total || 0));
        setHistoryError("");
      } catch (e) {
        if (!mounted) return;
        setHistoryRows([]);
        setHistoryError(String(e?.message || e || "Failed to load history"));
      } finally {
        if (mounted) setHistoryLoading(false);
      }
    }
    if (activeTab === "history") {
      loadHistory();
    }
    return () => {
      mounted = false;
    };
  }, [viewerId, historyPage, historyLimit, activeTab]);

  useEffect(() => {
    if (activeTab !== "dual") return undefined;
    if (!viewerId) return undefined;
    let cancelled = false;
    const run = async () => {
      if (cancelled || dualBusy) return;
      try {
        await refreshStatusAndDual();
      } catch {
        // ignore transient dual polling errors
      }
    };
    run();
    const id = setInterval(run, 1500);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [activeTab, viewerId, dualBusy]);

  async function submitTrade(action, symbol) {
    if (!viewerId) return;
    const qtyRaw = Number(tradeQtyBySymbol[symbol] || 1);
    const shares = Number.isFinite(qtyRaw) ? Math.max(1, Math.floor(qtyRaw)) : 1;
    const endpoint = action === "buy" ? "/api/activity/trade/buy" : "/api/activity/trade/sell";
    setTradeBusyBySymbol((prev) => ({ ...prev, [symbol]: true }));
    try {
      const result = await postJsonWithFallback(endpoint, {
        user_id: viewerId,
        symbol,
        shares,
      });
      if (!result?.ok) {
        throw new Error(String(result?.error || "Trade failed"));
      }
      setStocks((prev) =>
        prev.map((row) =>
          String(row.symbol) === String(symbol)
            ? { ...row, owned_shares: Number(result.owned_shares || 0) }
            : row
        )
      );
      setTradeMsg(
        action === "buy"
          ? `Bought ${shares} ${symbol} @ ${money(result.unit_price)} (total ${money(result.total_cost)}).`
          : `Sold ${shares} ${symbol} @ ${money(result.unit_price)} (net ${money(result.net_gain)}).`
      );
    } catch (e) {
      setTradeMsg(`Trade failed for ${symbol}: ${String(e?.message || e || "Unknown error")}`);
    } finally {
      setTradeBusyBySymbol((prev) => ({ ...prev, [symbol]: false }));
    }
  }

  async function submitShopBuy(name) {
    if (!viewerId) return;
    const quantity = 1;
    setShopBusyByName((prev) => ({ ...prev, [name]: true }));
    try {
      const result = await postJsonWithFallback("/api/activity/shop/buy", {
        user_id: viewerId,
        name,
        quantity,
      });
      if (!result?.ok) {
        throw new Error(String(result?.error || "Shop buy failed"));
      }
      const activated = Array.isArray(result?.activated_perks) ? result.activated_perks : [];
      const perkText = activated.length
        ? ` Perk activated: ${activated.map((p) => String(p)).join(", ")}.`
        : "";
      setTradeMsg(`Purchased ${name} @ ${money(result.unit_price)}.${perkText}`);
      const [shopJson, statusJson] = await Promise.all([
        fetchJsonWithFallback("/api/shop"),
        fetchJsonWithFallback(`/api/activity/status?user_id=${encodeURIComponent(viewerId)}`),
      ]);
      setShopItems(Array.isArray(shopJson?.items) ? shopJson.items : []);
      setShopBucket(shopJson?.bucket ?? null);
      setStatusData(statusJson || null);
    } catch (e) {
      setTradeMsg(`Shop buy failed for ${name}: ${String(e?.message || e || "Unknown error")}`);
    } finally {
      setShopBusyByName((prev) => ({ ...prev, [name]: false }));
    }
  }

  async function submitShopSellOne(name) {
    if (!viewerId) return;
    setShopBusyByName((prev) => ({ ...prev, [name]: true }));
    try {
      const result = await postJsonWithFallback("/api/activity/shop/sell", {
        user_id: viewerId,
        name,
      });
      if (!result?.ok) {
        throw new Error(String(result?.error || "Shop sell failed"));
      }
      setTradeMsg(`Sold 1x ${name} for ${money(result.total_gain)}.`);
      const [shopJson, statusJson] = await Promise.all([
        fetchJsonWithFallback("/api/shop"),
        fetchJsonWithFallback(`/api/activity/status?user_id=${encodeURIComponent(viewerId)}`),
      ]);
      setShopItems(Array.isArray(shopJson?.items) ? shopJson.items : []);
      setShopBucket(shopJson?.bucket ?? null);
      setStatusData(statusJson || null);
    } catch (e) {
      setTradeMsg(`Shop sell failed for ${name}: ${String(e?.message || e || "Unknown error")}`);
    } finally {
      setShopBusyByName((prev) => ({ ...prev, [name]: false }));
    }
  }

  async function submitDualCreate() {
    if (!viewerId) return;
    const bet = Number(dualCreateBet || 0);
    if (!Number.isFinite(bet) || bet <= 0) {
      setTradeMsg("DUAL create failed: bet must be greater than 0.");
      return;
    }
    setDualBusy(true);
    try {
      const result = await postJsonWithFallback("/api/activity/dual/create", {
        user_id: viewerId,
        display_name: viewerName || "",
        bet,
      });
      if (!result?.ok) throw new Error(String(result?.error || "Failed to create DUAL"));
      setTradeMsg(`DUAL room created: ${result?.game?.code || "-"}`);
      await refreshStatusAndDual();
    } catch (e) {
      setTradeMsg(`DUAL create failed: ${String(e?.message || e || "Unknown error")}`);
    } finally {
      setDualBusy(false);
    }
  }

  async function submitDualJoin(codeFromList = "") {
    if (!viewerId) return;
    const code = String(codeFromList || "").trim().toUpperCase();
    if (!code) {
      setTradeMsg("DUAL join failed: room code is required.");
      return;
    }
    setDualBusy(true);
    try {
      const result = await postJsonWithFallback("/api/activity/dual/join", {
        user_id: viewerId,
        display_name: viewerName || "",
        code,
      });
      if (!result?.ok) throw new Error(String(result?.error || "Failed to join DUAL"));
      setTradeMsg(`Joined DUAL room ${code}.`);
      await refreshStatusAndDual();
    } catch (e) {
      setTradeMsg(`DUAL join failed: ${String(e?.message || e || "Unknown error")}`);
    } finally {
      setDualBusy(false);
    }
  }

  async function submitDualReady() {
    if (!viewerId) return;
    const code = String(dualStatus?.current_game?.code || "").trim().toUpperCase();
    const bet = Number(dualCreateBet || 0);
    if (!code) return;
    if (!Number.isFinite(bet) || bet <= 0) {
      setTradeMsg("DUAL ready failed: bet must be greater than 0.");
      return;
    }
    setDualBusy(true);
    try {
      const result = await postJsonWithFallback("/api/activity/dual/ready", {
        user_id: viewerId,
        code,
        bet,
      });
      if (!result?.ok) throw new Error(String(result?.error || "Failed to ready up"));
      setTradeMsg("Ready submitted.");
      await refreshStatusAndDual();
    } catch (e) {
      setTradeMsg(`DUAL ready failed: ${String(e?.message || e || "Unknown error")}`);
    } finally {
      setDualBusy(false);
    }
  }

  async function submitDualGuess() {
    if (!viewerId) return;
    const code = String(dualStatus?.current_game?.code || "").trim().toUpperCase();
    const guess = Number(dualGuess);
    if (!code) return;
    if (!Number.isInteger(guess)) {
      setTradeMsg("DUAL guess failed: enter an integer.");
      return;
    }
    setDualBusy(true);
    try {
      const result = await postJsonWithFallback("/api/activity/dual/guess", {
        user_id: viewerId,
        code,
        guess,
      });
      if (!result?.ok) throw new Error(String(result?.error || "Failed to submit guess"));
      const hint = String(result?.hint || "").trim();
      setTradeMsg(hint ? `DUAL guess result: ${hint}.` : "DUAL guess submitted.");
      setDualGuess("");
      await refreshStatusAndDual();
    } catch (e) {
      setTradeMsg(`DUAL guess failed: ${String(e?.message || e || "Unknown error")}`);
    } finally {
      setDualBusy(false);
    }
  }

  useEffect(() => {
    if (activeTab !== "dual") return;
    if (!viewerId) return;
    if (dualBusy) return;
    if (!dualStatus) return;
    if (Date.now() - dualAutoActionAtRef.current < 2500) return;

    const current = dualStatus.current_game;
    if (current) return;

    const lobbies = Array.isArray(dualStatus.open_lobbies) ? dualStatus.open_lobbies : [];
    const joinable = lobbies.find(
      (l) =>
        String(l.status || "") === "lobby" &&
        String(l.host_user_id || "") !== String(viewerId || "") &&
        Number(l.player_count || 0) >= 1
    );
    if (joinable) {
      dualAutoActionAtRef.current = Date.now();
      submitDualJoin(String(joinable.code || ""));
      return;
    }
    dualAutoActionAtRef.current = Date.now();
    submitDualCreate();
  }, [activeTab, viewerId, dualBusy, dualStatus]);

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
        } else {
          setViewerName("");
          setViewerId("");
          setViewerAvatar("");
        }
      })
      .catch(() => {})
      .finally(() => {
        if (mounted) setIdentityLoading(false);
      });
    return () => {
      mounted = false;
    };
  }, []);

  if (identityLoading) {
    return (
      <main className="layout">
        <section className="content panel">
          <header className="contentHead">
            <div>
              <h2>716Stonks</h2>
              <p>Loading your Discord identity...</p>
            </div>
          </header>
          <div className="empty">Please wait...</div>
        </section>
      </main>
    );
  }

  return (
    <main className="layout">
      <aside className="sidebar panel">
        <div className="panelHead">
          {viewerAvatar ? <img className="avatar" src={viewerAvatar} alt="avatar" /> : null}
          <h1>716Stonks</h1>
          <p>{viewerName || "Unknown User"}</p>
        </div>
        <div className="menu">
          <button
            className={`menuBtn ${activeTab === "companies" ? "active" : ""}`}
            type="button"
            onClick={() => setActiveTab("companies")}
          >
            Companies
          </button>
          <button
            className={`menuBtn ${activeTab === "status" ? "active" : ""}`}
            type="button"
            onClick={() => setActiveTab("status")}
          >
            Status
          </button>
          <button
            className={`menuBtn ${activeTab === "shop" ? "active" : ""}`}
            type="button"
            onClick={() => setActiveTab("shop")}
          >
            Shop
          </button>
          <button
            className={`menuBtn ${activeTab === "dual" ? "active" : ""}`}
            type="button"
            onClick={() => setActiveTab("dual")}
          >
            DUAL
          </button>
          <button
            className={`menuBtn ${activeTab === "history" ? "active" : ""}`}
            type="button"
            onClick={() => { setHistoryPage(0); setActiveTab("history"); }}
          >
            History
          </button>
        </div>
      </aside>

      <section className="content panel">
        <header className="contentHead">
          <div>
            <h2>{activeTab === "status" ? "Status" : activeTab === "shop" ? "Shop" : activeTab === "dual" ? "DUAL" : activeTab === "history" ? "History" : "Companies"}</h2>
            <p>
              {activeTab === "status"
                ? "Your account summary"
                : activeTab === "shop"
                  ? "Current rotating commodity shop"
                  : activeTab === "dual"
                    ? "Multiplayer number duel (2+ players)"
                  : activeTab === "history"
                    ? "Your full transaction history"
                    : "Live market snapshot"}
            </p>
          </div>
          <div className="stamp">Updated: {lastUpdated}</div>
        </header>

        <section className="stats">
          <div className="stat">
            <span>Bank Balance</span>
            <strong>{statusData?.user ? money(statusData.user.bank) : "-"}</strong>
          </div>
          <div className="stat">
            <span>Total Daily Income</span>
            <strong>{statusData?.perk_summary ? money(statusData.perk_summary.income) : "-"}</strong>
          </div>
          <div className="stat">
            <span>Until Close</span>
            <strong>{stats?.until_close ?? stats?.until_close_hms ?? "-"}</strong>
          </div>
          <div className="stat">
            <span>Until Reset</span>
            <strong>{stats?.until_reset ?? "-"}</strong>
          </div>
          <div className="stat">
            <span>Until Stock Update</span>
            <strong>{stats?.until_tick ?? "-"}</strong>
          </div>
        </section>

        {activeTab === "status" && statusData?.user ? (
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
            <div className="statusLists">
              <div>
                <h4>
                  Stocks ({statusData.trade_limit?.enabled
                    ? `${statusData.trade_limit.remaining}/${statusData.trade_limit.limit}`
                    : "0/0"})
                </h4>
                {Array.isArray(statusData.stocks) && statusData.stocks.length
                  ? statusData.stocks.map((r) => (
                    <div key={r.symbol} className="rowItem">{r.symbol}: {r.shares}</div>
                  ))
                  : <div className="rowItem muted">None</div>}
              </div>
              <div>
                <h4>Commodities ({statusData.commodities_used ?? 0}/{statusData.commodities_limit ?? 0})</h4>
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
        {activeTab === "status" && !statusData?.user ? (
          <div className="empty">
            {viewerId
              ? (statusError || "Status data is not available for this user yet.")
              : "Status is unavailable because Activity identity is not resolved."}
          </div>
        ) : null}

        {loading ? <div className="empty">Loading...</div> : null}
        {error ? <div className="error">Error: {error}</div> : null}
        {tradeMsg ? <div className="empty">{tradeMsg}</div> : null}

        {!loading && !error && activeTab === "companies" ? (
          <div className="tableWrap">
            <table>
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>Name</th>
                  <th>Current</th>
                  <th>Change vs Prev Close</th>
                  <th>Owned</th>
                  <th>Trade</th>
                </tr>
              </thead>
              <tbody>
                {sorted.length === 0 ? (
                  <tr>
                    <td colSpan="6" className="empty">No companies found.</td>
                  </tr>
                ) : (
                  sorted.map((row) => {
                    const ref = row.previous_close_price ?? row.base_price;
                    const pct = changePct(row.current_price, ref);
                    const up = pct >= 0;
                    const symbol = String(row.symbol || "");
                    const owned = Number(row.owned_shares || 0);
                    const tradeQty = Number(tradeQtyBySymbol[symbol] || 1);
                    const busy = Boolean(tradeBusyBySymbol[symbol]);
                    return (
                      <tr key={symbol}>
                        <td className="mono">{row.symbol}</td>
                        <td>{row.name}</td>
                        <td className="mono">{money(row.current_price)}</td>
                        <td className={`mono ${up ? "up" : "down"}`}>
                          {up ? "+" : ""}
                          {pct.toFixed(2)}%
                        </td>
                        <td className="mono">{owned}</td>
                        <td>
                          <div style={{ display: "flex", gap: "6px", alignItems: "center" }}>
                            <input
                              type="number"
                              min="1"
                              value={tradeQty}
                              onChange={(e) =>
                                setTradeQtyBySymbol((prev) => ({
                                  ...prev,
                                  [symbol]: String(Math.max(1, Number(e.target.value || 1))),
                                }))
                              }
                              style={{ width: "74px" }}
                              disabled={busy || !viewerId}
                            />
                            <button
                              type="button"
                              disabled={busy || !viewerId}
                              onClick={() => submitTrade("buy", symbol)}
                            >
                              Buy
                            </button>
                            <button
                              type="button"
                              disabled={busy || !viewerId || owned <= 0}
                              onClick={() => submitTrade("sell", symbol)}
                            >
                              Sell
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        ) : null}
        {!loading && !error && activeTab === "shop" ? (
          <div className="shopWrap">
            <div className="stamp" style={{ marginBottom: "8px" }}>
              Rotation Bucket: {shopBucket ?? "-"}
            </div>
            {shopItems.length === 0 ? (
              <div className="shopGrid">
                <article className="shopCard inventoryCard">
                  <div className="shopBody">
                    <h3 className="shopTitle">
                      Your Owned Items ({statusData?.commodities_used ?? 0}/{statusData?.commodities_limit ?? 0})
                    </h3>
                    {Array.isArray(statusData?.commodities) && statusData.commodities.length ? (
                      <div className="shopDesc" style={{ minHeight: "unset" }}>
                        {statusData.commodities.map((row) => (
                          <div
                            key={row.name}
                            className="rowItem"
                            style={{ display: "flex", justifyContent: "space-between", gap: "8px", alignItems: "center" }}
                          >
                            <span>{row.name}: {row.quantity} ({money(row.value)})</span>
                            <button
                              type="button"
                              disabled={Boolean(shopBusyByName[row.name]) || !viewerId}
                              onClick={() => submitShopSellOne(row.name)}
                            >
                              Sell 1
                            </button>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <p className="shopDesc">You do not own any commodities yet.</p>
                    )}
                  </div>
                </article>
                <div className="empty">No shop items currently available.</div>
              </div>
            ) : (
              <div className="shopGrid">
                <article className="shopCard inventoryCard">
                  <div className="shopBody">
                    <h3 className="shopTitle">
                      Your Owned Items ({statusData?.commodities_used ?? 0}/{statusData?.commodities_limit ?? 0})
                    </h3>
                    {Array.isArray(statusData?.commodities) && statusData.commodities.length ? (
                      <div className="shopDesc" style={{ minHeight: "unset" }}>
                        {statusData.commodities.map((row) => (
                          <div
                            key={row.name}
                            className="rowItem"
                            style={{ display: "flex", justifyContent: "space-between", gap: "8px", alignItems: "center" }}
                          >
                            <span>{row.name}: {row.quantity} ({money(row.value)})</span>
                            <button
                              type="button"
                              disabled={Boolean(shopBusyByName[row.name]) || !viewerId}
                              onClick={() => submitShopSellOne(row.name)}
                            >
                              Sell 1
                            </button>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <p className="shopDesc">You do not own any commodities yet.</p>
                    )}
                  </div>
                </article>
                {shopItems.map((item) => {
                  const name = String(item.name || "");
                  const busy = Boolean(shopBusyByName[name]);
                  const inStock = Boolean(item.in_stock);
                  const imageUrl = String(item.image_url || "");
                  const rarity = String(item.rarity || "common");
                  return (
                    <article key={name} className={`shopCard ${rarityClass(rarity)}`}>
                      {imageUrl ? (
                        <img className="shopImage" src={shopImageSrc(imageUrl)} alt={name} />
                      ) : (
                        <div className="shopImage placeholder">No Image</div>
                      )}
                      <div className="shopBody">
                        <h3 className="shopTitle">{name}</h3>
                        <div className="shopMeta">
                          <span className="mono">{money(item.price)}</span>
                          <span>{rarity}</span>
                          <span>{inStock ? "In stock" : "Sold out"}</span>
                        </div>
                        <p className="shopDesc">{String(item.description || "No description.")}</p>
                      </div>
                      <div className="shopFooter">
                        <button
                          type="button"
                          disabled={busy || !viewerId || !inStock}
                          onClick={() => submitShopBuy(name)}
                        >
                          Purchase
                        </button>
                      </div>
                    </article>
                  );
                })}
              </div>
            )}
          </div>
        ) : null}
        {!loading && !error && activeTab === "dual" ? (
          <div className="shopWrap dualWrap">
            {!viewerId ? (
              <div className="empty">DUAL is unavailable because Activity identity is not resolved.</div>
            ) : (
              <>
                {dualError ? <div className="error">Error: {dualError}</div> : null}
                <section className="dualCard">
                  <h3>Auto Match</h3>
                  <p>Open this tab to queue. Match starts automatically when 2 players are present.</p>
                  <div className="dualControls">
                    <input
                      type="number"
                      min="0.01"
                      step="0.01"
                      value={dualCreateBet}
                      disabled={dualBusy}
                      onChange={(e) => setDualCreateBet(e.target.value)}
                    />
                    <span className="muted">Your queue bet (used if you become host)</span>
                  </div>
                </section>

                {dualStatus?.current_game ? (
                  <section className="dualCard">
                    <h3>
                      Current Game: {dualStatus.current_game.code} · {String(dualStatus.current_game.status || "").toUpperCase()}
                    </h3>
                    <div className="statusGrid">
                      <div><span>Pot</span><strong>{money(dualStatus.current_game.pot)}</strong></div>
                      <div><span>Round</span><strong>{dualStatus.current_game.round}</strong></div>
                      <div><span>Range</span><strong>{dualStatus.current_game.min_value} - {dualStatus.current_game.max_value}</strong></div>
                      <div>
                        <span>Ready</span>
                        <strong>{Number(dualStatus.current_game.ready_count || 0)}/{Number(dualStatus.current_game.player_count || 0)}</strong>
                      </div>
                    </div>
                    <div className="dualPlayers">
                      {(Array.isArray(dualStatus.current_game.players) ? dualStatus.current_game.players : []).map((p) => {
                        const isViewer = String(p.user_id || "") === String(viewerId || "");
                        const avatar = isViewer && viewerAvatar
                          ? viewerAvatar
                          : defaultDiscordAvatarUrl(p.user_id);
                        return (
                          <div key={`${dualStatus.current_game.code}-${p.user_id}`} className={`dualPlayer ${isViewer ? "isViewer" : ""}`}>
                            <img className="dualAvatar" src={avatar} alt={String(p.display_name || "player")} />
                            <strong>{p.display_name}{isViewer ? " (You)" : ""}</strong>
                          </div>
                        );
                      })}
                    </div>
                    {String(dualStatus.current_game.status || "") === "lobby" ? (
                      <div className="dualControls">
                        <input
                          type="number"
                          min="0.01"
                          step="0.01"
                          value={dualCreateBet}
                          disabled={dualBusy}
                          onChange={(e) => setDualCreateBet(e.target.value)}
                        />
                        <button
                          type="button"
                          className="readyBtn"
                          disabled={
                            dualBusy ||
                            Number(dualStatus.current_game.player_count || 0) < 2 ||
                            !Number.isFinite(Number(dualCreateBet || 0)) ||
                            Number(dualCreateBet || 0) <= 0 ||
                            Boolean(
                              (Array.isArray(dualStatus.current_game.players)
                                ? dualStatus.current_game.players
                                : []
                              ).find((p) => String(p.user_id || "") === String(viewerId || ""))?.ready
                            )
                          }
                          onClick={submitDualReady}
                        >
                          READY
                        </button>
                        <span className="muted">
                          {Number(dualStatus.current_game.player_count || 0) < 2
                            ? "Waiting for players..."
                            : "Press READY with a bet. Match starts automatically when all are ready."}
                        </span>
                      </div>
                    ) : null}
                    {String(dualStatus.current_game.status || "") === "active" ? (
                      <div className="dualControls">
                        <input
                          type="number"
                          min={dualStatus.current_game.min_value}
                          max={dualStatus.current_game.max_value}
                          step="1"
                          value={dualGuess}
                          disabled={dualBusy}
                          onChange={(e) => setDualGuess(e.target.value)}
                        />
                        <button type="button" disabled={dualBusy} onClick={submitDualGuess}>Submit Guess</button>
                      </div>
                    ) : null}
                    {String(dualStatus.current_game.status || "") === "finished" ? (
                      <div className="empty">
                        Winner: {
                          (Array.isArray(dualStatus.current_game.players) ? dualStatus.current_game.players : [])
                            .find((p) => String(p.user_id || "") === String(dualStatus.current_game.winner_user_id || ""))?.display_name
                          || dualStatus.current_game.winner_user_id
                          || "-"
                        }
                      </div>
                    ) : null}
                  </section>
                ) : (
                  <div className="empty">Queueing for DUAL... waiting on another player.</div>
                )}
              </>
            )}
          </div>
        ) : null}
        {!loading && !error && activeTab === "history" ? (
          <div className="tableWrap">
            {historyError ? <div className="error">Error: {historyError}</div> : null}
            <table>
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Type</th>
                  <th>Target</th>
                  <th>Qty</th>
                  <th>Unit</th>
                  <th>Delta</th>
                  <th>Details</th>
                </tr>
              </thead>
              <tbody>
                {historyLoading ? (
                  <tr><td colSpan="7" className="empty">Loading history...</td></tr>
                ) : historyRows.length === 0 ? (
                  <tr><td colSpan="7" className="empty">No history found.</td></tr>
                ) : historyRows.map((row) => (
                  <tr key={row.id}>
                    <td className="mono">{String(row.created_at || "").replace("T", " ").slice(0, 19)}</td>
                    <td>{row.action_type}</td>
                    <td>{row.target_type}:{row.target_symbol}</td>
                    <td className="mono">{Number(row.quantity || 0).toFixed(2)}</td>
                    <td className="mono">{money(row.unit_price)}</td>
                    <td
                      className={`mono ${
                        Number(row.delta || 0) > 0 ? "delta-pos" : Number(row.delta || 0) < 0 ? "delta-neg" : "delta-zero"
                      }`}
                    >
                      {moneySigned(row.delta)}
                    </td>
                    <td>{String(row.details || "") || "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div className="statusLine" style={{ margin: "8px 14px 12px" }}>
              <span>
                Page {historyPage + 1} / {Math.max(1, Math.ceil(historyTotal / historyLimit))} · Total {historyTotal}
              </span>
              <strong style={{ display: "flex", gap: "8px" }}>
                <button
                  type="button"
                  disabled={historyPage <= 0}
                  onClick={() => setHistoryPage((p) => Math.max(0, p - 1))}
                >
                  Prev
                </button>
                <button
                  type="button"
                  disabled={(historyPage + 1) * historyLimit >= historyTotal}
                  onClick={() => setHistoryPage((p) => p + 1)}
                >
                  Next
                </button>
              </strong>
            </div>
          </div>
        ) : null}
      </section>
    </main>
  );
}
