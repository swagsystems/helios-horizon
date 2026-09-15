const PROFILE_FALLBACK = [
  ["minecraft", "Minecraft", "crafty"],
  ["pz-rising", "Project Zomboid", "systemd"],
  ["terraria-vanilla", "Terraria Vanilla", "systemd"],
  ["terraria-tmod", "Terraria tModLoader", "systemd"],
];
const PROFILE_FAMILY_RULES = [
  { adapter: "crafty", prefix: "minecraft", key: "minecraft", label: "Minecraft" },
  { adapter: "systemd", prefix: "terraria", key: "terraria", label: "Terraria" },
  { adapter: "systemd", prefix: "pz-", key: "project-zomboid", label: "Project Zomboid" },
];
const THEMES = ["ember", "frost", "moss", "aurora", "paper"];
const SAMPLE_LIMIT = 90;
const DETAIL_LOG_BUFFER_LIMIT = 200;
const NOISE_PATTERNS = [
  /Closing TcpSocket/i,
  /\(Anonymous\)\] (Connecting|Closing)/i,
  /Tried to send data to a client after losing connection/i,
  /Thread RCON Client \/127\.0\.0\.1 (?:started|shutting down)/i,
];
const isNoise = (line) => NOISE_PATTERNS.some((pattern) => pattern.test(line.message || ""));
const detailLogKey = (line) => `${line.timestamp || ""}\u0000${line.severity || ""}\u0000${line.message || ""}`;

function markPerformance(name) {
  try { window.performance?.mark(name); } catch {}
}

function measurePerformance(name, startMark, endMark) {
  try {
    window.performance?.measure(name, startMark, endMark);
    window.performance?.clearMarks(startMark);
    window.performance?.clearMarks(endMark);
  } catch {}
}

markPerformance("horizon-load-start");

const state = {
  actor: null,
  csrf: null,
  profiles: new Map(),
  statuses: new Map(),
  selectedProfile: null,
  dialog: null,
  returnFocus: null,
  drawerReturnFocus: null,
  forceProfile: null,
  restoreProfile: null,
  updateProfile: null,
  updateApplySupported: false,
  logs: new Map(),
  cpuSamples: new Map(),
  metricSamples: new Map(),
  metricHistory: new Map(),
  metricCapacity: new Map(),
  schedules: null,
  incidents: { items: [], loaded: false },
  benchmarks: new Map(),
  session: { profileId: null, latestBackup: null, backupState: "loading", operation: null },
  detail: { id: null, tab: "console", backups: [], statsTimer: null, statsRequest: 0, statsAbort: null, metricTimer: null, metricRequest: 0, metricAbort: null, benchmarkTimer: null, benchmarkCursor: null, benchmarkRuns: [], statsBaseLoaded: false, commandCatalogKey: null, configRequest: 0 },
  statsCache: new Map(),
  configRestartRequired: new Map(),
  lastGeneration: 0,
  statusConfirmed: false,
  lastStatusAt: 0,
  // Accepted-sample counter plus the fence the estimate may not cross: a hidden
  // tab, a dropped stream, or a resume leaves cached statuses in memory that
  // are still recent, so only a sample accepted after the fence may render.
  statusSeq: 0,
  estimateFenceSeq: 0,
  loadFailed: false,
  perf: { firstStatusPaint: false, pendingMutations: new Map(), clientQueue: [], clientTimer: null },
};
let aggregateBackupRequest = 0;
let reauthenticating = false;
let sessionRefreshPromise = null;
let sessionExpired = false;
let sessionNoticeShown = false;
let sessionExpiryCount = 0;
let sessionGeneration = 0;
let loadRetryTimer = null;
let loadPromise = null;
let loadFailureNotice = null;
let testApiResponseDelay = 0;
const SESSION_EXPIRED_MESSAGE = "Session expired. Redirecting to sign in.";
const SESSION_REDIRECT_KEY = "horizon-session-redirected";
const SESSION_BOOTSTRAP_DELAYS = [0, 250, 1000];
const SESSION_BOOTSTRAP_TIMEOUT_MS = 4000;
const SESSION_BOOTSTRAP_DEADLINE_MS = 7000;
const STATUS_STALE_MS = 30000;
const stream = { source: null, lastEventAt: 0, lastStatusAt: 0, openedAt: 0, lastEventId: null, retryMs: 3000, watchdog: null, pollTimer: null, pollRequest: null, pollController: null, epoch: 0, reconnectTimer: null, reconnectStartedAt: null, suspended: false };
const STARTUP_ESTIMATE_STORAGE_KEY = "helios-startup-estimate";
// Experimental opt-in estimate: the fill never reaches 100 unless the
// authoritative readiness gate (running + healthy + required ports + owner)
// is satisfied by the current status.
const STARTUP_ESTIMATE_CAP = 95;
const STARTUP_ESTIMATE_TICK_MS = 1000;
// Successful starts recorded for one profile and version before a numeric
// median exists (mirrors the controller's MIN_SAMPLES).
const STARTUP_ESTIMATE_MIN_SAMPLES = 5;
// `null` means this tab has not made a choice yet, so the stored value (and any
// cross-tab change) is authoritative. A boolean is this tab's own choice, kept
// even when the storage write itself was denied.
let startupEstimatePreferred = null;
const startupEstimate = { profileId: null, attemptId: null, version: null, serverSample: null, ready: false, anchoredAt: 0, elapsedAtAnchor: null, medianSeconds: null, sampleCount: null, timer: null };
let updatePollDelay = 1200;
let updatePollAttempts = 12;
let reconnectJitter = () => Math.random() * 1000;
const MAX_STREAM_CURSOR = 9007199254740991n;
const canonicalStreamCursor = (raw) => {
  if (typeof raw !== "string" || raw.length === 0 || raw.length > 16 || !/^(0|[1-9][0-9]*)$/.test(raw)) return null;
  try { const value = BigInt(raw); return value <= MAX_STREAM_CURSOR ? raw : null; } catch { return null; }
};
function startOperationStorageKey(profileId) {
  return `horizon-operation:POST:/api/v1/profiles/${encodeURIComponent(profileId)}/start:{}`;
}

function retireResolvedStartKey(profileId) {
  try { sessionStorage.removeItem(startOperationStorageKey(profileId)); } catch {}
}
const recordStreamCursor = (source, event) => {
  if (stream.suspended || stream.source !== source) return;
  const cursor = canonicalStreamCursor(event?.lastEventId);
  if (cursor === null || (stream.lastEventId !== null && BigInt(cursor) <= BigInt(stream.lastEventId))) return;
  stream.lastEventId = cursor;
};

const pageVisible = () => document.visibilityState === "visible";
const CLIENT_PERF_METRICS = new Set(["stats_fetch", "recorder_draw", "first_status_paint"]);

function queueClientPerformance(metric, durationMs) {
  const duration = Number(durationMs);
  if (!pageVisible() || !CLIENT_PERF_METRICS.has(metric) || !Number.isFinite(duration) || duration < 0) return;
  state.perf.clientQueue.push({ metric, duration_ms: Math.min(30000, duration) });
  state.perf.clientQueue = state.perf.clientQueue.slice(-16);
  if (state.perf.clientTimer) return;
  state.perf.clientTimer = window.setTimeout(flushClientPerformance, 1000);
}

async function flushClientPerformance() {
  state.perf.clientTimer = null;
  if (!pageVisible() || !state.perf.clientQueue.length) return;
  const samples = state.perf.clientQueue.splice(0, 16);
  try { await api("/api/v1/perf/client", { method: "POST", body: JSON.stringify({ samples }) }); } catch {}
}

const byId = (id) => document.getElementById(id);
const cards = byId("profile-cards");
const announcer = byId("status-announcer");
const profileLabel = (id) => state.profiles.get(id)?.display_name || id;
const titleCase = (value) => String(value || "unknown").replaceAll("_", " ").replace(/\b\w/g, (char) => char.toUpperCase());
const statusLabel = (value) => {
  const label = value === "updating" ? "Updating" : titleCase(value);
  return ["starting", "stopping"].includes(value) ? `${label}…` : label;
};
const slotOwnerId = () => [...state.statuses.values()].find((status) => status?.slot_owner)?.slot_owner || null;

// Root-owned update activity.  ``update`` is the authoritative projection of a
// live updater reservation or an accepted/running update job; ``active_job_id``
// keeps older snapshots working without guessing from rendered text.
function updateActive(status) {
  return Boolean(status?.update) || status?.active_job_id === "update";
}

function displayState(status) {
  return updateActive(status) ? "updating" : status?.state || "unknown";
}

// One slot means one updater: while any profile holds a live update, every
// Start entrypoint stays paused (the server keeps its own fence as well).
function updateOwner() {
  for (const [id, status] of state.statuses) if (updateActive(status)) return id;
  return null;
}

function updateNotice(name = null) {
  return name
    ? `Updating ${name}… Start is unavailable until the update finishes.`
    : "Updating… Start is unavailable until the update finishes.";
}

function applyTheme(value, persist = false) {
  const theme = THEMES.includes(value) ? value : "ember";
  document.documentElement.dataset.theme = theme;
  const colorScheme = byId("color-scheme-meta") || document.querySelector('meta[name="color-scheme"]');
  if (colorScheme) colorScheme.setAttribute("content", theme === "paper" ? "light" : "dark");
  if (persist) {
    try { localStorage.setItem("helios-theme", theme); } catch {}
  }
  const picker = byId("theme-picker");
  if (picker && picker.value !== theme) picker.value = theme;
}

function loadTheme() {
  let stored = null;
  try { stored = localStorage.getItem("helios-theme"); } catch {}
  applyTheme(stored, false);
}

function updateServerNav() {
  const nav = byId("server-nav");
  const owner = [...state.statuses.values()].find((status) => status?.slot_owner)?.slot_owner || null;
  state.profiles.forEach((profile, id) => {
    let item = nav.querySelector(`[data-profile-nav="${CSS.escape(id)}"]`);
    if (!item) {
      item = document.createElement("a");
      item.className = "nav-item nav-server";
      item.href = `#/servers/${encodeURIComponent(id)}/console`;
      item.dataset.profileNav = id;
      item.innerHTML = '<span class="server-dot" aria-hidden="true"></span><span data-profile-label></span>';
      nav.append(item);
    }
    const label = item.querySelector("[data-profile-label]");
    if (label) label.textContent = profile?.display_name || id;
    const ownerStatus = owner === id ? state.statuses.get(id) : null;
    const dot = item.querySelector(".server-dot");
    dot?.classList.toggle("is-owner", owner === id);
    dot?.classList.toggle("is-transitional", ["starting", "stopping"].includes(ownerStatus?.state));
  });
}

function setDrawer(open, returnFocus = null) {
  const sidebar = byId("sidebar");
  const overlay = byId("drawer-overlay");
  const burger = byId("menu-toggle");
  const close = byId("drawer-close");
  const mobile = window.matchMedia("(max-width: 760px)").matches;
  const isOpen = Boolean(open && mobile);
  const visible = mobile ? isOpen : true;
  if (isOpen && returnFocus) state.drawerReturnFocus = returnFocus;
  sidebar.setAttribute("aria-hidden", String(!visible));
  sidebar.inert = mobile && !isOpen;
  overlay.hidden = !isOpen;
  burger.setAttribute("aria-expanded", String(isOpen));
  document.body.classList.toggle("drawer-open", isOpen);
  if (isOpen) {
    const first = close || sidebar.querySelector("a, button, input, select, textarea, [tabindex]:not([tabindex='-1'])");
    window.requestAnimationFrame(() => first?.focus());
  } else {
    const focus = state.drawerReturnFocus || returnFocus || burger;
    if (focus && document.contains(focus)) focus.focus();
  }
  if (!isOpen) state.drawerReturnFocus = null;
}

function showView(view) {
  const dashboard = byId("dashboard-view");
  const settings = byId("settings-view");
  const detail = byId("detail-view");
  const backups = byId("backups-view");
  const events = byId("events-view");
  const audit = byId("audit-view");
  const settingsActive = view === "settings";
  const detailActive = view === "detail";
  const backupsActive = view === "backups";
  const eventsActive = view === "events";
  const auditActive = view === "audit";
  settings.hidden = !settingsActive;
  detail.hidden = !detailActive;
  backups.hidden = !backupsActive;
  events.hidden = !eventsActive;
  audit.hidden = !auditActive;
  dashboard.hidden = settingsActive || detailActive || backupsActive || eventsActive || auditActive;
  document.querySelectorAll("[data-view]").forEach((item) => item.classList.toggle("is-active", item.dataset.view === view || (!settingsActive && !detailActive && !backupsActive && !eventsActive && !auditActive && item.dataset.view === "dashboard")));
  document.querySelectorAll("[data-profile-nav]").forEach((item) => item.classList.toggle("is-active", detailActive && item.dataset.profileNav === state.detail.id));
}

function setupShell() {
  loadTheme();
  const sidebar = byId("sidebar");
  const media = window.matchMedia("(max-width: 760px)");
  const overlay = byId("drawer-overlay");
  const burger = byId("menu-toggle");
  const syncBreakpoint = () => {
    const mobile = media.matches;
    sidebar.inert = mobile;
    sidebar.setAttribute("aria-hidden", String(mobile));
    overlay.hidden = true;
    burger.setAttribute("aria-expanded", "false");
    document.body.classList.remove("drawer-open");
    state.drawerReturnFocus = null;
  };
  syncBreakpoint();
  if (media.addEventListener) media.addEventListener("change", syncBreakpoint);
  else media.addListener?.(syncBreakpoint);
  window.addEventListener("resize", syncBreakpoint);
  byId("menu-toggle").addEventListener("click", (event) => setDrawer(true, event.currentTarget));
  byId("drawer-close").addEventListener("click", () => setDrawer(false));
  byId("drawer-overlay").addEventListener("click", () => setDrawer(false));
  byId("servers-toggle").addEventListener("click", (event) => {
    const expanded = event.currentTarget.getAttribute("aria-expanded") !== "true";
    event.currentTarget.setAttribute("aria-expanded", String(expanded));
    byId("server-nav").hidden = !expanded;
  });
  document.querySelectorAll("[data-view]").forEach((item) => item.addEventListener("click", () => {
    const view = ["settings", "backups", "events", "audit"].includes(item.dataset.view) ? item.dataset.view : "dashboard";
    if (view === "settings") window.location.hash = "#/settings";
    else if (view === "backups") window.location.hash = "#/backups";
    else if (view === "events") window.location.hash = "#/events";
    else if (view === "audit") window.location.hash = "#/audit";
    else window.location.hash = "#/";
    showView(view);
    if (media.matches) setDrawer(false);
  }));
  byId("server-nav").addEventListener("click", (event) => {
    if (!event.target.closest("[data-profile-nav]")) return;
    const id = event.target.closest("[data-profile-nav]").dataset.profileNav;
    window.location.hash = `#/servers/${encodeURIComponent(id)}/console`;
    if (media.matches) setDrawer(false);
  });
  byId("theme-picker").addEventListener("change", (event) => applyTheme(event.target.value, true));
  const estimateToggle = byId("startup-estimate-toggle");
  if (estimateToggle) {
    syncStartupEstimateToggle();
    estimateToggle.addEventListener("change", (event) => setStartupEstimateEnabled(event.target.checked));
  }
  window.addEventListener("storage", (event) => {
    if (!isStartupEstimateStorageEvent(event)) return;
    // Another same-origin tab (or a storage clear) is authoritative: drop any
    // choice this tab could not persist, then re-render from the stored value
    // so the toggle and the track never disagree with each other.
    startupEstimatePreferred = null;
    syncStartupEstimateToggle();
    patchActiveSlot();
  });
  byId("notification-profile").addEventListener("change", (event) => loadNotifications(event.target.value));
  document.querySelectorAll("[data-notification-test]").forEach((button) => button.addEventListener("click", () => testNotification(button.dataset.notificationTest)));
  byId("idle-stop-form").addEventListener("submit", saveIdleStop);
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !sidebar.inert && window.matchMedia("(max-width: 760px)").matches) setDrawer(false);
  });
}

function notify(message) {
  if (message === SESSION_EXPIRED_MESSAGE && sessionNoticeShown) return;
  if (message === SESSION_EXPIRED_MESSAGE) sessionNoticeShown = true;
  announcer.textContent = message;
  const toast = byId("toast-region");
  toast.textContent = message;
  window.setTimeout(() => { if (toast.textContent === message) toast.textContent = ""; }, 5000);
}

function expireSession() {
  if (sessionExpired) return;
  sessionExpired = true;
  sessionExpiryCount += 1;
  reauthenticating = true;
  suspendStream();
  notify(SESSION_EXPIRED_MESSAGE);
  let redirected = false;
  try { redirected = sessionStorage.getItem(SESSION_REDIRECT_KEY) === "1"; sessionStorage.setItem(SESSION_REDIRECT_KEY, "1"); } catch {}
  if (!redirected && !window.__HORIZON_TEST__?.preventNavigation) window.setTimeout(() => window.location.assign("/"), 0);
}

function clearLoadRetry() {
  if (loadRetryTimer) { window.clearTimeout(loadRetryTimer); loadRetryTimer = null; }
}

function scheduleLoadRetry() {
  if (loadRetryTimer || sessionExpired || !pageVisible()) return;
  loadRetryTimer = window.setTimeout(() => {
    loadRetryTimer = null;
    if (!pageVisible()) { scheduleLoadRetry(); return; }
    void load().finally(route);
  }, 5000);
}

async function fetchSessionBootstrap() {
  let lastError = null;
  let sawAuthRejection = false;
  let sawTransientFailure = false;
  const deadline = Date.now() + SESSION_BOOTSTRAP_DEADLINE_MS;
  for (let attempt = 0; attempt < SESSION_BOOTSTRAP_DELAYS.length; attempt += 1) {
    if (SESSION_BOOTSTRAP_DELAYS[attempt]) {
      await new Promise((resolve) => setTimeout(resolve, Math.min(SESSION_BOOTSTRAP_DELAYS[attempt], Math.max(0, deadline - Date.now()))));
    }
    if (Date.now() >= deadline) break;
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), Math.min(SESSION_BOOTSTRAP_TIMEOUT_MS, Math.max(1, deadline - Date.now())));
    try {
      const response = await fetch("/api/v1/session", {
        credentials: "same-origin",
        cache: "no-store",
        headers: { Accept: "application/json" },
        redirect: "manual",
        signal: controller.signal,
      });
      if (response.type === "opaqueredirect") { sawAuthRejection = true; lastError = new Error("Session redirect received."); continue; }
      if (response.ok) {
        // Fetch's timeout does not cover response.json(): a server can send
        // headers and then leave the body open forever. Race body parsing with
        // the same bounded attempt deadline and abort the transport too.
        const remaining = Math.max(1, deadline - Date.now());
        let bodyTimer;
        try {
          const bodyRead = response.json();
          const bodyTimeout = new Promise((_, reject) => {
            bodyTimer = window.setTimeout(() => {
              controller.abort();
              reject(new Error("Session bootstrap response timed out."));
            }, remaining);
          });
          const body = await Promise.race([bodyRead, bodyTimeout]);
          if (!body || typeof body !== "object" || typeof body.csrf_token !== "string") throw new Error("Invalid session bootstrap.");
          return body;
        } finally {
          if (bodyTimer) window.clearTimeout(bodyTimer);
        }
      }
      lastError = new Error(`Session bootstrap failed (${response.status}).`);
      if ([401, 403].includes(response.status)) sawAuthRejection = true;
      else if ([408, 429, 502, 503, 504].includes(response.status)) sawTransientFailure = true;
      if (![401, 403, 502, 503, 504].includes(response.status)) break;
    } catch (error) {
      lastError = error;
      sawTransientFailure = true;
    } finally {
      window.clearTimeout(timer);
    }
  }
  const failure = lastError || new Error("Session bootstrap failed.");
  // A network/timeout or gateway result mixed with an auth response is
  // recoverable: only an all-auth exhaustion is terminal.
  failure.authRejected = sawAuthRejection && !sawTransientFailure;
  throw failure;
}

function refreshSession() {
  if (sessionExpired) return Promise.reject(new Error(SESSION_EXPIRED_MESSAGE));
  if (sessionRefreshPromise) return sessionRefreshPromise;
  sessionRefreshPromise = fetchSessionBootstrap().then((body) => {
    state.actor = body.actor || state.actor;
    state.csrf = body.csrf_token || state.csrf;
    sessionGeneration += 1;
    reauthenticating = false;
    return body;
  }).catch((error) => {
    // A genuine exhausted auth rejection is terminal. Gateway/network failure
    // remains recoverable by the bounded retry and online/focus/load timer.
    if (error?.authRejected) {
      expireSession();
      throw new Error(SESSION_EXPIRED_MESSAGE, { cause: error });
    }
    throw error;
  }).finally(() => { sessionRefreshPromise = null; });
  return sessionRefreshPromise;
}

function stateClass(value) {
  return `state-${String(value || "unknown").replace(/[^a-z0-9-]/g, "")}`;
}

function uptime(seconds) {
  if (!Number.isFinite(Number(seconds))) return "—";
  const total = Math.max(0, Number(seconds));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  return hours ? `${hours}h ${minutes}m` : `${minutes}m`;
}

function lifecycleOffline(status) {
  return Boolean(state.statusConfirmed && status?.state === "stopped");
}

function formatBytes(bytes) {
  if (bytes == null || bytes === "" || typeof bytes === "boolean") return "—";
  if (!["number", "string"].includes(typeof bytes)) return "—";
  const value = typeof bytes === "string" && !bytes.trim() ? NaN : Number(bytes);
  if (!Number.isFinite(value) || value < 0) return "—";
  return `${(value / 1073741824).toFixed(1)} GB`;
}

function formatRate(value) {
  if (value == null || !Number.isFinite(Number(value))) return "Unavailable";
  const units = ["B/s", "KiB/s", "MiB/s", "GiB/s"];
  let amount = Math.max(0, Number(value));
  let index = 0;
  while (amount >= 1024 && index < units.length - 1) { amount /= 1024; index += 1; }
  return `${amount >= 100 ? amount.toFixed(0) : amount.toFixed(1)} ${units[index]}`;
}

function formatVersion(version) {
  if (version == null) return "—";
  const value = String(version).trim();
  const sentinel = ["", "false", "none", "null", "undefined", "unknown", "n/a", "na", "unavailable", "not available"];
  return sentinel.includes(value.toLowerCase()) ? "—" : value;
}

function createCard(id) {
  const card = document.createElement("article");
  card.className = "profile-card";
  card.dataset.profileId = id;
  card.innerHTML = `
    <div class="card-topline"><span class="profile-kicker"></span><span class="status-badge"><span class="status-dot" aria-hidden="true"></span><span class="status-text"></span></span></div>
    <h3 class="profile-name"></h3>
    <p class="profile-description"></p>
    <svg class="cpu-sparkline" viewBox="0 0 120 38" role="img" aria-label="CPU usage unavailable">
      <title>CPU usage</title><polyline class="cpu-sparkline-line" points="0,36 120,36"></polyline>
    </svg>
    <dl class="card-metrics">
      <div><dt>Players</dt><dd class="metric-players">—</dd></div>
      <div><dt>CPU</dt><dd class="metric-cpu">—</dd></div>
      <div><dt>Memory</dt><dd class="metric-memory">—</dd></div>
      <div><dt>Version</dt><dd class="metric-version">—</dd></div>
    </dl>
    <p class="card-reason" role="note"></p>
    <div class="card-actions">
      <button class="button button-small action-start" type="button">Start</button>
      <button class="button button-small button-quiet action-stop" type="button">Stop</button>
      <button class="button button-small button-quiet action-restart" type="button">Restart</button>
      <button class="button button-small button-quiet action-switch" data-action="switch" type="button">Switch</button>
      <a class="button button-small button-quiet manage-link" href="#server">Manage →</a>
    </div>`;
  cards.append(card);
  card.querySelector(".action-start").addEventListener("click", () => mutate(id, "start"));
  card.querySelector(".action-stop").addEventListener("click", () => mutate(id, "stop"));
  card.querySelector(".action-restart").addEventListener("click", () => mutate(id, "restart"));
  card.querySelector(".action-switch").addEventListener("click", (event) => openSwitchDialog(id, event.currentTarget));
  return card;
}

function profileOrder() {
  const known = PROFILE_FALLBACK.map(([id]) => id);
  const future = [...state.profiles.keys()].filter((id) => !known.includes(id));
  return [...known.filter((id) => state.profiles.has(id)), ...future];
}

function profileFamily(profileOrId) {
  const id = String(profileOrId?.id ?? profileOrId ?? "");
  const adapter = String(profileOrId?.adapter?.value ?? profileOrId?.adapter ?? "");
  return PROFILE_FAMILY_RULES.find((rule) => adapter === rule.adapter && id.startsWith(rule.prefix)) || {
    key: "other",
    label: "Other",
  };
}

function createFamilySection(family) {
  const section = document.createElement("section");
  section.className = "profile-family";
  section.dataset.profileFamily = family.key;
  const headingId = `profile-family-${family.key}-title`;
  section.innerHTML = `
    <div class="profile-family-header">
      <div class="profile-family-heading"><h3 id="${headingId}" class="profile-family-name"></h3><span class="profile-family-count" data-family-count></span></div>
      <span class="profile-family-owner" data-family-owner></span>
    </div>
    <div class="cards profile-family-cards" aria-labelledby="${headingId}"></div>`;
  section.querySelector(".profile-family-name").textContent = family.label;
  return section;
}

function patchFamilyHeaders() {
  const owner = slotOwnerId();
  cards.querySelectorAll("[data-profile-family]").forEach((section) => {
    const members = [...section.querySelectorAll("[data-profile-id]")];
    const ownerMember = members.find((card) => card.dataset.profileId === owner);
    section.classList.toggle("is-singleton", members.length === 1);
    section.querySelector("[data-family-count]").textContent = `${members.length} member${members.length === 1 ? "" : "s"}`;
    section.querySelector("[data-family-owner]").textContent = ownerMember ? `Slot: ${profileLabel(owner)}` : "Slot: —";
  });
}

function renderCards() {
  cards.querySelectorAll("[data-skeleton]").forEach((placeholder) => placeholder.remove());
  const sections = new Map();
  profileOrder().forEach((id) => {
    const family = profileFamily(state.profiles.get(id) || id);
    let section = sections.get(family.key);
    if (!section) {
      section = cards.querySelector(`[data-profile-family="${CSS.escape(family.key)}"]`) || createFamilySection(family);
      sections.set(family.key, section);
    }
    const card = cards.querySelector(`[data-profile-id="${CSS.escape(id)}"]`) || createCard(id);
    section.querySelector(".profile-family-cards").append(card);
    patchCard(id);
  });
  const orderedSections = PROFILE_FAMILY_RULES.map((rule) => sections.get(rule.key)).filter(Boolean);
  if (sections.has("other")) orderedSections.push(sections.get("other"));
  cards.replaceChildren(...orderedSections);
  patchFamilyHeaders();
}

function appendCpuSample(id, status) {
  const cpu = Number(status?.cpu_percent);
  if (!Number.isFinite(cpu) || cpu < 0 || ["stopped", "unknown"].includes(status?.state)) {
    state.cpuSamples.set(id, [0]);
    return;
  }
  const samples = state.cpuSamples.get(id) || [];
  samples.push(cpu);
  if (samples.length > SAMPLE_LIMIT) samples.splice(0, samples.length - SAMPLE_LIMIT);
  state.cpuSamples.set(id, samples);
}

function patchSparkline(card, id, status) {
  const samples = state.cpuSamples.get(id) || [0];
  const max = Math.max(100, ...samples);
  const width = 120;
  const height = 38;
  const points = samples.map((sample, index) => {
    const x = samples.length === 1 ? 0 : (index / (samples.length - 1)) * width;
    const y = height - 2 - (Math.min(max, Math.max(0, sample)) / max) * (height - 4);
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  }).concat(samples.length === 1 ? [`${width.toFixed(2)},${(height - 2).toFixed(2)}`] : []).join(" ");
  const line = card.querySelector(".cpu-sparkline-line");
  line.setAttribute("points", points);
  const current = Number(status?.cpu_percent);
  card.querySelector(".cpu-sparkline").setAttribute(
    "aria-label",
    Number.isFinite(current) && current >= 0 ? `CPU usage ${current.toFixed(1)} percent` : "CPU usage 0 percent",
  );
}

function patchCard(id) {
  const card = cards.querySelector(`[data-profile-id="${CSS.escape(id)}"]`) || createCard(id);
  const profile = state.profiles.get(id) || { id, display_name: id, operations: [] };
  const status = state.statuses.get(id) || { profile_id: id, state: "unknown", health: "unknown" };
  const display = profile.display_name || id;
  const rawCurrent = status.state || "unknown";
  const ownUpdate = updateActive(status);
  const ownerUpdate = updateOwner();
  const updating = ownUpdate || Boolean(ownerUpdate && ownerUpdate !== id);
  const current = updating ? "updating" : rawCurrent;
  const operationSet = new Set(profile.operations || ["start", "stop", "restart"]);
  card.classList.toggle("is-active", status.slot_owner === id);
  card.classList.remove("state-running", "state-starting", "state-stopping", "state-stopped", "state-blocked", "state-failed", "state-unknown", "state-updating");
  card.classList.add(stateClass(current));
  card.classList.toggle("is-updating", ownUpdate);
  card.querySelector(".profile-kicker").textContent = id;
  card.querySelector(".profile-name").textContent = display;
  card.querySelector(".profile-description").textContent = profile.public_endpoint?.host || "private";
  card.querySelector(".status-badge").className = `status-badge ${stateClass(current)}`;
  card.querySelector(".status-text").textContent = statusLabel(current);
  card.querySelector(".metric-players").textContent = lifecycleOffline(status)
    ? "Offline"
    : status.players_online == null ? "—" : `${status.players_online} player${status.players_online === 1 ? "" : "s"}`;
  card.querySelector(".metric-cpu").textContent = status.cpu_percent == null ? "—" : `${Number(status.cpu_percent).toFixed(1)}%`;
  card.querySelector(".metric-memory").textContent = formatBytes(status.rss_bytes);
  card.querySelector(".metric-version").textContent = formatVersion(status.installed_version);
  patchSparkline(card, id, status);
  const readiness = status.required_ports_ready ? "ready" : "process accepted; waiting for required ports";
  const reason = !state.statusConfirmed ? "Refreshing current status; actions are paused." :
    ownUpdate ? updateNotice() :
    updating ? updateNotice(profileLabel(ownerUpdate)) :
    current === "blocked" ? "Blocked: another server owns the active slot. Switch active server…" :
    current === "failed" ? "Previous health check failed; review details before starting." :
      current === "starting" ? (status.pid != null ? `Starting: process detected; ${readiness}.` : "Starting: request accepted; waiting for the server process.") :
        current === "stopping" ? "Stopping: actions are paused until shutdown completes." : "";
  card.querySelector(".card-reason").textContent = reason;
  const buttons = [
    [".action-start", "Start", "start", updating || !state.statusConfirmed || !["stopped", "failed", "blocked", "unknown"].includes(current) || !operationSet.has("start") || current === "blocked"],
    [".action-stop", "Stop", "stop", updating || !state.statusConfirmed || !["running", "starting"].includes(current) || !operationSet.has("stop")],
    [".action-restart", "Restart", "restart", updating || !state.statusConfirmed || current !== "running" || !operationSet.has("restart")],
  ];
  buttons.forEach(([selector, label, operation, disabled]) => {
    const button = card.querySelector(selector);
    button.textContent = label;
    button.disabled = Boolean(disabled);
    button.hidden = operation === "start"
      ? !["stopped", "failed", "blocked", "unknown"].includes(rawCurrent)
      : !["running", "starting"].includes(current);
    const why = reason ? ` (${reason})` : "";
    button.setAttribute("aria-label", `${label} ${display}`);
    button.title = button.disabled ? (why || "Action is unavailable in this state.") : "";
  });
  const switchButton = card.querySelector(".action-switch");
  switchButton.disabled = status.slot_owner === id || updating;
  switchButton.setAttribute("aria-label", `Switch to ${display}`);
  switchButton.title = switchButton.disabled
    ? (updating ? "An update is in progress; switching is paused." : "This server already owns the active slot.")
    : "Preselect this server in the switch dialog.";
  const manage = card.querySelector(".manage-link");
  manage.href = `#/servers/${encodeURIComponent(id)}/console`;
  manage.setAttribute("aria-label", `Manage ${display}`);
  updateServerNav();
}

function patchActiveSlot() {
  const targetId = sessionProfileId();
  const target = targetId ? state.statuses.get(targetId) : null;
  const profile = targetId ? state.profiles.get(targetId) : null;
  const ownerId = slotOwnerId();
  const slot = byId("active-slot");
  const title = byId("active-slot-title");
  const manage = byId("active-manage");
  const primary = byId("session-primary");
  if (!targetId || !target) {
    title.textContent = "Status unavailable";
    byId("active-slot-summary").textContent = "Horizon cannot identify the primary game profile.";
    byId("active-players").textContent = "—";
    byId("active-uptime").textContent = "—";
    byId("active-health").textContent = "—";
    byId("session-endpoint").textContent = "Unavailable";
    byId("session-copy-endpoint").disabled = true;
    primary.textContent = "Retry status";
    primary.dataset.sessionAction = "retry";
    primary.disabled = false;
    slot.classList.add("is-empty");
    slot.classList.remove("is-transitional");
    manage.hidden = true;
    patchSessionRunway(null, null);
    patchStartupEstimate(null, null);
    return;
  }
  const name = profileLabel(targetId);
  const endpoint = sessionEndpoint(profile);
  const copy = byId("session-copy-endpoint");
  title.textContent = name;
  byId("session-endpoint").textContent = endpoint || "Unavailable";
  copy.dataset.endpoint = endpoint || "";
  copy.setAttribute("aria-label", `Copy ${name} join address`);
  const offline = lifecycleOffline(target);
  const updating = updateActive(target);
  byId("active-players").textContent = offline ? "Offline" : target.players_online == null ? "Not observed" : String(target.players_online);
  byId("active-uptime").textContent = offline ? "Offline" : uptime(target.uptime_seconds);
  byId("active-health").textContent = offline ? "Offline" : titleCase(target.health);
  slot.classList.toggle("is-empty", target.state === "stopped" || !ownerId);
  slot.classList.toggle("is-transitional", ["starting", "stopping"].includes(target.state) || updating);
  manage.href = `#/servers/${encodeURIComponent(targetId)}/console`;
  manage.setAttribute("aria-label", `Manage ${name}`);
  byId("session-activity-link").href = `#/servers/${encodeURIComponent(targetId)}/stats`;
  byId("session-backups-link").href = `#/servers/${encodeURIComponent(targetId)}/backups`;
  const model = sessionModel(target, ownerId, targetId, endpoint);
  manage.hidden = model.action === "console";
  copy.disabled = !state.statusConfirmed || !endpoint || model.action !== "copy";
  byId("active-slot-summary").textContent = state.statusConfirmed
    ? model.summary
    : "Confirming current server status. Actions remain paused until Horizon responds.";
  primary.textContent = state.statusConfirmed ? model.actionLabel : "Checking status…";
  primary.dataset.sessionAction = state.statusConfirmed ? model.action : "none";
  primary.dataset.sessionProfileId = targetId;
  primary.disabled = !state.statusConfirmed || model.action === "none";
  primary.dataset.updating = updating ? "true" : "false";
  patchSessionRunway(target, ownerId);
  patchStartupEstimate(target, ownerId);
  if (state.session.profileId !== targetId) {
    state.session = { profileId: targetId, latestBackup: null, backupState: "loading", operation: null };
    loadSessionBackup(targetId);
  }
  patchSessionBackup();
  patchSessionOperation();
}

function sessionProfileId() {
  const owner = slotOwnerId();
  if (owner && state.profiles.has(owner)) return owner;
  if (state.profiles.has("minecraft-sunlit-cobblemon")) return "minecraft-sunlit-cobblemon";
  const minecraft = [...state.profiles].find(([id, profile]) => id.startsWith("minecraft") || String(profile?.adapter?.value || profile?.adapter || "") === "crafty");
  return minecraft?.[0] || slotOwnerId() || state.profiles.keys().next().value || null;
}

function sessionEndpoint(profile) {
  const endpoint = profile?.public_endpoint;
  const host = typeof endpoint === "string" ? endpoint : endpoint?.host;
  if (!host) return "";
  const port = Number(endpoint?.port);
  return Number.isInteger(port) && port > 0 && port !== 25565 ? `${host}:${port}` : String(host);
}

function sessionModel(status, ownerId, targetId, endpoint) {
  const current = status?.state || "unknown";
  const ownerUpdate = updateOwner();
  if (updateActive(status) || ownerUpdate) {
    return {
      summary: updateNotice(ownerUpdate && ownerUpdate !== targetId ? profileLabel(ownerUpdate) : null),
      action: "none",
      actionLabel: "Updating…",
    };
  }
  const conflict = ownerId && ownerId !== targetId;
  if (conflict || current === "blocked") {
    const owner = ownerId ? profileLabel(ownerId) : "another server";
    return { summary: `${owner} owns the active slot. Review the switch before starting ${profileLabel(targetId)}.`, action: "switch", actionLabel: "Review switch" };
  }
  if (current === "stopped") return { summary: "Offline. Horizon can start it now.", action: "start", actionLabel: `Start ${profileLabel(targetId)}` };
  if (current === "starting") {
    const processSeen = status.pid != null;
    return {
      summary: processSeen ? "The server process is loading. Waiting for the game port." : "Start accepted. Waiting for the server process.",
      action: "console",
      actionLabel: "Open console",
    };
  }
  const ownedAndHealthy = ownerId === targetId && status.health === "healthy";
  if (current === "running" && status.required_ports_ready && ownedAndHealthy) return { summary: "The server is healthy and its game port is ready.", action: endpoint ? "copy" : "console", actionLabel: endpoint ? "Copy join address" : "Open console" };
  if (current === "running" && ownerId !== targetId) return { summary: "The process is running without active-slot ownership. Review events before joining.", action: "events", actionLabel: "Review ownership" };
  if (current === "running" && status.required_ports_ready && status.health !== "healthy") return { summary: `The game port is open, but health is ${String(status.health || "unknown")}.`, action: "diagnose", actionLabel: "Diagnose health" };
  if (current === "running") return { summary: "The server process is running, but Horizon cannot confirm game readiness.", action: "diagnose", actionLabel: "Diagnose readiness" };
  if (current === "stopping") return { summary: "Ending the session safely. Actions are paused until shutdown completes.", action: "console", actionLabel: "Open console" };
  if (current === "failed") return { summary: "The last operation failed. Horizon will preserve the evidence if you retry.", action: "start", actionLabel: "Retry start" };
  return { summary: "Horizon cannot currently verify this server.", action: "retry", actionLabel: "Retry status" };
}

function patchSessionRunway(status, ownerId) {
  const phases = [...byId("session-runway").querySelectorAll("[data-session-phase]")];
  const current = status?.state || "unknown";
  const conflict = ownerId && status?.profile_id && ownerId !== status.profile_id;
  const complete = {
    request: ["starting", "running", "stopping"].includes(current),
    process: current === "running",
    ready: current === "running" && Boolean(status?.required_ports_ready) && status?.health === "healthy" && ownerId === status?.profile_id,
  };
  const active = current === "starting" ? "process" : current === "stopping" ? "request" : null;
  phases.forEach((phase) => {
    const key = phase.dataset.sessionPhase;
    let next = complete[key] ? "complete" : "waiting";
    if (key === active) next = "active";
    if (conflict && key === "request") next = "conflict";
    if (current === "failed" && key === "request") next = "failed";
    phase.dataset.state = next;
    if (next === "active") phase.setAttribute("aria-current", "step"); else phase.removeAttribute("aria-current");
    const detail = phase.querySelector("small");
    detail.textContent = next === "complete" ? "complete" : next === "active" ? "in progress" : next === "failed" ? "failed" : next === "conflict" ? "blocked" : "waiting";
    phase.setAttribute("aria-label", `${phase.querySelector("strong").textContent}, ${detail.textContent}`);
  });
  const completed = phases.filter((phase) => phase.dataset.state === "complete").length;
  const activePhase = phases.find((phase) => phase.dataset.state === "active");
  const failedPhase = phases.find((phase) => ["failed", "conflict"].includes(phase.dataset.state));
  byId("session-readiness-summary").textContent = failedPhase
    ? `Readiness: start ${failedPhase.dataset.state === "conflict" ? "blocked" : "failed"}`
    : activePhase
      ? `Readiness: ${completed} of ${phases.length} stages complete · ${activePhase.querySelector("strong").textContent.toLowerCase()} in progress`
      : `Readiness: ${completed} of ${phases.length} stages complete`;
}

function startupEstimateEnabled() {
  // An explicit choice in this tab wins: when the storage write was denied the
  // toggle must still control the feature for this page instead of silently
  // losing to a stale stored value.
  if (startupEstimatePreferred !== null) return startupEstimatePreferred;
  try {
    const stored = localStorage.getItem(STARTUP_ESTIMATE_STORAGE_KEY);
    if (stored === "1") return true;
    if (stored === "0") return false;
  } catch {}
  return false;
}

function setStartupEstimateEnabled(enabled) {
  startupEstimatePreferred = Boolean(enabled);
  try { localStorage.setItem(STARTUP_ESTIMATE_STORAGE_KEY, enabled ? "1" : "0"); } catch {}
  syncStartupEstimateToggle();
  patchActiveSlot();
}

function syncStartupEstimateToggle() {
  const toggle = byId("startup-estimate-toggle");
  if (toggle) toggle.checked = startupEstimateEnabled();
}

function isStartupEstimateStorageEvent(event) {
  // sessionStorage events from other frames are unrelated to this opt-in, and
  // the localStorage getter itself can throw in restricted contexts.
  let area = null;
  try { area = localStorage; } catch {}
  // Real localStorage events (including clear, which carries a null key) name
  // localStorage as their area; without a readable area nothing is
  // authoritative, so synthetic area-less events are ignored.
  if (!area || event.storageArea !== area) return false;
  return event.key === null || event.key === STARTUP_ESTIMATE_STORAGE_KEY;
}

function startupEstimateStatusFresh() {
  // The estimate is a read-only projection, so it does not use the stricter
  // action gate `state.statusConfirmed`; instead a sample must be fresh AND
  // accepted after the last hide/disconnect/resume fence, and the page must
  // still hold a live visible stream.
  if (!pageVisible() || stream.suspended || sessionExpired) return false;
  if (state.statusSeq <= state.estimateFenceSeq) return false;
  const at = state.lastStatusAt;
  return Boolean(at) && Date.now() - at < STATUS_STALE_MS;
}

function fenceStartupEstimate() {
  // Called whenever cached statuses stop being proof of current state: the tab
  // went hidden, the stream dropped, or a resume started.
  state.estimateFenceSeq = state.statusSeq;
  clearStartupEstimate();
}

function clearStartupEstimate() {
  if (startupEstimate.timer) { window.clearInterval(startupEstimate.timer); startupEstimate.timer = null; }
  startupEstimate.profileId = null;
  startupEstimate.attemptId = null;
  startupEstimate.version = null;
  startupEstimate.serverSample = null;
  startupEstimate.ready = false;
  startupEstimate.anchoredAt = 0;
  startupEstimate.elapsedAtAnchor = null;
  startupEstimate.medianSeconds = null;
  startupEstimate.sampleCount = null;
  const node = byId("startup-estimate");
  if (node) node.hidden = true;
}

function formatEstimateDuration(seconds) {
  const total = Math.max(0, Math.round(seconds));
  if (total < 60) return `${total}s`;
  const minutes = Math.floor(total / 60);
  const rest = total % 60;
  return minutes < 10 && rest ? `${minutes}m ${rest}s` : `${minutes}m`;
}

function renderStartupEstimate(ready = false) {
  const node = byId("startup-estimate");
  const track = byId("startup-estimate-track");
  const fill = byId("startup-estimate-fill");
  const note = byId("startup-estimate-note");
  if (!node || !track || !fill || !note) return;
  node.hidden = false;
  const setProgress = (percent, mode, text) => {
    track.dataset.mode = mode;
    if (percent == null) track.removeAttribute("aria-valuenow");
    else track.setAttribute("aria-valuenow", String(percent));
    track.setAttribute("aria-valuetext", text);
    fill.style.width = `${percent || 0}%`;
    note.textContent = text;
  };
  if (ready) {
    setProgress(100, "ready", "Ready to join");
    return;
  }
  const median = startupEstimate.medianSeconds;
  const anchor = startupEstimate.elapsedAtAnchor;
  // Numeric progress needs both bounded history and a server-provided attempt
  // elapsed; anything missing stays explicitly unestimated.
  if (median == null || anchor == null) {
    setProgress(null, "learning", learningStartupEstimateText());
    return;
  }
  const elapsed = anchor + Math.max(0, Date.now() - startupEstimate.anchoredAt) / 1000;
  if (elapsed >= median) {
    setProgress(STARTUP_ESTIMATE_CAP, "overrun", "Taking longer than usual…");
    return;
  }
  const percent = Math.max(1, Math.min(STARTUP_ESTIMATE_CAP - 1, Math.round((elapsed / median) * 100)));
  setProgress(percent, "estimated", `${percent}% · about ${formatEstimateDuration(median - elapsed)} remaining`);
}

function learningStartupEstimateText() {
  // Numeric progress needs five successful starts for this exact version; the
  // count shown is the server's recorded count, never a fabrication.
  const recorded = startupEstimate.sampleCount;
  if (!Number.isFinite(recorded) || recorded <= 0) {
    return `Learning startup time… ${STARTUP_ESTIMATE_MIN_SAMPLES} successful starts needed`;
  }
  return `Learning startup time… ${Math.min(recorded, STARTUP_ESTIMATE_MIN_SAMPLES)} of ${STARTUP_ESTIMATE_MIN_SAMPLES} starts recorded`;
}

function patchStartupEstimate(status, ownerId) {
  const node = byId("startup-estimate");
  if (!node) return;
  const estimate = status?.startup_estimate && typeof status.startup_estimate === "object" ? status.startup_estimate : null;
  const current = status?.state || "unknown";
  const profileId = typeof status?.profile_id === "string" ? status.profile_id : null;
  const ready = readinessGateSatisfied(status, ownerId);
  if (!startupEstimateEnabled() || !startupEstimateStatusFresh()) {
    clearStartupEstimate();
    return;
  }
  if (startupEstimate.ready) {
    // A completed run stays visible at 100 while that same run is still
    // authoritative-ready; leaving readiness, ownership, or the profile clears it.
    if (ready && profileId === startupEstimate.profileId) { renderStartupEstimate(true); return; }
    clearStartupEstimate();
    return;
  }
  // The genuine 100 arrives with the running/healthy/ports/owner status after
  // the attempt we were already tracking; the estimate itself is gone by then.
  if (ready && profileId && profileId === startupEstimate.profileId && startupEstimate.attemptId) {
    startupEstimate.ready = true;
    if (startupEstimate.timer) { window.clearInterval(startupEstimate.timer); startupEstimate.timer = null; }
    renderStartupEstimate(true);
    return;
  }
  if (current !== "starting" || !estimate || (ownerId && ownerId !== profileId)) {
    clearStartupEstimate();
    return;
  }
  const attemptId = typeof estimate.attempt_id === "string" && estimate.attempt_id ? estimate.attempt_id : null;
  const version = typeof estimate.version === "string" && estimate.version ? estimate.version : null;
  const elapsed = Number.isFinite(estimate.elapsed_seconds) && estimate.elapsed_seconds >= 0 ? Number(estimate.elapsed_seconds) : null;
  const median = Number.isFinite(estimate.median_seconds) && estimate.median_seconds > 0 ? Number(estimate.median_seconds) : null;
  const sampleCount = Number.isFinite(estimate.sample_count) && estimate.sample_count >= 0 ? Math.floor(Number(estimate.sample_count)) : null;
  if (profileId !== startupEstimate.profileId || attemptId !== startupEstimate.attemptId || version !== startupEstimate.version) {
    // A new profile, run, or version starts a fresh estimate.
    startupEstimate.serverSample = null;
    startupEstimate.elapsedAtAnchor = null;
    startupEstimate.anchoredAt = 0;
    startupEstimate.medianSeconds = null;
    startupEstimate.profileId = profileId;
    startupEstimate.attemptId = attemptId;
    startupEstimate.version = version;
  }
  // Only a NEW authoritative status sample re-anchors; incidental re-renders
  // must not pull an already-advanced estimate backwards.
  const serverSample = attemptId && elapsed != null ? `${attemptId}:${version || ""}:${elapsed}` : null;
  if (serverSample !== startupEstimate.serverSample) {
    startupEstimate.serverSample = serverSample;
    startupEstimate.elapsedAtAnchor = attemptId ? elapsed : null;
    startupEstimate.anchoredAt = Date.now();
  }
  startupEstimate.medianSeconds = median;
  startupEstimate.sampleCount = sampleCount;
  if (startupEstimate.elapsedAtAnchor != null) {
    if (!startupEstimate.timer) {
      startupEstimate.timer = window.setInterval(() => {
        if (!startupEstimateStatusFresh()) { clearStartupEstimate(); return; }
        patchStartupEstimate(state.statuses.get(startupEstimate.profileId), slotOwnerId());
      }, STARTUP_ESTIMATE_TICK_MS);
    }
  } else if (startupEstimate.timer) {
    window.clearInterval(startupEstimate.timer);
    startupEstimate.timer = null;
  }
  renderStartupEstimate(false);
}

function readinessGateSatisfied(status, ownerId) {
  // Same authoritative readiness criterion the session runway already uses.
  return Boolean(
    status &&
    status.state === "running" &&
    status.health === "healthy" &&
    status.required_ports_ready &&
    ownerId &&
    ownerId === status.profile_id
  );
}

function patchSessionBackup() {
  const node = byId("session-backup");
  const backup = state.session.latestBackup;
  if (state.session.backupState === "loading") { node.textContent = "Checking…"; return; }
  if (state.session.backupState === "error") { node.textContent = "Lookup unavailable"; return; }
  if (!backup) { node.textContent = "Not recorded"; return; }
  const date = backup.created_at ? new Date(backup.created_at) : null;
  const stamp = date && !Number.isNaN(date.getTime()) ? date.toLocaleDateString(undefined, { month: "short", day: "numeric" }) : "date unavailable";
  const suffix = backupAvailable(backup) ? "" : ` · ${availabilityLabel(backupAvailability(backup))}`;
  node.textContent = `${backup.verified ? "Verified" : "Unverified"} ${stamp}${suffix}`;
}

async function loadSessionBackup(id) {
  if (!id || state.session.backupState !== "loading") return;
  try {
    const page = await api(`/api/v1/profiles/${encodeURIComponent(id)}/backups?limit=1`);
    if (state.session.profileId !== id) return;
    state.session.latestBackup = Array.isArray(page.items) ? page.items[0] || null : null;
    state.session.backupState = "loaded";
  } catch (error) {
    if (error?.name === "AbortError") throw error;
    if (state.session.profileId !== id) return;
    state.session.latestBackup = null;
    state.session.backupState = "error";
  }
  patchSessionBackup();
}

function patchSessionOperation() {
  const node = byId("session-operation");
  const operation = state.session.operation;
  if (!operation || operation.profileId !== state.session.profileId) {
    node.hidden = true;
    node.textContent = "";
    node.removeAttribute("data-result");
    return;
  }
  node.hidden = false;
  node.dataset.result = operation.result;
  node.textContent = operation.message;
}

function observeSessionOperation(status) {
  const operation = state.session.operation;
  if (!operation || operation.profileId !== status?.profile_id || operation.result === "failed") return;
  const stateValue = status.state || "unknown";
  if (operation.kind === "start" && stateValue === "running" && status.required_ports_ready && status.health === "healthy" && status.slot_owner === status.profile_id) {
    operation.result = "complete";
    operation.message = `${profileLabel(status.profile_id)} is ready.`;
  } else if (operation.kind === "stop" && stateValue === "stopped") {
    operation.result = "complete";
    operation.message = `${profileLabel(status.profile_id)} stopped safely.`;
  } else if (stateValue === "failed") {
    operation.result = "failed";
    operation.message = `${titleCase(operation.kind)} failed. Review recent events and logs before retrying.`;
  }
}

function applyStatus(snapshot, { confirmed = false } = {}) {
  if (!snapshot || !Array.isArray(snapshot.profiles)) return false;
  const generation = Number(snapshot.generation);
  if (!Number.isSafeInteger(generation) || generation < state.lastGeneration) return false;
  if (snapshot.profiles.some((item) => !item || typeof item.profile_id !== "string" || !["unknown", "stopped", "starting", "running", "stopping", "failed", "blocked"].includes(item.state))) return false;
  if (Number.isFinite(generation)) state.lastGeneration = generation;
  if (confirmed) state.statusConfirmed = true;
  // Every applied snapshot is fresh data; the age is what bounds local
  // startup-estimate interpolation when the stream or poll goes quiet.
  state.lastStatusAt = Date.now();
  // Only a valid snapshot naming a known profile counts as a fresh sample for
  // the estimate fence: malformed or unrelated payloads never revive the track.
  if (snapshot.profiles.some((item) => item?.profile_id && state.profiles.has(item.profile_id))) state.statusSeq += 1;
  snapshot.profiles.forEach((item) => {
    if (!item?.profile_id) return;
    const previous = state.statuses.get(item.profile_id) || {};
    const previousRun = metricRunKey(previous);
    const nextRun = metricRunKey({ ...previous, ...item });
    if (previousRun !== nextRun) {
      const retained = state.metricHistory.get(item.profile_id);
      const runEnded = !nextRun && Boolean(previousRun) && ["stopped", "failed", "blocked"].includes(item.state);
      if (runEnded && retained?.key === previousRun) {
        // Keep the last session's retained observations instead of erasing the
        // Metrics tab.  They render as explicitly historical values; a run
        // switch or a new start still begins from a clean slate.
        const sampleTimes = ["cpu", "memory", "players"]
          .flatMap((name) => (retained[name] || []).map((point) => point?.t))
          .filter((time) => Number.isFinite(time));
        state.metricHistory.set(item.profile_id, {
          ...retained,
          historical: true,
          startedAt: Date.parse(previous.started_at) || retained.startedAt,
          // The range end is the last real sample, never the stop status time.
          endedAt: sampleTimes.length ? Math.max(...sampleTimes) : null,
        });
      } else if (!runEnded) {
        state.metricHistory.delete(item.profile_id);
      }
      state.metricCapacity.delete(item.profile_id);
      state.metricSamples.delete(item.profile_id);
    }
    const pending = state.perf.pendingMutations.get(item.profile_id);
    state.statuses.set(item.profile_id, { ...previous, ...item });
    if (state.statusConfirmed && item.state === "running" && item.required_ports_ready) {
      retireResolvedStartKey(item.profile_id);
    }
    observeSessionOperation(state.statuses.get(item.profile_id));
    markPerformance("horizon-card-reflect");
    if (pending) {
      measurePerformance("horizon-mutation-click-to-card-reflect", "horizon-mutation-click", "horizon-card-reflect");
      state.perf.pendingMutations.delete(item.profile_id);
    }
    appendCpuSample(item.profile_id, item);
    const samples = state.metricSamples.get(item.profile_id) || { cpu: [], memory: [], players: [] };
    const sampleTime = Date.parse(snapshot.observed_at) || Date.now();
    if (metricRunKey(state.statuses.get(item.profile_id))) {
      for (const [key, raw, divisor] of [["cpu", item.cpu_percent, 1], ["memory", item.rss_bytes, 1073741824], ["players", item.players_online, 1]]) {
        const value = metricNumber(raw);
        if (samples[key].at(-1)?.t !== sampleTime) samples[key].push({ t: sampleTime, v: value == null ? null : value / divisor, state: value == null ? "unavailable" : "available" });
      }
    }
    ["cpu", "memory", "players"].forEach((key) => { samples[key] = samples[key].slice(-SAMPLE_LIMIT); });
    state.metricSamples.set(item.profile_id, samples);
    patchCard(item.profile_id);
  });
  patchActiveSlot();
  patchFamilyHeaders();
  if (!state.perf.firstStatusPaint) {
    markPerformance("horizon-first-status-paint");
    measurePerformance("horizon-load-to-first-status-paint", "horizon-load-start", "horizon-first-status-paint");
    const entry = [...(window.performance?.getEntriesByName("horizon-load-to-first-status-paint") || [])].at(-1);
    if (entry) queueClientPerformance("first_status_paint", entry.duration);
    state.perf.firstStatusPaint = true;
  }
  byId("profile-cards").setAttribute("aria-busy", "false");
  window.dispatchEvent(new CustomEvent("horizon:status-applied", {
    detail: { generation: Number(snapshot.generation) },
  }));
  if (snapshot.observed_at) {
    const observed = new Date(snapshot.observed_at);
    const hours = String(observed.getHours()).padStart(2, "0");
    const minutes = String(observed.getMinutes()).padStart(2, "0");
    byId("last-updated").textContent = `Updated ${hours}:${minutes}`;
  } else {
    byId("last-updated").textContent = `Generation ${state.lastGeneration}`;
  }
  if (state.detail.id) patchDetail(state.detail.id);
  return true;
}

async function api(path, options = {}) {
  if (sessionExpired) throw new Error(SESSION_EXPIRED_MESSAGE);
  const requestSessionGeneration = sessionGeneration;
  const method = String(options.method || "GET").toUpperCase();
  const mutation = method !== "GET" && method !== "HEAD" && method !== "OPTIONS";
  let operationKey = options.idempotencyKey || null;
  let operationStorageKey = null;
  if (mutation) {
    operationStorageKey = `horizon-operation:${method}:${path}:${options.body || ""}`;
    try {
      operationKey = operationKey || sessionStorage.getItem(operationStorageKey);
      if (!operationKey) {
        operationKey = crypto.randomUUID();
        sessionStorage.setItem(operationStorageKey, operationKey);
      }
    } catch {
      operationKey = operationKey || crypto.randomUUID();
    }
  }
  const attempt = async () => {
    const headers = new Headers(options.headers || {});
    headers.set("Accept", "application/json");
    if (options.body) headers.set("Content-Type", "application/json");
    if (mutation && operationKey) headers.set("Idempotency-Key", operationKey);
    if (state.csrf && mutation) headers.set("X-CSRF-Token", state.csrf);
    return fetch(path, { credentials: "same-origin", ...options, headers, redirect: "manual" });
  };
  const clearOperationKey = () => {
    if (!mutation || !operationStorageKey || !operationKey) return;
    try {
      if (sessionStorage.getItem(operationStorageKey) === operationKey) sessionStorage.removeItem(operationStorageKey);
    } catch {}
  };
  let response;
  try {
    response = await attempt();
    if (testApiResponseDelay > 0) {
      testApiResponseDelay -= 1;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  } catch (error) {
    if (options.signal?.aborted || error?.name === "AbortError") throw error;
    if (!options.method || options.method === "GET") {
      await new Promise((resolve) => setTimeout(resolve, 1500));
      try { response = await attempt(); } catch (retryError) {
        if (options.signal?.aborted || retryError?.name === "AbortError") throw retryError;
        throw new Error("Connection lost. Retrying in the background…");
      }
    } else {
      const unknown = new Error("Outcome unknown. Retry with the same operation key.");
      unknown.outcomeUnknown = true;
      throw unknown;
    }
  }
  if (response.type === "opaqueredirect") {
    expireSession();
    throw new Error(SESSION_EXPIRED_MESSAGE);
  }
  let csrfFailure = false;
  if (response.status === 403) {
    try {
      const body = await response.clone().json();
      const detail = body?.detail || body?.error?.message || "";
      csrfFailure = String(detail).toLowerCase().includes("csrf validation failed");
    } catch {}
  }
  if ((response.status === 401 || csrfFailure) && !options._retried && path !== "/api/v1/session") {
    if (sessionGeneration !== requestSessionGeneration) {
      return api(path, { ...options, idempotencyKey: operationKey, _retried: true });
    }
    try {
      await refreshSession();
      return api(path, { ...options, idempotencyKey: operationKey, _retried: true });
    } catch (error) {
      if (sessionExpired) throw new Error(SESSION_EXPIRED_MESSAGE);
      throw error;
    }
  }
  if (response.status === 401 && path === "/api/v1/session") {
    expireSession();
    throw new Error(SESSION_EXPIRED_MESSAGE);
  }
  if (response.status === 401) {
    expireSession();
    throw new Error(SESSION_EXPIRED_MESSAGE);
  }
  if (!response.ok && !mutation && [502, 503, 504].includes(response.status) && !options._transientRetried) {
    await new Promise((resolve) => setTimeout(resolve, 250));
    return api(path, { ...options, _transientRetried: true });
  }
  if (!response.ok) {
    let detail = "Request failed.";
    let outcomeUnknown = mutation && [502, 503, 504].includes(response.status);
    try {
      const body = await response.json();
      const typed = body?.error?.message || body?.detail;
      if (typeof typed === "string" && typed.trim()) detail = typed.slice(0, 300);
      outcomeUnknown = outcomeUnknown || body?.error?.outcome_unknown === true || (mutation && response.status === 503);
    } catch {}
    // A typed HTTP response is definitive unless the upstream transport may
    // have lost a committed mutation response. Retain that key for replay.
    if (mutation && operationStorageKey && !outcomeUnknown) {
      clearOperationKey();
    }
    if (outcomeUnknown) {
      const unknown = new Error("Outcome unknown. Retry with the same operation key.");
      unknown.outcomeUnknown = true;
      throw unknown;
    }
    throw new Error(detail);
  }
  let result;
  try {
    result = await response.json();
  } catch (error) {
    if (!mutation) throw error;
    const unknown = new Error("Outcome unknown. Retry with the same operation key.");
    unknown.outcomeUnknown = true;
    throw unknown;
  }
  if (mutation && operationStorageKey) {
    clearOperationKey();
  }
  return result;
}

async function loadOnce() {
  clearLoadRetry();
  if (sessionExpired) {
    // The redirect latch survives navigation in sessionStorage. An explicit
    // Retry is a trusted reauthentication opportunity, not a dead-end latch.
    sessionExpired = false;
    reauthenticating = true;
    sessionNoticeShown = false;
  }
  state.statusConfirmed = false;
  try {
    const session = await refreshSession();
    state.actor = session.actor;
    state.csrf = session.csrf_token || null;
    byId("session-note").textContent = state.actor ? `Signed in as ${state.actor}` : "Signed-in operator";
    const [profileList, status] = await Promise.all([api("/api/v1/profiles"), api("/api/v1/status")]);
    (Array.isArray(profileList) ? profileList : PROFILE_FALLBACK.map(([id, display_name, adapter]) => ({ id, display_name, adapter }))).forEach((profile) => {
      const id = profile.id || profile.profile_id;
      if (id) state.profiles.set(id, { ...profile, id });
    });
    PROFILE_FALLBACK.forEach(([id, display_name]) => { if (!state.profiles.has(id)) state.profiles.set(id, { id, display_name }); });
    populateNotificationProfiles();
    renderCards();
    applyStatus(status, { confirmed: true });
    // Clear the navigation latch only after a protected request has succeeded
    // and the signed-in view is confirmed, avoiding redirect loops on a stale
    // bootstrap response.
    try { sessionStorage.removeItem(SESSION_REDIRECT_KEY); } catch {}
    void loadIncidents();
    try { await loadSchedules(); } catch { renderAutomationSummary(null); }
    populateTargets();
    populateNotificationProfiles();
    connectStream();
    state.loadFailed = false;
    loadFailureNotice = null;
  } catch (error) {
    state.loadFailed = true;
    byId("session-note").textContent = "Status unavailable";
    const message = error.message || "Status unavailable.";
    if (message !== loadFailureNotice) { notify(message); loadFailureNotice = message; }
    // Keep the last confirmed view intact during a gateway outage. The
    // fallback is only useful on an initial load with no known profiles.
    if (!state.profiles.size) {
      PROFILE_FALLBACK.forEach(([id, display_name, adapter]) => {
        state.profiles.set(id, { id, display_name, adapter });
        state.statuses.set(id, { profile_id: id, state: "unknown", health: "unknown" });
      });
      renderCards();
      patchActiveSlot();
      populateTargets();
    }
    byId("retry-load")?.removeAttribute("hidden");
    scheduleLoadRetry();
  }
}

function load() {
  if (loadPromise) return loadPromise;
  loadPromise = loadOnce().finally(() => { loadPromise = null; });
  return loadPromise;
}

function populateNotificationProfiles() {
  const select = byId("notification-profile");
  if (!select) return;
  const prior = select.value;
  select.replaceChildren();
  state.profiles.forEach((profile, id) => {
    const option = document.createElement("option"); option.value = id; option.textContent = profile.display_name || id; select.append(option);
  });
  select.value = [...select.options].some((option) => option.value === prior) ? prior : select.options[0]?.value || "";
}

function performanceMs(value) {
  return Number.isFinite(Number(value)) ? `${Number(value).toFixed(1)} ms` : "—";
}

function renderPerformance(snapshot) {
  const routes = byId("performance-routes");
  const backend = byId("performance-slotd");
  const marks = byId("performance-marks");
  if (!routes || !backend || !marks) return;
  routes.replaceChildren();
  Object.entries(snapshot || {})
    .filter(([key, value]) => !["rpc", "sse", "slotd"].includes(key) && value && typeof value === "object")
    .forEach(([route, value]) => {
      const row = document.createElement("li");
      row.className = "performance-row";
      const label = document.createElement("strong"); label.textContent = route;
      const detail = document.createElement("span"); detail.textContent = `p50 ${performanceMs(value.p50_ms)} · p95 ${performanceMs(value.p95_ms)} · max ${performanceMs(value.max_ms)}`;
      row.append(label, detail); routes.append(row);
    });
  if (!routes.children.length) {
    const empty = document.createElement("li"); empty.className = "empty-state"; empty.textContent = "No route samples recorded yet."; routes.append(empty);
  }
  const cycle = snapshot?.slotd?.cycle;
  const rpc = snapshot?.slotd?.rpc || snapshot?.rpc;
  backend.textContent = cycle
    ? `Status cycle p95 ${performanceMs(cycle.p95_ms)} · RPC p95 ${performanceMs(rpc?.p95_ms)}`
    : `Web RPC p95 ${performanceMs(rpc?.p95_ms)}`;
  marks.replaceChildren();
  const entries = window.performance?.getEntriesByType("measure") || [];
  ["horizon-load-to-first-status-paint", "horizon-mutation-click-to-optimistic-reflect", "horizon-mutation-click-to-card-reflect", "horizon-sse-reconnect-gap"].forEach((name) => {
    const entry = [...entries].reverse().find((item) => item.name === name);
    const row = document.createElement("li"); row.className = "performance-row";
    const label = document.createElement("strong"); label.textContent = name.replace("horizon-", "").replaceAll("-", " ");
    const detail = document.createElement("span"); detail.textContent = entry ? performanceMs(entry.duration) : "Not measured yet";
    row.append(label, detail); marks.append(row);
  });
}

async function loadPerformance() {
  try {
    renderPerformance(await api("/api/v1/perf"));
  } catch (error) {
    renderPerformance({});
    const panel = byId("performance-panel");
    if (panel) panel.dataset.error = error.message || "Performance unavailable";
  }
}

async function loadNotifications(id) {
  if (!id) return;
  const rules = byId("notification-rules");
  try {
    const config = await api(`/api/v1/profiles/${encodeURIComponent(id)}/notifications`);
    rules.replaceChildren();
    Object.entries(config.rules || {}).forEach(([event, enabled]) => {
      const label = document.createElement("label"); label.className = "check-row notification-rule";
      const input = document.createElement("input"); input.type = "checkbox"; input.checked = Boolean(enabled); input.dataset.event = event;
      input.addEventListener("change", async () => {
        input.disabled = true;
        try { await api(`/api/v1/profiles/${encodeURIComponent(id)}/notifications/rule`, { method: "POST", body: JSON.stringify({ event, enabled: input.checked }) }); setNotificationStatus("Rule saved."); }
        catch (error) { input.checked = !input.checked; setNotificationStatus(error.message || "Rule update failed."); }
        finally { input.disabled = false; }
      });
      label.append(input, document.createTextNode(titleCase(event))); rules.append(label);
    });
    setNotificationStatus("Notification settings loaded.");
  } catch (error) { rules.replaceChildren(); const empty = document.createElement("p"); empty.className = "empty-state"; empty.textContent = error.message || "Notifications unavailable."; rules.append(empty); }
}

function setNotificationStatus(message) { const node = byId("notification-status"); if (node) node.textContent = message; }

const INCIDENT_COPY = {
  start_timeout: ["Start timed out", "The service did not become ready. Horizon now stops any process left by this failed start."],
  health_failed: ["Health check failed", "The process or required game port did not reach a healthy state."],
  grace_timeout: ["Graceful stop timed out", "Use the confirmed force-stop flow only after checking player activity."],
  backup_failed: ["Backup failed", "Review the backup record before retrying; retention is not changed automatically."],
  restore_failed: ["Restore failed", "The requested restore did not complete. The current world remains authoritative."],
  update_failed: ["Update failed", "The release did not pass Horizon's guarded update path."],
  benchmark_failed: ["Benchmark failed", "The isolated benchmark did not produce accepted evidence."],
  low_disk: ["Storage gate blocked the action", "Free space fell below the configured safety threshold."],
  low_memory: ["Memory gate blocked the action", "Available memory fell below the configured safety threshold."],
  required_file_missing: ["Required file missing", "The profile failed its fixed-file preflight."],
  upstream_unavailable: ["Upstream unavailable", "A fixed dependency could not be reached."],
  slot_conflict: ["Slot ownership conflict", "Another operation, player fence, or server owns the single game slot."],
  profile_reserved: ["Profile reserved", "Another controller request currently owns this profile."],
  internal_error: ["Controller error", "Horizon rejected the operation without exposing internal details."],
};
const CRITICAL_INCIDENTS = new Set(["start_timeout", "health_failed", "backup_failed", "restore_failed", "update_failed", "internal_error"]);

function deriveIncidents(auditItems) {
  const items = (Array.isArray(auditItems) ? auditItems : [])
    .filter((item) => item && item.timestamp && item.action)
    .sort((left, right) => new Date(left.timestamp) - new Date(right.timestamp));
  const groups = new Map();
  items.forEach((item) => {
    if (!["failed", "rejected"].includes(item.result) || !item.error_code) return;
    const key = `${item.profile_id || "system"}\u0000${item.action}\u0000${item.error_code}`;
    const existing = groups.get(key) || {
      key,
      profileId: item.profile_id || null,
      action: item.action,
      code: item.error_code,
      firstSeen: item.timestamp,
      lastSeen: item.timestamp,
      occurrences: 0,
      resolvedAt: null,
    };
    existing.lastSeen = item.timestamp;
    existing.occurrences += 1;
    existing.resolvedAt = null;
    groups.set(key, existing);
  });
  groups.forEach((incident) => {
    const resolved = items.find((item) => item.result === "succeeded" && item.action === incident.action && (item.profile_id || null) === incident.profileId && new Date(item.timestamp) > new Date(incident.lastSeen));
    if (resolved) incident.resolvedAt = resolved.timestamp;
  });
  return [...groups.values()].sort((left, right) => new Date(right.lastSeen) - new Date(left.lastSeen));
}

function renderIncidentRail(listId, incidents, { limit = 50, summaryId } = {}) {
  const list = byId(listId);
  if (!list) return;
  list.replaceChildren();
  list.setAttribute("aria-busy", "false");
  const visible = incidents.slice(0, limit);
  visible.forEach((incident) => {
    const [title, hint] = INCIDENT_COPY[incident.code] || [`${titleCase(incident.action)} failed`, "Review the typed audit trail before retrying."];
    const active = !incident.resolvedAt;
    const row = document.createElement("li");
    row.className = "incident-item";
    row.dataset.state = active ? (CRITICAL_INCIDENTS.has(incident.code) ? "critical" : "open") : "resolved";
    const marker = document.createElement("span"); marker.className = "incident-marker"; marker.setAttribute("aria-hidden", "true");
    const body = document.createElement("div"); body.className = "incident-body";
    const top = document.createElement("div"); top.className = "incident-topline";
    const heading = document.createElement("strong"); heading.textContent = title;
    const stateLabel = document.createElement("span"); stateLabel.className = "incident-state"; stateLabel.textContent = active ? "Needs review" : "Resolved";
    top.append(heading, stateLabel);
    const meta = document.createElement("p"); meta.className = "incident-meta";
    const profile = incident.profileId ? profileLabel(incident.profileId) : "Horizon";
    const count = incident.occurrences > 1 ? ` · ${incident.occurrences} occurrences` : "";
    meta.textContent = `${profile} · ${new Date(incident.lastSeen).toLocaleString()}${count}`;
    const copy = document.createElement("p"); copy.className = "incident-copy"; copy.textContent = hint;
    body.append(top, meta, copy); row.append(marker, body); list.append(row);
  });
  if (!visible.length) {
    const empty = document.createElement("li"); empty.className = "incident-empty is-clear"; empty.textContent = "No recent controller failures. Horizon's typed record is clear."; list.append(empty);
  }
  const summary = byId(summaryId);
  if (summary) {
    const open = incidents.filter((item) => !item.resolvedAt).length;
    summary.textContent = incidents.length ? `${open} needing review · ${incidents.length - open} resolved in the recent record` : "No recent controller failures.";
  }
}

async function loadIncidents() {
  try {
    // Keep the projection below the controller's bounded response envelope.
    // A 500-row page can exceed the Unix-RPC frame even though the HTTP route
    // accepts the query parameter, which must not turn the whole rail into an
    // unavailable state.
    const page = await api("/api/v1/audit?limit=200");
    state.incidents = { items: deriveIncidents(page.items), loaded: true };
    renderIncidentRail("incident-list-compact", state.incidents.items, { limit: 3, summaryId: "incident-summary" });
    renderIncidentRail("incident-list", state.incidents.items, { summaryId: "incident-history-summary" });
  } catch (error) {
    ["incident-list-compact", "incident-list"].forEach((id) => {
      const list = byId(id); if (!list) return; list.replaceChildren(); list.setAttribute("aria-busy", "false");
      const item = document.createElement("li"); item.className = "incident-empty is-unavailable"; item.textContent = "Incident history is temporarily unavailable."; list.append(item);
    });
    ["incident-summary", "incident-history-summary"].forEach((id) => { const node = byId(id); if (node) node.textContent = "Could not read the typed audit record."; });
  }
}

async function testNotification(channel) {
  const id = byId("notification-profile")?.value;
  if (!id) return;
  try { await api(`/api/v1/profiles/${encodeURIComponent(id)}/notifications/test`, { method: "POST", body: JSON.stringify({ channel }) }); setNotificationStatus(`${titleCase(channel)} test requested.`); }
  catch (error) { setNotificationStatus(error.message || `${titleCase(channel)} test failed.`); }
}

function renderActivity(listId, items, kind) {
  const list = byId(listId); list.replaceChildren();
  (Array.isArray(items) ? items : []).forEach((item) => {
    const row = document.createElement("li"); row.className = "activity-row";
    const heading = document.createElement("strong"); heading.textContent = kind === "audit" ? `${item.actor || "unknown"} · ${item.action || "—"}` : `${item.code || "event"}${item.profile_id ? ` · ${profileLabel(item.profile_id)}` : ""}`;
    const time = document.createElement("time"); time.textContent = item.timestamp ? new Date(item.timestamp).toLocaleString() : "—";
    const detail = document.createElement("span"); detail.textContent = kind === "audit" ? `${item.result || "—"}${item.error_code ? ` · ${item.error_code}` : ""} · ${item.detail || ""}` : (item.message || "");
    row.append(heading, time, detail); list.append(row);
  });
  if (!items?.length) { const empty = document.createElement("li"); empty.className = "empty-state"; empty.textContent = `No ${kind} records loaded.`; list.append(empty); }
}

async function loadActivity(kind) {
  try { const page = await api(`/api/v1/${kind}?limit=200`); renderActivity(`${kind}-list`, page.items, kind); }
  catch (error) { renderActivity(`${kind}-list`, [] , kind); notify(error.message || `${titleCase(kind)} unavailable.`); }
}

function connectStream() {
  if (!pageVisible() || stream.suspended || sessionExpired) return;
  if (!window.EventSource) { startFallbackPolling(); return; }
  if (stream.source && stream.source.readyState !== window.EventSource.CLOSED) return;
  if (stream.source) { stream.source.close(); stream.source = null; }
  const cursor = stream.lastEventId === null ? "" : `?after=${encodeURIComponent(stream.lastEventId)}`;
  const source = new EventSource(`/api/v1/stream${cursor}`);
  stream.source = source;
  stream.openedAt = Date.now();
  stream.lastStatusAt = 0;
  setConnState("reconnecting");
  const alive = () => {
    if (stream.suspended || stream.source !== source) return;
    stream.lastEventAt = Date.now();
  };
  source.onopen = alive;
  source.addEventListener("heartbeat", alive);
  const applyStreamEvent = (event) => {
    if (stream.suspended || stream.source !== source) return;
    alive();
    let snapshot;
    try { snapshot = JSON.parse(event.data); } catch { return; }
    if (!applyStatus(snapshot)) return;
    recordStreamCursor(source, event);
    // An empty or irrelevant payload cannot establish freshness for the cards.
    if (!snapshot.profiles.some((item) => state.profiles.has(item.profile_id))) return;
    stream.lastStatusAt = Date.now();
    if (stream.reconnectStartedAt !== null) {
      markPerformance("horizon-sse-reconnect-end");
      measurePerformance("horizon-sse-reconnect-gap", "horizon-sse-reconnect-start", "horizon-sse-reconnect-end");
      stream.reconnectStartedAt = null;
    }
    stream.retryMs = 3000; setConnState("live"); stopFallbackPolling();
  };
  source.addEventListener("status", applyStreamEvent);
  source.onmessage = applyStreamEvent;
  source.onerror = () => {
    if (stream.suspended || stream.source !== source) return;
    setConnState("reconnecting");
    startFallbackPolling();
    if (source.readyState === EventSource.CLOSED) {
      scheduleReconnect();
    }
  };
  // Recurring timer: connection watchdog; callback is visibility-gated.
  if (!stream.watchdog) stream.watchdog = window.setInterval(checkStreamFreshness, 10000);
}

function checkStreamFreshness() {
  // Ready (timer stopped) and learning (no elapsed) estimates have no local
  // tick, so the watchdog must also drop a stale estimate before its own
  // transport early-return.
  if (!startupEstimateStatusFresh()) clearStartupEstimate();
  if (!pageVisible() || stream.suspended || sessionExpired || !stream.source) return;
  if (Date.now() - (stream.lastStatusAt || stream.openedAt) >= STATUS_STALE_MS || Date.now() - stream.lastEventAt > 45000) {
    setConnState("reconnecting");
    startFallbackPolling();
    scheduleReconnect();
  }
}

function setConnState(mode) {
  if (mode === "offline" || mode === "reconnecting") fenceStartupEstimate();
  const pill = byId("conn-state");
  if (!pill) return;
  pill.dataset.state = mode;
  pill.textContent = mode === "live" ? "Live" : mode === "polling" ? "Polling" : mode === "reconnecting" ? "Reconnecting…" : "Offline";
}

function scheduleReconnect() {
  if (!pageVisible() || stream.suspended) return;
  if (stream.reconnectTimer) return;
  if (stream.source) { stream.source.close(); stream.source = null; }
  if (stream.reconnectStartedAt === null) {
    stream.reconnectStartedAt = performance.now();
    markPerformance("horizon-sse-reconnect-start");
  }
  const delay = stream.retryMs + reconnectJitter();
  stream.retryMs = Math.min(stream.retryMs * 2, 60000);
  stream.reconnectTimer = window.setTimeout(async () => {
    stream.reconnectTimer = null;
    if (!pageVisible() || stream.suspended) return;
    try {
      await refreshSession();
    } catch {
      if (!sessionExpired) {
        setConnState("reconnecting");
        scheduleReconnect();
      }
      return;
    }
    if (sessionExpired) return;
    connectStream();
  }, delay);
}

function startFallbackPolling() {
  if (!pageVisible() || stream.suspended || sessionExpired) return;
  if (stream.pollTimer) return;
  // Recurring timer: REST status fallback; callback is visibility-gated.
  stream.pollTimer = window.setInterval(pollStatus, 10000);
  void pollStatus();
}

async function pollStatus() {
  if (!pageVisible() || stream.suspended || sessionExpired || !stream.pollTimer || stream.pollRequest) return;
  const epoch = stream.epoch;
  const controller = new AbortController();
  stream.pollController = controller;
  const timeout = window.setTimeout(() => controller.abort(), 10000);
  const request = api("/api/v1/status", { signal: controller.signal });
  stream.pollRequest = request;
  try {
    const snapshot = await request;
    if (epoch !== stream.epoch || !pageVisible() || stream.suspended || !stream.pollTimer) return;
    if (!applyStatus(snapshot, { confirmed: true }) || !snapshot.profiles.some((item) => state.profiles.has(item.profile_id))) throw new Error("Status unavailable");
    setConnState("polling");
  } catch {
    if (epoch === stream.epoch && pageVisible() && !stream.suspended && stream.pollTimer) setConnState("offline");
  } finally {
    window.clearTimeout(timeout);
    if (stream.pollRequest === request) stream.pollRequest = null;
    if (stream.pollController === controller) stream.pollController = null;
  }
}

function stopFallbackPolling() {
  if (stream.pollTimer) { window.clearInterval(stream.pollTimer); stream.pollTimer = null; }
  stream.pollController?.abort();
}

function suspendStream() {
  fenceStartupEstimate();
  stream.suspended = true;
  stream.epoch += 1;
  if (stream.reconnectTimer) { window.clearTimeout(stream.reconnectTimer); stream.reconnectTimer = null; }
  if (stream.watchdog) { window.clearInterval(stream.watchdog); stream.watchdog = null; }
  stopFallbackPolling();
  const source = stream.source;
  stream.source = null;
  if (source) source.close();
}

async function resumeStream() {
  if (!pageVisible() || sessionExpired) return;
  const wasSuspended = stream.suspended;
  stream.suspended = false;
  if (!wasSuspended && stream.source && stream.source.readyState !== window.EventSource?.CLOSED) return;
  state.statusConfirmed = false;
  fenceStartupEstimate();
  state.profiles.forEach((_profile, id) => patchCard(id));
  patchActiveSlot();
  // One full snapshot closes the event gap while the browser was hidden.  SSE
  // then resumes from a single fresh source; cached UI remains in place until
  // this snapshot wins.
  try { applyStatus(await api("/api/v1/status"), { confirmed: true }); } catch { setConnState("reconnecting"); }
  if (!pageVisible() || stream.suspended) return;
  connectStream();
}

function populateTargets(preferredTarget = null) {
  const current = [...state.statuses.values()].find((item) => item.slot_owner || item.state === "running")?.profile_id;
  const target = byId("switch-target");
  const prior = preferredTarget || target.value;
  target.replaceChildren();
  state.profiles.forEach((profile, id) => {
    if (id === current) return;
    const option = document.createElement("option");
    option.value = id;
    option.textContent = profile.display_name || id;
    target.append(option);
  });
  if ([...target.options].some((option) => option.value === prior)) target.value = prior;
  byId("switch-current").textContent = current ? profileLabel(current) : "No active server";
  byId("switch-target-summary").textContent = target.selectedOptions[0]?.textContent || "Choose a target";
  byId("switch-timeout").textContent = "Up to 5 minutes";
}

function openSwitchDialog(targetId, opener) {
  const switchDialog = byId("switch-dialog");
  populateTargets(targetId);
  byId("switch-confirm-text").value = "";
  setupDialog(switchDialog, opener);
  const expected = byId("switch-target").selectedOptions[0]?.textContent || "";
  const updating = updateOwner();
  byId("switch-target-summary").textContent = updating ? updateNotice(profileLabel(updating)) : expected || "Choose a target";
  byId("switch-confirm").disabled = true;
}

async function mutate(id, operation) {
  if (!state.statusConfirmed) {
    notify("Horizon is still confirming current status. Try again when the status check completes.");
    return;
  }
  const updating = updateOwner();
  if (operation === "start" && updating) {
    notify(updateNotice(updating === id ? null : profileLabel(updating)));
    return;
  }
  const owner = [...state.statuses.values()].find((status) => status?.slot_owner)?.slot_owner;
  if (operation === "start" && owner && owner !== id) {
    notify(`${profileLabel(id)} cannot start while ${profileLabel(owner)} owns the active slot. Switch active server…`);
    return;
  }
  if (state.perf.pendingMutations.has(id)) return;
  const previous = { ...(state.statuses.get(id) || { profile_id: id, state: "unknown", health: "unknown" }) };
  const optimistic = {
    ...previous,
    profile_id: id,
    state: operation === "start" ? "starting" : "stopping",
    slot_owner: id,
    health: operation === "start" ? "unknown" : previous.health,
    required_ports_ready: operation === "start" ? false : previous.required_ports_ready,
  };
  const pending = { operation, previous };
  const sessionOperation = id === sessionProfileId() ? {
    profileId: id,
    kind: operation,
    result: "pending",
    startedAt: new Date().toISOString(),
    jobId: null,
    message: `${titleCase(operation)} request is being sent to Horizon.`,
  } : null;
  if (id === sessionProfileId()) {
    state.session.operation = sessionOperation;
    patchSessionOperation();
  }
  state.perf.pendingMutations.set(id, pending);
  state.statuses.set(id, optimistic);
  markPerformance("horizon-mutation-click");
  markPerformance("horizon-mutation-optimistic-click");
  patchCard(id);
  patchActiveSlot();
  patchFamilyHeaders();
  if (state.detail.id === id) patchDetail(id);
  markPerformance("horizon-mutation-optimistic-reflect");
  measurePerformance("horizon-mutation-click-to-optimistic-reflect", "horizon-mutation-optimistic-click", "horizon-mutation-optimistic-reflect");
  try {
    const accepted = await api(`/api/v1/profiles/${encodeURIComponent(id)}/${operation}`, { method: "POST", body: JSON.stringify({}) });
    let current = state.statuses.get(id);
    if (operation === "start" && accepted?.state === "running") {
      // A completed replay may arrive before this tab has observed the
      // current lifecycle state. Reconcile once before presenting acceptance.
      try {
        const observed = await api("/api/v1/status");
        applyStatus(observed, { confirmed: true });
        current = state.statuses.get(id);
      } catch {}
    }
    if (operation === "start" && accepted?.state === "running" && current?.state === "stopped") {
      // A retained idempotency key can replay an older successful start after
      // that server has since been stopped. Reconcile to current status and
      // require a fresh user action; never issue a second POST automatically.
      if (state.perf.pendingMutations.get(id) === pending) {
        if (state.statuses.get(id)?.state === "starting") state.statuses.set(id, current);
        state.perf.pendingMutations.delete(id);
        patchCard(id);
        patchActiveSlot();
        patchFamilyHeaders();
        if (state.detail.id === id) patchDetail(id);
      }
      if (sessionOperation && state.session.operation === sessionOperation) {
        sessionOperation.result = "resolved";
        sessionOperation.jobId = null;
        sessionOperation.message = `A previous Start resolved, but ${profileLabel(id)} is currently stopped. Start it again when ready.`;
        patchSessionOperation();
      }
      notify(`${profileLabel(id)} is currently stopped; the previous Start was already resolved.`);
      return;
    }
    if (sessionOperation && state.session.operation === sessionOperation && sessionOperation.result === "pending") {
      sessionOperation.result = "accepted";
      sessionOperation.jobId = accepted?.job_id || null;
      const job = sessionOperation.jobId ? ` Job ${sessionOperation.jobId}.` : "";
      sessionOperation.message = `${titleCase(operation)} accepted by Horizon.${job} Waiting for observed readiness.`;
      patchSessionOperation();
    }
    notify(`${titleCase(operation)} requested for ${profileLabel(id)}.`);
  } catch (error) {
    if (state.perf.pendingMutations.get(id) === pending) {
      state.statuses.set(id, previous);
      state.perf.pendingMutations.delete(id);
      patchCard(id);
      patchActiveSlot();
      patchFamilyHeaders();
      if (state.detail.id === id) patchDetail(id);
    }
    if (sessionOperation && state.session.operation === sessionOperation && ["pending", "accepted"].includes(sessionOperation.result)) {
      sessionOperation.result = error?.outcomeUnknown ? "unknown" : "failed";
      sessionOperation.message = error?.outcomeUnknown
        ? `${titleCase(operation)} outcome is unknown. Horizon is reconciling the original operation; observed status will settle this notice.`
        : `${titleCase(operation)} was not accepted: ${error.message || "request failed"}`;
      patchSessionOperation();
    }
    notify(error.message || `${titleCase(operation)} failed.`);
  }
}

async function copySessionEndpoint() {
  const value = byId("session-copy-endpoint").dataset.endpoint || "";
  if (!value) return;
  try {
    await navigator.clipboard.writeText(value);
    notify(`Copied ${value}.`);
  } catch {
    notify(`Copy unavailable. Join at ${value}.`);
  }
}

function setupSessionDeck() {
  byId("session-copy-endpoint")?.addEventListener("click", copySessionEndpoint);
  byId("session-primary")?.addEventListener("click", (event) => {
    const control = event.currentTarget;
    const id = control.dataset.sessionProfileId || sessionProfileId();
    const action = control.dataset.sessionAction;
    if (action === "start" && id) mutate(id, "start");
    else if (action === "switch" && id) openSwitchDialog(id, control);
    else if (action === "console" && id) window.location.hash = `#/servers/${encodeURIComponent(id)}/console`;
    else if (action === "diagnose" && id) window.location.hash = `#/servers/${encodeURIComponent(id)}/logs`;
    else if (action === "events") window.location.hash = "#/events";
    else if (action === "copy") copySessionEndpoint();
    else if (action === "retry") load().finally(route);
  });
}

function setupDialog(dialog, opener) {
  state.dialog = dialog;
  state.returnFocus = opener || document.activeElement;
  dialog.showModal();
  const focusable = dialog.querySelectorAll("button, input, select, textarea, [tabindex]:not([tabindex='-1'])");
  focusable[0]?.focus();
}

function closeDialog(dialog) {
  if (dialog.open) dialog.close("cancel");
  const focus = state.returnFocus;
  state.dialog = null;
  state.returnFocus = null;
  if (focus && document.contains(focus)) focus.focus();
}

document.addEventListener("keydown", (event) => {
  const dialog = state.dialog;
  if (!dialog?.open) return;
  if (event.key === "Escape") { event.preventDefault(); closeDialog(dialog); return; }
  if (event.key !== "Tab") return;
  const focusable = [...dialog.querySelectorAll("button, input, select, textarea, [tabindex]:not([tabindex='-1'])")].filter((node) => !node.disabled);
  if (!focusable.length) return;
  const index = focusable.indexOf(document.activeElement);
  if (event.shiftKey && (index <= 0)) { event.preventDefault(); focusable.at(-1).focus(); }
  else if (!event.shiftKey && (index === focusable.length - 1)) { event.preventDefault(); focusable[0].focus(); }
});

function wireDialogForms() {
  const switchDialog = byId("switch-dialog");
  const switchTarget = byId("switch-target");
  const switchText = byId("switch-confirm-text");
  const switchButton = byId("switch-confirm");
  const validateSwitch = () => {
    const expected = switchTarget.selectedOptions[0]?.textContent || "";
    const updating = updateOwner();
    if (updating) {
      byId("switch-target-summary").textContent = updateNotice(profileLabel(updating));
      switchButton.disabled = true;
      return;
    }
    byId("switch-target-summary").textContent = expected || "Choose a target";
    switchButton.disabled = !expected || switchText.value.trim().toLowerCase() !== expected.trim().toLowerCase();
  };
  byId("switch-active").addEventListener("click", (event) => openSwitchDialog(null, event.currentTarget));
  switchTarget.addEventListener("change", validateSwitch);
  switchText.addEventListener("input", validateSwitch);
  byId("switch-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    if (event.submitter?.value === "cancel") { closeDialog(switchDialog); return; }
    if (updateOwner()) { notify(updateNotice()); return; }
    if (switchButton.disabled) return;
    const current = slotOwnerId() || [...state.statuses.values()].find((item) => item.state === "running")?.profile_id;
    const target = switchTarget.value;
    try {
      const confirmation = await api("/api/v1/switch/prepare", { method: "POST", body: JSON.stringify({ current_profile_id: current, target_profile_id: target, create_backup: byId("switch-backup-option").checked, force_after_timeout: byId("switch-force-option").checked, rollback_on_failure: byId("switch-rollback-option").checked }) });
      await api("/api/v1/switch/confirm", { method: "POST", body: JSON.stringify({ confirmation_id: confirmation.confirmation_id }) });
      closeDialog(switchDialog); notify(`Switch to ${profileLabel(target)} requested.`);
    } catch (error) { notify(error.message || "Switch was not accepted."); }
  });
  [switchDialog, byId("force-dialog"), byId("logs-dialog"), byId("restore-dialog"), byId("update-dialog"), byId("console-save-as-dialog")].forEach((dialog) => {
    dialog.addEventListener("click", (event) => { if (event.target === dialog) closeDialog(dialog); });
    dialog.addEventListener("close", () => {
      if (state.dialog !== dialog) return;
      const focus = state.returnFocus;
      state.dialog = null;
      state.returnFocus = null;
      if (focus && document.contains(focus)) focus.focus();
    });
  });

  const forceDialog = byId("force-dialog");
  byId("force-confirm-text").addEventListener("input", () => { byId("force-confirm").disabled = byId("force-confirm-text").value.trim().toLowerCase() !== profileLabel(state.forceProfile).toLowerCase(); });
  byId("force-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const id = state.forceProfile;
    try {
      const confirmation = await api(`/api/v1/profiles/${encodeURIComponent(id)}/force-stop/prepare`, { method: "POST", body: JSON.stringify({}) });
      await api("/api/v1/force-stop/confirm", { method: "POST", body: JSON.stringify({ confirmation_id: confirmation.confirmation_id }) });
      closeDialog(forceDialog); notify(`Force stop requested for ${profileLabel(id)}.`);
    } catch (error) { notify(error.message || "Force stop was not accepted."); }
  });

  byId("log-query").addEventListener("input", () => { const item = state.logs.get(state.selectedProfile); if (item) { item.query = byId("log-query").value; renderLogs(item); } });
  byId("log-severity").addEventListener("change", () => { const item = state.logs.get(state.selectedProfile); if (item) { item.severity = byId("log-severity").value; renderLogs(item); } });
  byId("log-pause").addEventListener("click", () => { const item = state.logs.get(state.selectedProfile); if (item) { item.paused = !item.paused; byId("log-pause").textContent = item.paused ? "Resume live logs" : "Pause live logs"; } });

  byId("restore-confirm-text").addEventListener("input", () => { byId("restore-confirm").disabled = byId("restore-confirm-text").value.trim().toLowerCase() !== profileLabel(state.restoreProfile).toLowerCase() || !byId("restore-backup-id").value.trim(); });
  byId("restore-backup-id").addEventListener("input", () => { byId("restore-confirm-text").dispatchEvent(new Event("input")); });
  byId("restore-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    if (event.submitter?.value === "cancel") { closeDialog(byId("restore-dialog")); return; }
    const id = state.restoreProfile;
    try {
      const confirmation = await api(`/api/v1/profiles/${encodeURIComponent(id)}/restore/prepare`, { method: "POST", body: JSON.stringify({ backup_id: byId("restore-backup-id").value.trim() }) });
      await api("/api/v1/restore/confirm", { method: "POST", body: JSON.stringify({ confirmation_id: confirmation.confirmation_id }) });
      closeDialog(byId("restore-dialog")); notify(`Restore requested for ${profileLabel(id)}.`);
    } catch (error) { notify(error.message || "Restore was not accepted."); }
  });

  byId("update-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    if (event.submitter?.value === "cancel") { closeDialog(byId("update-dialog")); return; }
    if (!state.updateApplySupported) return;
    const id = state.updateProfile;
    if (!id) return;
    const confirm = byId("update-confirm");
    confirm.disabled = true;
    confirm.textContent = "Applying…";
    try {
      const prepared = await api(`/api/v1/profiles/${encodeURIComponent(id)}/update/prepare`, { method: "POST", body: "{}" });
      await api("/api/v1/update/confirm", { method: "POST", body: JSON.stringify({ confirmation_id: prepared.confirmation_id }) });
      closeDialog(byId("update-dialog"));
      notify(`Update requested for ${profileLabel(id)}.`);
    } catch (error) {
      notify(error.message || "Update request failed.");
    } finally {
      confirm.disabled = false;
      confirm.textContent = "Apply update";
    }
  });

}

function openForce(id, opener) {
  state.forceProfile = id;
  byId("force-description").textContent = `This interrupts ${profileLabel(id)} without waiting for a clean shutdown.`;
  byId("force-confirm-text").value = "";
  byId("force-confirm").disabled = true;
  setupDialog(byId("force-dialog"), opener);
}

function openRestore(id, opener, backupId = "") {
  const record = (state.detail.backups || []).find((item) => item.id === backupId);
  if (record && !backupAvailable(record)) {
    notify(`Backup payload is not locally available (${backupAvailability(record)}).`);
    return;
  }
  state.restoreProfile = id;
  byId("restore-description").textContent = `Restoring replaces ${profileLabel(id)} world data.`;
  byId("restore-backup-id").value = backupId;
  byId("restore-confirm-text").value = "";
  byId("restore-confirm").disabled = !backupId;
  setupDialog(byId("restore-dialog"), opener);
}

async function openLogs(id, opener) {
  state.selectedProfile = id;
  const item = state.logs.get(id) || { query: "", severity: "all", paused: false, lines: [], hideNoise: true };
  state.logs.set(id, item);
  byId("logs-dialog-title").textContent = `Logs for ${profileLabel(id)}`;
  byId("log-query").value = item.query;
  byId("log-severity").value = item.severity;
  byId("log-pause").textContent = item.paused ? "Resume live logs" : "Pause live logs";
  setupDialog(byId("logs-dialog"), opener);
  try {
    const page = await api(`/api/v1/profiles/${encodeURIComponent(id)}/logs?limit=200&severity=all`);
    if (!item.paused && Array.isArray(page.items)) item.lines = page.items;
    renderLogs(item);
  } catch (error) { notify(error.message || "Logs unavailable."); }
}

function renderLogs(item) {
  const query = item.query.trim().toLowerCase();
  const lines = item.lines.filter((line) => (!query || String(line.message || "").toLowerCase().includes(query)) && (item.severity === "all" || line.severity === item.severity));
  const list = byId("log-list");
  list.replaceChildren();
  if (!lines.length) { const empty = document.createElement("li"); empty.className = "empty-state"; empty.textContent = "No matching log lines."; list.append(empty); return; }
  lines.forEach((line) => {
    const row = document.createElement("li");
    row.className = `log-line severity-${line.severity}`;
    const time = document.createElement("time");
    time.textContent = line.timestamp ? new Date(line.timestamp).toLocaleTimeString() : "—";
    const badge = document.createElement("span"); badge.className = "log-severity"; badge.textContent = line.severity;
    const message = document.createElement("span"); message.textContent = line.message || "";
    row.append(time, badge, message); list.append(row);
  });
}

function routeFromHash() {
  const hash = window.location.hash.replace(/^#/, "");
  if (!hash || hash === "/" || hash === "/dashboard" || hash === "dashboard-view") return { view: "dashboard" };
  if (hash === "/settings" || hash === "settings-view") return { view: "settings" };
  if (hash === "/backups" || hash === "backups") return { view: "backups" };
  if (hash === "/events" || hash === "events") return { view: "events" };
  if (hash === "/audit" || hash === "audit") return { view: "audit" };
  const match = hash.match(/^\/servers\/([^/]+)(?:\/([^/]+))?$/) || hash.match(/^server\/([^/]+)(?:\/([^/]+))?$/);
  if (match) {
    try {
      return { view: "detail", id: decodeURIComponent(match[1]), tab: match[2] || "console" };
    } catch {
      return { view: "dashboard" };
    }
  }
  return { view: "dashboard" };
}

function sparklinePoints(samples, width = 180, height = 90) {
  const values = samples.length ? samples.map((sample) => typeof sample === "object" ? sample.v : sample) : [0];
  const max = Math.max(1, ...values);
  return values.map((sample, index) => {
    const x = values.length === 1 ? 0 : index / (values.length - 1) * width;
    const y = height - 4 - Math.min(max, Math.max(0, Number(sample) || 0)) / max * (height - 8);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).concat(values.length === 1 ? [`${width.toFixed(1)},${(height - 4).toFixed(1)}`] : []).join(" ");
}

function metricNumber(value) { return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null; }

function renderMetricChart(svgId, samples = [], { unit = "", formatValue = (value) => String(value), capacity = null, domain = null, gapMs = 120000 } = {}) {
  const svg = byId(svgId);
  if (!svg) return;
  const plot = { x0: 30, x1: 215, y0: 8, y1: 90 };
  const ordered = [...new Map(samples.filter((sample) => sample && Number.isFinite(sample.t) && domain && sample.t >= domain.start && sample.t <= domain.end).map((sample) => [sample.t, { ...sample, v: metricNumber(sample.v) }])).values()].sort((a, b) => a.t - b.t);
  const values = ordered.map((sample) => sample.v).filter(Number.isFinite);
  const max = Number.isFinite(Number(capacity)) && Number(capacity) > 0 ? Number(capacity) : Math.max(1, ...values);
  const start = domain?.start;
  const end = domain?.end;
  const span = end > start ? end - start : 1;
  const segments = [[]];
  ordered.forEach((sample, index) => {
    if (index && sample.t - ordered[index - 1].t > gapMs) segments.push([]);
    const value = sample.v;
    if (value == null || (sample.state && sample.state !== "available")) { segments.push([]); return; }
    const x = plot.x0 + ((sample.t - start) / span) * (plot.x1 - plot.x0);
    const y = plot.y1 - (Math.min(max, Math.max(0, value)) / max) * (plot.y1 - plot.y0);
    segments.at(-1).push(`${x.toFixed(1)},${y.toFixed(1)}`);
  });
  const first = svg.querySelector(".chart-line");
  svg.querySelectorAll(".chart-line").forEach((line, index) => { if (index) line.remove(); });
  svg.querySelectorAll(".chart-dot").forEach((dot) => dot.remove());
  const lines = [];
  segments.filter((segment) => segment.length).forEach((segment, index) => {
    const line = index === 0 ? first : first.cloneNode(); line.setAttribute("points", segment.join(" ")); if (index > 0) first.parentNode.append(line); lines.push(line);
    if (segment.length === 1) {
      const dot = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      const [x, y] = segment[0].split(","); dot.setAttribute("cx", x); dot.setAttribute("cy", y); dot.setAttribute("r", "1.5"); dot.setAttribute("class", "chart-dot"); svg.append(dot);
    }
  });
  if (!lines.length) first.setAttribute("points", "");
  svg.querySelector(".chart-ymax").textContent = formatValue(max) + unit;
  svg.querySelector(".chart-ymid").textContent = formatValue(max / 2) + unit;
  const crossDay = Number.isFinite(start) && new Date(start).toDateString() !== new Date(end).toDateString();
  const fmt = (time) => new Date(time).toLocaleString([], { ...(crossDay ? { month: "short", day: "numeric" } : {}), hour: "2-digit", minute: "2-digit" });
  svg.querySelector(".chart-x0").textContent = Number.isFinite(start) ? fmt(start) : "—";
  svg.querySelector(".chart-x1").textContent = Number.isFinite(end) ? fmt(end) : "—";
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-label", `${svgId.includes("memory") ? "Memory RSS" : svgId.includes("players") ? "Players" : "CPU"}: ${values.length} observations${domain ? ` since ${new Date(start).toLocaleString()}` : "; no active run"}. ${Math.max(0, lines.length - 1)} gaps. ${metricNumber(capacity) > 0 ? `Capacity ${formatValue(capacity)}${unit}.` : "Scale follows observed values; capacity unavailable."}`);
}

const METRIC_WINDOWS = [["1h", 3600000], ["6h", 21600000], ["24h", 86400000], ["7d", 604800000], ["30d", 2592000000], ["1y", 31536000000]];
function metricWindow(startedAt) {
  const elapsed = Math.max(0, Date.now() - (Date.parse(startedAt || "") || Date.now()));
  return (METRIC_WINDOWS.find(([, ms]) => elapsed <= ms) || METRIC_WINDOWS.at(-1))[0];
}
function metricRunKey(status) {
  const started = Date.parse(status?.started_at);
  return ["running", "starting", "stopping"].includes(status?.state) && Number.isInteger(status?.pid) && status.pid > 0 && Number.isFinite(started) && started <= Date.now() ? `${status.pid}|${started}` : null;
}
const metricCapacityFlights = new Map();
function clearMetricTimer() {
  if (state.detail.metricTimer) window.clearInterval(state.detail.metricTimer);
  state.detail.metricTimer = null; state.detail.metricAbort?.abort(); state.detail.metricAbort = null;
  state.detail.metricRequest += 1;
}
function metricSeries(result) {
  const raw = result?.context?.series || result?.series || {};
  const convert = (name, divisor = 1) => (Array.isArray(raw[name]) ? raw[name] : []).map((item) => ({ t: Date.parse(item.ts || item.timestamp), v: metricNumber(item.value) == null ? null : item.value / divisor, state: item.state || "unavailable" })).filter((item) => Number.isFinite(item.t)).sort((a, b) => a.t - b.t);
  return { cpu: convert("cpu_percent"), memory: convert("rss_bytes", 1073741824), players: [] };
}
async function loadMetricHistory(id) {
  if (!pageVisible() || !id || state.detail.tab !== "metrics") return;
  const status = state.statuses.get(id) || {}; const key = metricRunKey(status);
  const retained = state.metricHistory.get(id);
  // A stopped profile with no in-memory history still has a bounded server-side
  // view; one fetch (no new persistence) shows recent observations on a cold
  // load.  A failed attempt is remembered so the tab does not retry in a loop.
  const cold = !key && status.state === "stopped" && !retained?.historical && !retained?.coldFailed;
  if ((!key && !cold) || state.detail.id !== id || state.detail.metricAbort) return;
  const request = ++state.detail.metricRequest; state.detail.metricAbort?.abort();
  const controller = new AbortController(); state.detail.metricAbort = controller;
  try {
    const windowKey = key ? metricWindow(status.started_at) : "24h";
    const resolution = key && Date.now() - Date.parse(status.started_at) <= 86400000 ? "1m" : "1h";
    const options = { signal: controller.signal };
    const hours = key ? Math.min(8760, Math.max(1, Math.ceil((Date.now() - Date.parse(status.started_at)) / 3600000))) : 24;
    const [history, summary] = await Promise.all([
      api(`/api/v1/profiles/${encodeURIComponent(id)}/stats/tps?window=${windowKey}&resolution=${resolution}&limit=720`, options),
      api(`/api/v1/profiles/${encodeURIComponent(id)}/stats/summary?hours=${hours}`, options).catch(() => null),
    ]);
    const current = state.statuses.get(id) || {};
    if (
      controller.signal.aborted || !pageVisible() || state.detail.tab !== "metrics"
      || request !== state.detail.metricRequest || state.detail.id !== id
      || (key ? metricRunKey(current) !== key : current.state !== "stopped")
    ) return;
    const series = metricSeries(history);
    if (!key) {
      const points = [...series.cpu, ...series.memory, ...series.players].filter((point) => point.state !== "unavailable");
      const times = points.map((point) => point.t).filter(Number.isFinite);
      state.metricHistory.set(id, {
        cold: true, historical: true, window: windowKey,
        resolution: history.resolution || resolution, fetchedAt: Date.now(), stale: false,
        startedAt: times.length ? Math.min(...times) : null,
        endedAt: times.length ? Math.max(...times) : null,
        ...series,
      });
      patchDetail(id);
      return;
    }
    const occupancy = summary?.occupancy?.samples;
    const previous = state.metricHistory.get(id);
    series.players = Array.isArray(occupancy)
      ? occupancy.map((point) => ({ t: Date.parse(point.ts), v: metricNumber(point.count) })).filter((point) => Number.isFinite(point.t)).sort((a, b) => a.t - b.t)
      : previous?.key === key ? previous.players || [] : [];
    state.metricHistory.set(id, { key, window: windowKey, resolution: history.resolution || resolution, fetchedAt: Date.now(), stale: false, playersStale: !Array.isArray(occupancy), ...series });
    patchDetail(id);
  } catch (error) {
    if (controller.signal.aborted || error?.name === "AbortError" || request !== state.detail.metricRequest || state.detail.id !== id) return;
    const latest = state.statuses.get(id) || {};
    if (key ? metricRunKey(latest) !== key : latest.state !== "stopped") return;
    const previous = state.metricHistory.get(id);
    state.metricHistory.set(id, key
      ? { ...previous, key, stale: true, attemptedAt: Date.now() }
      : { ...previous, cold: true, coldFailed: true, historical: true, stale: true, attemptedAt: Date.now() });
    patchDetail(id);
  }
  finally { if (state.detail.metricAbort === controller) state.detail.metricAbort = null; }
}
function startMetricRefresh(id) {
  clearMetricTimer();
  const refresh = () => { if (pageVisible() && state.detail.id === id) { void loadMetricCapacity(id); void loadMetricHistory(id); } };
  refresh(); state.detail.metricTimer = window.setInterval(refresh, 30000);
}
async function loadMetricCapacity(id) {
  const status = state.statuses.get(id); const key = metricRunKey(status);
  if (!pageVisible() || state.detail.id !== id || !key || metricCapacityFlights.has(id)) return;
  const cached = state.metricCapacity.get(id);
  if (cached?.key === key && Date.now() - cached.fetchedAt < 30000) return;
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), 7000);
  const pending = {};
  metricCapacityFlights.set(id, pending);
  try {
    const capacity = await api(`/api/v1/profiles/${encodeURIComponent(id)}/resource-capacity`, { signal: controller.signal });
    if (metricRunKey(state.statuses.get(id)) !== key) return;
    const matches = capacity?.pid === status.pid && Date.parse(capacity.started_at) === Date.parse(status.started_at);
    state.metricCapacity.set(id, { ...(matches ? capacity : {}), key, fetchedAt: Date.now() });
  } catch { if (metricRunKey(state.statuses.get(id)) === key) state.metricCapacity.set(id, { key, fetchedAt: Date.now() }); }
  finally { window.clearTimeout(timeout); if (metricCapacityFlights.get(id) === pending) metricCapacityFlights.delete(id); }
  if (pageVisible() && state.detail.id === id) patchDetail(id);
}
function patchCapacityTrack(id, value, maximum) {
  const node = byId(id); const known = metricNumber(value) != null && metricNumber(maximum) > 0;
  node.querySelector("i").style.width = known ? `${Math.min(100, value / maximum * 100)}%` : "0%";
  if (known) { node.setAttribute("role", "meter"); node.setAttribute("aria-valuemin", "0"); node.setAttribute("aria-valuemax", String(maximum)); node.setAttribute("aria-valuenow", String(Math.min(value, maximum))); }
  else { node.setAttribute("role", "img"); ["aria-valuemin", "aria-valuemax", "aria-valuenow"].forEach((name) => node.removeAttribute(name)); }
}

const STATS_PROFILES = new Set(["minecraft", "minecraft-sunlit-cobblemon", "terraria-vanilla", "terraria-tmod", "pz-rising"]);
const TICK_PROFILES = new Set(["minecraft", "minecraft-sunlit-cobblemon"]);
const STATS_WINDOWS = { "1h": 1, "6h": 6, "24h": 24, "7d": 168, "30d": 720 };
const HEATMAP_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

function clearStatsTimer() {
  if (state.detail.statsTimer) window.clearInterval(state.detail.statsTimer);
  state.detail.statsTimer = null;
  state.detail.statsAbort?.abort();
  state.detail.statsAbort = null;
}

function renderStatsSummary(summary) {
  const countOnly = summary?.player_tracking === "count";
  byId("stats-summary").hidden = countOnly;
  byId("stats-leaderboard").closest(".stats-block").hidden = countOnly;
  byId("stats-heatmap").closest(".stats-block").hidden = countOnly;
  byId("stats-occupancy-block").hidden = !countOnly;
  const total = Number(summary?.total_hours);
  const unique = Number(summary?.unique_players);
  byId("stats-total-hours").textContent = Number.isFinite(total) ? total.toFixed(2) : "—";
  byId("stats-unique-players").textContent = Number.isFinite(unique) ? String(unique) : "—";
  byId("stats-leaderboard-meta").textContent = Number.isFinite(unique) ? `${unique} player${unique === 1 ? "" : "s"}` : "—";
  const latest = Number(summary?.occupancy?.latest);
  byId("stats-occupancy-current").textContent = Number.isFinite(latest) ? `${latest} online` : "Unavailable";
}

function renderStatsLeaderboard(rows) {
  const body = byId("stats-leaderboard");
  body.replaceChildren();
  if (!Array.isArray(rows) || !rows.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.className = "empty-state";
    cell.colSpan = 4;
    cell.textContent = "No sessions recorded yet.";
    row.append(cell);
    body.append(row);
    return;
  }
  rows.forEach((item) => {
    const row = document.createElement("tr");
    const player = document.createElement("th"); player.scope = "row"; player.textContent = item.player || "Unknown player";
    const hours = document.createElement("td"); hours.textContent = Number.isFinite(Number(item.hours)) ? Number(item.hours).toFixed(2) : "—";
    const sessions = document.createElement("td"); sessions.textContent = Number.isFinite(Number(item.sessions)) ? String(item.sessions) : "—";
    const lastSeen = document.createElement("td");
    const parsed = item.last_seen ? new Date(item.last_seen) : null;
    lastSeen.textContent = parsed && !Number.isNaN(parsed.valueOf()) ? parsed.toLocaleString() : "—";
    row.append(player, hours, sessions, lastSeen);
    body.append(row);
  });
}

function renderStatsHeatmap(result) {
  const grid = byId("stats-heatmap");
  grid.replaceChildren();
  const buckets = Array.isArray(result?.buckets) ? result.buckets : [];
  const values = buckets.flatMap((row) => Array.isArray(row) ? row.map(Number) : []).filter(Number.isFinite);
  const maximum = Math.max(0, ...values);
  HEATMAP_LABELS.forEach((label, day) => {
    const row = document.createElement("div");
    row.className = "heatmap-row";
    const heading = document.createElement("span");
    heading.className = "heatmap-label";
    heading.textContent = label;
    row.append(heading);
    for (let hour = 0; hour < 24; hour += 1) {
      const value = Number(buckets[day]?.[hour]) || 0;
      const cell = document.createElement("span");
      cell.className = "heatmap-cell";
      cell.style.setProperty("--heat", maximum ? String(Math.min(1, value / maximum)) : "0");
      cell.title = `${label} ${String(hour).padStart(2, "0")}:00 UTC · ${value.toFixed(2)} player-hours`;
      cell.setAttribute("aria-label", cell.title);
      row.append(cell);
    }
    grid.append(row);
  });
}

function recorderSamples(result) {
  return (Array.isArray(result?.samples) ? result.samples : []).map((sample) => {
    const time = Date.parse(sample.ts || sample.timestamp);
    const finite = (value) => value === null || value === undefined || value === "" ? null : Number(value);
    const tps = finite(sample.tps);
    const mspt = finite(sample.mspt);
    const stateName = ["available", "inactive", "unavailable"].includes(sample.state)
      ? sample.state : (Number.isFinite(tps) && Number.isFinite(mspt) ? "available" : "unavailable");
    return { ...sample, time, tps: Number.isFinite(tps) ? tps : null, mspt: Number.isFinite(mspt) ? mspt : null, state: stateName };
  }).filter((sample) => Number.isFinite(sample.time)).sort((a, b) => a.time - b.time);
}

function recorderSignal(result, samples) {
  const latest = samples.at(-1);
  if (latest?.state === "inactive") return "offline";
  if (!latest || result?.stale === true || result?.state === "unknown") return "stale";
  return latest.state === "available" ? "live" : "stale";
}

function recorderLatestObservation(result, samples) {
  const explicit = result?.latest_observation;
  if (explicit && typeof explicit === "object") {
    const time = Date.parse(explicit.ts || explicit.timestamp);
    const tps = explicit.tps === null || explicit.tps === undefined ? null : Number(explicit.tps);
    const mspt = explicit.mspt === null || explicit.mspt === undefined ? null : Number(explicit.mspt);
    if (Number.isFinite(time) && Number.isFinite(tps) && Number.isFinite(mspt)) {
      return { time, tps, mspt, state: "available", stale: explicit.stale === true };
    }
  }
  return [...samples].reverse().find((sample) => sample.state === "available") || null;
}

function recorderContext(result) {
  const raw = result?.context?.series && typeof result.context.series === "object" ? result.context.series : {};
  const series = {};
  ["cpu_percent", "rss_bytes", "gc_pause"].forEach((metric) => {
    series[metric] = (Array.isArray(raw[metric]) ? raw[metric] : []).map((item) => ({
      time: Date.parse(item.ts || item.timestamp), value: Number(item.value), state: item.state || "available",
    })).filter((item) => Number.isFinite(item.time) && Number.isFinite(item.value));
  });
  const allowed = new Set(["backup", "update", "benchmark", "restart", "start", "stop", "switch"]);
  const jobs = (Array.isArray(result?.context?.jobs) ? result.context.jobs : []).filter((item) => allowed.has(item.kind)).map((item) => ({
    kind: item.kind, start: Date.parse(item.started_at), end: Date.parse(item.ended_at || item.started_at), state: item.state,
  })).filter((item) => Number.isFinite(item.start));
  return { series, jobs };
}

function recorderDomain(samples) {
  const hours = STATS_WINDOWS[byId("stats-window")?.value] || 6;
  const width = hours * 60 * 60 * 1000;
  const latest = samples.at(-1)?.time;
  const now = Date.now();
  const end = Number.isFinite(latest) && Math.abs(now - latest) > width ? latest : now;
  return { start: end - width, end };
}

function drawRecorderLine(ctx, items, x, y, color, { width = 1.6, dash = [] } = {}) {
  let open = false;
  ctx.beginPath(); ctx.strokeStyle = color; ctx.lineWidth = width; ctx.setLineDash(dash);
  items.forEach((item) => {
    if (item.state !== "available" || item.value == null || !Number.isFinite(item.value)) { open = false; return; }
    const px = x(item.time); const py = y(item.value);
    if (!open) { ctx.moveTo(px, py); open = true; } else ctx.lineTo(px, py);
  });
  ctx.stroke(); ctx.setLineDash([]);
}

function recorderComparisonSamples(mode, comparison, domain) {
  if (!Array.isArray(comparison?.samples)) return [];
  const normalized = recorderSamples({ samples: comparison.samples });
  if (!normalized.length) return [];
  if (mode === "restart") return normalized;
  if (mode === "yesterday") return normalized.map((item) => ({ ...item, time: item.time + 24 * 60 * 60 * 1000 }));
  if (mode === "previous") {
    const first = normalized[0].time;
    return normalized.map((item) => ({ ...item, time: domain.start + (item.time - first) }));
  }
  return [];
}

function drawFlightRecorder(result, samples) {
  const drawStarted = window.performance?.now?.() || 0;
  const canvas = byId("stats-tps-chart");
  if (!canvas) return;
  const context = recorderContext(result);
  const comparisonMode = byId("stats-comparison")?.value || "none";
  const comparison = comparisonMode === "none" ? null : result?.comparisons?.[comparisonMode];
  const css = getComputedStyle(document.documentElement);
  const color = (name, fallback) => css.getPropertyValue(name).trim() || fallback;
  const dpr = Math.min(2, Math.max(1, window.devicePixelRatio || 1));
  const width = Math.max(280, Math.round(canvas.clientWidth || 520)); const height = 220;
  const signature = JSON.stringify([width, dpr, result?.resolution, samples, context, comparisonMode, comparison]);
  if (canvas.dataset.signature === signature) return;
  canvas.dataset.signature = signature;
  canvas.width = Math.round(width * dpr); canvas.height = Math.round(height * dpr);
  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  ctx.scale(dpr, dpr);
  const plot = { left: 48, right: width - 14, top: 22, bottom: height - 32 };
  const domain = recorderDomain(samples);
  const x = (time) => plot.left + Math.max(0, Math.min(1, (time - domain.start) / (domain.end - domain.start))) * (plot.right - plot.left);
  const tpsMax = Math.max(20, ...samples.map((item) => item.tps || 0));
  const msptMax = Math.max(50, ...samples.map((item) => item.mspt || 0));
  const yTps = (value) => plot.bottom - Math.max(0, value) / tpsMax * (plot.bottom - plot.top);
  const yMspt = (value) => plot.bottom - Math.max(0, value) / msptMax * (plot.bottom - plot.top);
  ctx.fillStyle = color("--console-surface", "#071011"); ctx.fillRect(0, 0, width, height);
  ctx.strokeStyle = color("--line", "#253033"); ctx.fillStyle = color("--muted", "#8d9a9f"); ctx.font = "10px ui-monospace, monospace";
  [0, .5, 1].forEach((ratio) => {
    const py = plot.bottom - ratio * (plot.bottom - plot.top);
    ctx.beginPath(); ctx.moveTo(plot.left, py); ctx.lineTo(plot.right, py); ctx.stroke();
    ctx.fillText(`${(tpsMax * ratio).toFixed(0)} TPS`, 4, py + 3);
  });
  const inDomain = samples.filter((item) => item.time >= domain.start && item.time <= domain.end);
  inDomain.forEach((item, index) => {
    const next = inDomain[index + 1]; const end = next ? next.time : domain.end;
    const left = x(item.time); const bandWidth = Math.max(1, x(end) - left);
    if (item.state === "inactive") { ctx.fillStyle = "rgba(88, 101, 105, .22)"; ctx.fillRect(left, plot.top, bandWidth, plot.bottom - plot.top); }
    else if (Number(item.inactive_fraction) > 0) { ctx.fillStyle = "rgba(88, 101, 105, .22)"; ctx.fillRect(left, plot.top, bandWidth * Math.min(1, Number(item.inactive_fraction)), plot.bottom - plot.top); }
    if (item.state === "available") { ctx.fillStyle = "rgba(89, 217, 145, .45)"; ctx.fillRect(left, plot.bottom + 7, bandWidth, 3); }
  });
  drawRecorderLine(ctx, inDomain.map((item) => ({ time: item.time, value: item.tps, state: item.state })), x, yTps, color("--accent", "#59d991"), { width: 2 });
  drawRecorderLine(ctx, inDomain.map((item) => ({ time: item.time, value: item.mspt, state: item.state })), x, yMspt, "#d6e5a8", { width: 1.5 });
  ["cpu_percent", "rss_bytes"].forEach((metric, index) => {
    const items = context.series[metric].filter((item) => item.time >= domain.start && item.time <= domain.end);
    const maximum = Math.max(1, ...items.map((item) => item.value));
    drawRecorderLine(ctx, items, x, (value) => plot.bottom - (value / maximum) * (plot.bottom - plot.top) * .34 - index * 6, "rgba(101, 168, 162, .55)", { width: 1 });
  });
  context.series.gc_pause.forEach((item) => {
    if (item.time < domain.start || item.time > domain.end) return;
    const px = x(item.time); ctx.fillStyle = color("--warn", "#d7aa55"); ctx.beginPath(); ctx.arc(px, plot.top + 7, 2.5, 0, Math.PI * 2); ctx.fill();
  });
  context.jobs.forEach((job, index) => {
    if (job.start > domain.end || (job.end || job.start) < domain.start) return;
    const left = x(Math.max(domain.start, job.start)); const right = x(Math.min(domain.end, job.end || job.start));
    ctx.fillStyle = "rgba(215, 170, 85, .12)"; ctx.fillRect(left, plot.top, Math.max(2, right - left), plot.bottom - plot.top);
    if (index < 8) { ctx.fillStyle = color("--warn", "#d7aa55"); ctx.fillText(job.kind, Math.min(left + 3, plot.right - 55), plot.top + 12); }
  });
  const alignedComparison = recorderComparisonSamples(comparisonMode, comparison, domain);
  canvas.dataset.comparisonAlignment = comparisonMode === "restart" ? "wall-clock"
    : comparisonMode === "yesterday" ? "plus-24h" : comparisonMode === "previous" ? "relative" : "none";
  canvas.dataset.comparisonStart = alignedComparison.length ? String(alignedComparison[0].time) : "";
  if (alignedComparison.length) {
    drawRecorderLine(ctx, alignedComparison.map((item) => ({ time: item.time, value: item.tps, state: item.state })),
      x, yTps, "rgba(159, 176, 184, .8)", { width: 1, dash: [4, 4] });
  }
  if (comparisonMode === "preset") {
    const metric = comparison?.metrics?.mspt_p95;
    const safe = (value) => value === null || value === undefined || value === "" ? null : Number(value);
    const baseline = safe(metric?.baseline); const candidate = safe(metric?.candidate);
    if (Number.isFinite(baseline) && Number.isFinite(candidate) && baseline >= 0 && candidate >= 0) {
      [[baseline, "rgba(159, 176, 184, .82)", [4, 4]], [candidate, color("--accent", "#59d991"), [2, 3]]].forEach(([value, stroke, dash]) => {
        const py = yMspt(value); ctx.beginPath(); ctx.strokeStyle = stroke; ctx.lineWidth = 1; ctx.setLineDash(dash);
        ctx.moveTo(plot.left, py); ctx.lineTo(plot.right, py); ctx.stroke(); ctx.setLineDash([]);
      });
    }
  }
  const fmt = (time) => new Date(time).toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  ctx.fillStyle = color("--muted", "#8d9a9f"); ctx.fillText(fmt(domain.start), plot.left, height - 9);
  const endText = fmt(domain.end); ctx.fillText(endText, plot.right - ctx.measureText(endText).width, height - 9);
  queueClientPerformance("recorder_draw", (window.performance?.now?.() || drawStarted) - drawStarted);
}

function renderRecorderTable(result, samples) {
  const body = byId("stats-recorder-table");
  body.replaceChildren();
  const context = recorderContext(result);
  const causes = [...context.jobs.map((job) => ({ time: job.start, label: job.kind })),
    ...context.series.gc_pause.map((item) => ({ time: item.time, label: "GC pause" }))];
  samples.slice(-120).forEach((sample) => {
    const row = document.createElement("tr");
    const nearby = [...new Set(causes.filter((item) => Math.abs(item.time - sample.time) <= 60_000)
      .map((item) => item.label))];
    const cause = nearby.length ? nearby.join(", ") : "—";
    [new Date(sample.time).toLocaleString(), sample.state, sample.tps == null ? "—" : sample.tps.toFixed(2), sample.mspt == null ? "—" : `${sample.mspt.toFixed(2)} ms`, cause].forEach((value) => {
      const cell = document.createElement("td"); cell.textContent = value; row.append(cell);
    });
    body.append(row);
  });
  if (!body.children.length) { const row = document.createElement("tr"); const cell = document.createElement("td"); cell.colSpan = 5; cell.className = "empty-state"; cell.textContent = "No telemetry in this window."; row.append(cell); body.append(row); }
}

function renderComparisonOptions(result) {
  const select = byId("stats-comparison");
  if (!select) return;
  const comparisons = result?.comparisons && typeof result.comparisons === "object" ? result.comparisons : {};
  const definitions = {
    yesterday: ["Yesterday vs today", "Yesterday unavailable", (item) => Array.isArray(item?.samples) && recorderSamples({ samples: item.samples }).length > 0],
    restart: ["Before vs after restart", "Restart comparison unavailable", (item) => Array.isArray(item?.samples) && recorderSamples({ samples: item.samples }).length > 0],
    preset: ["Preset vs preset", "Preset comparison unavailable", (item) => {
      const metric = item?.metrics?.mspt_p95; const baseline = metric?.baseline; const candidate = metric?.candidate;
      return typeof baseline === "number" && Number.isFinite(baseline) && baseline >= 0 && typeof candidate === "number" && Number.isFinite(candidate) && candidate >= 0;
    }],
    previous: ["Previous active run", "Previous run unavailable", (item) => Array.isArray(item?.samples) && recorderSamples({ samples: item.samples }).length > 0],
  };
  Object.entries(definitions).forEach(([key, [availableLabel, unavailableLabel, validate]]) => {
    const option = select.querySelector(`option[value="${key}"]`); if (!option) return;
    const available = validate(comparisons[key]); option.disabled = !available;
    option.textContent = available ? availableLabel : unavailableLabel;
  });
  if (select.value !== "none" && select.selectedOptions[0]?.disabled) select.value = "none";
  const selected = comparisons[select.value];
  const note = byId("stats-comparison-note");
  if (!note) return;
  if (select.value === "preset" && selected) {
    const safePreset = (value) => typeof value === "string" && /^[a-z0-9_-]{1,32}$/.test(value) ? value : "preset";
    const metric = selected.metrics.mspt_p95;
    note.textContent = `${safePreset(selected.baseline_preset)} ${metric.baseline.toFixed(2)} ms p95 · ${safePreset(selected.candidate_preset)} ${metric.candidate.toFixed(2)} ms p95`;
  } else if (select.value !== "none" && selected) {
    note.textContent = typeof selected.label === "string" && selected.label.length <= 64 ? selected.label : "Comparison loaded.";
  } else note.textContent = "No comparison selected.";
}

function renderStatsTpsUnavailable(message, { preserve = false } = {}) {
  const block = byId("stats-tps-title")?.closest(".stats-tps-block");
  if (block) { block.dataset.signal = "stale"; block.setAttribute("aria-busy", "false"); }
  byId("stats-live-state").textContent = "Stale";
  byId("stats-tps-note").textContent = message;
  if (!preserve) {
    byId("stats-tps-current").textContent = "—"; byId("stats-mspt-current").textContent = "—";
    byId("stats-runtime-current").textContent = "—"; renderRecorderTable({}, []); drawFlightRecorder({}, []);
  }
}

function renderStatsUnavailable(message) {
  byId("stats-summary").hidden = false;
  byId("stats-leaderboard").closest(".stats-block").hidden = false;
  byId("stats-heatmap").closest(".stats-block").hidden = false;
  byId("stats-occupancy-block").hidden = true;
  if (!state.statsCache.get(state.detail.id)?.base) {
    renderStatsSummary({}); renderStatsLeaderboard([]); renderStatsHeatmap({ buckets: [] });
  }
  renderStatsTpsUnavailable(message, { preserve: Boolean(state.statsCache.get(state.detail.id)?.tps) });
}

function renderStatsTps(result) {
  const samples = recorderSamples(result); const latest = recorderLatestObservation(result, samples);
  const signal = recorderSignal(result, samples); const block = byId("stats-tps-title")?.closest(".stats-tps-block");
  if (block) { block.dataset.signal = signal; block.setAttribute("aria-busy", "false"); }
  byId("stats-live-state").textContent = signal === "live" ? "Live" : signal === "offline" ? "Server stopped" : "Stale";
  byId("stats-tps-current").textContent = latest?.tps != null ? `${latest.tps.toFixed(2)} TPS` : "—";
  byId("stats-mspt-current").textContent = latest?.mspt != null ? `${latest.mspt.toFixed(2)} ms/tick` : "—";
  const basis = result?.time_basis || {}; const active = Number(basis.active_runtime_seconds); const wall = Number(basis.wall_clock_seconds);
  byId("stats-runtime-current").textContent = Number.isFinite(active) && Number.isFinite(wall) ? `${Math.round(active / 60)}m / ${Math.round(wall / 3600)}h` : "—";
  const effective = result?.resolution || "raw"; byId("stats-effective-resolution").textContent = `${effective} · ${samples.length}/${result?.limit || 720}`;
  const observedAt = latest?.time ? new Date(latest.time).toLocaleString() : null;
  byId("stats-tps-note").textContent = signal === "offline" ? (observedAt
    ? `The server is stopped. Last tick observation: ${observedAt}. Offline time is shaded, not plotted as zero.`
    : "The server is stopped. Offline time is shaded, not plotted as zero.")
    : signal === "stale" ? (observedAt
      ? `No tick samples fall inside this window. Last observation: ${observedAt}.`
      : "The latest tick sample is stale. No last observation is available.")
      : "Current tick signal is fresh. Markers show nearby resource and maintenance activity.";
  renderComparisonOptions(result); drawFlightRecorder(result, samples); renderRecorderTable(result, samples);
}

async function loadStats(id, { includeBase = true } = {}) {
  if (!pageVisible() || !id || state.detail.tab !== "stats") return;
  const request = ++state.detail.statsRequest;
  const fetchStarted = window.performance?.now?.() || 0;
  state.detail.statsAbort?.abort();
  const controller = new AbortController(); state.detail.statsAbort = controller;
  if (!STATS_PROFILES.has(id)) { renderStatsUnavailable("Player stats not available for this game."); return; }
  const base = `/api/v1/profiles/${encodeURIComponent(id)}/stats`;
  const windowKey = byId("stats-window")?.value || "24h";
  const resolution = byId("stats-resolution")?.value || "auto";
  try {
    const hours = STATS_WINDOWS[windowKey] || 24;
    const requests = [];
    if (includeBase || !state.detail.statsBaseLoaded) {
      requests.push(api(`${base}/summary?hours=${hours}`, { signal: controller.signal }), api(`${base}/heatmap?hours=${hours}`, { signal: controller.signal }));
    }
    if (TICK_PROFILES.has(id)) requests.push(api(`${base}/tps?window=${encodeURIComponent(windowKey)}&resolution=${encodeURIComponent(resolution)}&limit=720`, { signal: controller.signal }));
    const results = await Promise.all(requests);
    if (request !== state.detail.statsRequest || state.detail.id !== id) return;
    let offset = 0;
    if (includeBase || !state.detail.statsBaseLoaded) {
      const [summary, heatmap] = results;
      state.detail.statsBaseLoaded = true;
      renderStatsSummary(summary); renderStatsLeaderboard(summary.leaderboard); renderStatsHeatmap(heatmap);
      const cached = state.statsCache.get(id) || {}; cached.base = { summary, heatmap }; state.statsCache.set(id, cached);
      offset = 2;
    }
    const tpsBlock = byId("stats-tps-title").closest(".stats-tps-block");
    tpsBlock.hidden = !TICK_PROFILES.has(id);
    if (TICK_PROFILES.has(id)) {
      const cached = state.statsCache.get(id) || {}; cached.tps = results[offset]; state.statsCache.set(id, cached);
      renderStatsTps(results[offset]);
    }
    queueClientPerformance("stats_fetch", (window.performance?.now?.() || fetchStarted) - fetchStarted);
  } catch (error) {
    if (controller.signal.aborted || error?.name === "AbortError") return;
    if (request !== state.detail.statsRequest) return;
    renderStatsUnavailable(error.message || "Stats unavailable.");
    notify(error.message || "Stats unavailable.");
  } finally {
    if (state.detail.statsAbort === controller) state.detail.statsAbort = null;
  }
}

function startStatsRefresh(id) {
  clearStatsTimer();
  const cached = state.statsCache.get(id);
  state.detail.statsBaseLoaded = Boolean(cached?.base);
  if (cached?.base) { renderStatsSummary(cached.base.summary); renderStatsLeaderboard(cached.base.summary?.leaderboard); renderStatsHeatmap(cached.base.heatmap); }
  if (cached?.tps) renderStatsTps(cached.tps);
  loadStats(id, { includeBase: true });
  // Recurring timer: live recorder refresh only; callback is visibility-gated.
  state.detail.statsTimer = window.setInterval(() => {
    if (pageVisible()) loadStats(id, { includeBase: false });
  }, 4000);
}

function clearBenchmarkTimer() {
  if (state.detail.benchmarkTimer) window.clearTimeout(state.detail.benchmarkTimer);
  state.detail.benchmarkTimer = null;
}

function benchmarkValue(name, value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "—";
  if (name.endsWith("Nanos")) return `${(number / 1_000_000).toFixed(3)} ms`;
  if (name.includes("Bytes")) {
    const units = ["B", "KiB", "MiB", "GiB"];
    let amount = Math.abs(number); let index = 0;
    while (amount >= 1024 && index < units.length - 1) { amount /= 1024; index += 1; }
    return `${number < 0 ? "−" : ""}${amount.toFixed(index ? 2 : 0)} ${units[index]}`;
  }
  if (name.includes("Utilization") || name.includes("CpuLoad")) return `${(number * 100).toFixed(2)}%`;
  if (name.endsWith("PauseMs")) return `${number.toFixed(3)} ms`;
  return Number.isInteger(number) ? String(number) : number.toFixed(3);
}

function benchmarkDelta(metric) {
  const percent = Number(metric?.delta_percent);
  if (!Number.isFinite(percent)) return "—";
  const prefix = percent > 0 ? "+" : "";
  return `${prefix}${(percent * 100).toFixed(2)}%`;
}

function validateBenchmarkForm() {
  const id = state.detail.id;
  const overview = id ? state.benchmarks.get(id) : null;
  const baseline = byId("benchmark-baseline")?.value;
  const candidate = byId("benchmark-candidate")?.value;
  const status = detailStatus(id);
  const slotIdle = !slotOwnerId();
  const stopped = ["stopped", "failed", "blocked", "unknown"].includes(status.state);
  const runningJob = overview?.runs?.some((run) => run.state === "running");
  const enabled = Boolean(overview?.available && baseline && candidate && baseline !== candidate && slotIdle && stopped && !runningJob);
  byId("benchmark-run").disabled = !enabled;
  if (!overview?.available) byId("benchmark-status").textContent = "SwagBench is not configured for this profile.";
  else if (runningJob) byId("benchmark-status").textContent = "A fresh-JVM comparison is running. Horizon will refresh this evidence automatically.";
  else if (!slotIdle || !stopped) byId("benchmark-status").textContent = "Stop all game profiles and leave the shared slot idle before benchmarking.";
  else if (baseline === candidate) byId("benchmark-status").textContent = "Choose two different root-owned JVM presets.";
  else byId("benchmark-status").textContent = "Ready. Each arm starts in a fresh JVM; production remains stopped.";
}

function renderBenchmarks(id, overview) {
  state.benchmarks.set(id, overview);
  const baseline = byId("benchmark-baseline");
  const candidate = byId("benchmark-candidate");
  const priorBaseline = baseline.value;
  const priorCandidate = candidate.value;
  baseline.replaceChildren(); candidate.replaceChildren();
  (overview.presets || []).forEach((preset) => {
    for (const select of [baseline, candidate]) {
      const option = document.createElement("option"); option.value = preset.id; option.textContent = preset.label; select.append(option);
    }
  });
  if ([...baseline.options].some((option) => option.value === priorBaseline)) baseline.value = priorBaseline;
  if ([...candidate.options].some((option) => option.value === priorCandidate)) candidate.value = priorCandidate;
  else if (candidate.options.length > 1) candidate.selectedIndex = 1;

  const runs = Array.isArray(overview.runs) ? overview.runs : [];
  state.detail.benchmarkRuns = state.detail.benchmarkCursor ? state.detail.benchmarkRuns.concat(runs) : runs;
  const allRuns = state.detail.benchmarkRuns;
  const latest = allRuns.find((run) => run.state === "succeeded");
  const verdict = byId("benchmark-verdict");
  verdict.dataset.verdict = latest?.overall_verdict || (overview.available ? "inconclusive" : "unavailable");
  verdict.textContent = latest?.overall_verdict || (overview.available ? "No verdict" : "Unavailable");
  byId("benchmark-latest-meta").textContent = latest
    ? `${latest.baseline_preset} → ${latest.candidate_preset} · ${new Date(latest.finished_at || latest.created_at).toLocaleString()}`
    : "No completed comparison.";
  const diagnostics = latest?.candidate_diagnostics;
  const baselineDiagnostics = latest?.baseline_diagnostics;
  const setDiagnostics = (prefix, value) => {
    byId(`benchmark-${prefix}-bottleneck`).textContent = value?.dominant_bottleneck || "—";
    byId(`benchmark-${prefix}-leak`).textContent = value?.leak_suspected == null ? "—" : value.leak_suspected ? "Suspected" : "Not detected";
    byId(`benchmark-${prefix}-load`).textContent = value?.load_reached_target == null ? "—" : value.load_reached_target ? `Reached · ${Number(value.peak_connected_clients_median || 0).toFixed(0)} clients` : "Not reached";
    byId(`benchmark-${prefix}-duration`).textContent = Number.isFinite(Number(value?.process_duration_seconds_median)) ? `${Number(value.process_duration_seconds_median).toFixed(1)} s` : "—";
  };
  setDiagnostics("baseline", baselineDiagnostics);
  setDiagnostics("candidate", diagnostics);
  const metrics = byId("benchmark-metrics"); metrics.replaceChildren();
  (latest?.metrics || []).forEach((metric) => {
    const row = document.createElement("tr");
    const name = document.createElement("th"); name.scope = "row"; name.textContent = metric.name;
    const left = document.createElement("td"); left.textContent = benchmarkValue(metric.name, metric.baseline_median);
    const right = document.createElement("td"); right.textContent = benchmarkValue(metric.name, metric.candidate_median);
    const delta = document.createElement("td"); delta.textContent = benchmarkDelta(metric);
    const metricVerdict = document.createElement("td"); metricVerdict.textContent = metric.verdict;
    row.append(name, left, right, delta, metricVerdict); metrics.append(row);
  });
  if (!metrics.children.length) {
    const row = document.createElement("tr"); const empty = document.createElement("td"); empty.className = "empty-state"; empty.colSpan = 5; empty.textContent = "No benchmark metrics recorded."; row.append(empty); metrics.append(row);
  }
  const history = byId("benchmark-history"); history.replaceChildren();
  allRuns.forEach((run) => {
    const row = document.createElement("li");
    const label = document.createElement("strong"); label.textContent = `${run.baseline_preset} → ${run.candidate_preset}`;
    const stateNode = document.createElement("span"); stateNode.textContent = run.overall_verdict || run.state;
    const time = document.createElement("time"); time.textContent = new Date(run.finished_at || run.created_at).toLocaleString();
    row.append(label, stateNode, time); history.append(row);
  });
  if (!history.children.length) { const empty = document.createElement("li"); empty.className = "empty-state"; empty.textContent = "No benchmark runs recorded."; history.append(empty); }
  const more = byId("benchmark-load-more");
  more.hidden = !overview.next_cursor;
  more.dataset.cursor = overview.next_cursor || "";
  state.detail.benchmarkCursor = overview.next_cursor || null;
  const warning = byId("benchmark-corrupt-warning");
  warning.hidden = !(overview.corrupt_runs > 0);
  warning.textContent = overview.corrupt_runs > 0 ? `${overview.corrupt_runs} historical run(s) could not be decoded and were omitted.` : "";
  byId("benchmark-history-meta").textContent = `${allRuns.length} loaded · trends ${Array.isArray(overview.trends) ? overview.trends.length : 0}`;
  const trend = byId("benchmark-trend"); trend.replaceChildren();
  (Array.isArray(overview.trends) ? overview.trends : []).slice().reverse().forEach((point) => {
    const group = document.createElement("div"); group.className = "benchmark-trend-run";
    group.setAttribute("aria-label", `${point.verdict || "inconclusive"} run ${new Date(point.finished_at || 0).toLocaleString()}`);
    (point.metrics || []).slice(0, 4).forEach((metric) => {
      const item = document.createElement("span"); item.className = "benchmark-trend-point";
      item.dataset.verdict = metric.verdict || point.verdict || "inconclusive";
      item.textContent = `${metric.name}: ${benchmarkValue(metric.name, metric.candidate_median)} (${benchmarkDelta(metric)})`;
      item.title = `${metric.name}: baseline ${benchmarkValue(metric.name, metric.baseline_median)} · candidate ${benchmarkValue(metric.name, metric.candidate_median)} · ${metric.verdict}`;
      item.setAttribute("aria-label", item.title); group.append(item);
    });
    if (group.children.length) trend.append(group);
  });
  validateBenchmarkForm();
}

async function loadBenchmarks(id, append = false) {
  if (!pageVisible() || !id || state.detail.tab !== "benchmarks") return;
  clearBenchmarkTimer();
  if (!append) { state.detail.benchmarkCursor = null; state.detail.benchmarkRuns = []; }
  try {
    const cursor = state.detail.benchmarkCursor ? `?cursor=${encodeURIComponent(state.detail.benchmarkCursor)}&limit=20` : "";
    const overview = await api(`/api/v1/profiles/${encodeURIComponent(id)}/benchmarks${cursor}`);
    if (state.detail.id !== id || state.detail.tab !== "benchmarks") return;
    renderBenchmarks(id, overview);
    if ((overview.runs || []).some((run) => run.state === "running")) {
      // One-shot continuation of a running benchmark; visibility-gated and resumed on return.
      state.detail.benchmarkTimer = window.setTimeout(() => { if (pageVisible()) loadBenchmarks(id, false); }, 5000);
    }
  } catch (error) {
    byId("benchmark-status").textContent = error.message || "Benchmark evidence is unavailable.";
    byId("benchmark-run").disabled = true;
  }
}

async function runBenchmark(event) {
  event.preventDefault();
  const id = state.detail.id;
  if (!id || byId("benchmark-run").disabled) return;
  byId("benchmark-run").disabled = true;
  byId("benchmark-status").textContent = "Submitting the fresh-JVM comparison…";
  try {
    const result = await api(`/api/v1/profiles/${encodeURIComponent(id)}/benchmarks`, {
      method: "POST",
      body: JSON.stringify({ baseline_preset: byId("benchmark-baseline").value, candidate_preset: byId("benchmark-candidate").value }),
    });
    byId("benchmark-status").textContent = `Benchmark ${result.job_id || "job"} accepted. Production starts remain blocked until it finishes.`;
    notify(`SwagBench comparison accepted for ${profileLabel(id)}.`);
    state.detail.benchmarkTimer = window.setTimeout(() => { if (pageVisible()) loadBenchmarks(id, false); }, 1500);
  } catch (error) {
    byId("benchmark-status").textContent = error.message || "Benchmark request failed.";
    validateBenchmarkForm();
  }
}

function detailProfile(id) {
  return state.profiles.get(id) || { id, display_name: id, operations: [] };
}

function detailStatus(id) {
  return state.statuses.get(id) || { profile_id: id, state: "unknown", health: "unknown" };
}

function detailTabUrl(id, tab) {
  return `#/servers/${encodeURIComponent(id)}/${encodeURIComponent(tab)}`;
}

function commandCatalog(id) {
  const source = window.HORIZON_COMMANDS?.[id];
  const commands = typeof source === "string" ? window.HORIZON_COMMANDS?.[source] : source;
  const result = Array.isArray(commands) ? [...commands] : [];
  if (id === "terraria-tmod") result.push({ cmd: "modlist", args: "", help: "List loaded mods" });
  return result;
}

function renderCommandCatalog(id) {
  const commands = commandCatalog(id);
  const key = `${id}:${JSON.stringify(commands)}`;
  if (state.detail.commandCatalogKey === key) return;
  state.detail.commandCatalogKey = key;
  const datalist = byId("command-suggestions");
  const catalog = byId("command-catalog");
  datalist.replaceChildren();
  catalog.replaceChildren();
  commands.forEach((entry) => {
    const option = document.createElement("option");
    option.value = entry.cmd;
    option.label = entry.help;
    datalist.append(option);
    const row = document.createElement("button");
    row.type = "button";
    row.className = "command-catalog-row";
    row.innerHTML = `<code></code><span></span><small></small>`;
    row.querySelector("code").textContent = entry.cmd;
    row.querySelector("span").textContent = entry.args;
    row.querySelector("small").textContent = entry.help;
    row.addEventListener("click", () => {
      const input = byId("command-input");
      input.value = `${entry.cmd}${entry.args ? " " : ""}`;
      input.focus();
    });
    catalog.append(row);
  });
}

function setDetailTab(tab) {
  const allowed = ["console", "metrics", "stats", "logs", "backups", "benchmarks", "config"];
  const requested = allowed.includes(tab) ? tab : "console";
  const supportsBenchmark = new Set(detailProfile(state.detail.id)?.operations || []).has("benchmark");
  const next = requested === "benchmarks" && !supportsBenchmark ? "console" : requested;
  clearStatsTimer();
  clearBenchmarkTimer();
  state.detail.tab = next;
  if (next !== "stats") state.detail.statsBaseLoaded = false;
  document.querySelectorAll("[data-detail-tab]").forEach((button) => {
    const selected = button.dataset.detailTab === next;
    button.setAttribute("aria-selected", String(selected));
    button.tabIndex = selected ? 0 : -1;
  });
  document.querySelectorAll("#detail-view [role=tabpanel]").forEach((panel) => { panel.hidden = panel.id !== `panel-${next}`; });
  if (next === "logs" || next === "console") loadDetailLogs(state.detail.id);
  if (next === "backups") loadBackups(state.detail.id);
  if (next === "benchmarks") loadBenchmarks(state.detail.id);
  if (next === "config") renderConfig(state.detail.id);
  if (next === "stats") {
    const tpsBlock = byId("stats-tps-title")?.closest(".stats-tps-block");
    if (tpsBlock) tpsBlock.hidden = !TICK_PROFILES.has(state.detail.id);
    startStatsRefresh(state.detail.id);
  }
  startMetricRefresh(state.detail.id);
}

function patchDetail(id) {
  if (!id) return;
  const profile = detailProfile(id);
  const status = detailStatus(id);
  renderCommandCatalog(id);
  byId("detail-breadcrumb-name").textContent = ` / ${profile.display_name || id}`;
  byId("detail-title").textContent = profile.display_name || id;
  byId("detail-subtitle").textContent = profile.public_endpoint?.host || "private";
  const rawCurrent = status.state || "unknown";
  const ownUpdate = updateActive(status);
  const detailUpdateOwner = updateOwner();
  const updating = ownUpdate || Boolean(detailUpdateOwner && detailUpdateOwner !== id);
  const current = ownUpdate ? "updating" : rawCurrent;
  const offline = lifecycleOffline(status);
  const metricAvailability = {
    cpu: status.cpu_percent != null,
    memory: status.rss_bytes != null,
    players: status.players_online != null,
    uptime: status.uptime_seconds != null || offline,
    disk: status.disk_free_bytes != null,
    "disk-io": status.disk_read_bps != null || status.disk_write_bps != null,
  };
  document.querySelectorAll("#panel-metrics [data-metric]").forEach((tile) => {
    tile.hidden = ["cpu", "memory", "players"].includes(tile.dataset.metric) ? false : metricAvailability[tile.dataset.metric] === false;
  });
  const badge = byId("detail-status");
  badge.className = `status-badge ${stateClass(current)}`;
  badge.querySelector(".status-text").textContent = statusLabel(current);
  const updateNote = byId("detail-update-note");
  if (updateNote) {
    updateNote.hidden = !updating;
    updateNote.dataset.source = ownUpdate ? status.update?.source || "job" : "";
    updateNote.textContent = updating ? updateNotice(ownUpdate ? null : profileLabel(detailUpdateOwner)) : "";
  }
  const operationSet = new Set(profile.operations || []);
  byId("tab-benchmarks").hidden = !operationSet.has("benchmark");
  if (state.detail.tab === "benchmarks") validateBenchmarkForm();
  const running = current === "running";
  const transitional = ["starting", "stopping"].includes(current);
  byId("detail-start").hidden = running || transitional;
  byId("detail-stop").hidden = !running && current !== "starting";
  byId("detail-restart").hidden = !running;
  byId("detail-force").hidden = !running;
  byId("detail-start").disabled = updating || !["stopped", "failed", "blocked", "unknown"].includes(current) || !operationSet.has("start");
  byId("detail-stop").disabled = updating || !operationSet.has("stop");
  byId("detail-restart").disabled = updating || !operationSet.has("restart");
  byId("detail-force").disabled = updating || !operationSet.has("stop");
  const commandEnabled = running && !updating && operationSet.has("command");
  byId("command-input").disabled = !commandEnabled;
  byId("command-send").disabled = !commandEnabled;
  byId("command-input").placeholder = operationSet.has("command")
    ? (running ? "Enter a server command" : "Start this server to use its console")
    : "Console input is unavailable for this profile";
  byId("command-note").textContent = !operationSet.has("command")
    ? "Console input is not available for this profile."
    : running
      ? "Command input is available for this running profile."
      : "Start this server to use its console.";
  const runKey = metricRunKey(status);
  const cachedCapacity = state.metricCapacity.get(id);
  const capacity = cachedCapacity?.key === runKey ? cachedCapacity : {};
  const cpuCapacity = metricNumber(capacity.cpu_capacity_percent);
  const memoryCapacity = metricNumber(capacity.memory_capacity_bytes);
  const cpuValue = runKey ? metricNumber(status.cpu_percent) : null;
  const memoryValue = runKey ? metricNumber(status.rss_bytes) : null;
  const cpuText = cpuValue == null ? "—" : `${cpuValue.toFixed(1)}% / ${cpuCapacity > 0 ? `${cpuCapacity.toFixed(0)}%` : "unknown"}`;
  const memoryText = memoryValue == null ? "—" : `${(memoryValue / 1073741824).toFixed(1)} / ${memoryCapacity > 0 ? `${(memoryCapacity / 1073741824).toFixed(1)} GiB` : "capacity unknown"}`;
  const retained = state.metricHistory.get(id);
  // A stopped profile shows retained observations as explicitly historical
  // values, never as live samples, healthy defaults, or fabricated zeros.
  const historical = !runKey && rawCurrent === "stopped" && retained?.historical === true;
  const historicalWindow = historical && Number.isFinite(retained?.startedAt) && Number.isFinite(retained?.endedAt)
    ? { start: retained.startedAt, end: retained.endedAt } : null;
  const sampleValue = (point) => (point && point.state !== "unavailable" ? metricNumber(point.v) : null);
  const historicalPoint = (seriesKey) => {
    const series = (historical && retained?.[seriesKey]) || [];
    for (let index = series.length - 1; index >= 0; index -= 1) {
      const point = series[index];
      if (!Number.isFinite(point?.t)) continue;
      if (historicalWindow && (point.t < historicalWindow.start || point.t > historicalWindow.end)) continue;
      const value = sampleValue(point);
      if (value != null) return { ...point, value };
    }
    return null;
  };
  const historicalCpu = historicalPoint("cpu");
  const historicalMemory = historicalPoint("memory");
  const cpuTile = historicalCpu ? `${historicalCpu.value.toFixed(1)}%` : cpuText;
  const memoryTile = historicalMemory ? `${historicalMemory.value.toFixed(1)} GiB` : memoryText;
  const lastObservedAt = historical ? (retained?.endedAt ?? null) : null;
  byId("rail-cpu").textContent = cpuText;
  byId("rail-memory").textContent = memoryText;
  patchCapacityTrack("rail-cpu-capacity", cpuValue, cpuCapacity);
  patchCapacityTrack("rail-memory-capacity", memoryValue, memoryCapacity);
  byId("rail-cpu-note").textContent = cpuCapacity > 0 ? "Current / available CPU · 100% per core" : "CPU capacity unavailable";
  byId("rail-memory-note").textContent = memoryCapacity > 0 ? "Current RSS / effective memory limit" : "Memory capacity unavailable";
  byId("rail-players").textContent = offline ? "Offline" : status.players_online == null ? "Unavailable" : String(status.players_online);
  byId("rail-players-note").textContent = offline ? "Server is offline" : status.players_online == null ? "Player count unavailable" : "Players observed";
  byId("rail-version").textContent = formatVersion(status.installed_version);
  const configRestart = state.configRestartRequired.get(id) || [];
  // Only claim acceptance when the server is actually starting. A stopped
  // profile is Offline, and an in-progress update says so plainly.
  byId("rail-version-note").textContent = configRestart.length ? "Config changed · restart required" : status.restart_required ? "Update available · restart required" : ownUpdate ? "Updating…" : ["stopped", "failed", "blocked", "unknown"].includes(rawCurrent) ? "Offline" : status.required_ports_ready ? "Ready on required ports" : "Accepted; waiting for readiness";
  const samples = state.metricSamples.get(id) || { cpu: [], memory: [], players: [] };
  const history = state.metricHistory.get(id);
  void loadMetricCapacity(id);
  const metricDomain = runKey ? { start: Date.parse(status.started_at), end: Date.now() }
    : historicalWindow ? { start: historicalWindow.start, end: historicalWindow.end } : null;
  // The label is only attached to a real retained value; a missing observation
  // stays "—" rather than implying historical data exists.
  byId("metric-cpu-current").textContent = historicalCpu ? `${cpuTile} (historical)` : cpuText;
  byId("metric-memory-current").textContent = historicalMemory ? `${memoryTile} (historical)` : memoryText;
  byId("metric-players-current").textContent = offline ? "Offline" : status.players_online == null ? "Unavailable" : String(status.players_online);
  const chartHistory = runKey && history?.key === runKey ? history : historical ? retained : {};
  const sampleTail = (key) => { const past = chartHistory[key] || []; const last = past.at(-1)?.t ?? 0; return [...past, ...(samples[key] || []).filter((point) => point.t > last)]; };
  const gapMs = chartHistory.resolution === "1h" ? 7200000 : chartHistory.resolution === "5m" ? 600000 : 120000;
  renderMetricChart("metric-cpu-chart", sampleTail("cpu"), { unit: "%", formatValue: (value) => value.toFixed(0), capacity: cpuCapacity, domain: metricDomain, gapMs });
  renderMetricChart("metric-memory-chart", sampleTail("memory"), { unit: " GiB", formatValue: (value) => value.toFixed(1), capacity: memoryCapacity == null ? null : memoryCapacity / 1073741824, domain: metricDomain, gapMs });
  renderMetricChart("metric-players-chart", sampleTail("players"), { formatValue: (value) => value.toFixed(0), domain: metricDomain });
  byId("metric-cpu-capacity").textContent = cpuCapacity > 0 ? "Current / available CPU capacity" : "Capacity unavailable · scale follows observed values";
  byId("metric-memory-capacity").textContent = memoryCapacity > 0 ? "Current RSS / effective memory limit" : "Capacity unavailable · scale follows observed values";
  const historyNote = byId("metrics-history-note");
  if (historyNote) {
    const cold = historical && retained?.cold === true;
    const observed = lastObservedAt ? new Date(lastObservedAt).toLocaleString() : null;
    const started = historicalWindow ? new Date(historicalWindow.start).toLocaleString() : null;
    const state_ =
      retained?.coldFailed ? "unavailable"
        : historical ? (cold ? "recent" : "historical")
          : rawCurrent === "stopped" ? "empty" : "live";
    historyNote.dataset.state = state_;
    historyNote.hidden = !historical && rawCurrent !== "stopped";
    historyNote.textContent =
      retained?.coldFailed ? "Recent history is unavailable right now. Live values return when the server starts."
        : historical && !observed ? "Recent history has no usable observations. Live values return when the server starts."
          : historical && cold
            ? `Recent history · last observed ${observed} · bounded 24-hour view that may include more than one server run. This range spans ${started} → ${observed}; values are last observed, not live.`
            : historical
              ? `Historical · server run started ${started || "unknown"} · last observed ${observed}. Values are last observed, not live; offline time is not plotted as zero.`
              : rawCurrent === "stopped" ? "No recent history available. Live values return when the server starts." : "";
  }
  byId("metrics-run-note").textContent = runKey && metricDomain
    ? `Since server started ${new Date(metricDomain.start).toLocaleString()} → now. ${chartHistory.stale ? "History refresh unavailable; retaining last observations." : chartHistory.fetchedAt ? "Retained history; gaps mean no observation." : "Loading retained history…"} Player history uses the latest 500 retained observations.${chartHistory.playersStale ? " Player history refresh unavailable." : ""}`
    : historical && metricDomain
      ? `Retained history ${new Date(metricDomain.start).toLocaleString()} → ${new Date(metricDomain.end).toLocaleString()} (last observed sample). ${chartHistory.stale ? "History refresh unavailable; retaining last observations. " : ""}Offline time is not plotted as zero.`
      : rawCurrent === "stopped" ? "Server stopped · no active run. No offline time is plotted as zero." : "Current run history unavailable until a process start is confirmed.";
  if (runKey && state.detail.tab === "metrics" && history?.key !== runKey) void loadMetricHistory(id);
  byId("metric-uptime").textContent = offline ? "Offline" : uptime(status.uptime_seconds);
  byId("metric-disk-free").textContent = formatBytes(status.disk_free_bytes);
  byId("metric-disk-free-note").textContent = profile.mutable_root || "Profile mutable root";
  byId("metric-disk-io").textContent = status.disk_read_bps == null && status.disk_write_bps == null
    ? "Unavailable"
    : `R ${formatRate(status.disk_read_bps)} · W ${formatRate(status.disk_write_bps)}`;
  const item = state.logs.get(id);
  const lines = (item?.lines || [])
    .filter((line) => !item?.clearedAt || Date.parse(line.timestamp || 0) > item.clearedAt)
    .filter((line) => item?.hideNoise === false || !isNoise(line));
  const hidden = (item?.lines || []).filter((line) => !item?.clearedAt || Date.parse(line.timestamp || 0) > item.clearedAt).filter(isNoise).length;
  const consoleOutput = byId("console-output");
  const consoleKey = `${item?.hideNoise !== false}:${item?.clearedAt || ""}:${lines.slice(-200).map((line) => `${line.timestamp}|${line.severity}|${line.message}`).join("\u0001")}`;
  if (consoleOutput && !consoleOutput.matches(":focus-within") && !item?.loading) {
    if (item?.consoleKey !== consoleKey) {
      consoleOutput.replaceChildren();
      lines.slice(-200).forEach((line) => {
        const row = document.createElement("div");
        row.className = `console-line severity-${line.severity || "info"}`;
        const time = document.createElement("time");
        time.textContent = line.timestamp ? new Date(line.timestamp).toLocaleTimeString() : "—";
        const message = document.createElement("span");
        message.textContent = line.message || "";
        row.append(time, message);
        consoleOutput.append(row);
      });
      if (item) item.consoleKey = consoleKey;
      if (!item || item.autoScroll !== false) consoleOutput.scrollTop = consoleOutput.scrollHeight;
    }
    byId("console-jump").hidden = !item || item.autoScroll !== false;
  }
  const noiseToggle = byId("console-noise-toggle");
  if (noiseToggle) noiseToggle.checked = item?.hideNoise !== false;
  byId("console-noise-label").textContent = `Hide network noise · ${hidden} hidden`;
}

function configChanges() {
  const changes = {};
  document.querySelectorAll("#config-panel [data-config-key]").forEach((input) => {
    if (input.type === "password" && !input.value) return;
    const value = input.type === "checkbox" ? input.checked : input.type === "number" ? Number(input.value) : input.value;
    if (JSON.stringify(value) !== input.dataset.initial) changes[input.dataset.configKey] = value;
  });
  return changes;
}

function updateConfigDiff() {
  const keys = Object.keys(configChanges());
  byId("config-diff").textContent = keys.length ? `${keys.length} change${keys.length === 1 ? "" : "s"} — apply? (${keys.join(", ")})` : "No pending changes.";
  byId("config-apply").disabled = !keys.length;
}

async function renderConfig(id) {
  const requestId = ++state.detail.configRequest;
  const profile = state.profiles.get(id) || { id };
  const summary = byId("config-summary");
  summary.replaceChildren();
  const values = [
    ["Profile ID", profile.id],
    ["Display name", profile.display_name],
    ["Operations", Array.isArray(profile.operations) ? profile.operations.join(", ") : "Unavailable"],
    ["Public endpoint", profile.public_endpoint ? `${profile.public_endpoint.protocol || "game"}://${profile.public_endpoint.host || "—"}:${profile.public_endpoint.port || "—"}` : "Private"],
    ["Auto-stop", Number(profile.idle_stop_minutes) > 0 ? `${profile.idle_stop_minutes}m idle` : "off"],
  ];
  values.forEach(([key, value]) => {
    const wrap = document.createElement("div");
    const term = document.createElement("dt"); term.textContent = key;
    const detail = document.createElement("dd"); detail.textContent = value || "Unavailable";
    wrap.append(term, detail); summary.append(wrap);
  });
  const minutes = Number(profile.idle_stop_minutes) || 0;
  byId("idle-stop-enabled").checked = minutes > 0;
  byId("idle-stop-minutes").value = minutes > 0 ? String(minutes) : "30";
  byId("idle-stop-status").textContent = minutes > 0 ? `Auto-stop: ${minutes}m idle` : "Auto-stop: off";
  const list = byId("config-panel");
  list.replaceChildren();
  byId("config-diff").textContent = "Loading editable settings…";
  try {
    const response = await api(`/api/v1/profiles/${encodeURIComponent(id)}/config`);
    if (requestId !== state.detail.configRequest || state.detail.id !== id || state.detail.tab !== "config") return;
    state.detail.config = response;
    (Array.isArray(response.settings) ? response.settings : []).forEach((setting) => {
      const row = document.createElement("div");
      const label = document.createElement("label"); label.textContent = setting.key;
      const input = setting.type === "enum" ? document.createElement("select") : document.createElement("input");
      input.id = `config-${String(setting.key).replace(/[^a-zA-Z0-9_-]/g, "-")}`;
      label.htmlFor = input.id;
      if (setting.type === "enum") (setting.bounds?.choices || []).forEach((choice) => { const option = document.createElement("option"); option.value = choice; option.textContent = choice; input.append(option); });
      else input.type = setting.type === "bool" ? "checkbox" : setting.secret ? "password" : setting.type === "int" ? "number" : "text";
      input.dataset.configKey = setting.key;
      if (input.type === "checkbox") input.checked = Boolean(setting.value); else if (setting.value != null) input.value = String(setting.value);
      if (setting.type === "int") { if (setting.bounds?.min != null) input.min = setting.bounds.min; if (setting.bounds?.max != null) input.max = setting.bounds.max; }
      if (setting.bounds?.max_length != null) input.maxLength = setting.bounds.max_length;
      if (setting.secret) input.placeholder = setting.configured ? "Set · enter to replace" : "Not set · write only";
      input.dataset.initial = JSON.stringify(setting.secret ? "" : setting.value);
      input.addEventListener("input", updateConfigDiff); input.addEventListener("change", updateConfigDiff);
      row.append(label, input); list.append(row);
    });
    updateConfigDiff();
  } catch (error) { byId("config-diff").textContent = error.message || "Config unavailable."; }
  await renderSchedules();
}

function renderScheduleProfiles() {
  const select = byId("schedule-profile");
  if (!select) return;
  const selected = select.value;
  select.replaceChildren();
  [...state.profiles.values()].forEach((profile) => {
    const option = document.createElement("option");
    option.value = profile.id;
    option.textContent = profile.display_name || profileLabel(profile.id);
    select.append(option);
  });
  if (selected && [...select.options].some((option) => option.value === selected)) select.value = selected;
}

function scheduleDateLabel(value, compact = false) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "unknown";
  if (compact) {
    const weekday = date.toLocaleDateString(undefined, { weekday: "short" });
    const hours = String(date.getHours()).padStart(2, "0");
    const minutes = String(date.getMinutes()).padStart(2, "0");
    return `${weekday} ${hours}:${minutes}`;
  }
  return date.toLocaleString(undefined, { weekday: "short", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}

function renderAutomationSummary(items) {
  const text = byId("session-automation");
  if (!text) return;
  if (!Array.isArray(items)) { text.textContent = "Unavailable"; return; }
  const soonest = items
    .filter((item) => item?.enabled !== false && item.next_fire && !Number.isNaN(new Date(item.next_fire).getTime()))
    .sort((left, right) => new Date(left.next_fire) - new Date(right.next_fire))[0];
  text.textContent = soonest ? `${profileLabel(soonest.profile)} · ${scheduleDateLabel(soonest.next_fire, true)}` : "None scheduled";
}

function renderScheduleRows(items) {
  const list = byId("schedule-list");
  if (!list) return;
  list.replaceChildren();
  if (!items.length) {
    const empty = document.createElement("p"); empty.className = "empty-state"; empty.textContent = "No scheduled switches."; list.append(empty); return;
  }
  items.forEach((item, index) => {
    const enabled = item.enabled !== false;
    const row = document.createElement("div"); row.className = `schedule-row${enabled ? "" : " is-disabled"}`; row.dataset.scheduleRow = String(index); row.dataset.enabled = String(enabled);
    const copy = document.createElement("div"); copy.className = "schedule-copy";
    const cron = document.createElement("code"); cron.textContent = item.cron;
    const profile = document.createElement("strong"); profile.textContent = profileLabel(item.profile);
    const next = document.createElement("span"); next.className = "schedule-state"; next.textContent = enabled ? `next: ${scheduleDateLabel(item.next_fire)}` : "Disabled · no next fire";
    copy.append(cron, profile, next);
    const actions = document.createElement("div"); actions.className = "schedule-actions";
    const toggle = document.createElement("button"); toggle.className = "button button-small button-quiet schedule-toggle"; toggle.type = "button"; toggle.setAttribute("role", "switch"); toggle.setAttribute("aria-checked", String(enabled)); toggle.textContent = enabled ? "Enabled" : "Disabled"; toggle.setAttribute("aria-label", `${enabled ? "Disable" : "Enable"} schedule for ${profileLabel(item.profile)} at ${item.cron}`);
    toggle.dataset.scheduleToggle = String(index);
    toggle.addEventListener("click", () => toggleSchedule(index, item, toggle));
    const remove = document.createElement("button"); remove.className = "button button-small button-danger"; remove.type = "button"; remove.dataset.scheduleRemove = String(index); remove.textContent = "Remove"; remove.setAttribute("aria-label", `Remove schedule for ${profileLabel(item.profile)} at ${item.cron}`);
    remove.addEventListener("click", () => removeSchedule(index, item));
    actions.append(toggle, remove); row.append(copy, actions); list.append(row);
  });
}

async function renderSchedules() {
  if (!byId("schedule-list")) return;
  renderScheduleProfiles();
  try {
    state.detail.schedules = await loadSchedules();
    renderScheduleRows(state.detail.schedules);
    byId("schedule-status").textContent = "Schedule changes apply without restarting Horizon.";
  } catch (error) {
    byId("schedule-list").replaceChildren();
    const message = document.createElement("p"); message.className = "empty-state"; message.textContent = error.message || "Schedules unavailable."; byId("schedule-list").append(message);
    byId("schedule-status").textContent = "Schedules could not be loaded.";
  }
}

async function loadSchedules() {
  const response = await api("/api/v1/schedules");
  state.schedules = Array.isArray(response.schedules) ? response.schedules : [];
  renderAutomationSummary(state.schedules);
  return state.schedules;
}

function schedulePayload(item) {
  return {
    cron: item.cron,
    profile: item.profile,
    enabled: item.enabled !== false,
    ...(item.backup_destination ? { backup_destination: item.backup_destination } : {}),
    ...(item.operation ? { operation: item.operation } : {}),
    ...(item.baseline_preset ? { baseline_preset: item.baseline_preset } : {}),
    ...(item.candidate_preset ? { candidate_preset: item.candidate_preset } : {}),
    ...(item.campaign ? { campaign: item.campaign } : {}),
    maintenance_window: item.maintenance_window === true,
    rollback_safe: item.rollback_safe === true,
    public_wake_policy: item.public_wake_policy || "disabled",
  };
}

async function replaceSchedules(entries, confirmation) {
  if (!window.confirm(confirmation)) return false;
  const response = await api("/api/v1/schedules", { method: "POST", body: JSON.stringify({ entries }) });
  state.detail.schedules = Array.isArray(response.schedules) ? response.schedules : [];
  state.schedules = state.detail.schedules;
  renderAutomationSummary(state.schedules);
  renderScheduleRows(state.detail.schedules);
  byId("schedule-status").textContent = "Schedule changes applied live.";
  return true;
}

async function addSchedule(event) {
  event.preventDefault();
  const cron = byId("schedule-cron").value.trim();
  const profile = byId("schedule-profile").value;
  if (!cron || !profile) return;
  const entries = [...(state.detail.schedules || []).map(schedulePayload), { cron, profile, enabled: true, operation: "switch", maintenance_window: false, rollback_safe: false, public_wake_policy: "disabled" }];
  try {
    const changed = await replaceSchedules(entries, `Add schedule ${cron} for ${profileLabel(profile)}?`);
    if (changed) {
      byId("schedule-cron").value = "";
      byId("schedule-cron").focus();
    }
  } catch (error) { byId("schedule-status").textContent = error.message || "Schedule update failed."; }
}

async function removeSchedule(index, item) {
  const entries = (state.detail.schedules || []).filter((_, candidate) => candidate !== index).map(schedulePayload);
  const nextFocusIndex = Math.min(index, entries.length - 1);
  try {
    const changed = await replaceSchedules(entries, `Remove schedule ${item.cron} for ${profileLabel(item.profile)}?`);
    if (changed) {
      const next = nextFocusIndex >= 0 ? byId("schedule-list").querySelectorAll("[data-schedule-remove]")[nextFocusIndex] : null;
      (next || byId("schedule-cron")).focus();
    }
  } catch (error) { byId("schedule-status").textContent = error.message || "Schedule update failed."; }
}

async function toggleSchedule(index, item, control) {
  const nextEnabled = item.enabled === false;
  const entries = (state.detail.schedules || []).map(schedulePayload);
  entries[index].enabled = nextEnabled;
  try {
    const changed = await replaceSchedules(entries, `${nextEnabled ? "Enable" : "Disable"} schedule ${item.cron} for ${profileLabel(item.profile)}?`);
    if (changed) {
      byId("schedule-status").textContent = `Schedule ${nextEnabled ? "enabled" : "disabled"} for ${profileLabel(item.profile)}.`;
      const next = byId("schedule-list").querySelectorAll("[data-schedule-toggle]")[index];
      (next || control).focus();
    }
  } catch (error) { byId("schedule-status").textContent = error.message || "Schedule update failed."; }
}

async function applyConfig(event) {
  event.preventDefault();
  const id = state.detail.id;
  const changes = configChanges();
  const keys = Object.keys(changes);
  if (!id || !keys.length || !window.confirm(`${keys.length} changes — apply?`)) return;
  byId("config-apply").disabled = true;
  try {
    const response = await api(`/api/v1/profiles/${encodeURIComponent(id)}/config`, { method: "POST", body: JSON.stringify({ changes }) });
    const restart = Array.isArray(response.restart_required) ? response.restart_required : [];
    state.configRestartRequired.set(id, restart);
    byId("config-status").textContent = restart.length ? `Applied. Restart required to take effect: ${restart.join(", ")}.` : "Applied. No restart required.";
    patchDetail(id);
    await renderConfig(id);
  } catch (error) { byId("config-status").textContent = error.message || "Config update failed."; updateConfigDiff(); }
}

// SetIdleStop RPC route: config changes stay behind the authenticated mutation path.
async function saveIdleStop(event) {
  event.preventDefault();
  const id = state.detail.id;
  if (!id) return;
  const enabled = byId("idle-stop-enabled").checked;
  const minutes = enabled ? Number(byId("idle-stop-minutes").value) : 0;
  if (enabled && (!Number.isInteger(minutes) || minutes < 5 || minutes > 1440)) {
    byId("idle-stop-status").textContent = "Choose 5–1440 minutes, or turn auto-stop off.";
    return;
  }
  const save = byId("idle-stop-save");
  save.disabled = true;
  try {
    const result = await api(`/api/v1/profiles/${encodeURIComponent(id)}/idle-stop`, { method: "PATCH", body: JSON.stringify({ minutes }) });
    const profile = state.profiles.get(id) || { id };
    profile.idle_stop_minutes = Number(result?.idle_stop_minutes ?? minutes);
    state.profiles.set(id, profile);
    renderConfig(id);
    notify(`${profileLabel(id)} auto-stop saved.`);
  } catch (error) {
    byId("idle-stop-status").textContent = error.message || "Auto-stop update failed.";
  } finally {
    save.disabled = false;
  }
}

async function loadDetailLogs(id) {
  if (!pageVisible() || !id) return;
  const item = state.logs.get(id) || { query: "", severity: "all", paused: false, lines: [], autoScroll: true, clearedAt: null, hideNoise: true, nextCursor: null };
  if (item.loading) return;
  item.loading = true;
  state.logs.set(id, item);
  byId("detail-log-query").value = item.query;
  byId("detail-log-severity").value = item.severity;
  byId("detail-log-pause").textContent = item.paused ? "Resume live logs" : "Pause live logs";
  try {
    const params = new URLSearchParams({ limit: "200", severity: "all" });
    if (item.nextCursor) params.set("cursor", item.nextCursor);
    else if (item.lines.length) params.set("since", item.lines.at(-1)?.timestamp || new Date().toISOString());
    const page = await api(`/api/v1/profiles/${encodeURIComponent(id)}/logs?${params}`);
    if (!item.paused && Array.isArray(page.items)) {
      if (item.nextCursor || item.lines.length) {
        const existing = new Set(item.lines.map(detailLogKey));
        item.lines.push(...page.items.filter((line) => !existing.has(detailLogKey(line))));
      } else item.lines = page.items;
      item.lines = item.lines.slice(-DETAIL_LOG_BUFFER_LIMIT);
      item.nextCursor = page.next_cursor || null;
    }
  } catch (error) { notify(error.message || "Logs unavailable."); }
  item.loading = false;
  renderDetailLogs(item);
  patchDetail(id);
}

function consoleLineText(line) {
  const date = line.timestamp ? new Date(line.timestamp) : null;
  const timestamp = date && !Number.isNaN(date.getTime())
    ? `${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}:${String(date.getSeconds()).padStart(2, "0")}.${String(date.getMilliseconds()).padStart(3, "0")}`
    : "--:--:--.---";
  return `[${timestamp}] [${line.severity || "info"}] ${line.message || ""}`;
}

function downloadConsole(id, lines, includeTimestamps = true) {
  const body = lines.map((line) => includeTimestamps ? consoleLineText(line) : (line.message || "")).join("\n");
  const stamp = new Date().toISOString().slice(0, 19).replace(/[-T:]/g, "");
  const blob = new Blob([body ? `${body}\n` : ""], { type: "text/plain;charset=utf-8" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = `${id}-console-${stamp}.txt`;
  link.click();
  URL.revokeObjectURL(link.href);
}

function exportStamp(value, fallback = "now") {
  return value ? value.replace(/[-:]/g, "").replace("T", "-") : fallback;
}

async function saveConsoleAs() {
  const id = state.detail.id;
  if (!id) return;
  const from = byId("console-export-from").value;
  const to = byId("console-export-to").value;
  const severity = byId("console-export-severity").value;
  const includeTimestamps = byId("console-export-timestamps").checked;
  const rangeSelected = Boolean(from || to);
  let lines = visibleConsoleLines(id);
  if (rangeSelected) {
    const params = new URLSearchParams({ limit: "5000", severity: "all" });
    if (from) params.set("since", new Date(from).toISOString());
    if (to) params.set("until", new Date(to).toISOString());
    try {
      const page = await api(`/api/v1/profiles/${encodeURIComponent(id)}/logs?${params}`);
      lines = Array.isArray(page.items) ? page.items : [];
    } catch (error) { notify(error.message || "Log export failed."); return; }
  }
  if (severity !== "all") lines = lines.filter((line) => line.severity === severity);
  const body = lines.map((line) => includeTimestamps ? consoleLineText(line) : (line.message || "")).join("\n");
  const blob = new Blob([body ? `${body}\n` : ""], { type: "text/plain;charset=utf-8" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = `${id}-console-${exportStamp(from)}-${exportStamp(to)}.txt`;
  link.click();
  URL.revokeObjectURL(link.href);
  closeDialog(byId("console-save-as-dialog"));
}

function visibleConsoleLines(id) {
  const item = state.logs.get(id);
  return (item?.lines || []).filter((line) => !item?.clearedAt || Date.parse(line.timestamp || 0) > item.clearedAt).slice(-200);
}

async function sendConsoleCommand() {
  const id = state.detail.id;
  const input = byId("command-input");
  const command = input.value;
  if (!id || !command.trim()) return;
  try {
    await api(`/api/v1/profiles/${encodeURIComponent(id)}/command`, {
      method: "POST",
      body: JSON.stringify({ command }),
    });
    input.value = "";
    notify("Command sent.");
    await loadDetailLogs(id);
  } catch (error) {
    notify(error.message || "Command was not accepted.");
  }
}

async function createBackup(id, protectedBackup = false) {
  try {
    await api(`/api/v1/profiles/${encodeURIComponent(id)}/backups`, { method: "POST", body: JSON.stringify({ protected: protectedBackup }) });
    notify(`${protectedBackup ? "Protected " : ""}backup requested for ${profileLabel(id)}.`);
    await loadBackups(id);
  } catch (error) { notify(error.message || "Backup request failed."); }
}

async function fetchUpdateStatus(id) {
  let status = null;
  for (let attempt = 0; attempt < updatePollAttempts; attempt += 1) {
    status = await api(`/api/v1/profiles/${encodeURIComponent(id)}/update`);
    if (status.state !== "checking" || attempt + 1 >= updatePollAttempts) return status;
    await new Promise((resolve) => setTimeout(resolve, updatePollDelay));
  }
  return status;
}

async function checkForUpdate(id) {
  try {
    const status = await fetchUpdateStatus(id);
    const checkState = status.state || (status.available_version ? "available" : "current");
    if (checkState === "failed") {
      notify(status.message || `Update check failed for ${profileLabel(id)}.`);
      return;
    }
    if (checkState === "checking") {
      notify(status.message || `The update check for ${profileLabel(id)} is still running.`);
      return;
    }
    if (checkState === "unsupported") {
      notify(status.message || `Update checks are not available for ${profileLabel(id)}.`);
      return;
    }
    if (checkState === "deferred" && !status.available_version) {
      notify(status.message || `The automatic update for ${profileLabel(id)} is deferred.`);
      return;
    }
    if (checkState === "stale" && !status.available_version) {
      notify(status.message || `The update status for ${profileLabel(id)} is stale.`);
      return;
    }
    if (!status.available_version || status.available_version === status.installed_version) {
      notify(status.message || (checkState === "current" ? `No update available for ${profileLabel(id)}.` : `No verified update available for ${profileLabel(id)}.`));
      return;
    }
    const automatic = !status.apply_supported;
    state.updateProfile = id;
    state.updateApplySupported = !automatic;
    byId("update-profile").textContent = profileLabel(id);
    byId("update-installed").textContent = formatVersion(status.installed_version);
    byId("update-available").textContent = formatVersion(status.available_version);
    byId("update-dialog-title").textContent = automatic ? "Automatic update available" : "Apply server update";
    byId("update-description").textContent = automatic
      ? (status.message || `Horizon found ${formatVersion(status.available_version)}. The automatic updater handles it during its scheduled safe stopped window.`)
      : "Horizon found an update. Review the version change before applying it.";
    const confirm = byId("update-confirm");
    confirm.hidden = automatic;
    confirm.disabled = automatic;
    confirm.textContent = "Apply update";
    setupDialog(byId("update-dialog"), byId("check-update"));
  } catch (error) { notify(error.message || "Update check failed."); }
}

function renderDetailLogs(item) {
  const query = item.query.trim().toLowerCase();
  const matching = item.lines.filter((line) => (!query || String(line.message || "").toLowerCase().includes(query)) && (item.severity === "all" || line.severity === item.severity));
  const hidden = matching.filter(isNoise).length;
  const lines = matching.filter((line) => item.hideNoise === false || !isNoise(line));
  const list = byId("detail-log-list");
  const rows = item.detailLogRows || new Map();
  const retainedKeys = new Set(item.lines.map(detailLogKey));
  rows.forEach((row, key) => {
    if (!retainedKeys.has(key)) { row.remove(); rows.delete(key); }
  });
  const desired = [];
  const desiredKeys = new Set();
  lines.forEach((line) => {
    const key = detailLogKey(line);
    let row = rows.get(key);
    if (!row) {
      row = document.createElement("li"); row.className = `log-line severity-${line.severity}`;
      const time = document.createElement("time"); time.textContent = line.timestamp ? new Date(line.timestamp).toLocaleTimeString() : "—";
      const severity = document.createElement("span"); severity.className = "log-severity"; severity.textContent = line.severity;
      const message = document.createElement("span"); message.textContent = line.message || "";
      row.append(time, severity, message);
      rows.set(key, row);
    }
    desired.push(row);
    desiredKeys.add(key);
  });
  rows.forEach((row, key) => { if (!desiredKeys.has(key)) row.remove(); });
  if (!lines.length) {
    let empty = item.detailLogEmpty;
    if (!empty) { empty = document.createElement("li"); empty.className = "empty-state"; empty.textContent = "No matching log lines."; item.detailLogEmpty = empty; }
    if (list.children.length !== 1 || list.firstElementChild !== empty) {
      list.replaceChildren(empty);
    }
  } else {
    item.detailLogEmpty?.remove();
    desired.forEach((row, index) => {
      if (list.children[index] !== row) list.insertBefore(row, list.children[index] || null);
    });
  }
  item.detailLogRows = rows;
  byId("detail-log-footer").textContent = `${lines.length} lines · ${hidden} network-noise lines hidden · secrets redacted`;
  byId("detail-noise-toggle").checked = item.hideNoise !== false;
  byId("detail-noise-label").textContent = `Hide network noise · ${hidden} hidden`;
}

function backupAvailability(backup) {
  return (backup && backup.local_payload_state) || "present";
}

function backupAvailable(backup) {
  return backupAvailability(backup) === "present";
}

function availabilityLabel(state) {
  if (state === "present") return "";
  if (state === "quarantined" || state === "purged") return `Retired · remote copy only (${state})`;
  if (state === "missing") return "Local copy missing";
  return `Unavailable (${state})`;
}

async function loadBackups(id) {
  if (!id) return;
  const list = byId("backup-list");
  try {
    const page = await api(`/api/v1/profiles/${encodeURIComponent(id)}/backups?limit=200`);
    state.detail.backups = Array.isArray(page.items) ? page.items : [];
    const latest = state.detail.backups[0];
    byId("switch-backup").textContent = latest?.created_at ? new Date(latest.created_at).toLocaleString() : "No recent backup recorded";
  } catch { state.detail.backups = []; }
  list.replaceChildren();
  state.detail.backups.forEach((backup) => {
    const row = document.createElement("li"); row.className = "backup-row";
    const availability = backupAvailability(backup);
    if (availability !== "present") row.classList.add("backup-unavailable");
    const idNode = document.createElement("strong"); idNode.textContent = backup.id || "—";
    const time = document.createElement("time"); time.textContent = backup.created_at ? new Date(backup.created_at).toLocaleString() : "—";
    const size = document.createElement("span"); size.textContent = formatBytes(backup.size_bytes);
    const restore = document.createElement("button"); restore.type = "button"; restore.className = "button button-small button-quiet";
    if (availability === "present") {
      restore.textContent = `Restore ${backup.id || ""}`.trim();
      restore.addEventListener("click", (event) => openRestore(id, event.currentTarget, backup.id));
    } else {
      restore.textContent = "Restore unavailable";
      restore.disabled = true;
      restore.title = availabilityLabel(availability);
    }
    const status = document.createElement("span"); status.className = "backup-availability";
    status.textContent = availabilityLabel(availability);
    row.append(idNode, time, size, status, restore); list.append(row);
  });
  if (!state.detail.backups.length) { const empty = document.createElement("li"); empty.className = "empty-state"; empty.textContent = "No backups loaded."; list.append(empty); }
}

async function renderAggregateBackups() {
  if (!pageVisible()) return;
  const list = byId("aggregate-backup-list");
  const requestId = ++aggregateBackupRequest;
  let page;
  try { page = await api("/api/v1/backups?limit=500"); } catch { page = { items: [] }; }
  if (requestId !== aggregateBackupRequest) return;
  list.replaceChildren();
  const items = (Array.isArray(page.items) ? page.items : []).map((backup) => ({
    ...backup,
    profile_name: state.profiles.get(backup.profile_id)?.display_name || backup.profile_id || "Server",
  }));
  const uniqueItems = [...new Map(items.map((backup) => [`${backup.profile_id}:${backup.id || ""}`, backup])).values()];
  uniqueItems.forEach((backup) => {
    const row = document.createElement("li"); row.className = "backup-row";
    const availability = backupAvailability(backup);
    if (availability !== "present") row.classList.add("backup-unavailable");
    const label = document.createElement("strong"); label.textContent = `${backup.profile_name} · ${backup.id || "—"}`;
    const time = document.createElement("time"); time.textContent = backup.created_at ? new Date(backup.created_at).toLocaleString() : "—";
    const size = document.createElement("span"); size.textContent = formatBytes(backup.size_bytes);
    const status = document.createElement("span"); status.className = "backup-availability"; status.textContent = availabilityLabel(availability);
    const link = document.createElement("a"); link.className = "button button-small button-quiet"; link.href = detailTabUrl(backup.profile_id, "backups"); link.textContent = "View backups";
    row.append(label, time, size, status, link); list.append(row);
  });
  if (!uniqueItems.length) { const empty = document.createElement("li"); empty.className = "empty-state"; empty.textContent = "No backups loaded."; list.append(empty); }
}

function route() {
  const parsed = routeFromHash();
  if (parsed.view === "detail") {
    if (!state.profiles.has(parsed.id)) {
      state.detail.id = null;
      showView("dashboard");
      return;
    }
    state.detail.id = parsed.id;
    showView("detail"); patchDetail(state.detail.id); setDetailTab(parsed.tab);
  } else {
    clearStatsTimer();
    clearMetricTimer();
    clearBenchmarkTimer();
    state.detail.id = null;
    showView(parsed.view);
    if (parsed.view === "backups") renderAggregateBackups();
    if (parsed.view === "events") { loadIncidents(); loadActivity("events"); }
    if (parsed.view === "audit") loadActivity("audit");
    if (parsed.view === "settings") {
      loadNotifications(byId("notification-profile")?.value || [...state.profiles.keys()][0]);
      loadPerformance();
    }
  }
}

function setupDetail() {
  document.querySelectorAll("[data-detail-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      if (!state.detail.id) return;
      setDetailTab(button.dataset.detailTab);
      window.location.hash = detailTabUrl(state.detail.id, button.dataset.detailTab).slice(1);
    });
    button.addEventListener("keydown", (event) => {
      if (!["ArrowRight", "ArrowDown", "ArrowLeft", "ArrowUp", "Home", "End"].includes(event.key)) return;
      event.preventDefault();
      const tabs = [...document.querySelectorAll("[data-detail-tab]")];
      const index = tabs.indexOf(button);
      const next = event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : (index + (event.key.includes("Right") || event.key.includes("Down") ? 1 : -1) + tabs.length) % tabs.length;
      tabs[next].focus(); window.location.hash = detailTabUrl(state.detail.id, tabs[next].dataset.detailTab).slice(1);
    });
  });
  ["start", "stop", "restart"].forEach((operation) => byId(`detail-${operation}`).addEventListener("click", () => mutate(state.detail.id, operation)));
  byId("detail-force").addEventListener("click", (event) => openForce(state.detail.id, event.currentTarget));
  byId("create-backup")?.addEventListener("click", () => createBackup(state.detail.id, false));
  byId("create-protected-backup")?.addEventListener("click", () => createBackup(state.detail.id, true));
  byId("check-update")?.addEventListener("click", async (event) => {
    const button = event.currentTarget;
    if (button.disabled) return;
    button.disabled = true;
    try { await checkForUpdate(state.detail.id); }
    finally { button.disabled = false; }
  });
  byId("command-send").addEventListener("click", sendConsoleCommand);
  byId("console-output").addEventListener("scroll", (event) => {
    const id = state.detail.id;
    const item = id ? state.logs.get(id) : null;
    if (!item) return;
    const output = event.currentTarget;
    item.autoScroll = output.scrollTop + output.clientHeight >= output.scrollHeight - 8;
    byId("console-jump").hidden = item.autoScroll;
  });
  byId("console-jump").addEventListener("click", () => {
    const item = state.logs.get(state.detail.id);
    if (!item) return;
    item.autoScroll = true;
    const output = byId("console-output");
    output.scrollTop = output.scrollHeight;
    byId("console-jump").hidden = true;
  });
  byId("console-clear").addEventListener("click", () => {
    const item = state.logs.get(state.detail.id);
    if (!item || !item.lines.length) return;
    const last = item.lines[item.lines.length - 1];
    item.clearedAt = Date.parse(last.timestamp || "") || Date.now();
    patchDetail(state.detail.id);
  });
  byId("console-save").addEventListener("click", () => {
    if (state.detail.id) downloadConsole(state.detail.id, visibleConsoleLines(state.detail.id));
  });
  byId("console-noise-toggle").addEventListener("change", (event) => {
    const item = state.logs.get(state.detail.id);
    if (item) { item.hideNoise = event.currentTarget.checked; patchDetail(state.detail.id); }
  });
  byId("console-save-as").addEventListener("click", (event) => {
    byId("console-export-severity").value = state.logs.get(state.detail.id)?.severity || "all";
    setupDialog(byId("console-save-as-dialog"), event.currentTarget);
  });
  byId("console-save-as-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    await saveConsoleAs();
  });
  byId("command-input").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      sendConsoleCommand();
    }
  });
  byId("detail-log-query").addEventListener("input", () => { const item = state.logs.get(state.detail.id); if (item) { item.query = byId("detail-log-query").value; renderDetailLogs(item); } });
  byId("detail-log-severity").addEventListener("change", () => { const item = state.logs.get(state.detail.id); if (item) { item.severity = byId("detail-log-severity").value; renderDetailLogs(item); } });
  byId("detail-log-pause").addEventListener("click", () => { const item = state.logs.get(state.detail.id); if (item) { item.paused = !item.paused; byId("detail-log-pause").textContent = item.paused ? "Resume live logs" : "Pause live logs"; } });
  byId("stats-window").addEventListener("change", () => { if (state.detail.tab === "stats") { state.detail.statsBaseLoaded = false; loadStats(state.detail.id, { includeBase: true }); } });
  byId("stats-resolution")?.addEventListener("change", () => { if (state.detail.tab === "stats") loadStats(state.detail.id, { includeBase: false }); });
  byId("stats-comparison")?.addEventListener("change", () => {
    const cached = state.statsCache.get(state.detail.id)?.tps;
    if (cached) { byId("stats-tps-chart").dataset.signature = ""; renderStatsTps(cached); }
  });
  byId("benchmark-form")?.addEventListener("submit", runBenchmark);
  byId("benchmark-load-more")?.addEventListener("click", () => { state.detail.benchmarkCursor = byId("benchmark-load-more").dataset.cursor || null; loadBenchmarks(state.detail.id, true); });
  byId("benchmark-export-json")?.addEventListener("click", () => { if (state.detail.id) window.open(`/api/v1/benchmarks/${encodeURIComponent(state.detail.id)}/export?format=json`, "_blank", "noopener"); });
  byId("benchmark-export-csv")?.addEventListener("click", () => { if (state.detail.id) window.open(`/api/v1/benchmarks/${encodeURIComponent(state.detail.id)}/export?format=csv`, "_blank", "noopener"); });
  byId("benchmark-baseline")?.addEventListener("change", validateBenchmarkForm);
  byId("benchmark-candidate")?.addEventListener("change", validateBenchmarkForm);
  byId("config-form")?.addEventListener("submit", applyConfig);
  byId("schedule-form")?.addEventListener("submit", addSchedule);
  byId("detail-noise-toggle").addEventListener("change", (event) => { const item = state.logs.get(state.detail.id); if (item) { item.hideNoise = event.currentTarget.checked; renderDetailLogs(item); patchDetail(state.detail.id); } });
  // Recurring timer: console/log append poll; callback is visibility-gated.
  window.setInterval(() => {
    if (!pageVisible()) return;
    const id = state.detail.id;
    const item = id ? state.logs.get(id) : null;
    if (id && item && !item.paused && ["console", "logs"].includes(state.detail.tab)) loadDetailLogs(id);
  }, 3000);
  byId("retry-load")?.addEventListener("click", () => load().finally(route));
  byId("logout")?.addEventListener("click", async () => { try { await api("/api/v1/session/revoke", { method: "POST", body: "{}" }); location.reload(); } catch (error) { notify(error.message || "Logout failed."); } });
}

function refreshVisiblePanels() {
  if (!pageVisible() || !state.detail.id) return;
  const id = state.detail.id;
  if (["console", "logs"].includes(state.detail.tab)) {
    const item = state.logs.get(id);
    if (!item?.paused) loadDetailLogs(id);
  } else if (state.detail.tab === "stats") {
    loadStats(id, { includeBase: !state.detail.statsBaseLoaded });
  } else if (state.detail.tab === "metrics") {
    loadMetricHistory(id);
  } else if (state.detail.tab === "benchmarks") {
    loadBenchmarks(id);
  }
}

function paletteRoute(id, tab = "console") {
  return `#/servers/${encodeURIComponent(id)}/${encodeURIComponent(tab)}`;
}

function paletteCommands() {
  const commands = [];
  const addNavigation = (id, label, hash, keywords = "") => commands.push({
    id,
    label,
    group: "Navigate",
    keywords: `${label} ${keywords}`,
    run: () => { window.location.hash = hash; },
  });
  addNavigation("dashboard", "Go to Dashboard", "#/", "home overview");
  addNavigation("backups", "Go to Backups", "#/backups", "archives snapshots");
  addNavigation("events", "Go to Events", "#/events", "activity history");
  addNavigation("audit", "Go to Audit", "#/audit", "log review security");
  addNavigation("settings", "Go to Settings", "#/settings", "preferences notifications performance");

  const tabLabels = { console: "Console", metrics: "Metrics", stats: "Stats", logs: "Logs", backups: "Backups", benchmarks: "Benchmarks", config: "Config" };
  const owner = slotOwnerId();
  profileOrder().forEach((id) => {
    const profile = detailProfile(id);
    const display = profile.display_name || id;
    Object.entries(tabLabels).filter(([tab]) => tab !== "benchmarks" || new Set(profile.operations || []).has("benchmark")).forEach(([tab, tabLabel]) => addNavigation(
      `${id}-${tab}`,
      `${display} / ${tabLabel}`,
      paletteRoute(id, tab),
      `${id} ${display} ${tab} ${tabLabel}`,
    ));

    const status = detailStatus(id);
    const current = status.state || "unknown";
    const operations = new Set(profile.operations || []);
    const addMutation = (operation, states) => {
      if (!operations.has(operation) || !states.includes(current) || (operation === "start" && owner && owner !== id)) return;
      commands.push({
        id: `${id}-${operation}`,
        label: `${operation[0].toUpperCase()}${operation.slice(1)} ${display}`,
        group: "Server actions",
        keywords: `${id} ${display} ${operation} server action`,
        run: () => mutate(id, operation),
      });
    };
    addMutation("start", ["stopped", "failed", "blocked", "unknown"]);
    addMutation("stop", ["running", "starting"]);
    addMutation("restart", ["running"]);
    if (id !== owner && state.profiles.has(id)) {
      commands.push({
        id: `${id}-switch`,
        label: `Switch to ${display}`,
        group: "Server actions",
        keywords: `${id} ${display} switch active server`,
        run: () => {
          byId("switch-active")?.click();
          const target = byId("switch-target");
          if (!target || ![...target.options].some((option) => option.value === id)) return;
          target.value = id;
          target.dispatchEvent(new Event("change", { bubbles: true }));
        },
      });
    }
  });

  THEMES.forEach((theme) => commands.push({
    id: `theme-${theme}`,
    label: `Switch theme to ${titleCase(theme)}`,
    group: "Appearance",
    keywords: `theme ${theme} color appearance`,
    run: () => applyTheme(theme, true),
  }));
  return commands;
}

window.HORIZON_PALETTE = { getCommands: paletteCommands };

window.addEventListener("game-control-status", (event) => applyStatus(event.data || event.detail));
window.addEventListener("resize", () => {
  if (state.detail.tab !== "stats" || !state.detail.id) return;
  const cached = state.statsCache.get(state.detail.id)?.tps;
  if (cached) renderStatsTps(cached);
});
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState !== "visible") {
    state.detail.statsAbort?.abort();
    clearStartupEstimate();
    suspendStream();
    return;
  }
  flushClientPerformance();
  if (sessionExpired) { void load().finally(route); return; }
  refreshVisiblePanels();
  resumeStream();
});
window.addEventListener("focus", () => {
  if (!pageVisible()) return;
  if (sessionExpired || state.loadFailed) { void load().finally(route); return; }
  if (stream.suspended) resumeStream();
});
window.addEventListener("online", () => {
  if (!pageVisible()) return;
  if (sessionExpired) { void load().finally(route); return; }
  if (stream.suspended) resumeStream(); else connectStream();
});
window.__horizonOpenLogs = openLogs;
if (window.__HORIZON_TEST__) {
  window.__horizonTest = {
    api,
    load,
    scheduleReconnect,
    checkStreamFreshness,
    pollStatus,
    startupEstimateTick: () => {
      patchStartupEstimate(
        startupEstimate.profileId ? state.statuses.get(startupEstimate.profileId) : null,
        slotOwnerId(),
      );
    },
    patchActiveSlot: () => patchActiveSlot(),
    startupEstimateState: () => ({
      attemptId: startupEstimate.attemptId,
      version: startupEstimate.version,
      elapsedAtAnchor: startupEstimate.elapsedAtAnchor,
      anchoredAt: startupEstimate.anchoredAt,
      medianSeconds: startupEstimate.medianSeconds,
      ready: startupEstimate.ready,
      serverSample: startupEstimate.serverSample,
      enabled: startupEstimateEnabled(),
      fresh: startupEstimateStatusFresh(),
      statusConfirmed: state.statusConfirmed,
      lastStatusAt: state.lastStatusAt,
    }),
    sessionState: () => ({ expired: sessionExpired, refreshing: Boolean(sessionRefreshPromise), noticeShown: sessionNoticeShown, expiryCount: sessionExpiryCount, generation: sessionGeneration }),
    setReconnectTestTiming: (delay, jitter = () => 0) => { stream.retryMs = delay; reconnectJitter = jitter; },
    setUpdatePollTiming: (delay, attempts = updatePollAttempts) => { updatePollDelay = delay; updatePollAttempts = attempts; },
    delayNextApiResponse: () => { testApiResponseDelay += 1; },
  };
}
window.addEventListener("hashchange", route);
setupShell();
setupSessionDeck();
wireDialogForms();
setupDetail();
load().finally(route);
