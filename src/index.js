import { neon } from "@neondatabase/serverless";

const NEWS_CATEGORY_NAMES = ["AI", "국무부", "국방부", "텍사스", "관세"];
const SOCIAL_CATEGORY_NAMES = ["소셜 (Helberg)", "소셜 (Rubio)", "소셜 (Hegseth)"];
const CATEGORY_NAMES = [...NEWS_CATEGORY_NAMES, ...SOCIAL_CATEGORY_NAMES];

const SOCIAL_ACCOUNTS = {
  "소셜 (Helberg)": [
    "Jacob S. Helberg · 국무부 공식 (@UnderSecE)",
    "Jacob Helberg · 개인 공개 (@jacobhelberg)",
  ],
  "소셜 (Rubio)": [
    "Marco Rubio · 국무장관 공식 (@SecRubio)",
    "Marco Rubio · 개인 공개 (@marcorubio)",
  ],
  "소셜 (Hegseth)": [
    "Pete Hegseth · 장관 공식 (@SecWar)",
    "Pete Hegseth · 개인 공개 (@PeteHegseth)",
  ],
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function getSql(env) {
  if (!env.DATABASE_URL) throw new Error("DATABASE_URL secret is not configured.");
  return neon(env.DATABASE_URL);
}

async function readJson(request) {
  try {
    return await request.json();
  } catch {
    return {};
  }
}

async function secureEqual(a, b) {
  if (!a || !b) return false;
  const enc = new TextEncoder();
  const [da, db] = await Promise.all([
    crypto.subtle.digest("SHA-256", enc.encode(String(a))),
    crypto.subtle.digest("SHA-256", enc.encode(String(b))),
  ]);
  const aa = new Uint8Array(da);
  const bb = new Uint8Array(db);
  let diff = aa.length ^ bb.length;
  const len = Math.min(aa.length, bb.length);
  for (let i = 0; i < len; i++) diff |= aa[i] ^ bb[i];
  return diff === 0;
}

async function requireAdmin(request, env) {
  if (!env.ADMIN_PASSWORD) return false;
  const supplied = request.headers.get("x-admin-password") || "";
  return secureEqual(supplied, env.ADMIN_PASSWORD);
}

function normalizeKeyword(value) {
  return String(value || "").trim().replace(/\s+/g, " ");
}

function normalizeDomain(value) {
  let v = String(value || "").trim().toLowerCase();
  if (!v) return "";
  try {
    if (!v.includes("://")) v = "https://" + v;
    const u = new URL(v);
    return u.hostname.replace(/^www\./, "").replace(/\/$/, "");
  } catch {
    return "";
  }
}

function stripOuterQuotes(value) {
  let v = normalizeKeyword(value);
  if (v.length >= 2 && ((v[0] === '"' && v.at(-1) === '"') || (v[0] === "'" && v.at(-1) === "'"))) {
    v = v.slice(1, -1).trim();
  }
  return normalizeKeyword(v);
}

function parseKeywordExpression(keyword) {
  const v = normalizeKeyword(keyword);
  if (!v) return [];
  return v.split(/\s+AND\s+/i).map(stripOuterQuotes).filter(Boolean);
}

const IRREGULAR = {
  analysis: "analyses", basis: "bases", crisis: "crises", criterion: "criteria",
  index: "indices", matrix: "matrices", person: "people", child: "children",
  man: "men", woman: "women"
};
const REVERSE_IRREGULAR = Object.fromEntries(Object.entries(IRREGULAR).map(([a,b]) => [b,a]));
const UNCOUNTABLE = new Set(["ai","data","information","intelligence","research","news","equipment","software","hardware","media"]);

function pluralize(word) {
  const lower = word.toLowerCase();
  if (!word || UNCOUNTABLE.has(lower)) return null;
  if (IRREGULAR[lower]) return IRREGULAR[lower];
  if (REVERSE_IRREGULAR[lower]) return null;
  if (/[^aeiou]y$/.test(lower)) return lower.slice(0,-1) + "ies";
  if (/(s|x|z|ch|sh)$/.test(lower)) return lower + "es";
  return lower + "s";
}

function singularize(word) {
  const lower = word.toLowerCase();
  if (!word || UNCOUNTABLE.has(lower)) return null;
  if (REVERSE_IRREGULAR[lower]) return REVERSE_IRREGULAR[lower];
  if (/[^aeiou]ies$/.test(lower)) return lower.slice(0,-3) + "y";
  if (/(ches|shes|xes|zes|sses)$/.test(lower)) return lower.slice(0,-2);
  if (lower.endsWith("s") && !/(ss|us|is)$/.test(lower)) return lower.slice(0,-1);
  return null;
}

function phraseVariants(phrase) {
  const p = normalizeKeyword(phrase);
  if (!p) return [];
  const words = p.split(" ");
  const last = words.at(-1);
  const out = new Set([p.toLowerCase()]);
  for (const alt of [pluralize(last), singularize(last)]) {
    if (alt) out.add([...words.slice(0,-1), alt].join(" ").toLowerCase());
  }
  return [...out];
}

function phrasePresent(text, phrase) {
  const hay = String(text || "").toLowerCase();
  return phraseVariants(phrase).some(v => hay.includes(v));
}

function exactPhraseMatch(keyword, text) {
  const parts = parseKeywordExpression(keyword);
  return parts.length > 0 && parts.every(part => phrasePresent(text, part));
}

function hostOf(value) {
  try { return new URL(value).hostname.toLowerCase().replace(/^www\./, ""); }
  catch { return ""; }
}

function sourceKey(value) {
  return String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").replace(/\s+/g, " ").trim();
}

function governmentScopeAllowed(article) {
  const host = hostOf(article.link);
  const s = sourceKey(article.source);
  const isState = host === "state.gov" || s.includes("department of state") || s === "state department";
  if (isState) {
    if (host !== "state.gov") return false;
    let path = "";
    try { path = new URL(article.link).pathname; } catch {}
    if (!path.startsWith("/releases/")) return false;
    const matched = (article.tags || []).filter(tag => exactPhraseMatch(tag, article.title || ""));
    article.tags = matched;
    return matched.length > 0;
  }
  if (host === "whitehouse.gov") {
    let path = "";
    try { path = new URL(article.link).pathname; } catch {}
    return ["/releases/","/briefings-statements/","/presidential-actions/","/fact-sheets/","/remarks/","/research/"]
      .some(prefix => path.startsWith(prefix));
  }
  return true;
}

function laDay(value = new Date()) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Los_Angeles", year: "numeric", month: "2-digit", day: "2-digit"
  }).format(value);
}

async function quotaStatus(sql) {
  const rows = await sql.query(
    "SELECT mode, used_at FROM summary_usage WHERE used_at >= NOW() - INTERVAL '2 days'",
    []
  );
  const today = laDay();
  let autoUsed = 0, manualUsed = 0;
  for (const row of rows) {
    const d = laDay(new Date(row.used_at));
    if (d !== today) continue;
    if (row.mode === "auto") autoUsed++;
    if (row.mode === "manual") manualUsed++;
  }
  return { auto_used: autoUsed, auto_limit: 10, manual_used: manualUsed, manual_limit: 10 };
}

async function bootstrap(env) {
  const sql = getSql(env);
  const [cats, kws, sources, countRows, quota] = await Promise.all([
    sql.query("SELECT id, name, sort_order FROM categories ORDER BY sort_order, id", []),
    sql.query("SELECT c.name AS category, k.keyword FROM keywords k JOIN categories c ON c.id=k.category_id ORDER BY c.sort_order, k.id", []),
    sql.query("SELECT domain, enabled FROM sources ORDER BY id", []),
    sql.query("SELECT COUNT(*)::int AS count FROM articles", []),
    quotaStatus(sql),
  ]);
  const keywordMap = Object.fromEntries(NEWS_CATEGORY_NAMES.map(name => [name, []]));
  for (const row of kws) {
    if (!keywordMap[row.category]) keywordMap[row.category] = [];
    keywordMap[row.category].push(row.keyword);
  }
  return {
    categories: cats.map(x => x.name).filter(name => CATEGORY_NAMES.includes(name)),
    keywordMap,
    sources: sources.filter(x => x.enabled).map(x => x.domain),
    totalArticles: Number(countRows[0]?.count || 0),
    quota,
    socialAccounts: SOCIAL_ACCOUNTS,
  };
}

async function fetchArticles(request, env) {
  const url = new URL(request.url);
  const category = url.searchParams.get("category") || "AI";
  if (!CATEGORY_NAMES.includes(category)) return json({ error: "Bad category" }, 400);
  const basis = url.searchParams.get("basis") === "detected" ? "detected" : "published";
  const rawPeriod = url.searchParams.get("period");
  const period = rawPeriod === "all" ? null : Math.max(1, Math.min(24 * 365, Number(rawPeriod || 48)));
  const cutoff = period ? new Date(Date.now() - period * 3600_000).toISOString() : null;
  const sql = getSql(env);
  const timeExpr = basis === "published" ? "COALESCE(a.published_at, a.detected_at)" : "a.detected_at";
  let q = `
    SELECT a.id, a.title, a.link, a.source, a.published_at, a.detected_at, a.description, a.summary,
           array_agg(am.keyword ORDER BY am.id) AS tags
    FROM articles a
    JOIN article_matches am ON am.article_id = a.id
    JOIN categories c ON c.id = am.category_id
    WHERE c.name = $1
  `;
  const params = [category];
  if (cutoff) {
    q += ` AND ${timeExpr} >= $2 `;
    params.push(cutoff);
  }
  q += ` GROUP BY a.id ORDER BY ${timeExpr} DESC, a.detected_at DESC LIMIT 100 `;
  const rows = await sql.query(q, params);
  return json({ articles: rows.filter(governmentScopeAllowed) });
}

async function settingsGet(request, env) {
  if (!(await requireAdmin(request, env))) return json({ error: "Unauthorized" }, 401);
  const data = await bootstrap(env);
  return json({ keywordMap: data.keywordMap, domains: data.sources });
}

async function settingsSave(request, env) {
  if (!(await requireAdmin(request, env))) return json({ error: "Unauthorized" }, 401);
  const body = await readJson(request);
  const keywordMap = body.keywordMap && typeof body.keywordMap === "object" ? body.keywordMap : {};
  const sql = getSql(env);

  // Each category is replaced in ONE PostgreSQL statement.
  // This prevents a temporary empty category when a request is interrupted
  // between DELETE and INSERT. A missing category in the request is preserved.
  for (const name of NEWS_CATEGORY_NAMES) {
    if (!Object.prototype.hasOwnProperty.call(keywordMap, name)) continue;
    if (!Array.isArray(keywordMap[name])) continue;

    const seen = new Set();
    const cleanKeywords = [];
    for (const raw of keywordMap[name]) {
      const kw = normalizeKeyword(raw);
      const key = kw.toLowerCase();
      if (!kw || seen.has(key)) continue;
      seen.add(key);
      cleanKeywords.push(kw);
    }

    await sql.query(
      `WITH cat AS (
         SELECT id FROM categories WHERE name=$1 LIMIT 1
       ),
       deleted AS (
         DELETE FROM keywords k
         USING cat
         WHERE k.category_id=cat.id
         RETURNING k.id
       )
       INSERT INTO keywords(category_id, keyword)
       SELECT cat.id, value
       FROM cat
       CROSS JOIN LATERAL jsonb_array_elements_text($2::jsonb) AS j(value)`,
      [name, JSON.stringify(cleanKeywords)]
    );
  }

  if (Array.isArray(body.domains)) {
    const seenDomains = new Set();
    const cleanDomains = [];
    for (const raw of body.domains) {
      const domain = normalizeDomain(raw);
      if (!domain || seenDomains.has(domain)) continue;
      seenDomains.add(domain);
      cleanDomains.push(domain);
    }

    await sql.query(
      `WITH deleted AS (
         DELETE FROM sources
         RETURNING id
       )
       INSERT INTO sources(domain, enabled)
       SELECT value, TRUE
       FROM jsonb_array_elements_text($1::jsonb) AS j(value)`,
      [JSON.stringify(cleanDomains)]
    );
  }

  const saved = await bootstrap(env);
  return json({ ok: true, keywordMap: saved.keywordMap, domains: saved.sources });
}

async function summarize(request, env) {
  if (!env.GEMINI_API_KEY) return json({ error: "GEMINI_API_KEY is not configured." }, 503);
  const body = await readJson(request);
  const articleId = Number(body.articleId);
  if (!Number.isFinite(articleId)) return json({ error: "Bad article id" }, 400);

  const sql = getSql(env);
  const quota = await quotaStatus(sql);
  if (quota.manual_used >= quota.manual_limit) return json({ error: "오늘 수동 요약 10회를 모두 사용했습니다." }, 429);

  const rows = await sql.query("SELECT id,title,description,summary FROM articles WHERE id=$1 LIMIT 1", [articleId]);
  if (!rows.length) return json({ error: "Article not found" }, 404);
  if (rows[0].summary) return json({ summary: rows[0].summary, quota });

  const prompt = `아래 뉴스 기사 또는 공개 소셜 게시물의 제목/원문을 바탕으로, 글로벌 대외협력(GPA) 담당자가 빠르게 핵심을 파악할 수 있도록 한국어로 요약해 주세요.

규칙:
- 정확히 3개의 짧은 불릿으로 작성
- 확인되지 않은 내용을 추측하지 말 것
- 회사명, 기관명, 정책명 등 핵심 고유명사는 가능하면 유지
- 각 불릿은 한 문장 정도로 간결하게 작성

제목:
${rows[0].title}

원문/설명:
${String(rows[0].description || "").slice(0, 2500)}`;

  const model = env.GEMINI_MODEL || "gemini-3.6-flash";
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(env.GEMINI_API_KEY)}`;

  let resp = null;
  for (let attempt = 1; attempt <= 3; attempt++) {
    resp = await fetch(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] }),
    });

    if (resp.ok || resp.status !== 503 || attempt === 3) break;

    // Gemini가 일시적으로 혼잡할 때 잠깐 기다렸다가 자동 재시도합니다.
    await new Promise(resolve => setTimeout(resolve, attempt * 1200));
  }

  if (!resp.ok) {
    if (resp.status === 429) {
      return json({ error: "Gemini 무료 한도가 소진되었습니다. 다음 한도 갱신 후 다시 시도해 주세요." }, 429);
    }

    if (resp.status === 503) {
      return json({ error: "Gemini 서버가 지금 혼잡합니다. 잠시 후 다시 눌러 주세요. 이번 실패는 수동 요약 횟수에 포함되지 않습니다." }, 503);
    }

    const txt = await resp.text();
    return json({ error: `요약 서비스 오류(${resp.status}). 잠시 후 다시 시도해 주세요.`, detail: txt.slice(0,120) }, 502);
  }

  const data = await resp.json();
  const summary = data?.candidates?.[0]?.content?.parts?.map(x => x.text || "").join("").trim();
  if (!summary) return json({ error: "요약 결과가 비어 있습니다." }, 502);

  await sql.query("UPDATE articles SET summary=$1 WHERE id=$2", [summary, articleId]);
  await sql.query("INSERT INTO summary_usage(article_id,mode,used_at) VALUES($1,'manual',NOW())", [articleId]);
  return json({ summary, quota: await quotaStatus(sql) });
}

async function dispatchCollector(env) {
  if (!env.GITHUB_ACTIONS_TOKEN) throw new Error("GITHUB_ACTIONS_TOKEN secret is not configured.");
  const resp = await fetch("https://api.github.com/repos/domidnight/news-sensing/actions/workflows/collector.yml/dispatches", {
    method: "POST",
    headers: {
      "authorization": `Bearer ${env.GITHUB_ACTIONS_TOKEN}`,
      "accept": "application/vnd.github+json",
      "x-github-api-version": "2022-11-28",
      "user-agent": "news-sensing-cloudflare",
      "content-type": "application/json",
    },
    body: JSON.stringify({ ref: "main" }),
  });
  if (!resp.ok) throw new Error(`GitHub dispatch failed: ${resp.status} ${(await resp.text()).slice(0,200)}`);
  return true;
}

async function collectNow(request, env) {
  if (!(await requireAdmin(request, env))) return json({ error: "Unauthorized" }, 401);
  try {
    await dispatchCollector(env);
    return json({ ok: true, message: "수집 작업을 시작했습니다. 보통 몇 분 안에 DB에 반영됩니다." });
  } catch (e) {
    return json({ error: e.message }, 503);
  }
}

async function api(request, env) {
  const url = new URL(request.url);
  try {
    if (url.pathname === "/api/health") return json({ ok: true });
    if (url.pathname === "/api/bootstrap" && request.method === "GET") return json(await bootstrap(env));
    if (url.pathname === "/api/articles" && request.method === "GET") return fetchArticles(request, env);
    if (url.pathname === "/api/settings" && request.method === "GET") return settingsGet(request, env);
    if (url.pathname === "/api/settings" && request.method === "POST") return settingsSave(request, env);
    if (url.pathname === "/api/summary" && request.method === "POST") return summarize(request, env);
    if (url.pathname === "/api/collect" && request.method === "POST") return collectNow(request, env);
    return json({ error: "Not found" }, 404);
  } catch (e) {
    console.error(e);
    return json({ error: e?.message || String(e) }, 500);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/api/")) return api(request, env);
    return env.ASSETS.fetch(request);
  },

  async scheduled(_controller, env, ctx) {
    ctx.waitUntil(
      dispatchCollector(env).catch(err => console.error("Scheduled collector dispatch failed", err))
    );
  },
};
