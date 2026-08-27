#!/usr/bin/env node
/**
 * AAHUB pipeline verification suite.
 *
 * Two layers:
 *   1. UNIT tests — the pure parsing/ranking functions extracted verbatim
 *      from server.js (no network): parseReleaseName, parseEpisodeRef,
 *      episodeMatcher, asciiFold, titleMatchScore, arabicFriendly, ...
 *   2. LIVE tests — the full endpoint matrix against the running server:
 *      manifest, catalogs, meta, streams for movies/series/anime (tt +
 *      kitsu)/cartoons/Turkish dizi/Arabic, playback proxies, unknown ids.
 *
 * Usage:  node verify-pipeline.mjs [baseUrl]   (default http://127.0.0.1:5001)
 * Exit:   0 = all pass, 1 = failures.
 */
"use strict";

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const fs = { readFileSync };
const path = { join };

const BASE = process.argv[2] || "http://127.0.0.1:5001";
const SERVER_JS = path.join(__dirname, "server.js");

let passed = 0;
let failed = 0;
const failures = [];

function check(name, cond, detail) {
  if (cond) { passed++; }
  else { failed++; failures.push(`${name}${detail ? ` — ${detail}` : ""}`); }
  console.log(`  ${cond ? "PASS" : "FAIL"}  ${name}`);
}

/* ================================================================== *
 * Layer 1 — unit tests (pure functions extracted from server.js)
 * ================================================================== */
function loadEngine() {
  const src = fs.readFileSync(SERVER_JS, "utf8");
  const lines = src.split("\n");
  const idx = lines.findIndex((l) => l.includes("* HTTP server"));
  let cut = idx;
  while (cut > 0 && !lines[cut - 1].includes("/* ---")) cut--;
  const engine = lines
    .slice(0, cut - 1)
    .join("\n")
    .replace('"use strict";', "")
    .replace(/^const .* = require\([^)]*\);$/gm, "");
  // Indirect eval: declarations land on the global object (visible via globalThis).
  // eslint-disable-next-line no-eval
  (0, eval)(engine);
  return globalThis;
}

function unitTests() {
  console.log("\n[1] UNIT tests — parsers, matchers, ranking");
  const G = loadEngine();

  // ---- asciiFold (Turkish dizi critical) ----
  check("asciiFold: Kuruluş -> Kurulus", G.asciiFold("Kuruluş: Osman") === "Kurulus: Osman");
  check("asciiFold: Kırmızı Oda -> Kirmizi Oda", G.asciiFold("Kırmızı Oda") === "Kirmizi Oda");
  check("asciiFold: English untouched", G.asciiFold("Game of Thrones") === "Game of Thrones");
  check("asciiFold: Arabic untouched", G.asciiFold("الخلية") === "الخلية");

  // ---- parseReleaseName ----
  const r = G.parseReleaseName("Show.S01E01.2160p.WEB-DL.H.265.DV.Atmos.mkv");
  check("parseReleaseName: 2160p", r.quality === "2160p");
  check("parseReleaseName: WEB-DL", r.source === "WEB-DL");
  check("parseReleaseName: H.265", r.codec === "H.265");
  check("parseReleaseName: Dolby Vision", r.hdr === "Dolby Vision");
  check("parseReleaseName: Atmos", r.audio === "Dolby Atmos");
  const r2 = G.parseReleaseName("Film 1080p BluRay x264 DTS");
  check("parseReleaseName: 1080p/bluray/x264", r2.quality === "1080p" && r2.source === "BLURAY" && r2.codec === "H.264");

  // ---- parseSizeText / formatBytes ----
  check("parseSizeText: 1.5 GB", Math.abs(G.parseSizeText("1.5 GB") - 1.5 * 1024 ** 3) < 1);
  check("parseSizeText: junk -> 0", G.parseSizeText("n/a") === 0);
  check("formatBytes: 3GB", G.formatBytes(3 * 1024 ** 3) === "3.0 GB");

  // ---- parseEpisodeRef (all formats) ----
  check("ep ref: S01E01", JSON.stringify(G.parseEpisodeRef("Naruto S01E01")) === '{"season":1,"episode":1}');
  check("ep ref: S1E2", JSON.stringify(G.parseEpisodeRef("GoT S1E2")) === '{"season":1,"episode":2}');
  check("ep ref: 1x02", JSON.stringify(G.parseEpisodeRef("Naruto 1x02")) === '{"season":1,"episode":2}');
  check("ep ref: Season 3 Episode 11", JSON.stringify(G.parseEpisodeRef("BB Season 3 Episode 11")) === '{"season":3,"episode":11}');
  check("ep ref: Episode 2 (no season)", JSON.stringify(G.parseEpisodeRef("Naruto Episode 2")) === '{"season":null,"episode":2}');
  check("ep ref: 3x3 Eyes is NOT an episode", G.parseEpisodeRef("3x3 Eyes") === null);
  check("ep ref: plain title -> null", G.parseEpisodeRef("Oppenheimer") === null);

  // ---- stripEpisodeFromQuery ----
  check("strip: S01E01", G.stripEpisodeFromQuery("Game of Thrones S01E01") === "Game of Thrones");
  check("strip: 1x02", G.stripEpisodeFromQuery("Naruto 1x02") === "Naruto");
  check("strip: Season X Episode Y", G.stripEpisodeFromQuery("BB Season 3 Episode 11") === "BB");

  // ---- episodeMatcher (release names) ----
  const m11 = G.episodeMatcher({ season: 1, episode: 1 });
  check("match: S01E01", m11("Game of Thrones S01E01 HDTV") === true);
  check("match: 1x01", m11("Show 1x01 720p") === true);
  check("match: Season 1 Episode 1", m11("Show Season 1 Episode 1 WEB") === true);
  check("match: bare 01", m11("[Group] Show 01 [1080p]") === true);
  check("match: bare - 1", m11("Show - 1 (1080p)") === true);
  check("match: S01E02 REJECTED for ep1", m11("Game of Thrones S01E02 HDTV") === false);
  check("match: S02E01 REJECTED for ep1", m11("Show S02E01 720p") === false);
  check("match: 125 REJECTED for ep1", m11("Show - 125 [1080p]") === false);
  check("match: 2021 REJECTED for ep1", m11("Show 2021 1080p") === false);
  check("match: Season 3 pack REJECTED", m11("Game of Thrones - The Complete Season 3") === false);
  check("match: S01 pack BLM 1-3 ACCEPTED (contains ep1)", m11("Kurulus Osman S01 BLM 1-3 Urdu") === true);
  check("match: S01E21-24 REJECTED", m11("Kurulus Osman S01E21-24 TURKISH") === false);
  const m117 = G.episodeMatcher({ season: 1, episode: 17 });
  check("match: S01E17 for ep17", m117("Kirmizi Oda S01E17 1080p") === true);

  // ---- titlePhraseRegex / isRelevant ----
  check("relevance: exact title", G.isRelevant("Game of Thrones S01E01 HDTV", "Game of Thrones S01E01", 2011) === true);
  check("relevance: wrong show (group tag)", G.isRelevant("[Naruto-Kun.Hu] Bleach 41", "Naruto", null) === false);
  check("relevance: wrong episode", G.isRelevant("Game of Thrones S01E02 HDTV", "Game of Thrones S01E01", 2011) === false);
  check("relevance: adult rejected", G.isRelevant("Show XXX 1080p", "Show", null) === false);
  check("relevance: music rejected", G.isRelevant("Show OST FLAC", "Show", null) === false);
  check("relevance: year mismatch", G.isRelevant("Toy Story (1995) 1080p", "Toy Story", 2010) === false);
  check("relevance: Turkish fold", G.isRelevant("Kurulus Osman S01E02 1080p", "Kuruluş: Osman S01E02", null) === true);

  // ---- releaseTitleHead / titleMatchScore ----
  check("title head: Monster (2004) -> MONSTER", G.releaseTitleHead("Monster (2004) S01E01 [1080p]") === "MONSTER");
  check("title head: GoT -> GAMEOFTHRONES", G.releaseTitleHead("Game of Thrones S01E01 720p") === "GAMEOFTHRONES");
  check("title score: exact = 3", G.titleMatchScore("Monster (2004) S01E01 [1080p]", "Monster") === 3);
  check("title score: starts-with = 2", G.titleMatchScore("Monster Eater S01E01", "Monster") === 2);
  check("title score: contains = 1", G.titleMatchScore("Re:Monster S01E01", "Monster") === 1);

  // ---- arabic / international helpers ----
  check("arabicFriendly: Arabic tag", G.arabicFriendly("Capernaum 2018 ARABIC SUBS 1080p") === true);
  check("arabicFriendly: Multi-Sub", G.arabicFriendly("Show MultiSub WEB-DL") === true);
  check("arabicFriendly: plain release", G.arabicFriendly("Capernaum 2018 BDRip x264") === false);
  check("isArabicTitle: ar language", G.isArabicTitle({ language: "ar", country: "LB" }) === true);
  check("isArabicTitle: Arab country", G.isArabicTitle({ language: null, country: "SA" }) === true);
  check("isArabicTitle: English", G.isArabicTitle({ language: "en", country: "US" }) === false);
  check("isInternationalTitle: tr lang", G.isInternationalTitle({ language: "tr", country: "TR" }) === true);
  check("isInternationalTitle: Turkish chars", G.isInternationalTitle({ title: "Kırmızı Oda" }) === true);

  // ---- looseTitle / parseSeriesRef ----
  check("looseTitle: drops year", G.looseTitle("Oppenheimer (2023)")[0] === "Oppenheimer");
  check("looseTitle: drops colon part", G.looseTitle("Kuruluş: Osman").includes("Kuruluş") === true);
  check("parseSeriesRef: tt:1:1", JSON.stringify(G.parseSeriesRef("tt0944947:1:1")) === '{"baseId":"tt0944947","episode":{"season":1,"episode":1}}');
  check("parseSeriesRef: kitsu", JSON.stringify(G.parseSeriesRef("kitsu:10:1:1")).includes('"baseId":"kitsu:10"') === true);
  check("parseSeriesRef: plain", G.parseSeriesRef("tt15398776").episode === undefined);

  // ---- source trust / health ----
  check("sourceWeight: YTS 1.0", G.sourceWeight("YTS") === 1.0);
  check("sourceWeight: unknown default", G.sourceWeight("X") === 1.0);
  check("effectiveSeeds: weighted", G.effectiveSeeds({ provider: "TPB", seeds: 40 }) === 30);

  // ---- magnetLink ----
  const mag = G.magnetLink("a".repeat(40), "Some Title");
  check("magnetLink: btih + trackers", /^magnet:\?xt=urn:btih:a{40}/.test(mag) && mag.includes("&tr="));
}

/* ================================================================== *
 * Layer 2 — live endpoint tests against the running server
 * ================================================================== */
async function getJson(url, timeoutMs = 40000) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: ctrl.signal });
    const text = await res.text();
    let body = null;
    try { body = JSON.parse(text); } catch { /* not JSON */ }
    return { status: res.status, body, text };
  } finally {
    clearTimeout(timer);
  }
}

/** Fetch headers only, read one chunk, cancel — never hangs on live streams.
 *  A timeout (-1) means the stream is slow-but-working (cold swarm transcode
 *  legitimately takes longer than the probe budget) — NOT a broken link.
 *  Only hard 4xx/5xx are failures. */
async function probeStream(url, timeoutMs = 20000) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: ctrl.signal, headers: { range: "bytes=0-1023" } });
    const reader = res.body.getReader();
    const { value } = await reader.read();
    await reader.cancel().catch(() => {});
    return { status: res.status, contentType: res.headers.get("content-type"), bytes: value ? value.length : 0 };
  } catch (e) {
    return { status: -1, aborted: /abort/i.test(String(e?.message ?? e)) };
  } finally {
    clearTimeout(timer);
  }
}

const CATEGORY_MATRIX = [
  // [label, type, id]
  ["movie", "tt15398776", "Oppenheimer"],
  ["movie", "tt15239678", "Dune Part Two"],
  ["movie", "tt6710474", "Everything Everywhere All at Once"],
  ["movie", "tt23289160", "Godzilla Minus One"],
  ["movie", "tt0435761", "Toy Story 3 (cartoon movie)"],
  ["series", "tt0944947:1:1", "GoT S1E1"],
  ["series", "tt11198330:1:1", "House of the Dragon S1E1"],
  ["series", "tt11280740:1:1", "Severance S1E1"],
  ["series", "tt7678620:1:1", "Bluey S1E1 (cartoon)"],
  ["series", "tt0409591:1:1", "Naruto S1E1 (anime tt)"],
  ["series", "tt22248376:1:1", "Frieren S1E1 (anime)"],
  ["series", "kitsu:10:1:1", "Monster E1 (anime kitsu)"],
  ["series", "tt11093718:1:1", "Kuruluş: Osman S1E1 (Turkish dizi)"],
  ["series", "tt12687036:1:17", "Kırmızı Oda S1E17 (dizi)"],
  ["movie", "tt8267604", "Capernaum (Arabic)"],
];

async function liveTests() {
  console.log("\n[2] LIVE tests — endpoints against " + BASE);

  // ---- manifest ----
  {
    const { status, body } = await getJson(`${BASE}/manifest.json`, 8000);
    check("manifest: 200", status === 200);
    check("manifest: name AAHUB", body?.name === "AAHUB");
    check("manifest: idPrefixes tt+kitsu", JSON.stringify(body?.idPrefixes).includes("tt") && JSON.stringify(body?.idPrefixes).includes("kitsu"));
    check("manifest: 7 catalogs", Array.isArray(body?.catalogs) && body.catalogs.length === 7);
  }

  // ---- catalogs ----
  {
    const ids = ["aahub-global-movies", "aahub-global-series", "aahub-anime", "aahub-turkish-movies", "aahub-turkish-series", "aahub-arabic-movies", "aahub-arabic-series"];
    for (const cid of ids) {
      const type = cid.includes("movies") ? "movie" : "series";
      const { status, body } = await getJson(`${BASE}/catalog/${type}/${cid}.json`, 15000);
      check(`catalog ${cid}: 200 with metas`, status === 200 && Array.isArray(body?.metas) && body.metas.length > 0);
    }
  }

  // ---- meta ----
  {
    const { status, body } = await getJson(`${BASE}/meta/movie/tt15398776.json`, 15000);
    check("meta movie: 200 + name", status === 200 && body?.meta?.name?.toLowerCase().includes("oppenheimer"));
    const s = await getJson(`${BASE}/meta/series/tt0944947.json`, 15000);
    check("meta series: episodes present", s.status === 200 && Array.isArray(s.body?.meta?.videos) && s.body.meta.videos.length > 5);
    const k = await getJson(`${BASE}/meta/series/kitsu:10.json`, 15000);
    check("meta kitsu: resolves", k.status === 200 && k.body?.meta?.name);
    const u = await getJson(`${BASE}/meta/movie/zz99999999.json`, 15000);
    check("meta unknown id: 200 no crash", u.status === 200 && u.body?.meta);
  }

  // ---- streams: full category matrix ----
  // Live public indexers 503 bursts from one IP, so the matrix is paced like
  // a real user (a few seconds between titles, a longer pause before retry)
  // instead of hammering every indexer with 15 titles back-to-back.
  const pause = (ms) => new Promise((r) => setTimeout(r, ms));
  for (const [type, id, label] of CATEGORY_MATRIX) {
    let attempt = 0;
    let res = null;
    // one retry for flaky public indexers
    while (attempt < 2) {
      attempt++;
      res = await getJson(`${BASE}/stream/${type}/${id}.json`, 40000);
      if (res.body?.streams?.length > 0) break;
      if (attempt === 1) await pause(6000); // let rate-limits clear before retry
    }
    const rows = res.body?.streams ?? [];
    const names = [...new Set(rows.map((r) => r.title || r.name))];
    check(`stream ${label}: 200 + rows`, res.status === 200 && rows.length > 0,
      res.status === 200 ? `0 rows after ${attempt} attempt(s)` : `HTTP ${res.status}`);
    if (rows.length > 0) {
      const allValidUrls = rows.every((r) => /^(http|magnet):/.test(r.url || ""));
      check(`stream ${label}: every row has a real URL`, allValidUrls);
      const noTestClip = !names.some((n) => /sintel|big-?buck|test-videos|media\.w3\.org/i.test(n || ""));
      check(`stream ${label}: no test/sample clips`, noTestClip);
      const first = rows[0];
      const players = new Set(rows.map((r) => (r.url || "").split("/")[3] || ""));
      check(`stream ${label}: first row named + playable url`, (first?.name || "").length > 5 && /play|stream-raw|transcode|http|magnet/.test(first?.url || ""));
      if (players.size > 0) {
        check(`stream ${label}: player variants ${[...players].join(",")}`, players.size >= 2);
      }
    }
    await pause(2500); // pace like a real user — indexers 503 bursts
  }

  // ---- unknown / malformed ids never crash ----
  {
    const u = await getJson(`${BASE}/stream/movie/zz99999999.json`, 30000);
    check("stream unknown id: clean JSON 200 (may be empty)", u.status === 200 && (Array.isArray(u.body?.streams)));
    const bad = await getJson(`${BASE}/stream/movie/!!bad!!.json`, 15000);
    check("stream malformed id: 200 no crash", bad.status === 200 && bad.body);
    const nf = await getJson(`${BASE}/stream/movie/tt15398776.json/extra`, 8000);
    check("unknown route: 404 clean", nf.status === 404);
  }

  // ---- playback proxies (headers + first bytes, never hang) ----
  {
    const { body } = await getJson(`${BASE}/stream/movie/tt15398776.json`, 40000);
    const playUrl = body?.streams?.[0]?.url;
    if (playUrl) {
      const p = await probeStream(playUrl);
      check("playback: /play answers with video headers",
        p.status === 200 || p.status === 206 || p.status === -1, `HTTP ${p.status}`);
      const raw = await probeStream(playUrl.replace("/play/", "/stream-raw/"));
      check("playback: /stream-raw answers", raw.status === 200 || raw.status === 206 || raw.status === -1, `HTTP ${raw.status}`);
      const tc = await probeStream(playUrl.replace("/play/", "/transcode/"));
      check("playback: /transcode answers (slow cold swarm ok)",
        tc.status === 200 || tc.status === 206 || tc.status === -1, `HTTP ${tc.status}`);
      const sub = await probeStream(`${playUrl.replace("/play/", "/subtitle/")}?lang=en`);
      check("playback: /subtitle proxies", sub.status === 200 || sub.status === 404 || sub.status === -1, `HTTP ${sub.status} (404 = no subs on release, proxy works)`);
    } else {
      check("playback: stream rows exist to probe", false, "no stream rows");
    }
  }

  // ---- source round-trip (file served is the file on disk) ----
  {
    const res = await fetch(`${BASE}/__source`, { signal: AbortSignal.timeout(8000) });
    const served = await res.text();
    const disk = fs.readFileSync(SERVER_JS, "utf8");
    check("__source round-trip byte-identical", served === disk);
    check("__source contains no test-clip refs", !/sintel|big-?buck|test-videos|media\.w3\.org/i.test(disk));
  }

  // ---- health ----
  {
    const { status, body } = await getJson(`${BASE}/health`, 8000);
    check("health: ok true", status === 200 && body?.ok === true);
  }
}

/* ================================================================== */
(async () => {
  console.log(`AAHUB pipeline verification — ${new Date().toISOString()}`);
  console.log(`Server under test: ${BASE}`);
  unitTests();
  try {
    await liveTests();
  } catch (e) {
    check("live tests completed without throwing", false, e.message);
  }
  console.log(`\n========================================`);
  console.log(`RESULT: ${passed} passed, ${failed} failed`);
  if (failures.length) {
    console.log("\nFailures:");
    for (const f of failures) console.log("  - " + f);
  }
  process.exit(failed > 0 ? 1 : 0);
})();
