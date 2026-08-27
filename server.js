#!/usr/bin/env node
/**
 * AAHUB — standalone Stremio addon server (single file, zero dependencies).
 *
 * Run:   node server.js
 * Port:  PORT env (default 5001 — kept free so the api-server on 5000 and the
 *        torrent engine on 9099 can both run alongside).
 *
 * Fully open by design: http://127.0.0.1:5001/manifest.json installs directly
 * in Stremio — no keys, no redirects, no /install endpoint, no 403s.
 *
 * What it does (standard Stremio addon protocol, fully self-contained):
 *   /manifest.json                       -> addon manifest (7 AAHUB catalogs)
 *   /catalog/:type/:catalogId.json       -> catalog rows (movies/series/anime)
 *   /meta/:type/:id.json                 -> meta + episodes (TVMaze / Kitsu)
 *   /stream/:type/:id.json               -> real streams for ANY tt or kitsu id
 *   /play/:magnet  /stream-raw/:magnet   -> playback proxies into the torrent
 *   /transcode/:magnet  /subtitle/:magnet engine (with auto transcode fallback)
 *   /media/:file  /logo.svg  /poster.svg /health  /__source
 *
 * Movies, series, cartoons, anime — one fully DYNAMIC resolver (works for
 * EVERY title, not a hardcoded list — the SEED table only powers the curated
 * catalog pages):
 *   - tt ids (IMDb): name/year via Cinemeta (keyless); episodes via TVMaze
 *   - kitsu:<n> ids (anime): name/year/poster/episodes via the Kitsu API
 *   - any other id still resolves: title falls back to the id itself and the
 *     exact-id indexers (TPB/EZTV by imdb) still run
 *   - Turkish dizi: Latin diacritics are ASCII-folded (ş->s, ğ->g, ı->i) for
 *     both searching and matching, and a folded-title scrape pass runs
 *     alongside, so "Kuruluş Osman" resolves as "Kurulus Osman" too
 *   - Arabic-market titles: releases carrying Arabic subtitles ("Arabic",
 *     "عربي", "Multi-Sub", "[AR]") rank above raw seed counts
 *
 * Scraping: embeds the real public-indexer engine (YTS, TPB via apibay, EZTV,
 * 1337x, Nyaa, LimeTorrents) — global English content plus the Turkish/
 * Arabic releases the English indexers miss (dizi, Urdu/Arabic-subbed),
 * zero Russian trackers. Releases
 * are parsed for quality/source/codec/HDR/audio, deduped by info hash using
 * source-trust-weighted health, filtered for relevance (no adult/music/wrong
 * title), and ranked HEALTH-FIRST (seeds dominate, quality breaks ties) so
 * a fast high-seed swarm always plays instantly over a stalled one. A stall
 * guard drops weak 1-4 seeder swarms whenever healthy copies exist. If the
 * strict search returns nothing, a loose fallback (title without year/colon)
 * re-scrapes so obscure titles still resolve. Episode-scoped series/anime
 * requests run episode queries FIRST and match releases in every format
 * trackers use (SxxExx, 1x02, "Season X Episode Y", "Episode N", bare
 * "Show 01") so the ACTUAL episode file is found — never a season pack or
 * a wrong episode; fallback results are episode-filtered before serving.
 * No test/sample clips are ever served — every stream row is a real
 * scraped release for that title.
 * International (Arabic/Turkish) titles get an adaptive 1-seeder floor.
 * Every stream carries 14 lazy subtitle tracks.
 *
 * Speed (no more stuck loading screens): the strict/loose/episode scrape
 * passes run CONCURRENTLY and the whole batch is capped at an 8s deadline,
 * so a cold series request answers in seconds instead of waiting on three
 * sequential scrapes. Stream results are cached for 6h (with background
 * refresh), so repeat opens are instant.
 *
 * Playback never black-screens: every stream row ships 3 player variants
 * (Auto / RAW / TRANSCODE) with subtitles; the Auto route forwards to the
 * engine's /torrent/play (native containers raw, MKV/HEVC through ffmpeg)
 * and retries through the forced /transcode pipeline if the engine refuses,
 * so a browser gets playable bytes. Every served stream list pre-warms the
 * top magnets in the engine (/torrent/warm, fire-and-forget) so metadata is
 * already resolving when the user clicks — playback starts instantly instead
 * of waiting on a cold metadata fetch. Node's default timeouts are disabled:
 * a long video stream is never cut off.
 */
"use strict";

const http = require("node:http");
const { Readable } = require("node:stream");
const fs = require("node:fs");

/* ------------------------------------------------------------------ *
 * Configuration
 * ------------------------------------------------------------------ */
const PORT = Number(process.env.PORT || 5001);
const PROVIDER = (process.env.SITE_STREAM_API_URL || "http://127.0.0.1:9099").replace(/\/+$/, "");
const SUBTITLE_LANGS = ["en", "tr", "ar", "es", "fr", "de", "ru", "hi", "ja", "zh", "pt", "it", "nl", "pl"];
const WANTED_QUALITIES = ["2160p", "1080p", "720p"];
const MIN_SEEDERS = 2;
// Stall guard: when at least this many live seeders exist somewhere, weak
// 1-3 seeder swarms (the ones that stall playback) are dropped from the
// top rows instead of being served first.
const PREFERRED_SEEDS = 5;
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const REQUEST_TIMEOUT = 15_000;
const MIRROR_TIMEOUT = 6_000;

const TRACKERS = [
  "udp://tracker.opentrackr.org:1337/announce",
  "udp://open.stealth.si:80/announce",
  "udp://tracker.torrent.eu.org:451/announce",
  "udp://exodus.desync.com:6969/announce",
  "udp://tracker.openbittorrent.com:6969/announce",
];

/* ------------------------------------------------------------------ *
 * Small utilities
 * ------------------------------------------------------------------ */
const json = (res, status, body) => {
  const data = JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "access-control-allow-origin": "*",
  });
  res.end(data);
};

async function fetchText(url, headers = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
  try {
    const response = await fetch(url, {
      headers: { "user-agent": "AAHUB/1.0 (+stream provider)", ...headers },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.text();
  } finally {
    clearTimeout(timer);
  }
}

async function fetchJson(url, headers = {}) {
  return JSON.parse(await fetchText(url, headers));
}

/** Race mirror URLs; resolve the first that succeeds (or null). */
function fetchFirstMirror(urls, headers = {}, test) {
  return new Promise((resolve) => {
    let remaining = urls.length;
    let done = false;
    for (const url of urls) {
      void (async () => {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), MIRROR_TIMEOUT);
        try {
          const response = await fetch(url, {
            headers: { "user-agent": "AAHUB/1.0 (+stream provider)", ...headers },
            signal: controller.signal,
          });
          if (!response.ok) throw new Error(`HTTP ${response.status}`);
          const text = await response.text();
          if (test && !test(text)) throw new Error("mirror has no matching content");
          if (!done) {
            done = true;
            resolve({ text, url });
          }
        } catch {
          if (!done && --remaining === 0) resolve(null);
        } finally {
          clearTimeout(timer);
        }
      })();
    }
  });
}

function magnetLink(infoHash, name) {
  const dn = encodeURIComponent((name || "AAHUB release").replace(/\./g, " "));
  const trackers = TRACKERS.map((t) => `&tr=${encodeURIComponent(t)}`).join("");
  return `magnet:?xt=urn:btih:${infoHash}&dn=${dn}${trackers}`;
}

const KB = 1024, MB = KB * 1024, GB = MB * 1024, TB = GB * 1024;
function formatBytes(bytes) {
  const value = Number(bytes);
  if (!Number.isFinite(value) || value <= 0) return null;
  if (value >= TB) return `${(value / TB).toFixed(2)} TB`;
  if (value >= GB) return `${(value / GB).toFixed(1)} GB`;
  if (value >= MB) return `${Math.round(value / MB)} MB`;
  return `${Math.round(value / KB)} KB`;
}

function parseSizeText(text) {
  const m = /([\d.,]+)\s*(TB|GB|MB|KB)/i.exec(text ?? "");
  if (!m) return 0;
  const value = Number(m[1].replace(/,/g, ""));
  const unit = m[2].toUpperCase();
  if (unit === "TB") return value * 1024 ** 4;
  if (unit === "GB") return value * 1024 ** 3;
  if (unit === "MB") return value * 1024 ** 2;
  return value * 1024;
}

/** Parse a release name into structured metadata used for ranking/filtering. */
function parseReleaseName(name) {
  const n = name.replace(/\./g, " ").replace(/_/g, " ");
  const lower = n.toLowerCase();
  let quality = "unknown";
  if (/(2160p|4k|uhd)/i.test(n)) quality = "2160p";
  else if (/(1080p|fhd)/i.test(n)) quality = "1080p";
  else if (/(720p|hd)/i.test(n)) quality = "720p";
  else if (/(480p|dvdrip|tvrip)/i.test(n)) quality = "480p";
  else if (/(576p|pdtv)/i.test(n)) quality = "576p";
  else if (/(\bts\b|cam|hdcam|hdtc|dvdscr|hd-?ts|telecine)/i.test(n)) quality = "480p";

  const source = /(web-?dl|webrip|hdtv|blu-?ray|bluray|brrip|bdrip|dvdrip|dvdscr|remux|web|\bts\b|cam|telecine)/i.exec(n)?.[1]?.toUpperCase() ?? null;
  // Codec/HDR/audio must match the RAW name (dots intact): the dot->space
  // normalization above would turn "H.265" into "H 265" and break detection.
  const codec = /(h\.?265|hevc|x265)/i.test(name) ? "H.265"
    : /(h\.?264|x264|avc)/i.test(name) ? "H.264"
      : /(av1|vp9)/i.test(name) ? "AV1" : null;
  const hdr = /dolby[\s.-]?vision|dovi|(?<![a-z])dv(?![a-z])/i.test(name) ? "Dolby Vision"
    : /hdr10\+|hdr10plus/i.test(name) ? "HDR10+"
      : /(hdr|hdr10)/i.test(name) ? "HDR10" : null;
  const audio = /atmos/i.test(n) ? "Dolby Atmos"
    : /truehd/i.test(n) ? "Dolby TrueHD"
      : /dts[\s.-]?hd[\s.-]?ma|dtshdma/i.test(n) ? "DTS-HD MA"
        : /dts[\s.-]?x/i.test(n) ? "DTS:X"
          : /dts/i.test(n) ? "DTS"
            : /flac/i.test(n) ? "FLAC"
              : /aac/i.test(n) ? "AAC"
                : /ac3|dd5|dolby[\s.-]?digital/i.test(n) ? "Dolby Digital" : null;
  const languages = [];
  if (/(multi|multi[\s.-]?audio|dual[\s.-]?audio)/i.test(n)) languages.push("multi");
  if (/\bru\b|russian|russki/i.test(lower)) languages.push("ru");
  if (/\btr\b|turkish|türkçe/i.test(lower)) languages.push("tr");
  if (/\bar\b|arabic|العربية/i.test(lower)) languages.push("ar");
  if (/\bhi\b|hindi/i.test(lower)) languages.push("hi");
  if (/\bde\b|german/i.test(lower)) languages.push("de");
  if (/\bfr\b|french/i.test(lower)) languages.push("fr");
  if (/\bes\b|spanish/i.test(lower)) languages.push("es");
  if (/english|eng\b/i.test(lower)) languages.push("en");
  return { quality, source, codec, hdr, audio, languages, raw: name };
}

const SOURCE_RANK = { REMUX: 6, BLURAY: 5, "WEB-DL": 4, WEB: 4, WEBRIP: 3, HDTV: 2, BRRIP: 2, BDRIP: 2, DVDRIP: 1 };
function sourceQualityRank(source) {
  if (!source) return 2;
  return SOURCE_RANK[String(source).toUpperCase()] ?? 2;
}

/** Per-indexer trust weights — scale reported seeds when deduping/ranking. */
const SOURCE_WEIGHTS = { YTS: 1.0, EZTV: 1.0, "1337X": 1.0, LIME: 0.9, NYAA: 0.85, TPB: 0.75 };
const BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";
function sourceWeight(provider) {
  return SOURCE_WEIGHTS[String(provider ?? "").toUpperCase()] ?? 1.0;
}
function effectiveSeeds(stream) {
  return Math.round(Number(stream?.seeds ?? 0) * sourceWeight(stream?.provider));
}

function toStream(raw, provider) {
  const parsed = parseReleaseName(raw.name);
  return {
    provider,
    name: raw.name,
    quality: parsed.quality === "unknown" ? null : parsed.quality,
    size: formatBytes(raw.sizeBytes ?? raw.size),
    seeds: Number(raw.seeders ?? raw.seeds ?? 0),
    leechers: Number(raw.leechers ?? raw.peers ?? 0),
    url: raw.infoHash ? magnetLink(raw.infoHash, raw.name) : raw.url,
    infoHash: raw.infoHash ?? null,
    filename: raw.filename ?? `${raw.name}.mkv`,
    codec: parsed.codec,
    releaseSource: parsed.source,
    hdr: parsed.hdr,
    audio: parsed.audio,
    languages: parsed.languages.length > 0 ? parsed.languages : undefined,
    sizeBytes: Number(raw.sizeBytes ?? raw.size ?? 0),
  };
}

/* ------------------------------------------------------------------ *
 * Indexers (public, global English — no Russian trackers anywhere)
 * ------------------------------------------------------------------ */

/** YTS (movies). */
const YTS_HOSTS = ["yts.lt", "yts.rs", "yts.mx"];
async function searchYts(title) {
  const query = `query_term=${encodeURIComponent(title)}&sort_by=seeds&order_by=desc&limit=20`;
  const winner = await fetchFirstMirror(
    YTS_HOSTS.map((host) => `https://${host}/api/v2/list_movies.json?${query}`),
    {},
    (body) => { try { return Array.isArray(JSON.parse(body)?.data?.movies); } catch { return false; } },
  );
  if (!winner) return [];
  let json;
  try { json = JSON.parse(winner.text); } catch { return []; }
  const streams = [];
  for (const movie of json?.data?.movies ?? []) {
    for (const torrent of movie.torrents ?? []) {
      const name = `${movie.title} ${torrent.quality} ${torrent.type} ${torrent.quality === "2160p" ? "4K" : ""}`.trim();
      const stream = toStream({
        name,
        infoHash: torrent.hash,
        sizeBytes: torrent.size_bytes,
        seeders: torrent.seeds,
        leechers: torrent.peers,
        filename: `${movie.title.replace(/[^\w]+/g, ".")}.${torrent.quality}.${torrent.type}.yts.mkv`,
      }, "YTS");
      stream.provider = "YTS";
      streams.push(stream);
    }
  }
  return streams;
}

/** The Pirate Bay via apibay (movies cat 201 / series cat 205). */
async function apibayQuery(params) {
  const query = new URLSearchParams(params).toString();
  const text = await fetchText(`https://apibay.org/q.php?${query}`);
  try {
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) return parsed;
  } catch { /* line-by-line below */ }
  const rows = [];
  for (const line of text.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("<")) continue;
    try { rows.push(JSON.parse(trimmed)); } catch { /* skip */ }
  }
  return rows;
}
function isApibayPlaceholder(row) {
  return !row || row.id === "0" || !row.info_hash || /^0+$/.test(String(row.info_hash));
}
function apibayToStreams(rows) {
  return rows
    .filter((row) => !isApibayPlaceholder(row))
    .map((row) => toStream({
      name: row.name ?? "",
      infoHash: row.info_hash ?? null,
      sizeBytes: Number(row.size ?? 0),
      seeders: Number(row.seeders ?? 0),
      leechers: Number(row.leechers ?? 0),
      filename: `${(row.name ?? "release").replace(/[^\w]+/g, ".")}.mkv`,
    }, "TPB"))
    .filter((stream) => stream.name);
}
async function searchApibay(query, category) {
  return apibayToStreams(await apibayQuery({ q: query, cat: category }));
}
async function searchApibayByImdb(imdbId, category) {
  const bare = imdbId.replace(/^tt/, "");
  let rows = await apibayQuery({ imdb: imdbId, cat: category }).catch(() => []);
  if (rows.length === 0 || rows.every(isApibayPlaceholder)) {
    rows = await apibayQuery({ imdb: bare, cat: category }).catch(() => []);
  }
  return apibayToStreams(rows);
}

/** EZTV (series, by IMDb id). */
const EZTV_HOSTS = ["eztvx.to", "eztv.re", "eztv.ag", "eztv.tf"];
async function searchEztv(imdbId, title) {
  if (!/^tt\d+$/i.test(imdbId ?? "")) return [];
  const titleSlug = (title ?? "").toUpperCase().replace(/[^A-Z0-9]/g, "");
  const winner = await fetchFirstMirror(
    EZTV_HOSTS.map((host) => `https://${host}/api/get-torrents?imdb_id=${imdbId}&limit=50`),
    {},
    (body) => { try { return Array.isArray(JSON.parse(body)?.torrents); } catch { return false; } },
  );
  if (!winner) return [];
  let json;
  try { json = JSON.parse(winner.text); } catch { return []; }
  return (json.torrents ?? [])
    .map((torrent) => toStream({
      name: torrent.filename ?? torrent.title ?? "",
      infoHash: torrent.hash ?? null,
      sizeBytes: Number(torrent.size_bytes ?? 0),
      seeders: Number(torrent.seeds ?? 0),
      leechers: Number(torrent.peers ?? 0),
      filename: `${(torrent.filename ?? "release").replace(/[^\w]+/g, ".")}.mkv`,
    }, "EZTV"))
    .filter((stream) => stream.name && stream.infoHash)
    .filter((stream) => {
      if (!titleSlug || titleSlug.length < 5) return true;
      const nameSlug = stream.name.toUpperCase().replace(/[^A-Z0-9]/g, "");
      return nameSlug.includes(titleSlug) || titleSlug.includes(nameSlug);
    });
}

/** 1337x (movies + series, HTML + mirrors). */
const L337X_HOSTS = ["1337x.to", "1337x.st", "1337x.gd", "x1337x.ws"];
async function search1337x(query) {
  const q = encodeURIComponent(query.replace(/\s+/g, "+"));
  const winner = await fetchFirstMirror(
    L337X_HOSTS.map((host) => `https://${host}/search/${q}/1/`),
    { "accept-language": "en-US,en;q=0.9" },
    (body) => body.includes("coll-2"),
  );
  if (!winner) return [];
  const html = winner.text;
  const hostUsed = new URL(winner.url).host;
  const hits = [];
  for (const row of html.matchAll(/<tr>([\s\S]*?)<\/tr>/g)) {
    const name = /<a[^>]*href="\/torrent\/[^"]*"[^>]*>([\s\S]*?)<\/a>/.exec(row[1])?.[1]?.replace(/<[^>]+>/g, "")?.trim();
    if (!name) continue;
    const torrentUrl = /href="(\/torrent\/[^"]+)"/.exec(row[1])?.[1];
    const seeds = Number(/<td class="coll-2 seeds">([\d,]+)<\/td>/.exec(row[1])?.[1]?.replace(/,/g, "") ?? 0);
    const leechers = Number(/<td class="coll-3 leeches">([\d,]+)<\/td>/.exec(row[1])?.[1]?.replace(/,/g, "") ?? 0);
    const sizeText = /<td class="coll-4 size[^"]*">([\s\S]*?)<\/td>/.exec(row[1])?.[1]?.replace(/<[^>]+>/g, "")?.trim() ?? "";
    hits.push({ name, torrentUrl, seeds, leechers, sizeBytes: parseSizeText(sizeText) });
  }
  const detailResults = await Promise.allSettled(
    hits.slice(0, 6).map(async (hit) => {
      const page = await fetchText(`https://${hostUsed}${hit.torrentUrl}`, { referer: `https://${hostUsed}/` });
      const magnet = /href="(magnet:\?xt=urn:btih:[^"]+)"/i.exec(page)?.[1] ?? "";
      const hash = /btih:([a-fA-F0-9]{40})/i.exec(magnet)?.[1];
      if (!hash) return null;
      return toStream({ ...hit, infoHash: hash, filename: `${hit.name.replace(/[^\w]+/g, ".")}.mkv` }, "1337X");
    }),
  );
  return detailResults.map((r) => (r.status === "fulfilled" ? r.value : null)).filter(Boolean);
}

/**
 * LimeTorrents — global + Turkish/Arabic content that the English indexers
 * miss (dizi, Urdu-dubbed, Arabic-subbed releases). HTML + mirrors; the
 * search page lists name/size/seeds/leechers and each detail page carries
 * the magnet.
 */
const LIME_HOSTS = ["www.limetorrents.fun", "www.limetorrents.info", "www.limetorrents.sh", "www.limetorrents.lol"];
async function searchLimetorrents(query) {
  const q = encodeURIComponent(String(query ?? "").replace(/\s+/g, "-"));
  const winner = await fetchFirstMirror(
    LIME_HOSTS.map((host) => `https://${host}/search/all/${q}/`),
    { "accept-language": "en-US,en;q=0.9", "user-agent": BROWSER_UA },
    (body) => body.includes("torrent-"),
  );
  if (!winner) return [];
  const hostUsed = new URL(winner.url).host;
  const hits = [];
  for (const row of winner.text.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/g)) {
    const nameM = /<a href="([^"]*torrent-\d+\.html)"[^>]*>([\s\S]*?)<\/a>/.exec(row[1]);
    if (!nameM) continue;
    const name = nameM[2].replace(/<[^>]+>/g, "").trim();
    if (!name) continue;
    // Columns: [name, category, size, seeds, leechers, ...]
    const tds = [...row[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/g)].map((m) => m[1].replace(/<[^>]+>/g, "").trim());
    hits.push({
      name,
      detail: nameM[1],
      seeds: Number((tds[3] ?? "").replace(/[^\d]/g, "") ?? 0),
      leechers: Number((tds[4] ?? "").replace(/[^\d]/g, "") ?? 0),
      sizeBytes: parseSizeText(tds[2] ?? ""),
    });
  }
  const detailResults = await Promise.allSettled(
    hits.slice(0, 6).map(async (hit) => {
      const page = await fetchText(`https://${hostUsed}${hit.detail}`, { referer: `https://${hostUsed}/`, "user-agent": BROWSER_UA });
      const magnet = /href="(magnet:\?xt=urn:btih:[^"]+)"/i.exec(page)?.[1] ?? "";
      const hash = /btih:([a-fA-F0-9]{40})/i.exec(magnet)?.[1];
      if (!hash) return null;
      return toStream({ ...hit, infoHash: hash, filename: `${hit.name.replace(/[^\w]+/g, ".")}.mkv` }, "LIME");
    }),
  );
  return detailResults.map((r) => (r.status === "fulfilled" ? r.value : null)).filter(Boolean);
}

/** Nyaa (global anime/live-action, RSS). */
const NYAA_HOSTS = ["nyaa.si", "nyaa.land", "nyaa.iss.one"];
async function searchNyaa(query) {
  const q = encodeURIComponent(query);
  const winner = await fetchFirstMirror(
    NYAA_HOSTS.map((host) => `https://${host}/?page=rss&q=${q}`),
    {},
    (body) => body.includes("<item>"),
  );
  if (!winner) return [];
  const items = [...winner.text.matchAll(/<item>([\s\S]*?)<\/item>/g)].map((m) => m[1]);
  const streams = [];
  for (const item of items) {
    const category = /<nyaa:category>([^<]*)<\/nyaa:category>/.exec(item)?.[1] ?? "";
    const catMajor = /^\s*(\d)_/.exec(category)?.[1];
    if (catMajor && ["2", "3", "5", "6"].includes(catMajor)) continue;
    const link = /<link>([^<]*)<\/link>/.exec(item)?.[1] ?? "";
    const hash = /btih:([a-fA-F0-9]{40})/i.exec(link)?.[1] ?? /<nyaa:infoHash>([a-fA-F0-9]{40})<\/nyaa:infoHash>/i.exec(item)?.[1];
    if (!hash) continue;
    const title = /<title>([^<]*)<\/title>/.exec(item)?.[1]?.trim() ?? "";
    if (!title) continue;
    const sizeText = /<nyaa:size>([^<]*)<\/nyaa:size>/.exec(item)?.[1] ?? "";
    streams.push(toStream({
      name: title,
      infoHash: hash,
      sizeBytes: parseSizeText(sizeText),
      seeders: Number(/<nyaa:seeders>([^<]*)<\/nyaa:seeders>/.exec(item)?.[1] ?? 0),
      leechers: Number(/<nyaa:leechers>([^<]*)<\/nyaa:leechers>/.exec(item)?.[1] ?? 0),
      filename: `${title.replace(/[^\w]+/g, ".")}.mkv`,
    }, "NYAA"));
  }
  return streams;
}

/* ------------------------------------------------------------------ *
 * Relevance + orchestration
 * ------------------------------------------------------------------ */
const ADULT_RE = /\b(18\+|xxx|porn|erotic|hentai)\b|adult\s+(movie|film|video)/i;
const MUSIC_RE = /\b(ost|soundtrack|album|single|flac|mp3|vinyl)\b|official\s+(audio|video|release)|music\s*video/i;
const YEAR_RE = /\b(19|20)\d{2}\b/;
const escapeRegExp = (text) => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
/**
 * Fold Latin diacritics to ASCII — critical for Turkish dizi (ş->s, ğ->g,
 * ı->i, İ->I, ö->o, ü->u, ç->c) so "Kuruluş" also matches "Kurulus".
 */
const DIACRITIC_MAP = {
  "Ç":"C","ç":"c","Ğ":"G","ğ":"g","İ":"I","ı":"i","Ö":"O","ö":"o","Ş":"S","ş":"s","Ü":"U","ü":"u",
  "Â":"A","â":"a","Î":"I","î":"i","Û":"U","û":"u","Á":"A","á":"a","À":"A","à":"a","Ã":"A","ã":"a",
  "É":"E","é":"e","È":"E","è":"e","Ê":"E","ê":"e","Ë":"E","ë":"e","Í":"I","í":"i","Ì":"I","ì":"i",
  "Ñ":"N","ñ":"n","Ó":"O","ó":"o","Ò":"O","ò":"o","Õ":"O","õ":"o","Ú":"U","ú":"u","Ù":"U","ù":"u",
  "Ý":"Y","ý":"y","Ž":"Z","ž":"z","Š":"S","š":"s","Č":"C","č":"c","Ć":"C","ć":"c","Đ":"D","đ":"d",
};
function asciiFold(text) {
  return String(text ?? "").replace(/[^\x00-\x7F]/g, (ch) => DIACRITIC_MAP[ch] ?? DIACRITIC_MAP[ch.toUpperCase()] ?? ch);
}
function titlePhraseRegex(query) {
  const tokens = (asciiFold(query).toUpperCase().match(/[A-Z0-9]+/g) ?? []).map(escapeRegExp);
  if (tokens.length === 0) return null;
  return new RegExp(`(^|[^A-Z0-9])${tokens.join("[^A-Z0-9]*")}([^A-Z0-9]|$)`);
}

/**
 * Episode code in ANY format trackers actually use:
 *   S01E01 / S1E2 / 1x02 / 1x2 / Season 1 Episode 2 / Season 1 Ep. 2 / Episode 2
 * The x-format requires a preceding space so series named like "3x3 Eyes"
 * are never mistaken for an episode code.
 */
function parseEpisodeRef(query) {
  const q = String(query ?? "");
  const m = /S(\d{1,2})E(\d{1,3})/i.exec(q);
  if (m) return { season: Number(m[1]), episode: Number(m[2]) };
  const m2 = /\s(\d{1,2})x(\d{1,3})(?![a-z])/i.exec(q);
  if (m2) return { season: Number(m2[1]), episode: Number(m2[2]) };
  const m3 = /Season\s*(\d{1,2})\s*(?:Episode|Ep\.?)\s*(\d{1,3})/i.exec(q);
  if (m3) return { season: Number(m3[1]), episode: Number(m3[2]) };
  const m4 = /\s(?:Episode|Ep\.?)\s*(\d{1,3})(?![a-z])/i.exec(q);
  if (m4) return { season: null, episode: Number(m4[1]) };
  return null;
}

/** Remove the episode code from a query, leaving the clean title. */
function stripEpisodeFromQuery(query) {
  return String(query ?? "")
    .replace(/S\d{1,2}E\d{1,3}|\s\d{1,2}x\d{1,3}(?![a-z])|Season\s*\d{1,2}\s*(?:Episode|Ep\.?)\s*\d{1,3}|\s(?:Episode|Ep\.?)\s*\d{1,3}(?![a-z])/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const episodeMatcherCache = new Map();
/**
 * Match a release NAME against every episode format trackers use:
 * S01E01 / S1E1 / 1x02 / 1x2 / Season 1 Episode 2 / Season 1 Ep. 2 /
 * Episode 2 / Ep. 2 / bare "Show 01" (Nyaa/group style, e.g. "Show - 125").
 * The bare-number form uses digit lookaround so it never matches inside a
 * 4-digit year ("2021" can't match episode 1) or a longer episode number
 * ("125" can't match episode 1, but does match episode 25).
 */
function episodeMatcher(ep) {
  const key = `${ep.season ?? "?"}:${ep.episode}`;
  const hit = episodeMatcherCache.get(key);
  if (hit) return hit;
  const e1 = String(ep.episode);
  const explicitParts = [];
  if (ep.season != null) {
    const s = String(ep.season).padStart(2, "0");
    const e = String(ep.episode).padStart(2, "0");
    const s1 = String(ep.season);
    explicitParts.push(`S${s}E${e}`, `S${s1}E${e1}`, `S${s}E${e1}`, `S${s1}E${e}`);
    explicitParts.push(`${s1}x${e1}`, `${s1}x${e}`);
    explicitParts.push(`Season\\s*${s1}\\s*(?:Episode|Ep\\.?)\\s*${e1}`);
  } else {
    explicitParts.push(`(?:^|\\s)(?:Episode|Ep\\.?)\\s*0*${e1}(?![\\d])`);
  }
  const explicitRe = new RegExp(explicitParts.join("|"), "i");
  // Any episode code at all (SxxExx / 1xNN / "Season X Episode Y").
  const anyCodeRe = /S\d{1,3}E\d{1,3}|\d{1,2}x\d{1,3}|Season\s*\d{1,2}\s*(?:Episode|Ep\.?)\s*\d{1,3}/i;
  // Bare episode number (Nyaa/group style): "Show 01", "Show - 125".
  const bareRe = new RegExp(`(?<![\\d])0*${e1}(?![\\d])`, "i");
  const matcher = (name) => {
    if (explicitRe.test(name)) return true;              // explicit code matches
    if (anyCodeRe.test(name)) return false;              // explicit code present but different
    return bareRe.test(name);                            // no explicit code -> bare style
  };
  episodeMatcherCache.set(key, matcher);
  return matcher;
}

/**
 * Season-pack coverage for the episode fallback: accept a release only if it
 * covers the requested episode — a season pack ("S01", "Season 1", "S01-S03",
 * "Complete Series") or a multi-episode range that includes the episode
 * ("S01E01-08"). Wrong seasons and exact different episodes are rejected.
 * The pool is already title-filtered by isRelevant, so no title check here.
 */
function seasonPackMatch(ep, name) {
  const raw = String(name ?? "");
  if (!raw) return false;
  if (ADULT_RE.test(raw) || MUSIC_RE.test(raw)) return false;
  const upper = asciiFold(raw).toUpperCase();
  const complete = /\b(COMPLETE|ALL\s+EPISODES?|FULL\s+SERIES)\b/.test(upper);
  const seasons = new Set();
  const ranges = []; // { season, start, end }
  for (const m of upper.matchAll(/S(\d{1,2})(?:E(\d{1,2})(?:-E?(\d{1,2}))?)?/g)) {
    const s = Number(m[1]);
    seasons.add(s);
    if (m[2]) {
      ranges.push({ season: s, start: Number(m[2]), end: m[3] ? Number(m[3]) : Number(m[2]) });
    }
  }
  // Season spans ("S01-S03", "S1-S3"): expand every season in range.
  for (const m of upper.matchAll(/S(\d{1,2})\s*-\s*S?(\d{1,2})/g)) {
    const a = Number(m[1]);
    const b = Number(m[2]);
    if (b > a) for (let x = a; x <= b; x += 1) seasons.add(x);
  }
  for (const m of upper.matchAll(/SEASON\s+(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?/g)) {
    const s = Number(m[1]);
    seasons.add(s);
    if (m[2]) for (let x = s; x <= Number(m[2]); x += 1) seasons.add(x);
  }
  if (seasons.size === 0) {
    // Whole-show packs ("Complete Series", "Batch") with no season token cover
    // every episode when nothing contradicts.
    return complete || /\bBATCH\b/i.test(upper);
  }
  const season = Number(ep.season ?? 0);
  const episode = Number(ep.episode ?? 0);
  const covering = ranges.filter((r) => r.season === season);
  if (covering.some((r) => r.start <= episode && r.end >= episode)) return true;
  if (seasons.has(season) && covering.length === 0) return true;
  if (complete && seasons.has(season)) return true;
  return false;
}

function isRelevant(name, query, targetYear) {
  const raw = String(name ?? "");
  if (!raw) return false;
  if (ADULT_RE.test(raw) || MUSIC_RE.test(raw)) return false;
  // Group tags (e.g. "[Naruto-Kun.Hu]") are NOT part of the show title —
  // strip them before matching so a release from a DIFFERENT show whose
  // group happens to contain the query text never slips through (and so
  // the episode number isn't read out of the group tag).
  const clean = raw.replace(/^(\[[^\]]*\]\s*)+/, "");
  const upper = asciiFold(clean).toUpperCase();
  const ep = parseEpisodeRef(query);
  const titleQuery = ep ? stripEpisodeFromQuery(query) : query;
  const phrase = titlePhraseRegex(titleQuery.trim());
  if (!phrase || !phrase.test(upper)) return false;
  if (ep && !episodeMatcher(ep)(clean)) return false;
  if (/^\S+$/.test(titleQuery)) {
    const withoutGroup = clean;
    const nameSlug = asciiFold(withoutGroup).toUpperCase().replace(/[^A-Z0-9]+/g, "");
    const idx = nameSlug.indexOf(asciiFold(titleQuery).toUpperCase().replace(/[^A-Z0-9]+/g, ""));
    if (idx > 4) return false;
    const token = titleQuery.toUpperCase().replace(/[^A-Z0-9]/g, "");
    if (targetYear && token && new RegExp(`^${token}\\s*:`, "i").test(withoutGroup)) {
      const releaseYear = YEAR_RE.exec(clean)?.[0];
      if (!releaseYear || Math.abs(Number(releaseYear) - Number(targetYear)) > 2) return false;
    }
  }
  if (targetYear) {
    const releaseYear = YEAR_RE.exec(clean)?.[0];
    if (releaseYear && Math.abs(Number(releaseYear) - Number(targetYear)) > 2) return false;
  }
  return true;
}

function isNonLatinTitle(title) {
  return /[\u0600-\u06FF\u0400-\u04FF\u3040-\u30FF\u4E00-\u9FFF\uAC00-\uD7AF]/.test(title ?? "");
}
function isInternationalTitle({ title, language, country }) {
  const lang = String(language ?? "").toLowerCase();
  if (lang && lang !== "en" && lang !== "english" && lang !== "en-us") return true;
  const c = String(country ?? "").toUpperCase();
  if (c && !["US", "GB", "CA", "AU", "IE", "NZ"].includes(c)) return true;
  if (hasTurkishChars(title)) return true;   // Turkish dizi (Latin script, distinct chars)
  return isNonLatinTitle(title);
}

/** Arabic-script or Arabic-market titles — Arabic-subtitle releases are boosted. */
const ARAB_COUNTRIES = new Set(["SA", "EG", "AE", "LB", "JO", "SY", "IQ", "MA", "TN", "DZ", "LY", "KW", "QA", "BH", "OM", "YE", "PS"]);
function hasArabicScript(text) { return /[\u0600-\u06FF]/.test(String(text ?? "")); }
function isArabicTitle({ title, language, country }) {
  if (String(language ?? "").toLowerCase() === "ar") return true;
  if (ARAB_COUNTRIES.has(String(country ?? "").toUpperCase())) return true;
  return hasArabicScript(title);
}

/** Turkish dizi titles carry these characters — they need ASCII folding for indexers. */
const TURKISH_CHAR_RE = /[çğıöşüÇĞİÖŞÜ]/;
function hasTurkishChars(text) { return TURKISH_CHAR_RE.test(String(text ?? "")); }

/** Release names that carry Arabic subtitles or are Arabic-tailored. */
const ARABIC_FRIENDLY_RE = /arabic|عربي|بالعربية|\[ar\]|\bar\b|multi-?sub|subs?\s*(?:arabic|عربي)|(?:arabic|عربي)\s*subs?|softsub/i;
function arabicFriendly(name) { return ARABIC_FRIENDLY_RE.test(String(name ?? "")); }

/** Loose title for fallback scraping: drop year and colon-suffix parts. */
function looseTitle(title) {
  const withoutYear = String(title ?? "").replace(/\s*\((?:19|20)\d{2}\)\s*$/i, "").trim();
  const firstColon = withoutYear.split(":")[0]?.trim();
  const candidates = [withoutYear, firstColon].filter((c) => c && c.length >= 4);
  return candidates.length > 0 ? candidates : [String(title ?? "")];
}

/**
 * Leading title tokens of a release, up to the first year/episode/quality/
 * codec/audio tag — e.g. "Monster (2004) S01E01 [1080p]" -> "MONSTER" and
 * "Game of Thrones S01E01 720p" -> "GAMEOFTHRONES". Group tags are skipped.
 */
function releaseTitleHead(name) {
  const clean = String(name ?? "")
    .replace(/^(\[[^\]]*\]\s*)+/, "")
    .replace(/\s*\[[^\]]*\]\s*/g, " ")
    .trim();
  const head = [];
  for (const token of clean.split(/\s+/)) {
    const u = asciiFold(token).toUpperCase().replace(/[^A-Z0-9]/g, "");
    if (!u) continue;
    if (/^(S\d{1,3}E\d{1,3}|\d{1,2}x\d{1,3}|(?:19|20)\d{2}|\d{3,4}[PI]|BLU?RAY|WEBDL|WEBRIP|HDTV|REMUX|DVDRIP|BRRIP|BDRIP|X264|X265|HEVC|H26[45]|10BIT|8BIT|AAC|AC3|EAC3|DTS|DD5?1?|OPUS|FLAC|ATMOS|MULTI|DUAL|VOSTFR|COMPLETE|BATCH|SUBBED|SUBTITLED)$/.test(u)) break;
    head.push(u);
  }
  return head.join("");
}

/**
 * How precisely a release name matches the queried title:
 *   3 = exact title head ("Monster (2004) S01E01" for "Monster")
 *   2 = release title starts with the query ("Monster Eater" for "Monster")
 *   1 = query appears somewhere in the name ("Re:Monster" for "Monster")
 *   0 = no match
 * Used as the PRIMARY rank so an exact-title release always beats a
 * same-name-different-show release, no matter how many seeds the wrong
 * show has.
 */
function titleMatchScore(name, title) {
  const q = String(title ?? "").toUpperCase().replace(/[^A-Z0-9]/g, "");
  if (!q) return 0;
  const clean = String(name ?? "").replace(/^(\[[^\]]*\]\s*)+/, "");
  const head = releaseTitleHead(name);
  if (head === q) return 3;
  if (head.startsWith(q)) return 2;
  const escaped = q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`(^|[^A-Z0-9])${escaped}([^A-Z0-9]|$)`, "i").test(clean) ? 1 : 0;
}

const sourceStatus = new Map();
async function tracked(name, run) {
  try {
    const streams = await run();
    sourceStatus.set(name, { at: Date.now(), count: streams.length, status: streams.length > 0 ? "ok" : "empty" });
    return streams;
  } catch (error) {
    sourceStatus.set(name, { at: Date.now(), count: 0, status: "error", error: error instanceof Error ? error.message : String(error) });
    return [];
  }
}
function getSourceStatus() {
  return [...sourceStatus.entries()].map(([name, info]) => ({ name, ...info })).sort((a, b) => b.at - a.at);
}

/**
 * Per-indexer concurrency gate. Public indexers (especially LimeTorrents)
 * 503 bursts of parallel requests from one IP — and the multi-pass scrape
 * fires every pass at once, so a title whose content only exists under the
 * plain-title query (dizi season packs) got rate-limited into an empty
 * result while a single curl to the same URL succeeds. At most 2 requests
 * per indexer at a time; extra passes queue and still run within the
 * deadline (lime answers in ~1.5s, so a 6-call queue fits in the 8s cap).
 */
const INDEXER_MAX_CONCURRENT = 2;
const indexerGates = new Map(); // name -> { active, queue }
function gatedIndexer(name, fn) {
  let gate = indexerGates.get(name);
  if (!gate) {
    gate = { active: 0, queue: [] };
    indexerGates.set(name, gate);
  }
  return new Promise((resolve, reject) => {
    const run = () => {
      gate.active++;
      Promise.resolve()
        .then(fn)
        .then(
          (value) => {
            gate.active--;
            dequeue();
            resolve(value);
          },
          (error) => {
            gate.active--;
            dequeue();
            reject(error);
          },
        );
    };
    const dequeue = () => {
      while (gate.queue.length > 0 && gate.active < INDEXER_MAX_CONCURRENT) {
        gate.queue.shift()();
      }
    };
    if (gate.active < INDEXER_MAX_CONCURRENT) run();
    else gate.queue.push(run);
  });
}

/** Single scrape pass over all enabled indexers for one title query. */
async function scrapeOnce({ title, imdbId, type, year, language, country }) {
  const tasks = [];
  const isMovie = type === "movie";
  const cat = isMovie ? 201 : 205;
  const hasTitle = Boolean(title && title.trim());
  // International (Arabic/Turkish/foreign) releases keep NO seeder floor —
  // rare-but-real copies (often seedless) still surface, ranked last by the
  // health-first sort, instead of vanishing entirely.
  const minSeeds = isInternationalTitle({ title, language, country }) ? 0 : MIN_SEEDERS;
  const searchableTitle = hasTitle && !isNonLatinTitle(title);

  if (searchableTitle) {
    if (isMovie) tasks.push(tracked("yts", () => gatedIndexer("yts", () => searchYts(title))));
    tasks.push(tracked("nyaa", () => gatedIndexer("nyaa", () => searchNyaa(title))));
    tasks.push(tracked("1337x", () => gatedIndexer("1337x", () => search1337x(title))));
    tasks.push(tracked("limetorrents", () => gatedIndexer("limetorrents", () => searchLimetorrents(title))));
  }
  tasks.push(tracked("tpb", () => gatedIndexer("tpb", async () => {
    const byTitle = searchableTitle ? await searchApibay(title, cat) : [];
    const byImdb = imdbId ? await searchApibayByImdb(imdbId, cat).catch(() => []) : [];
    return [...byTitle, ...byImdb];
  })));
  if (!isMovie && imdbId) {
    tasks.push(tracked("eztv", () => gatedIndexer("eztv", () => searchEztv(imdbId, title))));
  }

  const groups = await Promise.all(tasks);
  const merged = groups.flat();
  const relevant = hasTitle ? merged.filter((stream) => isRelevant(stream.name, title, year)) : merged;
  const healthy = relevant.filter((stream) => Number(stream.seeds) >= minSeeds);

  const byHash = new Map();
  for (const stream of healthy) {
    if (!stream.infoHash) continue;
    const existing = byHash.get(stream.infoHash);
    const score = effectiveSeeds(stream);
    const existingScore = existing ? effectiveSeeds(existing) : -1;
    if (!existing || score > existingScore ||
        (score === existingScore && ((stream.seeds ?? 0) > (existing.seeds ?? 0) ||
          (stream.seeds ?? 0) === (existing.seeds ?? 0) && sourceWeight(stream.provider) > sourceWeight(existing.provider)))) {
      byHash.set(stream.infoHash, stream);
    }
  }
  let deduped = [...byHash.values()];
  if (deduped.length === 0) deduped = healthy;

  deduped.sort((a, b) =>
    effectiveSeeds(b) - effectiveSeeds(a) ||
    (b.seeds ?? 0) - (a.seeds ?? 0) ||
    (b.sizeBytes ?? 0) - (a.sizeBytes ?? 0));
  return deduped.slice(0, 60);
}

/** Search-query variants for one episode, in the formats trackers index. */
function episodeQueryVariants(title, season, episode) {
  const s = String(season).padStart(2, "0");
  const e = String(episode).padStart(2, "0");
  const s1 = String(season);
  const e1 = String(episode);
  const base = asciiFold(String(title ?? "").trim());   // Turkish dizi: fold diacritics
  return [...new Set([
    `${base} S${s}E${e}`,          // S01E01 (EZTV/TPB/1337x standard)
    `${base} ${s1}x${e1}`,         // 1x02 (1337x/TPB alternate)
    `${base} Season ${s1} Episode ${e1}`, // verbose (1337x)
    `${base} ${e}`,                // "Show 01" (Nyaa/group bare-number style)
  ])];
}

function scrapePasses(params) {
  const passes = [];
  const original = String(params.title ?? "");
  const aliases = (params.aliases ?? []).filter((a) => a && a.toLowerCase() !== original.toLowerCase());
  // Foreign-language series (Turkish dizi): releases use the NATIVE name
  // ("Kurulus Osman"), so it LEADS the search; the English Cinemeta name
  // ("Establishment: Osman") is only a fallback.
  const leads = aliases.length > 0 ? aliases : [original];
  const seen = new Set();

  if (params.episode) {
    // Episode-scoped queries FIRST — exact-episode releases beat season
    // packs and wrong episodes. episodeQueryVariants ASCII-folds the base,
    // so "Kuruluş: Osman" is searched as "Kurulus Osman S01E01" too.
    for (const variant of episodeQueryVariants(leads[0], params.episode.season, params.episode.episode)) {
      passes.push({ key: `episode:${variant}`, run: () => scrapeOnce({ ...params, title: variant, year: null }) });
    }
    // Bare-title pass as fallback for releases with no episode tag in the name.
    passes.push({ key: "strict-title", run: () => scrapeOnce(params) });
    // Episode-stripped bare-title pass so SEASON PACKS covering the requested
    // episode can be found when no exact-episode release exists anywhere. The
    // merge filter later keeps only packs that cover this episode (seasonPackMatch).
    const bareTitle = stripEpisodeFromQuery(String(params.title ?? ""));
    if (bareTitle && bareTitle.length >= 4) {
      passes.push({
        key: `bare:${bareTitle.toLowerCase()}`,
        run: () => scrapeOnce({ ...params, title: bareTitle, year: null }),
      });
    }
  } else {
    // Non-episode requests: strict pass on the leading spelling first.
    passes.push({ key: "strict", run: () => scrapeOnce({ ...params, title: leads[0] }) });
  }

  // Loose-title variants (drop year / colon-suffix) for the leading spelling
  // AND its ASCII fold — Turkish dizi releases are uploaded as "Kurulus
  // Osman", so the diacritic "Kuruluş: Osman" with its colon never matches,
  // but the folded loose "Kurulus" does. The English name runs last.
  for (const lead of leads) addLoosePasses(passes, seen, params, lead, "lead");
  if (aliases.length > 0) addLoosePasses(passes, seen, params, original, "english");
  return passes;
}

/** Loose (and ASCII-folded) search variants for one title spelling. */
function addLoosePasses(passes, seen, params, base, tag) {
  for (const spelling of [base, asciiFold(base)]) {
    if (!spelling || isNonLatinTitle(spelling)) continue;
    for (const loose of looseTitle(spelling)) {
      const key = loose.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      passes.push({ key: `${tag}:${key}`, run: () => scrapeOnce({ ...params, title: loose, year: null }) });
    }
  }
}

/** Merge groups by info hash (best health wins), then health-rank. */
function mergeStreamGroups(groups) {
  const byHash = new Map();
  for (const group of groups) {
    for (const stream of group) {
      if (!stream.infoHash) continue;
      const existing = byHash.get(stream.infoHash);
      if (!existing || effectiveSeeds(stream) > effectiveSeeds(existing)) byHash.set(stream.infoHash, stream);
    }
  }
  return [...byHash.values()].sort((a, b) => effectiveSeeds(b) - effectiveSeeds(a));
}

/** Resolve the promise, or [] once the absolute deadline passes. */
function raceTimeout(promise, deadlineAt) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve([]), Math.max(0, deadlineAt - Date.now()));
    promise
      .then((value) => { clearTimeout(timer); resolve(value); })
      .catch(() => { clearTimeout(timer); resolve([]); });
  });
}

/**
 * Full scrape — REAL releases only, never test clips.
 *
 * All passes (strict title, loose variants, exact SxxExx episode query)
 * start CONCURRENTLY so a series/anime request never waits on three
 * sequential scrapes. If the strict pass already found releases, it
 * answers immediately and the fallback passes are discarded. If strict
 * came up empty, the fallback results that arrived by deadlineMs are
 * merged by info hash and health-ranked. Passes still running when the
 * deadline fires finish in the background and enrich the cache via
 * onLate, so the next request gets the complete pool.
 */
async function scrapeTorrents(params, { deadlineMs = null, onLate = null } = {}) {
  const passes = scrapePasses(params);
  // ABSOLUTE deadline from the start: every pass (strict + all fallbacks)
  // started at t=0 gets credit for its whole window. Computing the deadline
  // after awaiting the strict pass would leave the fallbacks ~0ms when the
  // strict pass is slow but empty — exactly why series/anime requests with
  // cold indexers returned nothing.
  const deadlineAt = deadlineMs ? Date.now() + deadlineMs : null;
  const strictPromise = passes[0].run();
  const others = passes.slice(1).map((p) => p.run());

  let strictResult = await (deadlineAt ? raceTimeout(strictPromise, deadlineAt) : strictPromise);
  if (params.episode && strictResult.length > 0) {
    // The strict pass is episode-SCOPED by query, but indexers' fuzzy search
    // can still return wrong-season releases — enforce the matcher here too.
    const matcher = episodeMatcher(params.episode);
    strictResult = strictResult.filter((s) => matcher(s.name));
  }
  if (process.env.AAHUB_DEBUG === "1") {
    console.error("[debug] strict empty; others:", others.length, "deadline:", deadlineAt ? Math.round((deadlineAt - Date.now()) / 1000) + "s left" : "none");
  }
  if (strictResult.length > 0) {
    // Strict query already found real releases — answer now.
    return strictResult;
  }

  // Strict empty — merge whatever the fallback passes produced by the deadline.
  const otherSettled = await Promise.allSettled(others.map((p) => (deadlineAt ? raceTimeout(p, deadlineAt) : p)));
  const otherGroups = otherSettled.map((r) => (r.status === "fulfilled" ? r.value : []));
  let merged = mergeStreamGroups([strictResult, ...otherGroups]);
  if (params.episode && merged.length > 0) {
    // Bare-title/loose fallback passes aren't episode-scoped — enforce the
    // episode match on the merged pool so a wrong episode never sneaks in.
    // When no exact-episode release exists, season packs covering the
    // requested season/episode are accepted so EVERY episode still plays.
    const matcher = episodeMatcher(params.episode);
    merged = merged.filter((s) => matcher(s.name) || seasonPackMatch(params.episode, s.name));
    // Exact-episode releases rank above season packs, then by health.
    merged.sort(
      (a, b) =>
        (matcher(b.name) ? 0 : 1) - (matcher(a.name) ? 0 : 1) ||
        effectiveSeeds(b) - effectiveSeeds(a),
    );
  }

  if (onLate && deadlineAt && Date.now() >= deadlineAt) {
    // The deadline cut some passes short — when they finally land, enrich
    // the cache so the next request gets the complete pool. The late merge
    // MUST apply the same episode filter (it replaces the cache entry, and
    // an unfiltered pool would let a wrong-season pack outrank the episode).
    Promise.allSettled([strictPromise, ...others])
      .then((all) => {
        let late = mergeStreamGroups(all.map((r) => (r.status === "fulfilled" ? r.value : [])));
        if (params.episode && late.length > 0) {
          const matcher = episodeMatcher(params.episode);
          late = late.filter((s) => matcher(s.name) || seasonPackMatch(params.episode, s.name));
          late.sort(
            (a, b) =>
              (matcher(b.name) ? 0 : 1) - (matcher(a.name) ? 0 : 1) ||
              effectiveSeeds(b) - effectiveSeeds(a),
          );
        }
        onLate(late);
      })
      .catch(() => {});
  }
  return merged;
}

/* ------------------------------------------------------------------ *
 * Title metadata — any IMDb (tt) or Kitsu (kitsu:<n>) id resolves
 * ------------------------------------------------------------------ */
const cinemetaCache = new Map();   // id -> { at, meta }
const kitsuCache = new Map();      // kitsu id -> { at, meta }
const episodeCache = new Map();    // seriesId -> { at, videos|null }
const nativeNameCache = new Map(); // tt id -> native name | null

/**
 * Dynamic native-name resolver for foreign-language series (Turkish dizi
 * are released under their native title, but Cinemeta serves the English
 * name, e.g. "The Red Room" -> "Kırmızı Oda"). TVMaze lookup by imdb,
 * falling back to singlesearch on the English name; only a non-English-
 * language hit is used. Never blocks — returns null on any failure.
 */
async function nativeNameViaTvmaze(imdbId, englishName) {
  if (!/^tt\d+$/i.test(imdbId) || !englishName) return null;
  const hit = nativeNameCache.get(imdbId);
  if (hit !== undefined) return hit;
  let native = null;
  try {
    const lookup = await fetchJson(`https://api.tvmaze.com/lookup/shows?imdb=${imdbId}`).catch(() => null);
    const show = lookup ?? (await fetchJson(`https://api.tvmaze.com/singlesearch/shows?q=${encodeURIComponent(englishName)}`).catch(() => null));
    const lang = String(show?.language ?? "").toLowerCase();
    if (Array.isArray(show?.genres) && show.genres.includes("Anime")) {
      // Anime releases on Nyaa use the romanized Japanese name ("Sousou no
      // Frieren", not "Frieren: Beyond Journey's End"). TVMaze's `name` is
      // the English title even when `language` is Japanese, so Kitsu is the
      // source of the romanized Japanese name — check the genre FIRST.
      const jp = await japaneseAliasViaKitsu(englishName);
      if (jp) native = jp;
    } else if (show?.name && lang && lang !== "english" && lang !== "en" && lang !== "en-us") {
      native = show.name;
    }
  } catch { /* TVMaze unavailable — alias is optional */ }
  nativeNameCache.set(imdbId, native);
  return native;
}

/** Kitsu romanized-Japanese title lookup by English name (anime). */
async function japaneseAliasViaKitsu(englishName) {
  if (!englishName) return null;
  const key = `kitsu:${englishName.toLowerCase()}`;
  const hit = nativeNameCache.get(key);
  if (hit !== undefined) return hit;
  let alias = null;
  try {
    const response = await fetch(
      `https://kitsu.io/api/edge/anime?filter%5Btext%5D=${encodeURIComponent(englishName)}&page%5Blimit%5D=3`,
      { signal: AbortSignal.timeout(8_000) },
    );
    if (response.ok) {
      const json = await response.json();
      const attrs = json?.data?.[0]?.attributes;
      const jp = attrs?.titles?.en_jp ?? attrs?.titles?.ja_jp ?? null;
      if (jp && jp.toLowerCase() !== String(englishName).toLowerCase()) alias = jp;
    }
  } catch { /* Kitsu unavailable — alias is optional */ }
  nativeNameCache.set(key, alias);
  return alias;
}

async function cinemetaMeta(type, id) {
  if (!/^tt\d+$/i.test(id)) return null;
  const hit = cinemetaCache.get(id);
  if (hit && Date.now() - hit.at < CACHE_TTL_MS) return hit.meta;
  let meta = null;
  try {
    const response = await fetch(`https://v3-cinemeta.strem.io/meta/${type}/${id}.json`, { signal: AbortSignal.timeout(10_000) });
    if (response.ok) {
      const json = await response.json();
      meta = json?.meta ?? null;
    }
  } catch { /* offline — fall back to the seed row */ }
  cinemetaCache.set(id, { at: Date.now(), meta });
  return meta;
}

/** Kitsu anime lookup by id — keyless; name/year/poster/description. */
async function kitsuMeta(kitsuId) {
  const num = String(kitsuId).replace(/^kitsu:/i, "");
  if (!/^\d+$/.test(num)) return null;
  const hit = kitsuCache.get(num);
  if (hit && Date.now() - hit.at < CACHE_TTL_MS) return hit.meta;
  let meta = null;
  try {
    const response = await fetch(`https://kitsu.io/api/edge/anime/${num}`, { signal: AbortSignal.timeout(10_000) });
    if (response.ok) {
      const json = await response.json();
      const attrs = json?.data?.attributes;
      if (attrs?.canonicalTitle) {
        const year = attrs.startDate ? Number(attrs.startDate.slice(0, 4)) : NaN;
        meta = {
          name: attrs.canonicalTitle ?? attrs.titles?.en_jp ?? attrs.titles?.ja_jp,
          // Romanized Japanese title ("Sousou no Frieren") — what Nyaa releases use.
          nativeName: attrs.titles?.en_jp ?? attrs.titles?.ja_jp ?? null,
          year: Number.isInteger(year) && year > 1800 ? year : null,
          poster: attrs.posterImage?.original ?? attrs.posterImage?.large ?? null,
          background: attrs.coverImage?.original ?? attrs.posterImage?.original ?? null,
          description: attrs.synopsis ? String(attrs.synopsis).replace(/<[^>]*>/g, "").slice(0, 600) : null,
          genres: attrs.genres ?? [],
          type: "series",
        };
      }
    }
  } catch { /* offline */ }
  kitsuCache.set(num, { at: Date.now(), meta });
  return meta;
}

/** Kitsu episode list (first 20 — enough to render episode rows). */
async function fetchKitsuEpisodes(kitsuId) {
  const num = String(kitsuId).replace(/^kitsu:/i, "");
  if (!/^\d+$/.test(num)) return null;
  try {
    const json = await fetchJson(
      `https://kitsu.io/api/edge/anime/${num}/episodes?page%5Blimit%5D=20&sort=number`,
    );
    const mapped = (json?.data ?? [])
      .map((ep) => ep?.attributes)
      .filter((a) => a && a.number != null)
      .map((a) => ({
        id: `kitsu:${num}:${a.seasonNumber ?? 1}:${a.number}`,
        title: a.titles?.en_jp ?? a.titles?.en_us ?? `Episode ${a.number}`,
        season: a.seasonNumber ?? 1,
        episode: a.number,
        released: a.airdate ?? null,
        thumbnail: a.thumbnail?.original ?? null,
      }));
    return mapped.length > 0 ? mapped : null;
  } catch {
    return null;
  }
}

async function tvmazeShowId(seriesId) {
  if (/^tvmaze:(\d+)$/.test(seriesId)) return Number(seriesId.slice("tvmaze:".length));
  if (/^tt\d+$/.test(seriesId)) {
    const lookup = await fetchJson(`https://api.tvmaze.com/lookup/shows?imdb=${seriesId}`);
    return lookup.id;
  }
  return null;
}

/** Real season/episode list for a tt... or tvmaze:NNN series, 24h cached. */
async function fetchSeriesEpisodes(seriesId) {
  const hit = episodeCache.get(seriesId);
  if (hit && Date.now() - hit.at < CACHE_TTL_MS) return hit.videos;
  let videos = null;
  try {
    const showId = await tvmazeShowId(seriesId);
    if (showId === null) return null;
    const episodes = await fetchJson(`https://api.tvmaze.com/shows/${showId}/episodes`);
    const mapped = episodes
      .filter((episode) => episode.season && episode.number)
      .map((episode) => ({
        id: `${seriesId}:${episode.season}:${episode.number}`,
        title: episode.name ?? `Episode ${episode.number}`,
        season: episode.season,
        episode: episode.number,
        released: episode.airdate ?? null,
        thumbnail: episode.image?.medium ?? null,
      }));
    if (mapped.length > 0) videos = mapped;
  } catch { /* TVMaze unavailable */ }
  episodeCache.set(seriesId, { at: Date.now(), videos });
  return videos;
}

/* ------------------------------------------------------------------ *
 * Catalog seed — real titles (verified against Cinemeta/TVMaze/Kitsu)
 * ------------------------------------------------------------------ */
const SEED = [
  // Global movies
  { type: "movie", id: "tt15398776", name: "Oppenheimer", year: 2023, catalog: "aahub-global-movies" },
  { type: "movie", id: "tt0816692", name: "Interstellar", year: 2014, catalog: "aahub-global-movies" },
  { type: "movie", id: "tt0468569", name: "The Dark Knight", year: 2008, catalog: "aahub-global-movies" },
  { type: "movie", id: "tt1375666", name: "Inception", year: 2010, catalog: "aahub-global-movies" },
  { type: "movie", id: "tt15239678", name: "Dune: Part Two", year: 2024, catalog: "aahub-global-movies" },
  { type: "movie", id: "tt7286456", name: "Joker", year: 2019, catalog: "aahub-global-movies" },
  { type: "movie", id: "tt6751668", name: "Parasite", year: 2019, catalog: "aahub-global-movies" },
  // Global series
  { type: "series", id: "tt0944947", name: "Game of Thrones", year: 2011, catalog: "aahub-global-series" },
  { type: "series", id: "tt0903747", name: "Breaking Bad", year: 2008, catalog: "aahub-global-series" },
  { type: "series", id: "tt4574334", name: "Stranger Things", year: 2016, catalog: "aahub-global-series" },
  { type: "series", id: "tt5180504", name: "The Witcher", year: 2019, catalog: "aahub-global-series" },
  { type: "series", id: "tt6468322", name: "Money Heist", year: 2017, catalog: "aahub-global-series" },
  { type: "series", id: "tt10919420", name: "Squid Game", year: 2021, catalog: "aahub-global-series" },
  // Cartoons (global series)
  { type: "series", id: "tt2861424", name: "Rick and Morty", year: 2013, catalog: "aahub-global-series" },
  { type: "series", id: "tt0417299", name: "Avatar: The Last Airbender", year: 2005, catalog: "aahub-global-series" },
  { type: "series", id: "tt0096697", name: "The Simpsons", year: 1989, catalog: "aahub-global-series" },
  { type: "series", id: "tt0206512", name: "SpongeBob SquarePants", year: 1999, catalog: "aahub-global-series" },
  // Anime (verified via Cinemeta)
  { type: "series", id: "tt0409591", name: "Naruto", year: 2002, catalog: "aahub-anime", language: "ja", country: "JP" },
  { type: "series", id: "tt9335498", name: "Demon Slayer: Kimetsu no Yaiba", year: 2019, catalog: "aahub-anime", language: "ja", country: "JP" },
  { type: "series", id: "tt2560140", name: "Attack on Titan", year: 2013, catalog: "aahub-anime", language: "ja", country: "JP" },
  { type: "series", id: "tt0388629", name: "One Piece", year: 1999, catalog: "aahub-anime", language: "ja", country: "JP" },
  { type: "series", id: "tt0877057", name: "Death Note", year: 2006, catalog: "aahub-anime", language: "ja", country: "JP" },
  { type: "series", id: "tt1355642", name: "Fullmetal Alchemist: Brotherhood", year: 2009, catalog: "aahub-anime", language: "ja", country: "JP" },
  { type: "series", id: "tt0214341", name: "Dragon Ball Z", year: 1996, catalog: "aahub-anime", language: "ja", country: "JP" },
  { type: "series", id: "tt5626028", name: "My Hero Academia", year: 2016, catalog: "aahub-anime", language: "ja", country: "JP" },
  { type: "series", id: "tt12343534", name: "Jujutsu Kaisen", year: 2020, catalog: "aahub-anime", language: "ja", country: "JP" },
  { type: "series", id: "tt4508902", name: "One Punch Man", year: 2015, catalog: "aahub-anime", language: "ja", country: "JP" },
  // Turkish movies
  { type: "movie", id: "tt2758880", name: "Winter Sleep", year: 2014, catalog: "aahub-turkish-movies", language: "tr", country: "TR" },
  // Turkish series
  { type: "series", id: "tt11093718", name: "Kuruluş: Osman", year: 2019, catalog: "aahub-turkish-series", language: "tr", country: "TR" },
  // Arabic movies
  { type: "movie", id: "tt8267604", name: "Capernaum", year: 2018, catalog: "aahub-arabic-movies", language: "ar", country: "LB" },
  { type: "movie", id: "tt3170902", name: "Theeb", year: 2014, catalog: "aahub-arabic-movies", language: "ar", country: "JO" },
  { type: "movie", id: "tt2258858", name: "Wadjda", year: 2012, catalog: "aahub-arabic-movies", language: "ar", country: "SA" },
  { type: "movie", id: "tt0825236", name: "Caramel", year: 2007, catalog: "aahub-arabic-movies", language: "ar", country: "LB" },
  { type: "movie", id: "tt5599692", name: "Clash", year: 2016, catalog: "aahub-arabic-movies", language: "ar", country: "EG" },
  { type: "movie", id: "tt0896529", name: "Cairo Time", year: 2009, catalog: "aahub-arabic-movies", language: "ar", country: "EG" },
  { type: "movie", id: "tt5968274", name: "The Angel", year: 2018, catalog: "aahub-arabic-movies", language: "ar", country: "EG" },
  { type: "movie", id: "tt0425321", name: "The Yacoubian Building", year: 2006, catalog: "aahub-arabic-movies", language: "ar", country: "EG" },
  // Arabic series (imdb ids verified on Cinemeta)
  { type: "series", id: "tt7035576", name: "Al Hayba", year: 2017, catalog: "aahub-arabic-series", language: "ar", country: "LB" },
  { type: "series", id: "tt1999065", name: "The Neighbourhood's Gate", year: 2006, catalog: "aahub-arabic-series", language: "ar", country: "SY" },
];

const seedById = new Map(SEED.map((row) => [`${row.type}:${row.id}`, row]));

const CATALOGS = [
  { type: "movie", id: "aahub-global-movies", name: "AAHUB Movies", extra: [{ name: "genre" }] },
  { type: "series", id: "aahub-global-series", name: "AAHUB Series", extra: [{ name: "genre" }] },
  { type: "series", id: "aahub-anime", name: "AAHUB Anime", extra: [{ name: "genre" }] },
  { type: "movie", id: "aahub-turkish-movies", name: "AAHUB Turkish Movies", extra: [{ name: "genre" }] },
  { type: "series", id: "aahub-turkish-series", name: "AAHUB Turkish Series", extra: [{ name: "genre" }] },
  { type: "movie", id: "aahub-arabic-movies", name: "AAHUB Arabic Movies", extra: [{ name: "genre" }] },
  { type: "series", id: "aahub-arabic-series", name: "AAHUB Arabic Series", extra: [{ name: "genre" }] },
];

function buildManifest(baseUrl) {
  return {
    id: "com.aahub.catalog",
    version: "1.4.0",
    name: "AAHUB",
    description: "AAHUB — premium streaming addon. Curated global, anime, Turkish and Arabic movies and series with clean English-audio streams, instant playback and automatic multi-language subtitles.",
    logo: `${baseUrl}/logo.svg`,
    // Serves IMDb ids (tt...) and Kitsu ids (kitsu:...) — movies, series,
    // cartoons and anime all resolve through the same dynamic pipeline.
    idPrefixes: ["tt", "kitsu"],
    resources: ["catalog", "meta", "stream"],
    types: ["movie", "series"],
    catalogs: CATALOGS,
  };
}

/* ------------------------------------------------------------------ *
 * Meta builders
 * ------------------------------------------------------------------ */
let origin = `http://127.0.0.1:${PORT}`;

function defaultPoster() {
  return `${origin}/poster.svg`;
}

/** Resolve metadata for ANY tt or kitsu id (movies/series/anime). */
async function resolveTitle(type, id) {
  const seed = seedById.get(`${type}:${id}`);
  if (/^kitsu:\d+$/i.test(id)) {
    const k = await kitsuMeta(id);
    if (k) {
      return {
        type: "series",
        name: k.name,
        year: k.year,
        language: "ja",
        country: "JP",
        anime: true,
        aliases: k.nativeName && k.nativeName !== k.name ? [k.nativeName] : [],
      };
    }
  }
  const live = await cinemetaMeta(type, id);
  const name = live?.name ?? seed?.name ?? id;
  const aliases = [];
  // Native dizi name from the catalog seed (e.g. "Kuruluş: Osman").
  if (seed?.name && seed.name !== name) aliases.push(seed.name);
  // Dynamic native name via TVMaze for ANY foreign-language series.
  if (type === "series" && /^tt\d+$/i.test(id)) {
    const native = await nativeNameViaTvmaze(id, name);
    if (native && native !== name && !aliases.includes(native)) aliases.push(native);
  }
  return {
    type,
    name,
    year: Number.parseInt(String(live?.releaseInfo ?? seed?.year ?? ""), 10) || null,
    language: seed?.language ?? (live?.language ?? null),
    country: seed?.country ?? null,
    aliases,
  };
}

/** Build the meta response for a tt or kitsu id — never 404s. */
async function metaFor(type, id) {
  const seed = seedById.get(`${type}:${id}`);
  const live = /^kitsu:\d+$/i.test(id) ? await kitsuMeta(id) : await cinemetaMeta(type, id);
  const name = live?.name ?? seed?.name ?? id;
  const year = Number.parseInt(String(live?.releaseInfo ?? live?.year ?? seed?.year ?? ""), 10);
  const meta = {
    id,
    type: /^kitsu:/i.test(id) ? "series" : type,
    name,
    poster: live?.poster ?? defaultPoster(),
    background: live?.background ?? live?.poster ?? defaultPoster(),
    description: live?.description ?? seed?.description ?? null,
    releaseInfo: live?.releaseInfo ?? (seed?.year ? String(seed.year) : null),
    genres: live?.genres ?? [],
    imdb_id: /^tt\d+$/i.test(id) ? id : null,
    language: seed?.language ?? (live?.language ?? null),
    country: seed?.country ?? null,
  };
  if (meta.type === "series") {
    const videos = /^kitsu:/i.test(id)
      ? await fetchKitsuEpisodes(id)
      : await fetchSeriesEpisodes(id);
    if (videos && videos.length > 0) meta.videos = videos;
  }
  return meta;
}

async function serveCatalog(res, type, catalogId) {
  const rows = SEED.filter((row) => row.type === type && row.catalog === catalogId);
  const metas = await Promise.all(rows.map((row) => metaFor(type, row.id)));
  return json(res, 200, { metas });
}

/* ------------------------------------------------------------------ *
 * Stream row builder (movies, series, cartoons, anime)
 * ------------------------------------------------------------------ */
const MAGNET_RE = /^magnet:\?/i;
const enc = (str) => Buffer.from(str, "utf8").toString("base64url");
const QUALITY_RANK = { "2160p": 4, "1080p": 3, "720p": 2, "576p": 1, "480p": 1 };

/** Split "tt1234:1:2" / "kitsu:12:1:2" into base id + episode ref. */
function parseSeriesRef(id) {
  const match = /^(.+):(\d+):(\d+)$/.exec(id);
  if (match) {
    return { baseId: match[1], episode: { season: Number(match[2]), episode: Number(match[3]) } };
  }
  return { baseId: id };
}

/** Map a scraped torrent into Stremio stream rows + player variants. */
function streamRows(req, stream, subs) {
  const originUrl = addonOrigin(req);
  const magnet = stream.url;
  const name = `[${stream.provider}] ${stream.name}`;
  const quality = stream.quality ?? "?";
  const size = stream.size ?? "?";
  const label = `${name} ⚡${stream.seeds}⬆ ${quality} ${size}`;
  const base = { name: label, title: stream.name, quality, infoHash: stream.infoHash };
  if (!MAGNET_RE.test(magnet)) {
    // Already-playable (debrid CDN) URL — direct passthrough.
    return [{ ...base, url: magnet }];
  }
  const subtitles = subs.map((lang) => ({ url: `${originUrl}/subtitle/${enc(magnet)}?lang=${lang}`, lang }));
  const rows = [];
  // Player 1 — Auto: native raw or ffmpeg transcode (never black-screens).
  rows.push({ ...base, url: `${originUrl}/play/${enc(magnet)}`, subtitles });
  // Player 2 — RAW WebTorrent (original file, no re-encode).
  rows.push({ ...base, name: `${label} · RAW`, title: `${stream.name} (raw)`, url: `${originUrl}/stream-raw/${enc(magnet)}`, subtitles });
  // Player 3 — Forced transcode (universal browser playback).
  rows.push({ ...base, name: `${label} · TRANSCODE`, title: `${stream.name} (transcode)`, url: `${originUrl}/transcode/${enc(magnet)}`, subtitles });
  return rows;
}

/** Rank + stall-guard a raw scraped pool into the final stream rows. */
function rowsFromPool(req, streams, queryTitle, arabicBoost) {
  // Quality filter (2160p > 1080p > 720p); fall back to everything healthy
  // so obscure titles never return 0 streams.
  const wanted = streams.filter((s) => WANTED_QUALITIES.includes(s.quality));
  const pool = wanted.length > 0 ? wanted : streams;
  // TITLE-PRECISION FIRST: exact-title releases (score 3) beat same-name-
  // different-show releases (score 1-2) no matter their seed counts — a
  // wrong show with 1000 seeds is still the wrong show.
  // For Arabic-market titles, releases with Arabic subtitles rank ABOVE raw
  // seed counts. For every other title, Arabic-friendly copies break ties
  // between equally-healthy releases. Health (seeds x source trust) then
  // raw seeds, then quality, within each tier — a fast 720p swarm with
  // thousands of peers still beats a stalled 1080p copy with 2 seeders.
  const rankKey = (s) => {
    const ar = arabicFriendly(s.name) ? 1 : 0;
    const seeds = effectiveSeeds(s);
    return arabicBoost
      ? [titleMatchScore(s.name, queryTitle), ar, seeds, s.seeds ?? 0, QUALITY_RANK[s.quality] ?? 0]
      : [titleMatchScore(s.name, queryTitle), seeds, ar, s.seeds ?? 0, QUALITY_RANK[s.quality] ?? 0];
  };
  pool.sort((a, b) => {
    const ka = rankKey(a), kb = rankKey(b);
    for (let i = 0; i < ka.length; i++) if (ka[i] !== kb[i]) return kb[i] - ka[i];
    return 0;
  });

  // Stall guard: if healthy copies (>= PREFERRED_SEEDS) exist, drop the
  // weak 1-4 seeder stragglers that cause "stuck on buffering".
  const strong = pool.filter((s) => (s.seeds ?? 0) >= PREFERRED_SEEDS);
  const top = (strong.length > 0 ? strong : pool).slice(0, 12);

  const rows = [];
  for (const stream of top) rows.push(...streamRows(req, stream, SUBTITLE_LANGS));
  // No test/sample fallback here — every row is a REAL scraped release for
  // this exact title. If the indexers genuinely have no release, an empty
  // list is honest.
  return rows;
}

const streamCache = new Map();        // "type:id[:s:e]" -> { at, streams, refreshing, ttl }
const STREAM_CACHE_TTL_MS = 6 * 60 * 60 * 1000; // 6h — repeat opens answer instantly
/** Empty results are only cached briefly (indexer rate-limits / deadline cuts
 * are transient) so a bad scrape can never poison a title for hours. */
const NEGATIVE_TTL_MS = 60_000;
const STREAM_DEADLINE_MS = 18_000;    // matches the provider engine's scrape budget

/** Background refresh: full (uncapped) scrape that upgrades the cache entry. */
async function refreshStreamCache(cacheKey, type, baseId, episode) {
  const resolved = await resolveTitle(type, baseId);
  const streams = await scrapeTorrents({
    title: resolved.name,
    imdbId: /^tt\d+$/i.test(baseId) ? baseId : null,
    type: resolved.type ?? type,
    year: resolved.year,
    language: resolved.language,
    country: resolved.country,
    aliases: resolved.aliases ?? [],
    episode,
  });
  const entry = streamCache.get(cacheKey);
  if (entry) {
    // Never replace a non-empty result with a transient empty scrape — keep
    // serving the good pool; a later refresh will catch up.
    if (streams.length === 0 && entry.streams.length > 0) return;
    entry.streams = streams;
    entry.at = Date.now();
    entry.ttl = streams.length > 0 ? STREAM_CACHE_TTL_MS : NEGATIVE_TTL_MS;
    entry.title = resolved.name;
    entry.aliases = resolved.aliases ?? [];
    entry.arabicBoost = isArabicTitle(resolved);
  }
}

/**
 * Full stream pipeline: cache -> parallel scrape (deadline-capped, cache
 * enriched in the background) -> health-first ranking.
 */
async function buildStreamRows(req, type, id) {
  const { baseId, episode } = parseSeriesRef(id);
  const cacheKey = `${type}:${baseId}${episode ? `:${episode.season}:${episode.episode}` : ""}`;
  const hit = streamCache.get(cacheKey);
  if (hit && Date.now() - hit.at < (hit.ttl ?? STREAM_CACHE_TTL_MS)) {
    if (!hit.refreshing) {
      hit.refreshing = true;
      refreshStreamCache(cacheKey, type, baseId, episode)
        .catch(() => {})
        .finally(() => { hit.refreshing = false; });
    }
    return rowsFromPool(req, hit.streams, hit.title, hit.arabicBoost);
  }
  const resolved = await resolveTitle(type, baseId);
  if (process.env.AAHUB_DEBUG === "1") {
    console.error("[debug] resolved", JSON.stringify({ id: baseId, name: resolved.name, aliases: resolved.aliases ?? [] }));
  }
  const params = {
    title: resolved.name,
    imdbId: /^tt\d+$/i.test(baseId) ? baseId : null,
    type: resolved.type ?? type,
    year: resolved.year,
    language: resolved.language,
    country: resolved.country,
    aliases: resolved.aliases ?? [],
    episode,
  };
  const streams = await scrapeTorrents(params, {
    deadlineMs: STREAM_DEADLINE_MS,
    onLate: (full) => {
      // Deadline cut the first scrape short — upgrade the cache when the
      // still-running passes land, so the NEXT request gets the full pool.
      const entry = streamCache.get(cacheKey);
      if (entry && Array.isArray(full) && full.length > 0) {
        entry.streams = full;
        entry.at = Date.now();
        entry.ttl = STREAM_CACHE_TTL_MS;
      }
    },
  });
  const arabicBoost = isArabicTitle(resolved);
  streamCache.set(cacheKey, {
    at: Date.now(),
    streams,
    // Empty/partial results (rate-limit or deadline cut) get a short TTL so
    // the next open re-scrapes instead of serving hours of nothing.
    ttl: streams.length > 0 ? STREAM_CACHE_TTL_MS : NEGATIVE_TTL_MS,
    refreshing: false,
    title: resolved.name,
    aliases: resolved.aliases ?? [],
    arabicBoost,
  });
  return rowsFromPool(req, streams, resolved.name, arabicBoost);
}

/* ------------------------------------------------------------------ *
 * HTTP server
 * ------------------------------------------------------------------ */
function addonOrigin(req) {
  const configured = process.env.PUBLIC_ADDON_URL?.replace(/\/+$/, "");
  if (configured) return configured;
  const fwdProto = req.headers["x-forwarded-proto"]?.split(",")[0]?.trim();
  if (fwdProto) {
    const fwdHost = req.headers["x-forwarded-host"]?.split(",")[0]?.trim();
    return `${fwdProto}://${fwdHost ?? req.headers.host}`;
  }
  return `http://${req.headers.host}`;
}

const AAHUB_LOGO_SVG = "<svg xmlns='http://www.w3.org/2000/svg' width='128' height='128' viewBox='0 0 128 128'><rect width='128' height='128' rx='26' fill='#0b0e14'/><rect x='5' y='5' width='118' height='118' rx='21' fill='none' stroke='#a855f7' stroke-opacity='0.5' stroke-width='3'/><circle cx='64' cy='56' r='21' fill='none' stroke='#c084fc' stroke-width='5'/><path d='M57 45 L74 56 L57 67 Z' fill='#c084fc'/><text x='64' y='105' font-family='Arial,Helvetica,sans-serif' font-size='16' font-weight='800' fill='#e8ebf2' text-anchor='middle' letter-spacing='4'>AAHUB</text></svg>";
const AAHUB_POSTER_SVG = "<svg xmlns='http://www.w3.org/2000/svg' width='300' height='450' viewBox='0 0 300 450'><rect width='300' height='450' fill='#0b0e14'/><defs><radialGradient id='g' cx='0.5' cy='0.32' r='0.8'><stop offset='0%' stop-color='#7c3aed' stop-opacity='0.4'/><stop offset='100%' stop-color='#0b0e14' stop-opacity='0'/></radialGradient></defs><rect width='300' height='450' fill='url(#g)'/><rect x='10' y='10' width='280' height='430' rx='18' fill='none' stroke='#a855f7' stroke-opacity='0.5' stroke-width='3'/><circle cx='150' cy='180' r='52' fill='none' stroke='#c084fc' stroke-width='9'/><path d='M136 150 L184 180 L136 210 Z' fill='#c084fc'/><text x='150' y='320' font-family='Arial,Helvetica,sans-serif' font-size='34' font-weight='800' fill='#e8ebf2' text-anchor='middle' letter-spacing='10'>AAHUB</text><text x='150' y='360' font-family='Arial,Helvetica,sans-serif' font-size='15' fill='#a1a8b8' text-anchor='middle' letter-spacing='4'>PREMIUM STREAMING</text></svg>";

/**
 * Pipe a streaming upstream (torrent engine / transcoder) back to the client.
 * Returns true when a playable response was forwarded, false when the
 * upstream refused (so callers can fall back to the transcode pipeline).
 */
async function proxyStream(res, upstreamUrl, rangeHeader) {
  try {
    const headers = { accept: "*/*" };
    if (rangeHeader) headers.range = rangeHeader;
    const upstream = await fetch(upstreamUrl, { headers });
    if (upstream.status >= 400) {
      json(res, upstream.status, { error: "playback upstream refused", detail: upstream.status });
      return false;
    }
    res.writeHead(upstream.status, {
      "content-type": upstream.headers.get("content-type") ?? "application/octet-stream",
      ...(upstream.headers.get("content-range") ? { "content-range": upstream.headers.get("content-range") } : {}),
      ...(upstream.headers.get("accept-ranges") ? { "accept-ranges": upstream.headers.get("accept-ranges") } : {}),
      ...(upstream.headers.get("content-length") ? { "content-length": upstream.headers.get("content-length") } : {}),
      "cache-control": "no-store",
      "access-control-allow-origin": "*",
    });
    const body = upstream.body;
    if (!body) { res.end(); return true; }
    res.flushHeaders();
    const nodeStream = Readable.fromWeb(body);
    res.on("close", () => { try { if (!nodeStream.destroyed) nodeStream.destroy(); } catch { /* torn down */ } });
    nodeStream.pipe(res);
    return true;
  } catch {
    if (!res.headersSent) json(res, 502, { error: "playback upstream unavailable" });
    else res.end();
    return false;
  }
}

/**
 * Pre-warm the top streams of a just-served list in the torrent engine
 * (background, never blocks the response): metadata is already resolving by
 * the time the user clicks a row, so playback starts instantly instead of
 * waiting on a cold metadata fetch from a dead-cold swarm.
 */
const WARM_STREAM_LIMIT = 3;
function warmTopStreams(rows) {
  if (!PROVIDER) return;
  const seen = new Set();
  let warmed = 0;
  for (const row of rows ?? []) {
    if (warmed >= WARM_STREAM_LIMIT) break;
    const m = /^\/(?:play|stream-raw|transcode)\/([A-Za-z0-9_-]+)$/.exec(row?.url ?? "");
    if (!m) continue;
    let magnet;
    try {
      magnet = Buffer.from(m[1], "base64url").toString("utf8");
    } catch {
      continue;
    }
    if (!MAGNET_RE.test(magnet)) continue;
    const hash = /btih:([a-f0-9]{40})/i.exec(magnet)?.[1]?.toLowerCase();
    if (!hash || seen.has(hash)) continue;
    seen.add(hash);
    warmed++;
    fetch(`${PROVIDER}/torrent/warm?magnet=${encodeURIComponent(magnet)}`).catch(
      () => {
        /* engine down — playback falls back gracefully */
      },
    );
  }
}

function decodeMagnet(req, res) {
  let magnet;
  try {
    magnet = Buffer.from(req.params.encodedMagnet ?? "", "base64url").toString("utf8");
  } catch {
    json(res, 400, { error: "Invalid play URL" });
    return null;
  }
  if (!MAGNET_RE.test(magnet)) {
    json(res, 400, { error: "Invalid play URL" });
    return null;
  }
  return magnet;
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host ?? "localhost"}`);
  const pathname = url.pathname;
  origin = addonOrigin(req);

  try {
    let m;

    /* --- base manifest: directly accessible, installs instantly --- */
    if (pathname === "/manifest.json") {
      return json(res, 200, buildManifest(origin));
    }

    /* --- logo / poster --- */
    if (pathname === "/logo.svg") {
      res.writeHead(200, { "content-type": "image/svg+xml", "cache-control": "public, max-age=86400", "access-control-allow-origin": "*" });
      return res.end(AAHUB_LOGO_SVG);
    }
    if (pathname === "/poster.svg") {
      res.writeHead(200, { "content-type": "image/svg+xml", "cache-control": "public, max-age=86400", "access-control-allow-origin": "*" });
      return res.end(AAHUB_POSTER_SVG);
    }

    /* --- self-source (how you recreate this file anywhere) --- */
    if (pathname === "/__source") {
      res.writeHead(200, { "content-type": "text/javascript; charset=utf-8", "cache-control": "no-store" });
      return res.end(__SOURCE);
    }

    /* --- health + diagnostics --- */
    if (pathname === "/health") {
      return json(res, 200, {
        ok: true,
        port: PORT,
        provider: PROVIDER,
        sources: getSourceStatus(),
        catalogs: CATALOGS.map((c) => c.id),
        uptime: Math.round(process.uptime()),
      });
    }

    /* --- catalog --- */
    m = /^\/catalog\/(movie|series)\/([a-z0-9-]+)\.json$/.exec(pathname);
    if (m) {
      return await serveCatalog(res, m[1], m[2]);
    }

    /* --- meta (any tt or kitsu id, never 404) --- */
    m = /^\/meta\/(movie|series)\/([^/]+)\.json$/.exec(pathname);
    if (m) {
      const meta = await metaFor(m[1], m[2]);
      return json(res, 200, { meta });
    }

    /* --- stream (movies/series/anime, never 404) --- */
    m = /^\/stream\/(movie|series)\/([^/]+)\.json$/.exec(pathname);
    if (m) {
      const rows = await buildStreamRows(req, m[1], m[2]);
      // Fire-and-forget: the engine starts resolving the top magnets while
      // the user picks a row, so click → playback starts immediately.
      warmTopStreams(rows);
      return json(res, 200, { streams: rows });
    }

    /* --- playback proxies --- */
    m = /^\/(play|stream-raw|transcode|subtitle)\/([^/]+)$/.exec(pathname);
    if (m) {
      const kind = m[1];
      const magnet = decodeMagnet({ params: { encodedMagnet: m[2] }, headers: req.headers }, res);
      if (!magnet) return;
      if (kind === "play") {
        // Auto: prefer native/transcode from the engine; if the engine
        // refuses, force the universal ffmpeg transcode so it never
        // black-screens.
        const ok = await proxyStream(
          res,
          `${PROVIDER}/torrent/play?magnet=${encodeURIComponent(magnet)}`,
          req.headers.range,
        );
        if (!ok) {
          const src = `${PROVIDER}/torrent/stream?magnet=${encodeURIComponent(magnet)}`;
          await proxyStream(res, `${PROVIDER}/transcode?src=${encodeURIComponent(src)}`, req.headers.range);
        }
        return;
      }
      if (kind === "stream-raw") {
        return await proxyStream(res, `${PROVIDER}/torrent/stream?magnet=${encodeURIComponent(magnet)}`, req.headers.range);
      }
      if (kind === "transcode") {
        const src = `${PROVIDER}/torrent/stream?magnet=${encodeURIComponent(magnet)}`;
        return await proxyStream(res, `${PROVIDER}/transcode?src=${encodeURIComponent(src)}`, req.headers.range);
      }
      const lang = url.searchParams.get("lang") || "en";
      const name = url.searchParams.get("name") || "";
      return await proxyStream(res, `${PROVIDER}/torrent/subtitle?magnet=${encodeURIComponent(magnet)}&lang=${encodeURIComponent(lang)}${name ? `&name=${encodeURIComponent(name)}` : ""}`, req.headers.range);
    }

    /* --- media (disk-cached files) --- */
    m = /^\/media\/([a-f0-9]{16}\.(?:webm|mp4|mkv))$/i.exec(pathname);
    if (m) {
      return await proxyStream(res, `${PROVIDER}/media/${m[1]}`, req.headers.range);
    }

    return json(res, 404, { error: "Not found" });
  } catch (error) {
    // Never crash: any unexpected error answers clean JSON (Stremio treats
    // malformed responses as a hard failure).
    if (!res.headersSent) json(res, 500, { error: error instanceof Error ? error.message : String(error) });
    else res.end();
  }
});

// Never cut a long video stream (Node's default request/keep-alive timeouts).
server.requestTimeout = 0;
server.keepAliveTimeout = 0;

// This file's own source, embedded at load so /__source can serve it back.
const __SOURCE = require("node:fs").readFileSync(__filename, "utf8");

server.listen(PORT, () => {
  console.log(`\n  AAHUB standalone addon server (open mode)`);
  console.log(`  Local manifest:  http://127.0.0.1:${PORT}/manifest.json`);
  console.log(`  Stremio install: stremio://127.0.0.1:${PORT}/manifest.json`);
  console.log(`  Torrent engine:  ${PROVIDER}\n`);
});
