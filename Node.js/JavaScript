// Global coverage battery — resolves ids via Cinemeta (movie+series), then
// hits the addon stream endpoint on :7000. Paced to dodge rate limits.
const BASE = process.env.ADDON_URL || "http://127.0.0.1:7000";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function cinemetaSearch(type, q) {
  const url = `https://v3-cinemeta.strem.io/catalog/${type}/top/search=${encodeURIComponent(q)}.json`;
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(10_000) });
    if (!res.ok) return null;
    const j = await res.json();
    return (j.metas || []).find((m) => m.id) || null;
  } catch {
    return null;
  }
}

async function resolve(q) {
  // Prefer exact-ish name match; fall back to any result.
  const norm = (s) => String(s || "").toLowerCase().replace(/[^a-z0-9\u0600-\u06ff]/g, "");
  const want = norm(q);
  for (const type of ["movie", "series"]) {
    const meta = await cinemetaSearch(type, q);
    if (!meta) continue;
    if (norm(meta.name) === want || norm(meta.id) === want) return { ...meta, type };
    // cache the first non-exact hit in case nothing better shows up
    if (!fallback[type]) fallback[type] = { ...meta, type };
  }
  return fallback.movie || fallback.series || null;
}
const fallback = {};

async function addonStreams(type, id) {
  const url = `${BASE}/stream/${type}/${id}.json`;
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(25_000) });
    if (!res.ok) return { status: res.status, rows: -1 };
    const j = await res.json();
    return { status: res.status, rows: (j.streams || []).length };
  } catch (e) {
    return { status: "ERR", rows: -1, err: String(e).slice(0, 60) };
  }
}

const CASES = [
  // Hollywood movies
  ["Oppenheimer", "movie", "tt15398776"],
  ["The Dark Knight", "movie", "tt0468569"],
  ["Dune Part Two", "movie", "tt15239678"],
  // Hollywood series (S1E1)
  ["Breaking Bad S1E1", "series", "tt0903747:1:1"],
  ["Game of Thrones S1E1", "series", "tt0944947:1:1"],
  ["Stranger Things S1E1", "series", "tt4574334:1:1"],
  // Arabic
  ["Wadjda (Arabic movie)", "movie", "tt2258858"],
  ["Theeb (Arabic movie)", "movie", "tt3170902"],
  ["Al Hayba (Arabic series) S1E1", "series", "tt7035576:1:1"],
  ["Bab Al-Hara (Arabic series) S1E1", "series", "tt1999065:1:1"],
  // Turkish
  ["Kurulus Osman S1E1", "series", "tt11093718:1:1"],
  ["Yali Capkini S1E1", "series", "tt21105088:1:1"],
  // Asian
  ["Parasite (Korean movie)", "movie", "tt6751668"],
  ["Squid Game S1E1", "series", "tt10919420:1:1"],
  ["The Glory S1E1", "series", "tt21344706:1:1"],
  // Anime / cartoons
  ["Spirited Away (anime movie)", "movie", "tt0245429"],
  ["Attack on Titan S1E1", "series", "tt2560140:1:1"],
  ["Demon Slayer S1E1", "series", "tt9335498:1:1"],
];

console.log(`Battery against ${BASE}\n`);
for (const [label, type, id] of CASES) {
  const r = await addonStreams(type, id);
  console.log(`${r.rows >= 0 ? "✅" : "❌"} ${label.padEnd(38)} ${String(r.rows).padStart(3)} rows  (${r.status}${r.err ? " " + r.err : ""})`);
  await sleep(2500); // pace to avoid indexer rate limits
}
console.log("\nDone.");
