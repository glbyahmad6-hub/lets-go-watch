const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36";
async function get(url){
  const r = await fetch(url, { headers: { "user-agent": UA, referer: "https://krmzi.org/" }, signal: AbortSignal.timeout(15000) });
  return { s: r.status, t: await r.text(), u: r.url };
}
(async () => {
  for (const path of ["/series/", "/series-list/", "/"] ) {
    const p = await get("https://krmzi.org" + path);
    console.log("===", path, "→", p.s, "len:", p.t.length);
    // series links
    const links = [...new Set([...p.t.matchAll(/href="(https:\/\/krmzi\.org\/series\/[^"]+)"/g)].map(m => m[1]))];
    console.log("series links:", links.length, links.slice(0, 5));
    // pagination
    const pages = [...new Set([...p.t.matchAll(/href="([^"]*page[/=]\d+[^"]*)"/g)].map(m => m[1]))];
    console.log("pagination:", pages.slice(0, 6));
    // post titles
    const titles = [...p.t.matchAll(/<h\d[^>]*>([^<]{4,80})<\/h\d>/g)].map(m => m[1].trim());
    console.log("titles sample:", titles.slice(0, 8));
    if (p.s === 200 && links.length > 0) { require("fs").writeFileSync("krmzi-series-list.html", p.t); break; }
  }
})().catch(e => console.log("ERR", e.message));
