// Mock qBittorrent Web API — used to test the engine's QBIT_MODE without
// installing qBittorrent. Serves auth/login, torrents/add, torrents/info,
// torrents/files, and set* endpoints, backed by a real 2MB "video" file on
// disk so the engine can Range-serve actual bytes.
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const DATA = path.join(path.dirname(fileURLToPath(import.meta.url)), "mock-qbit-data");
fs.mkdirSync(DATA, { recursive: true });
const FILE = path.join(DATA, "Mock.Video.1080p.mp4");
if (!fs.existsSync(FILE)) {
  const buf = Buffer.alloc(2 * 1024 * 1024, 7);
  // valid mp4 ftyp header so content sniffing sees a real container
  const ftyp = Buffer.from(
    "00000018" + "66747970" + "69736F6D" + "00000200" + "69736F6D" + "69736F32" + "61766331" + "6D703431",
    "hex",
  );
  ftyp.copy(buf, 0);
  fs.writeFileSync(FILE, buf);
}
const SIZE = fs.statSync(FILE).size;

const PORT = Number(process.env.MOCK_PORT || 18080);
const known = new Map(); // hash -> { name }

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const json = (code, body, extra = {}) => {
    res.writeHead(code, { "content-type": "application/json", ...extra });
    res.end(typeof body === "string" ? body : JSON.stringify(body));
  };

  if (url.pathname === "/api/v2/auth/login" && req.method === "POST") {
    res.writeHead(200, { "set-cookie": "SID=mock-session" });
    res.end("mock-session");
    return;
  }

  if (url.pathname === "/api/v2/torrents/add" && req.method === "POST") {
    let raw = "";
    for await (const c of req) raw += c;
    const urls = new URLSearchParams(raw).get("urls") || "";
    const m = urls.match(/btih:([a-fA-F0-9]{40})/);
    if (m) known.set(m[1].toLowerCase(), { name: "Mock Video" });
    res.writeHead(200);
    res.end("Ok.");
    return;
  }

  if (/^\/api\/v2\/torrents\/set/.test(url.pathname)) {
    res.writeHead(200);
    res.end("Ok.");
    return;
  }

  if (url.pathname === "/api/v2/torrents/info") {
    const hash = (url.searchParams.get("hashes") || "").split(",")[0].toLowerCase();
    if (!known.has(hash)) {
      json(200, []);
      return;
    }
    json(200, [
      {
        hash,
        name: "Mock Video",
        save_path: DATA.replace(/\\/g, "/"),
        content_path: FILE.replace(/\\/g, "/"),
        num_complete: 12,
        num_incomplete: 3,
        dlspeed: 512000,
        state: "downloading",
        progress: 0.5,
      },
    ]);
    return;
  }

  if (url.pathname === "/api/v2/torrents/files") {
    const hash = (url.searchParams.get("hash") || "").toLowerCase();
    if (!known.has(hash)) {
      json(200, []);
      return;
    }
    json(200, [{ index: 0, name: "Mock.Video.1080p.mp4", size: SIZE, progress: 1.0 }]);
    return;
  }

  json(404, { error: "not found" });
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`mock qBittorrent on http://127.0.0.1:${PORT} save_path=${DATA}`);
});
