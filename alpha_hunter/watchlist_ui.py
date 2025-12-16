import json
import os
import webbrowser
from typing import Any, Dict, List, Tuple

from flask import Flask, jsonify, render_template_string, request


def normalize_addr(s: str) -> str:
    """
    Normalize address identifiers: lower-case, drop chain prefixes like 'bsc:' if present.
    Returns empty string for falsy input.
    """
    if not s:
        return ""
    s = s.strip().lower()
    if ":" in s:
        # keep the part after the first colon (e.g., bsc:0xabc -> 0xabc)
        s = s.split(":", 1)[1]
    return s


def _load_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _merge_actor_data(
    actors_path: str,
    watchlist_path: str,
    pnl_path: str,
    timeline_path: str,
    overlap_path: str,
) -> Tuple[
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    Dict[str, Any],
    Dict[str, Any],
    Dict[str, Any],
]:
    actors_data: List[Dict[str, Any]] = _load_json(actors_path) or []
    watchlist_data: List[Dict[str, Any]] = _load_json(watchlist_path) or []
    pnl_data: Dict[str, Any] = _load_json(pnl_path) or {}
    timeline_data: Dict[str, Any] = _load_json(timeline_path) or {}
    overlap_data: Dict[str, Any] = _load_json(overlap_path) or {}

    watchlist_set = {
        f"{w.get('chain', '').lower()}:{w.get('address', '').lower()}"
        for w in watchlist_data
        if isinstance(w, dict)
    }
    pnl_map = {
        f"{a.get('chain', '').lower()}:{a.get('address', '').lower()}": a
        for a in pnl_data.get("actors", [])
        if isinstance(a, dict)
    }

    merged: Dict[str, Dict[str, Any]] = {}
    for base in actors_data:
        key = f"{base.get('chain', '').lower()}:{base.get('address', '').lower()}"
        merged[key] = dict(base)

    # include pnl-only actors
    for key, pa in pnl_map.items():
        if key not in merged:
            merged[key] = {"address": pa.get("address"), "chain": pa.get("chain")}

    # enrich with pnl fields and watchlist flags
    for key, actor in merged.items():
        pa = pnl_map.get(key, {})
        actor.update({k: v for k, v in pa.items() if k not in {"address", "chain"}})
        actor["in_watchlist"] = key in watchlist_set
        actor.setdefault("role_guess", actor.get("role_guess"))
        actor.setdefault("role_confidence", actor.get("role_confidence"))
        actor.setdefault("role_factors", actor.get("role_factors", []))
        actor.setdefault("badges", actor.get("badges", []))
        actor.setdefault(
            "likely_next_pump_candidates",
            actor.get("likely_next_pump_candidates", actor.get("candidates", [])),
        )

    cache_meta = {}
    if isinstance(pnl_data, dict):
        cache_meta = (
            pnl_data.get("cache_meta")
            or pnl_data.get("meta", {}).get("cache_meta")
            or {}
        )

    return (
        list(merged.values()),
        watchlist_data,
        timeline_data,
        overlap_data,
        cache_meta,
    )


def _load_cache_meta(
    pnl_path: str,
    birdeye_cache_meta_path: str,
    pnl_progress_path: str,
    cache_health_path: str = "data/alpha_profiler/warehouse/cache_health.json",
) -> Dict[str, Any]:
    # prefer dedicated cache_health if present
    health = _load_json(cache_health_path) or {}
    if isinstance(health, dict) and health.get("cache_meta"):
        return health.get("cache_meta")

    pnl_data = _load_json(pnl_path) or {}
    cov = {}
    if isinstance(pnl_data, dict):
        cov = (
            pnl_data.get("cache_meta")
            or pnl_data.get("meta", {}).get("cache_meta")
            or {}
        )

    bir = _load_json(birdeye_cache_meta_path) or {}
    prog = _load_json(pnl_progress_path) or {}

    out = {
        "covalent": cov if isinstance(cov, dict) else {},
        "birdeye": bir if isinstance(bir, dict) else {},
        "progress": prog if isinstance(prog, dict) else {},
    }
    out["first_seen"] = out["covalent"].get("first_seen") or out["birdeye"].get(
        "first_seen"
    )
    out["last_updated"] = out["covalent"].get("last_updated") or out["birdeye"].get(
        "last_updated"
    )
    return out


def run_watchlist_ui(
    actors_path: str = "data/alpha_profiler/actors.json",
    watchlist_path: str = "data/alpha_profiler/watchlist.json",
    pnl_path: str = "data/alpha_profiler/pnl_watchlist.json",
    timeline_path: str = "data/alpha_profiler/timeline_watchlist.json",
    overlap_path: str = "data/alpha_profiler/holdings_overlap.json",
    network_path: str = "data/alpha_profiler/network_watchlist.json",
    network_warehouse_path: str = "data/alpha_profiler/warehouse/network_watchlist.json",
    birdeye_cache_meta_path: str = "data/raw_cache/birdeye/cache_meta.json",
    pnl_progress_path: str = "data/alpha_profiler/pnl_progress.json",
    cache_health_path: str = "data/alpha_profiler/warehouse/cache_health.json",
    host: str = "127.0.0.1",
    port: int = 8050,
) -> None:
    app = Flask(__name__)

    def load_network_artifact() -> Dict[str, Any]:
        for path in (network_warehouse_path, network_path):
            if os.path.exists(path):
                data = _load_json(path) or {}
                if isinstance(data, dict):
                    return data
        return {
            "generated_at": None,
            "nodes": [],
            "edges": [],
            "meta": {"error": "NO_NETWORK_FILE"},
        }

    def load_all():
        # prefer warehouse network artifact if present
        actors, watchlist, timeline_data, overlap_data, _legacy_cache = (
            _merge_actor_data(
                actors_path, watchlist_path, pnl_path, timeline_path, overlap_path
            )
        )
        network_data = load_network_artifact()
        cache_meta = _load_cache_meta(
            pnl_path, birdeye_cache_meta_path, pnl_progress_path, cache_health_path
        )
        return actors, watchlist, timeline_data, overlap_data, network_data, cache_meta

    @app.route("/api/actors")
    def api_actors():
        actors, *_ = load_all()
        return jsonify(actors)

    @app.route("/api/watchlist")
    def api_watchlist():
        _, watchlist, *_, __ = load_all()
        return jsonify(watchlist)

    @app.route("/api/timeline")
    def api_timeline():
        _, _, timeline_data, _, _, _ = load_all()
        return jsonify(timeline_data or {})

    @app.route("/api/overlap")
    def api_overlap():
        _, _, _, overlap_data, _, _ = load_all()
        return jsonify(overlap_data or {})

    @app.route("/api/network")
    def api_network():
        _, _, _, _, network_data, _ = load_all()
        net = network_data or {}
        addr = normalize_addr(request.args.get("addr") or "")
        chain = normalize_addr(request.args.get("chain") or "")

        def norm_edge(e: Dict[str, Any]) -> Tuple[str, str]:
            src = normalize_addr(e.get("source") or e.get("from") or e.get("src") or "")
            dst = normalize_addr(e.get("target") or e.get("to") or e.get("dst") or "")
            return src, dst

        nodes = net.get("nodes") or []
        edges = net.get("edges") or []

        if addr:
            sub_edges: List[Dict[str, Any]] = []
            involved = {addr}
            for e in edges:
                s, t = norm_edge(e)
                if not s and not t:
                    continue
                if s == addr or t == addr:
                    if s == t:
                        continue  # self-loop
                    peer_addr = t if s == addr else s
                    if not peer_addr or peer_addr == addr:
                        continue
                    e = dict(e)
                    e["source"] = s
                    e["target"] = t
                    sub_edges.append(e)
                    involved.add(peer_addr)

            sub_nodes: List[Dict[str, Any]] = []
            if sub_edges:
                for n in nodes:
                    nid = (n.get("id") or n.get("address") or "").lower()
                    if nid in involved:
                        sub_nodes.append(n)
                if addr not in involved:
                    sub_nodes.append(
                        {"id": addr, "chain": chain or None, "is_actor": True}
                    )

            meta = {
                "schema_detected": "source_target",
                "generated_at": net.get("generated_at")
                or (net.get("meta") or {}).get("generated_at"),
                "edges_total": len(edges),
                "nodes_total": len(nodes),
                "edges_for_addr": len(sub_edges),
                "nodes_for_addr": len(sub_nodes),
                "addr": addr,
                "chain": chain,
                "loaded_edges": len(edges),
                "loaded_nodes": len(nodes),
                "neighbors": len(involved) - 1 if sub_edges else 0,
            }
            return jsonify({"nodes": sub_nodes, "edges": sub_edges, "meta": meta})

        return jsonify(
            {
                "nodes": nodes,
                "edges": edges,
                "meta": {
                    "generated_at": net.get("generated_at"),
                    "loaded_edges": len(edges),
                    "loaded_nodes": len(nodes),
                },
            }
        )

    @app.route("/api/debug/files")
    def api_debug_files():
        files = {
            "pnl_watchlist": pnl_path,
            "cache_health": "data/alpha_profiler/warehouse/cache_health.json",
            "features": "data/alpha_profiler/warehouse/features.json",
            "watchlist": watchlist_path,
            "actors": actors_path,
        }
        out = {}
        for k, path in files.items():
            try:
                st = os.stat(path)
                out[k] = {
                    "exists": True,
                    "size": st.st_size,
                    "mtime": st.st_mtime,
                    "path": path,
                }
            except FileNotFoundError:
                out[k] = {"exists": False, "size": 0, "mtime": None, "path": path}
        return jsonify(out)

    @app.route("/api/cache_meta")
    def api_cache_meta():
        *_, cache_meta = load_all()
        return jsonify(cache_meta or {})

    @app.route("/api/linked")
    def api_linked():
        _, _, _, _, network_data, _ = load_all()
        net = network_data or {}
        addr = normalize_addr(request.args.get("addr") or "")
        chain = normalize_addr(request.args.get("chain") or "")

        def norm_edge(e: Dict[str, Any]) -> Tuple[str, str]:
            src = normalize_addr(e.get("source") or e.get("from") or e.get("src") or "")
            dst = normalize_addr(e.get("target") or e.get("to") or e.get("dst") or "")
            return src, dst

        nodes = net.get("nodes") or []
        edges = net.get("edges") or []

        rows: List[Dict[str, Any]] = []
        if addr:
            agg: Dict[str, Dict[str, Any]] = {}
            for e in edges:
                s, t = norm_edge(e)
                if not s and not t:
                    continue
                if s == addr or t == addr:
                    if s == t:
                        continue
                    peer = t if s == addr else s
                    if not peer or peer == addr:
                        continue
                    direction = "out" if s == addr else "in"
                    rec = agg.setdefault(
                        peer,
                        {"peer": peer, "dir_flags": set(), "transfers": 0, "usd": 0.0},
                    )
                    rec["dir_flags"].add(direction)
                    rec["transfers"] += e.get("transfers") or e.get("count") or 0
                    rec["usd"] += float(e.get("value") or e.get("usd") or 0)
            node_map = {
                (n.get("id") or n.get("address") or "").lower(): n for n in nodes
            }
            for rec in agg.values():
                peer_node = node_map.get(rec["peer"].lower())
                rows.append(
                    {
                        "peer": rec["peer"],
                        "dir": "both"
                        if len(rec["dir_flags"]) > 1
                        else (list(rec["dir_flags"])[0] if rec["dir_flags"] else ""),
                        "transfers": rec["transfers"],
                        "usd": rec["usd"],
                        "peer_is_actor": bool(peer_node and peer_node.get("is_actor")),
                        "peer_score": peer_node.get("score") if peer_node else None,
                        "peer_pnl": peer_node.get("pnl") if peer_node else None,
                    }
                )
            rows.sort(key=lambda x: x.get("usd", 0), reverse=True)

        out = {
            "generated_at": net.get("generated_at")
            or (net.get("meta") or {}).get("generated_at"),
            "addr": addr,
            "chain": chain,
            "peers": rows,
            "loaded_edges": len(edges),
            "loaded_nodes": len(nodes),
            "meta": {"peers": len(rows), "edges_used": len(edges), "nodes": len(nodes)},
        }
        return jsonify(out)

    template = r"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <title>Alpha Hunter — Watchlist</title>
      <style>
        body { margin:0; padding:0; font-family: 'Inter', sans-serif; background:#050812; color:#E5ECFF; }
        .container { display:grid; grid-template-columns: 320px 1fr 520px; gap:16px; padding:20px; }
        .card { background:#0B1018; border-radius:16px; padding:16px; box-shadow:0 10px 30px rgba(0,0,0,0.25); }
        h1 { margin:0; font-size:22px; color:#12F4C0; }
        h2 { margin:0 0 6px; font-size:16px; color:#8B9BBE; }
        .badge { display:inline-block; padding:4px 10px; border-radius:12px; font-size:12px; margin-right:4px; }
        .core_operator { background:rgba(18,244,192,0.15); color:#12F4C0; }
        .inner_circle { background:rgba(18,166,244,0.15); color:#12A6F4; }
        .outer_circle { background:rgba(255,193,7,0.15); color:#FFC107; }
        .retail { background:rgba(255,255,255,0.08); color:#8B9BBE; }
        .role { background:rgba(255,255,255,0.08); color:#E5ECFF; }
        input, select { width:100%; padding:10px 14px; background:#0F1520; color:#E5ECFF; border:1px solid rgba(255,255,255,0.08); border-radius:10px; outline:none; }
        input:focus { border-color:#12F4C0; }
        .filters button { width:100%; padding:10px; background:rgba(18,244,192,0.1); border:1px solid rgba(18,244,192,0.3); color:#12F4C0; border-radius:10px; cursor:pointer; }
        .filters button:hover { background:rgba(18,244,192,0.2); }
        table { width:100%; border-collapse:collapse; }
        th, td { padding:9px; border-bottom:1px solid rgba(255,255,255,0.05); text-align:left; font-size:13px; }
        tr:hover { background:rgba(255,255,255,0.04); cursor:pointer; }
        .small { color:#8B9BBE; font-size:12px; }
        .stat-grid { display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:8px; }
        .stat { background:#0F1520; border-radius:12px; padding:10px; }
        .pill { background:#0F1520; border-radius:12px; padding:6px 10px; display:inline-block; margin:2px 4px 0 0; }
        .tabs { display:flex; flex-wrap:wrap; gap:8px; margin-top:8px; }
        .tab-btn { padding:6px 10px; border-radius:10px; border:1px solid rgba(255,255,255,0.08); background:#0F1520; color:#E5ECFF; cursor:pointer; }
        .tab-btn.active { border-color:#12F4C0; color:#12F4C0; }
        .tab-content { display:none; margin-top:10px; }
        .tab-content.active { display:block; }
        .chip { display:inline-block; padding:4px 8px; background:rgba(255,255,255,0.08); border-radius:999px; margin:2px; font-size:12px; }
        .row-selected { background:rgba(18,244,192,0.08); font-weight:600; }
        .row-selected td { border-left:3px solid #12F4C0; }
        .row-dimmed { opacity:0.55; }
      </style>
    </head>
    <body>
      <div class="container">
        <div class="card filters">
          <h1>Alpha Hunter — Watchlist</h1>
          <h2>Actors monitoring</h2>
          <div class="stat-grid">
            <div class="stat">Actors in base: <b id="stat-actors">0</b></div>
            <div class="stat">In watchlist: <b id="stat-watchlist">0</b></div>
          </div>
          <h2 style="margin-top:12px;">Cache health</h2>
          <div id="cache-meta" class="stat-grid"></div>
          <h2 style="margin-top:16px;">Search</h2>
          <input id="filter-search" placeholder="Address or token" oninput="applyFilters()" />
          <h2 style="margin-top:12px;">Pamper level</h2>
          <select id="filter-level" onchange="applyFilters()">
            <option value="">Any</option>
            <option value="core_operator">core_operator</option>
            <option value="inner_circle">inner_circle</option>
            <option value="outer_circle">outer_circle</option>
            <option value="retail">retail</option>
          </select>
          <h2 style="margin-top:12px;">Role</h2>
          <select id="filter-role" onchange="applyFilters()">
            <option value="">Any</option>
            <option value="operator">operator</option>
            <option value="relay">relay</option>
            <option value="payout">payout</option>
            <option value="noise">noise</option>
          </select>
          <h2 style="margin-top:12px;">Min pumps</h2>
          <input id="filter-min-pumps" type="number" min="0" value="0" oninput="applyFilters()" />
          <div style="margin-top:12px;">
            <label><input type="checkbox" id="filter-watchlist" onchange="applyFilters()" /> Only watchlist</label>
          </div>
          <div style="margin-top:8px;">
            <label><input type="checkbox" id="filter-success" onchange="applyFilters()" /> Covalent success only</label>
          </div>
          <div style="margin-top:8px;">
            <label><input type="checkbox" id="filter-badges" onchange="applyFilters()" /> Has badges</label>
          </div>
          <div style="margin-top:8px;">
            <label><input type="checkbox" id="filter-roi-missing" onchange="applyFilters()" /> ROI not calculated</label>
          </div>
          <h2 style="margin-top:12px;">Active since (UTC)</h2>
          <input id="filter-active-since" type="text" placeholder="YYYY-MM-DD or ISO" oninput="applyFilters()" />
          <div style="margin-top:12px;">
            <button onclick="resetFilters()">Reset filters</button>
          </div>
        </div>

        <div class="card">
          <h2>Actors</h2>
          <table id="actors-table">
            <thead>
              <tr>
                <th>Address</th>
                <th>Level</th>
                <th>Role</th>
                <th>Badges</th>
                <th>Pumps</th>
                <th>Max score</th>
                <th>Avg ROI, %</th>
                <th>Realized, $</th>
                <th>Unrealized, $</th>
                <th>Total PnL, $</th>
                <th>Avg share, %</th>
                <th>Actor score</th>
                <th>Portfolio, $</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody id="actors-body"></tbody>
          </table>
        </div>

        <div class="card" id="actor-detail">
          <h2>Actor details</h2>
          <div id="detail-header" class="small">Select actor…</div>
          <div class="tabs">
            <button class="tab-btn active" onclick="showTab('summary')">Summary</button>
            <button class="tab-btn" onclick="showTab('pnl')">PnL by pump</button>
            <button class="tab-btn" onclick="showTab('holdings')">Holdings</button>
            <button class="tab-btn" onclick="showTab('links')">Linked</button>
            <button class="tab-btn" onclick="showTab('timeline')">Timeline</button>
            <button class="tab-btn" onclick="showTab('overlap')">Overlap</button>
            <button class="tab-btn" onclick="showTab('network')">Network</button>
          </div>
          <div id="tab-summary" class="tab-content active"></div>
          <div id="tab-pnl" class="tab-content"></div>
          <div id="tab-holdings" class="tab-content"></div>
          <div id="tab-links" class="tab-content"></div>
          <div id="tab-timeline" class="tab-content"></div>
          <div id="tab-overlap" class="tab-content"></div>
          <div id="tab-network" class="tab-content"></div>
        </div>
      </div>

      <script>
        let actors = [];
        let watchlist = [];
        let timeline = {};
        let overlap = {};
        let network = {};
        let networkForActor = {};
        let linkedForActor = {};
        let current = null;
        let cacheMeta = {};
        let selectedPeer = null;
        let selectedDir = null;
        let hoveredPeer = null;
        let hoveredDir = null;
        let lastScrolledKey = null;
        const EDGE_COLORS = [
          '#00E5FF','#FF3D00','#FFD600','#00E676','#7C4DFF','#FF4081',
          '#18FFFF','#FF9100','#76FF03','#FF6D00','#00B0FF','#AA00FF'
        ];
        function colorForPeer(peer) {
          const s = normalizeId(peer||'');
          let h = 5381;
          for (let i = 0; i < s.length; i++) {
            h = ((h << 5) + h) + s.charCodeAt(i);
          }
          const idx = Math.abs(h) % EDGE_COLORS.length;
          return EDGE_COLORS[idx];
        }

        function normalizeId(x) {
          if (!x) return '';
          let s = (''+x).toLowerCase();
          if (s.includes(':')) {
            const parts = s.split(':');
            s = parts[parts.length-1];
          }
          return s;
        }

        function computeNetworkView(graph, actorAddr) {
          const addr = normalizeId(actorAddr || '');
          const edgesAll = graph && graph.edges ? graph.edges : [];
          const filtered = (graph && graph.meta && graph.meta.addr) ? edgesAll : edgesAll.filter(e => {
            const s = normalizeId(e.source || e.from || e.src);
            const t = normalizeId(e.target || e.to || e.dst);
            return s === addr || t === addr;
          });
          const incoming = [];
          const outgoing = [];
          const neighbors = new Set();
          let inVal = 0, outVal = 0, inTx = 0, outTx = 0;
          filtered.forEach(e => {
            const s = normalizeId(e.source || e.from || e.src);
            const t = normalizeId(e.target || e.to || e.dst);
            if (!s && !t) return;
            const isOut = s === addr;
            const peer = isOut ? t : s;
            if (!peer || peer === addr) return;
            const rec = {
              peer,
              dir: isOut ? 'out' : 'in',
              transfers: e.transfers || e.count || 0,
              value: e.value || e.usd || 0,
              edge: e,
            };
            neighbors.add(peer);
            if (rec.dir === 'out') {
              outgoing.push(rec);
              outVal += rec.value || 0;
              outTx += rec.transfers || 0;
            } else {
              incoming.push(rec);
              inVal += rec.value || 0;
              inTx += rec.transfers || 0;
            }
          });
          incoming.sort((a,b)=> (b.value||0) - (a.value||0));
          outgoing.sort((a,b)=> (b.value||0) - (a.value||0));
          return {
            incoming,
            outgoing,
            totals: {in_value: inVal, out_value: outVal, in_tx: inTx, out_tx: outTx},
            neighbors: neighbors.size,
            edges_count: filtered.length,
            generated_at: (graph && graph.meta && graph.meta.generated_at) || graph.generated_at || '-',
            all_edges: edgesAll.length,
            all_nodes: (graph && graph.nodes ? graph.nodes.length : 0),
          };
        }

        async function loadData() {
          actors = await (await fetch('/api/actors')).json();
          watchlist = await (await fetch('/api/watchlist')).json();
          timeline = await (await fetch('/api/timeline')).json();
          overlap = await (await fetch('/api/overlap')).json();
          network = await (await fetch('/api/network')).json();
          cacheMeta = await (await fetch('/api/cache_meta')).json();
          console.debug('NETWORK_LOADED', {
            nodes: (network.nodes||[]).length,
            edges: (network.edges||[]).length,
            generated_at: network.generated_at || (network.meta||{}).generated_at || null
          });
          document.getElementById('stat-actors').innerText = actors.length;
          document.getElementById('stat-watchlist').innerText = watchlist.length;
          renderCacheMeta();
          applyFilters();
        }

        function renderCacheMeta() {
          const cm = cacheMeta || {};
          const cov = cm.covalent || {};
          const bir = cm.birdeye || {};
          const prog = cm.progress || {};
          const el = document.getElementById('cache-meta');
          if (!el) return;
          const safe = (v,d='0') => (v===undefined || v===null || v==='') ? d : v;
          const progress = cm.progress || {};
          el.innerHTML = `
            <div class="stat"><b>Covalent</b><div class="small">
              hits: ${safe(cov.cache_hits,0)} | misses: ${safe(cov.cache_misses,0)} | calls: ${safe(cov.api_calls,0)}
              <br/>buckets: ${safe(cov.buckets_cached,0)}/${safe(cov.buckets_missing,0)}
              <br/>updated: ${safe(cov.last_updated,'-')}
            </div></div>

            <div class="stat"><b>BirdEye</b><div class="small">
              hits: ${safe(bir.cache_hits,0)} | misses: ${safe(bir.cache_misses,0)} | calls: ${safe(bir.api_calls,0)}
              <br/>pages: ${safe(bir.pages_cached,0)}/${safe(bir.pages_missing,0)}
              <br/>updated: ${safe(bir.last_updated,'-')}
            </div></div>

            <div class="stat"><b>Progress</b><div class="small">
              started: ${safe(progress.started_at,'-')}
              <br/>committed_bucket: ${safe(progress.committed_bucket,'-')}
              <br/>actor: ${safe(progress.actor_i,'-')}/${safe(progress.actor_n,'-')}
              <br/>status: ${safe(progress.status,'-')}
              <br/>last_error: ${safe(progress.last_error,'-')}
            </div></div>

            <div class="stat"><b>Meta</b><div class="small">
              first_seen: ${safe(cm.first_seen,'-')}
              last_updated: ${safe(cm.last_updated,'-')}
            </div></div>
          `;
        }

        function badge(level) {
            const cls = level || 'retail';
            return `<span class="badge ${cls}">${level||'retail'}</span>`;
        }
        function roleBadge(role) {
            if(!role) return '';
            return `<span class="badge role">${role}</span>`;
        }

        function applyFilters() {
          const term = document.getElementById('filter-search').value.toLowerCase();
          const level = document.getElementById('filter-level').value;
          const role = document.getElementById('filter-role').value;
          const minPumps = parseInt(document.getElementById('filter-min-pumps').value || '0');
          const onlyWatchlist = document.getElementById('filter-watchlist').checked;
          const onlySuccess = document.getElementById('filter-success').checked;
          const onlyBadges = document.getElementById('filter-badges').checked;
          const roiMissing = document.getElementById('filter-roi-missing').checked;
          const activeSince = document.getElementById('filter-active-since').value.trim();
          let activeSinceTs = null;
          if (activeSince) {
            const d = new Date(activeSince);
            if (!isNaN(d.getTime())) activeSinceTs = d.getTime();
          }
          let filtered = actors;
          if (term) {
            filtered = filtered.filter(a =>
              (a.address||'').toLowerCase().includes(term) ||
              (a.pumps || []).some(p => (p.symbol||'').toLowerCase().includes(term))
            );
          }
          if (level) filtered = filtered.filter(a => (a.best_pamper_level||'') === level);
          if (role) filtered = filtered.filter(a => (a.role_guess||'') === role);
          filtered = filtered.filter(a => (a.pumps_count||0) >= minPumps);
          if (onlyWatchlist) {
            const wlSet = new Set(watchlist.map(w => `${(w.chain||'').toLowerCase()}:${(w.address||'').toLowerCase()}`));
            filtered = filtered.filter(a => wlSet.has(`${(a.chain||'').toLowerCase()}:${(a.address||'').toLowerCase()}`));
          }
          if (onlySuccess) filtered = filtered.filter(a => a.covalent_success !== false);
          if (onlyBadges) filtered = filtered.filter(a => (a.badges||[]).length > 0);
          if (roiMissing) {
            filtered = filtered.filter(a => (a.pumps||[]).some(p => p.roi_reason));
          }
          if (activeSinceTs != null) {
            filtered = filtered.filter(a => {
              if (!a.last_seen) return false;
              const t = new Date(a.last_seen).getTime();
              return !isNaN(t) && t >= activeSinceTs;
            });
          }
          renderTable(filtered);
        }

        function resetFilters() {
          document.getElementById('filter-search').value = '';
          document.getElementById('filter-level').value = '';
          document.getElementById('filter-role').value = '';
          document.getElementById('filter-min-pumps').value = '0';
          document.getElementById('filter-watchlist').checked = false;
          document.getElementById('filter-success').checked = false;
          document.getElementById('filter-badges').checked = false;
          document.getElementById('filter-active-since').value = '';
          applyFilters();
        }

        function renderTable(rows) {
          const body = document.getElementById('actors-body');
          body.innerHTML = '';
          rows.forEach(a => {
            const tr = document.createElement('tr');
            const addrShort = `${(a.address||'').slice(0,6)}…${(a.address||'').slice(-4)}`;
            const badgesCnt = (a.badges||[]).length;
            const cached = (a.cache_misses||0)===0 && (a.cache_hits||0)>0;
            tr.innerHTML = `
              <td>${addrShort}</td>
              <td>${badge(a.best_pamper_level)}</td>
              <td>${roleBadge(a.role_guess)}</td>
              <td>${badgesCnt}</td>
              <td>${a.pumps_count||0}</td>
              <td>${(a.max_pamper_score||0).toFixed(2)}</td>
              <td>${((a.avg_roi||0)*100).toFixed(1)}</td>
              <td>${(a.cov_realized_pnl_usd||0).toFixed(0)}</td>
              <td>${(a.cov_unrealized_pnl_usd||0).toFixed(0)}</td>
              <td>${(a.cov_total_pnl_usd||a.cov_pnl_total_usd||0).toFixed(0)}</td>
              <td>${((a.avg_share_of_pump_volume||0)*100).toFixed(2)}</td>
              <td>${(a.actor_score||0).toFixed(2)}</td>
              <td>${(a.portfolio_value_usd||0).toFixed(0)}</td>
              <td>${cached ? '<span class="chip">cached</span>' : ''} ${a.covalent_success===false ? 'fail' : 'ok'}</td>
            `;
            tr.onclick = () => selectActor(a);
            body.appendChild(tr);
          });
        }

        function showTab(id) {
          document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
          document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
          document.querySelector(`[onclick="showTab('${id}')"]`).classList.add('active');
          document.getElementById('tab-'+id).classList.add('active');
        }

        function selectActor(a) {
          current = a;
          document.getElementById('detail-header').innerText = `${a.address} (${a.chain||''})`;
          renderSummary(a);
          renderPnl(a);
          renderHoldings(a);
          renderTimeline(a);
          renderOverlap(a);
          // fetch subgraph for this actor
          fetch(`/api/network?addr=${encodeURIComponent(a.address||'')}&chain=${encodeURIComponent(a.chain||'')}`)
            .then(r => r.json())
            .then(d => { networkForActor = d || {}; renderNetwork(a); })
            .catch(() => { networkForActor = {}; renderNetwork(a); });
          // fetch linked peers separately
          fetch(`/api/linked?addr=${encodeURIComponent(a.address||'')}&chain=${encodeURIComponent(a.chain||'')}`)
            .then(r => r.json())
            .then(d => { linkedForActor = d || {}; renderLinkedTab(a); })
            .catch(() => { linkedForActor = {}; renderLinkedTab(a); });
        }

        function selectActorByAddr(addr) {
          if (!addr) return;
          const found = actors.find(x => (x.address||'').toLowerCase() === (addr||'').toLowerCase());
          if (found) {
            selectActor(found);
          } else {
            console.warn('Actor not found for addr', addr);
          }
        }

        function listBadges(badges) {
          if (!badges || badges.length===0) return '—';
          return badges.map(b => `<span class="chip">${b}</span>`).join('');
        }
        function listFactors(factors) {
          if (!factors || factors.length===0) return '—';
          return factors.map(f => `<div class="pill">${f}</div>`).join('');
        }

        function renderSummary(a) {
          const candidates = a.likely_next_pump_candidates || a.candidates || [];
          const factors = a.actor_score_factors || {};
          const prog = cacheMeta.progress || {};
          const html = `
            <div class="stat-grid">
              <div class="stat">Level: ${badge(a.best_pamper_level)}</div>
              <div class="stat">Role: ${roleBadge(a.role_guess)} (conf ${(a.role_confidence||0).toFixed(2)})</div>
              <div class="stat">Pumps: <b>${a.pumps_count||0}</b></div>
              <div class="stat">Actor score: <b>${(a.actor_score||0).toFixed(2)}</b></div>
              <div class="stat">Realized PnL: <b>${(a.cov_realized_pnl_usd||0).toFixed(0)}$</b></div>
              <div class="stat">Unrealized PnL: <b>${(a.cov_unrealized_pnl_usd||0).toFixed(0)}$</b></div>
              <div class="stat">Total PnL: <b>${(a.cov_total_pnl_usd||a.cov_pnl_total_usd||0).toFixed(0)}$</b></div>
              <div class="stat">Portfolio: <b>${(a.portfolio_value_usd||0).toFixed(0)}$</b></div>
              <div class="stat">Status: <b>${a.covalent_success===false ? (a.fail_reason||'fail') : 'ok'}</b></div>
              <div class="stat">Last seen: <b>${a.last_seen||'-'}</b></div>
              <div class="stat">Buckets: <b>${a.buckets_fetched||0}/${a.buckets_needed||0}</b></div>
              <div class="stat">Cache: <b>hits ${a.cache_hits||0} / misses ${a.cache_misses||0}</b></div>
              <div class="stat">API calls: <b>${a.api_calls||0}</b> | 429: ${a.http_429_count||0}</div>
            </div>
            <h3>Progress</h3>
            <div class="pill">committed_bucket: ${prog.committed_bucket||'-'}</div>
            <div class="pill">started_bucket: ${prog.started_bucket||'-'}</div>
            <div class="pill">status: ${prog.status||'-'}</div>
            <div class="pill">actor progress: ${prog.actor_i||'-'}/${prog.actor_n||'-'}</div>
            <h3>Role factors</h3>
            ${listFactors(a.role_factors)}
            <h3>Actor score factors</h3>
            ${Object.keys(factors).length ? Object.entries(factors).map(([k,v])=>`<div class="pill">${k}: ${(v||0).toFixed(3)}</div>`).join('') : '—'}
            <h3>Badges</h3>
            ${listBadges(a.badges)}
            <h3>Likely next pump candidates</h3>
            ${candidates.length? candidates.map(c=>`<div class="pill">${c.symbol||c.token||'?' } ${c.usd?(''+Number(c.usd).toFixed(0)+'$'):''} (${c.reason||''})</div>`).join('') : '—'}
          `;
          document.getElementById('tab-summary').innerHTML = html;
        }

        function fmtTs(ts, iso) {
          if (ts && ts > 1e12) {
            const d = new Date(ts);
            return isNaN(d.getTime()) ? (iso||'-') : d.toISOString().slice(0,19).replace('T',' ');
          }
          if (ts && ts > 0) {
            const d = new Date(ts*1000);
            return isNaN(d.getTime()) ? (iso||'-') : d.toISOString().slice(0,19).replace('T',' ');
          }
          if (iso) return iso;
          return '-';
        }

        function renderPnl(a) {
          const pumps = a.pumps || [];
          const rows = pumps.map(p => `
            <tr>
              <td>${p.symbol||'-'}</td>
              <td>${fmtTs(p.pump_start_ts, p.pump_start)}</td>
              <td>${fmtTs(p.pump_end_ts, p.pump_end)}</td>
              <td>${(p.pnl_before_usd||0).toFixed(1)}</td>
              <td>${(p.pnl_during_usd||0).toFixed(1)}</td>
              <td>${(p.pnl_after_usd||0).toFixed(1)}</td>
              <td>${(p.pnl_total_usd||0).toFixed(1)}</td>
              <td>${p.tx_count||0}</td>
              <td>${p.roi_reason||''}</td>
            </tr>
          `).join('');
          document.getElementById('tab-pnl').innerHTML = `
            <table>
              <thead><tr><th>Symbol</th><th>Start</th><th>End</th><th>PnL before</th><th>PnL during</th><th>PnL after</th><th>Total</th><th>Tx</th><th>ROI reason</th></tr></thead>
              <tbody>${rows}</tbody>
            </table>`;
        }

        function renderHoldings(a) {
          const h = a.current_holdings || [];
          const rows = h.map(x => `
            <tr>
              <td>${x.symbol||'-'}</td>
              <td>${x.contract_address||''}</td>
              <td>${(x.balance||0).toFixed(4)}</td>
              <td>${x.usd_value!=null ? x.usd_value.toFixed(2) : '—'}</td>
            </tr>
          `).join('');
          document.getElementById('tab-holdings').innerHTML = `
            <table>
              <thead><tr><th>Symbol</th><th>Contract</th><th>Balance</th><th>USD</th></tr></thead>
              <tbody>${rows}</tbody>
            </table>`;
        }

        function renderLinkedTab(a) {
          const rowsData = linkedForActor.peers || [];
          console.debug('LINKED_PEERS', {addr:a.address, peers: rowsData.length, meta: linkedForActor.meta||{}});
          if (!rowsData.length) {
            document.getElementById('tab-links').innerHTML = `
              <div>No linked peers for ${a.address}. loaded_edges=${linkedForActor.loaded_edges||0}</div>
            `;
            return;
          }
          const rows = rowsData.map(l => `
            <tr>
              <td>${l.peer||'-'} ${l.peer_is_actor ? '<span class="chip">actor</span>' : ''}</td>
              <td>${l.dir||'-'}</td>
              <td>${l.transfers||0}</td>
              <td>${l.usd!=null? Number(l.usd).toFixed(2):'—'}</td>
              <td>${l.peer_score!=null? Number(l.peer_score).toFixed(2):''}</td>
              <td>${l.peer_pnl!=null? Number(l.peer_pnl).toFixed(2):''}</td>
            </tr>`).join('');
          document.getElementById('tab-links').innerHTML = `
            <div class="small">Peers: ${rowsData.length}, loaded_edges=${linkedForActor.loaded_edges||0}, generated_at=${linkedForActor.generated_at||'-'}</div>
            <table>
              <thead><tr><th>Peer</th><th>Dir</th><th>Transfers</th><th>USD</th><th>Score</th><th>PNL</th></tr></thead>
              <tbody>${rows}</tbody>
            </table>`;
        }

        function renderTimeline(a) {
          const key = `${(a.chain||'').toLowerCase()}:${(a.address||'').toLowerCase()}`;
          const actorTl = (timeline.actors||{})[key] || {};
          const pumpKeys = Object.keys(actorTl.pumps||{});
          if (pumpKeys.length===0) {
            document.getElementById('tab-timeline').innerHTML = 'No timeline data';
            return;
          }
          let html = '';
          pumpKeys.forEach(pk => {
            const p = actorTl.pumps[pk];
            const rows = (p.t||[]).map((t,i)=>`
              <tr>
                <td>${t}</td>
                <td>${(p.net_usd[i]||0).toFixed(2)}</td>
                <td>${p.price && p.price[i]!=null ? p.price[i].toFixed(4) : '—'}</td>
                <td>${p.tx_count && p.tx_count[i]!=null ? p.tx_count[i] : ''}</td>
              </tr>`).join('');
            html += `<h3>${pk}</h3><table>
              <thead><tr><th>Time</th><th>Net USD</th><th>Price</th><th>Tx</th></tr></thead>
              <tbody>${rows}</tbody></table>`;
          });
          document.getElementById('tab-timeline').innerHTML = html;
        }

        function renderOverlap(a) {
          if (!overlap.tokens || !overlap.actors) {
            document.getElementById('tab-overlap').innerHTML = 'No overlap data';
            return;
          }
          const key = `${(a.chain||'').toLowerCase()}:${(a.address||'').toLowerCase()}`;
          const idx = overlap.actors.indexOf(key);
          if (idx === -1) {
            document.getElementById('tab-overlap').innerHTML = 'No overlap row for actor';
            return;
          }
          const row = overlap.matrix_usd[idx] || [];
          const pairs = overlap.tokens.map((t,i)=>({token:t, usd: row[i]||0}));
          pairs.sort((x,y)=> (y.usd||0)-(x.usd||0));
          const rows = pairs.slice(0,30).map(p=>`<tr><td>${p.token}</td><td>${p.usd.toFixed(2)}</td><td>${(overlap.token_rarity||{})[p.token]||''}</td></tr>`).join('');
          document.getElementById('tab-overlap').innerHTML = `
            <div>Top common tokens: ${(overlap.top_common_tokens||[]).join(', ')}</div>
            <div>Suspicious tokens: ${(overlap.top_suspicious_tokens||[]).join(', ')}</div>
            <table><thead><tr><th>Token</th><th>USD</th><th>Rarity</th></tr></thead><tbody>${rows}</tbody></table>`;
        }

        function renderNetwork(a) {
          const graph = (networkForActor && (networkForActor.edges||[]).length) ? networkForActor : network;
          const view = computeNetworkView(graph, a.address||'');
          console.debug('NETWORK_VIEW', {incoming:view.incoming.length, outgoing:view.outgoing.length, neighbors:view.neighbors, edges:view.edges_count});
          const nodeMap = {};
          if (graph && graph.nodes) {
            graph.nodes.forEach(n => {
              const nid = normalizeId(n.id || n.address || '');
              if (nid) nodeMap[nid] = n;
            });
          }
          const incoming = view.incoming;
          const outgoing = view.outgoing;
          if (!incoming.length && !outgoing.length) {
            document.getElementById('tab-network').innerHTML = `
              <div>No edges for ${a.address}. loaded_edges=${view.all_edges||0}, loaded_nodes=${view.all_nodes||0}, generated_at=${view.generated_at||'-'}</div>
            `;
            return;
          }
          const renderGraph = () => {
            const neighbors = [...view.incoming, ...view.outgoing];
            const uniquePeers = Array.from(new Set(neighbors.map(p => p.peer)));
            if (uniquePeers.length === 0 || uniquePeers.length > 30) {
              return '<div class="small">Graph skipped (neighbors ' + uniquePeers.length + ')</div>';
            }
            const centerX = 220, centerY = 150, r = 120;
            let maxVal = 0;
            neighbors.forEach(n => { maxVal = Math.max(maxVal, n.value||0); });
            const norm = (v) => maxVal > 0 ? (1 + 4 * Math.log10(1+v) / Math.log10(1+maxVal)) : 1;
            let lines = '';
            uniquePeers.forEach((peer, idx) => {
              const angle = (idx/uniquePeers.length) * Math.PI * 2;
              const px = centerX + r * Math.cos(angle);
              const py = centerY + r * Math.sin(angle);
              const edge = neighbors.find(n => n.peer === peer) || {};
              const w = norm(edge.value||0);
              const color = colorForPeer(peer);
              const isSelected = selectedPeer && normalizeId(selectedPeer) === normalizeId(peer);
              const isHovered = hoveredPeer && normalizeId(hoveredPeer) === normalizeId(peer);
              const lineOpacity = selectedPeer ? (isSelected ? 1 : 0.35) : (isHovered ? 0.9 : 0.75);
              const strokeW = isSelected ? (w + 3) : (isHovered ? (w + 2) : w);
              const dir = edge.dir || '';
              lines += `<line x1="${centerX}" y1="${centerY}" x2="${px}" y2="${py}" stroke="transparent" stroke-width="16" opacity="0.01" data-peer="${peer}" data-dir="${dir}" class="edge-hit" style="cursor:pointer;pointer-events:stroke" />
                        <line x1="${centerX}" y1="${centerY}" x2="${px}" y2="${py}" stroke="${color}" stroke-width="${strokeW}" opacity="${lineOpacity}" data-peer="${peer}" data-dir="${dir}" class="edge-line" style="cursor:pointer" />
                        <text x="${(centerX+px)/2}" y="${(centerY+py)/2}" fill="#8B9BBE" font-size="10">$${(edge.value||0).toFixed(0)}</text>
                        <circle cx="${px}" cy="${py}" r="${isSelected?10:(isHovered?9:8)}" fill="${color}" ${isSelected?'stroke="white" stroke-width="2"':''} data-peer="${peer}" class="peer-node" />`;
            });
            const svg = `<svg id="net-graph" width="440" height="300" viewBox="0 0 440 300" style="background:#0F1520;border-radius:12px;padding:8px;">
              <circle cx="${centerX}" cy="${centerY}" r="10" fill="#12F4C0" />
              ${lines}
            </svg>`;
            return svg;
          };
          const renderRows = (arr) => arr.map(r => {
            const pn = nodeMap[normalizeId(r.peer||'')] || {};
            const badge = pn.is_actor ? '<span class="chip">actor</span>' : '';
            const score = pn.score!=null ? ` <span class="small">score ${Number(pn.score).toFixed(2)}</span>` : '';
            const pnl = pn.pnl!=null ? ` <span class="small">pnl ${Number(pn.pnl).toFixed(0)}$</span>` : '';
            const isSel = selectedPeer && normalizeId(selectedPeer) === normalizeId(r.peer||'');
            const color = colorForPeer(r.peer);
            const cls = isSel ? 'row-selected' : (selectedPeer ? 'row-dimmed' : '');
            const rid = `row-${r.dir||'out'}-${normalizeId(r.peer||'')}`;
            return `
            <tr id="${rid}" class="${cls}" data-peer="${r.peer}" data-dir="${r.dir||''}">
              <td style="border-left:3px solid ${isSel?color:'transparent'}">${r.peer} ${badge}${score}${pnl}</td>
              <td>${r.transfers}</td>
              <td>${(Number(r.value)||0).toFixed(2)}</td>
            </tr>`;
          }).join('');
          const outRows = renderRows(outgoing);
          const inRows = renderRows(incoming);
          document.getElementById('tab-network').innerHTML = `
            <div class="small">Incoming: $${view.totals.in_value.toFixed(2)} (${view.totals.in_tx} tx) |
            Outgoing: $${view.totals.out_value.toFixed(2)} (${view.totals.out_tx} tx) |
            Neighbors: ${view.neighbors} | Edges: ${view.edges_count}/${view.all_edges} | generated_at=${view.generated_at}</div>
            ${renderGraph()}
            <h4>Outgoing</h4>
            <table><thead><tr><th>Peer</th><th>Transfers</th><th>Value</th></tr></thead><tbody>${outRows}</tbody></table>
            <h4>Incoming</h4>
            ${incoming.length ? `<table><thead><tr><th>Peer</th><th>Transfers</th><th>Value</th></tr></thead><tbody>${inRows}</tbody></table>` : '<div class="small">No incoming edges for this actor in current window.</div>'}
          `;
          document.querySelectorAll('#tab-network .peer-node').forEach(el => {
            el.addEventListener('click', () => selectActorByAddr && selectActorByAddr(el.dataset.peer));
          });
          document.querySelectorAll('#tab-network tr[data-peer]').forEach(el => {
            el.style.cursor = 'pointer';
            el.addEventListener('click', () => {
              const peer = normalizeId(el.dataset.peer);
              const dir = el.dataset.dir || '';
              if (selectedPeer && normalizeId(selectedPeer) === peer) {
                selectedPeer = null; selectedDir = null;
              } else {
                selectedPeer = peer; selectedDir = dir;
              }
              renderNetwork(current);
            });
          });
          document.querySelectorAll('#tab-network .edge-hit').forEach(el => {
            el.addEventListener('mouseenter', () => { hoveredPeer = normalizeId(el.dataset.peer); hoveredDir = el.dataset.dir||''; renderNetwork(current); });
            el.addEventListener('mouseleave', () => { hoveredPeer = null; hoveredDir = null; renderNetwork(current); });
            el.addEventListener('click', (ev) => {
              ev.stopPropagation();
              const peer = normalizeId(el.dataset.peer);
              const dir = el.dataset.dir || '';
              if (selectedPeer && normalizeId(selectedPeer) === peer) {
                selectedPeer = null; selectedDir = null;
              } else {
                selectedPeer = peer; selectedDir = dir;
              }
              renderNetwork(current);
            });
          });
          const scrollToSelectedRow = () => {
            if (!selectedPeer) return;
            const key = `${selectedDir||'any'}:${normalizeId(selectedPeer)}`;
            if (key === lastScrolledKey) return;
            const peerLower = normalizeId(selectedPeer);
            const tryIds = [
              `row-${selectedDir||'out'}-${peerLower}`,
              `row-out-${peerLower}`,
              `row-in-${peerLower}`,
            ];
            for (const rid of tryIds) {
              const el = document.getElementById(rid);
              if (el) {
                el.scrollIntoView({behavior:'smooth', block:'center'});
                lastScrolledKey = key;
                return;
              }
            }
          };
          scrollToSelectedRow();
        }

        document.addEventListener('DOMContentLoaded', loadData);
      </script>
    </body>
    </html>
    """

    @app.route("/")
    def index():
        return render_template_string(template)

    webbrowser.open(f"http://{host}:{port}/")
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    run_watchlist_ui()
