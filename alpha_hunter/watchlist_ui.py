import json
import os
import webbrowser
from typing import Any, Dict, List

from flask import Flask, jsonify, render_template_string


def _load_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def run_watchlist_ui(
    actors_path: str = "data/alpha_profiler/actors.json",
    watchlist_path: str = "data/alpha_profiler/watchlist.json",
    pnl_path: str = "data/alpha_profiler/pnl_watchlist.json",
    host: str = "127.0.0.1",
    port: int = 8050,
) -> None:
    app = Flask(__name__)

    actors_data: List[Dict[str, Any]] = _load_json(actors_path) or []
    watchlist_data: List[Dict[str, Any]] = _load_json(watchlist_path) or []
    pnl_data: Dict[str, Any] = _load_json(pnl_path) or {}
    pnl_actors = (
        {f"{a.get('address','').lower()}:{a.get('chain','').lower()}": a for a in (pnl_data.get("actors") or [])}
        if isinstance(pnl_data, dict)
        else {}
    )

    template = r"""
    <!DOCTYPE html>
    <html lang="uk">
    <head>
      <meta charset="UTF-8" />
      <title>Alpha Hunter — Watchlist</title>
      <style>
        body { margin:0; padding:0; font-family: 'Inter', sans-serif; background:#050812; color:#E5ECFF; }
        .container { display:grid; grid-template-columns: 280px 1fr 460px; gap:16px; padding:20px; }
        .card { background:#0B1018; border-radius:16px; padding:16px; box-shadow:0 10px 30px rgba(0,0,0,0.25); }
        h1 { margin:0; font-size:22px; color:#12F4C0; }
        h2 { margin:0 0 6px; font-size:16px; color:#8B9BBE; }
        .badge { display:inline-block; padding:4px 10px; border-radius:12px; font-size:12px; }
        .core_operator { background:rgba(18,244,192,0.15); color:#12F4C0; }
        .inner_circle { background:rgba(18,166,244,0.15); color:#12A6F4; }
        .outer_circle { background:rgba(255,193,7,0.15); color:#FFC107; }
        .retail { background:rgba(255,255,255,0.08); color:#8B9BBE; }
        input, select { width:100%; padding:10px 14px; background:#0F1520; color:#E5ECFF; border:1px solid rgba(255,255,255,0.08); border-radius:999px; outline:none; }
        input:focus { border-color:#12F4C0; }
        .filters button { width:100%; padding:10px; background:rgba(18,244,192,0.1); border:1px solid rgba(18,244,192,0.3); color:#12F4C0; border-radius:999px; cursor:pointer; }
        .filters button:hover { background:rgba(18,244,192,0.2); }
        table { width:100%; border-collapse:collapse; }
        th, td { padding:10px; border-bottom:1px solid rgba(255,255,255,0.05); text-align:left; }
        tr:hover { background:rgba(255,255,255,0.04); cursor:pointer; }
        .small { color:#8B9BBE; font-size:12px; }
        .stat-grid { display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:8px; }
        .stat { background:#0F1520; border-radius:12px; padding:10px; }
        .pill { background:#0F1520; border-radius:12px; padding:6px 10px; display:inline-block; margin-right:6px; }
        .tabs { display:flex; gap:8px; margin-top:8px; }
        .tab-btn { padding:6px 10px; border-radius:10px; border:1px solid rgba(255,255,255,0.08); background:#0F1520; color:#E5ECFF; cursor:pointer; }
        .tab-btn.active { border-color:#12F4C0; color:#12F4C0; }
        .tab-content { display:none; margin-top:10px; }
        .tab-content.active { display:block; }
      </style>
    </head>
    <body>
      <div class="container">
        <div class="card filters">
          <h1>Alpha Hunter — Watchlist</h1>
          <h2>Моніторинг акторів пампів</h2>
          <div class="stat-grid">
            <div class="stat">Акторів у базі: <b id="stat-actors">0</b></div>
            <div class="stat">У watchlist: <b id="stat-watchlist">0</b></div>
          </div>
          <h2 style="margin-top:16px;">Пошук</h2>
          <input id="filter-search" placeholder="Адреса або тикер" oninput="applyFilters()" />
          <h2 style="margin-top:12px;">Рівень</h2>
          <select id="filter-level" onchange="applyFilters()">
            <option value="">будь-який</option>
            <option value="core_operator">core_operator</option>
            <option value="inner_circle">inner_circle</option>
            <option value="outer_circle">outer_circle</option>
            <option value="retail">retail</option>
          </select>
          <h2 style="margin-top:12px;">Мін. кількість пампів</h2>
          <input id="filter-min-pumps" type="number" min="0" value="0" oninput="applyFilters()" />
          <div style="margin-top:12px;">
            <label><input type="checkbox" id="filter-watchlist" onchange="applyFilters()" /> тільки з watchlist</label>
          </div>
          <div style="margin-top:12px;">
            <button onclick="resetFilters()">Скинути фільтри</button>
          </div>
        </div>

        <div class="card">
          <h2>Список акторів</h2>
          <table id="actors-table">
            <thead>
              <tr>
                <th>Адреса</th>
                <th>Рівень</th>
                <th>Пампів</th>
                <th>Max score</th>
                <th>Avg ROI, %</th>
                <th>Realized PnL, $</th>
                <th>Unrealized, $</th>
                <th>Total PnL, $</th>
                <th>Частка обсягу, %</th>
                <th>Actor score</th>
                <th>Portfolio, $</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody id="actors-body"></tbody>
          </table>
        </div>

        <div class="card" id="actor-detail">
          <h2>Деталі актора</h2>
          <div id="detail-header" class="small">Оберіть актора з таблиці...</div>
          <div class="tabs">
            <button class="tab-btn active" onclick="showTab('summary')">Summary</button>
            <button class="tab-btn" onclick="showTab('pnl')">PnL by pump</button>
            <button class="tab-btn" onclick="showTab('holdings')">Holdings</button>
            <button class="tab-btn" onclick="showTab('links')">Linked</button>
          </div>
          <div id="tab-summary" class="tab-content active"></div>
          <div id="tab-pnl" class="tab-content"></div>
          <div id="tab-holdings" class="tab-content"></div>
          <div id="tab-links" class="tab-content"></div>
        </div>
      </div>

      <script>
        let actors = [];
        let watchlist = [];
        let current = null;

        async function loadData() {
          const aResp = await fetch('/api/actors');
          actors = await aResp.json();
          const wResp = await fetch('/api/watchlist');
          watchlist = await wResp.json();
          document.getElementById('stat-actors').innerText = actors.length;
          document.getElementById('stat-watchlist').innerText = watchlist.length;
          applyFilters();
        }

        function badge(level) {
            const cls = level || 'retail';
            return `<span class="badge ${cls}">${level||'retail'}</span>`;
        }

        function applyFilters() {
          const term = document.getElementById('filter-search').value.toLowerCase();
          const level = document.getElementById('filter-level').value;
          const minPumps = parseInt(document.getElementById('filter-min-pumps').value || '0');
          const onlyWatchlist = document.getElementById('filter-watchlist').checked;
          let filtered = actors;
          if (term) {
            filtered = filtered.filter(a =>
              a.address.toLowerCase().includes(term) ||
              (a.pumps || []).some(p => (p.symbol||'').toLowerCase().includes(term))
            );
          }
          if (level) filtered = filtered.filter(a => (a.best_pamper_level||'') === level);
          filtered = filtered.filter(a => (a.pumps_count||0) >= minPumps);
          if (onlyWatchlist) {
            const wlSet = new Set(watchlist.map(w => (w.address||'').toLowerCase()));
            filtered = filtered.filter(a => wlSet.has((a.address||'').toLowerCase()));
          }
          renderTable(filtered);
        }

        function resetFilters() {
          document.getElementById('filter-search').value = '';
          document.getElementById('filter-level').value = '';
          document.getElementById('filter-min-pumps').value = '0';
          document.getElementById('filter-watchlist').checked = false;
          applyFilters();
        }

        function renderTable(rows) {
          const body = document.getElementById('actors-body');
          body.innerHTML = '';
          rows.forEach(a => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
              <td>${(a.address||'').slice(0,6)}…${(a.address||'').slice(-4)}</td>
              <td>${badge(a.best_pamper_level)}</td>
              <td>${a.pumps_count||0}</td>
              <td>${(a.max_pamper_score||0).toFixed(2)}</td>
              <td>${((a.avg_roi||0)*100).toFixed(1)}</td>
              <td>${(a.cov_realized_pnl_usd||0).toFixed(0)}</td>
              <td>${(a.cov_unrealized_pnl_usd||0).toFixed(0)}</td>
              <td>${(a.cov_total_pnl_usd||0).toFixed(0)}</td>
              <td>${((a.avg_share_of_pump_volume||0)*100).toFixed(2)}</td>
              <td>${(a.actor_score||0).toFixed(2)}</td>
              <td>${(a.portfolio_value_usd||0).toFixed(0)}</td>
              <td>${(a.covalent_success===false?'FAIL':(a.covalent_success?'OK':'N/A'))}</td>
            `;
            tr.onclick = () => renderDetail(a);
            body.appendChild(tr);
          });
        }

        function showTab(name) {
          document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
          document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
          document.querySelector(`.tab-btn[onclick="showTab('${name}')"]`).classList.add('active');
          document.getElementById('tab-'+name).classList.add('active');
        }

        function renderDetail(a) {
          current = a;
          document.getElementById('detail-header').innerHTML = `
            <div><b>Адреса:</b> ${a.address}</div>
            <div><b>Рівень:</b> ${a.best_pamper_level||'n/a'} | Пампів: ${a.pumps_count||0}</div>
            <div><b>Max pamper score:</b> ${(a.max_pamper_score||0).toFixed(2)} | Actor score: ${(a.actor_score||0).toFixed(2)}</div>
            <div><b>ROI:</b> ${((a.avg_roi||0)*100).toFixed(2)}%</div>
          `;

          const pnlAgg = a.pnl_aggregate || {};
          document.getElementById('tab-summary').innerHTML = `
            <div><b>Realized / Unrealized / Total:</b>
              ${(a.cov_realized_pnl_usd||0).toFixed(2)} /
              ${(a.cov_unrealized_pnl_usd||0).toFixed(2)} /
              ${(a.cov_total_pnl_usd||0).toFixed(2)}
            </div>
            <div><b>Портфель:</b> ${(a.portfolio_value_usd||0).toFixed(0)} $</div>
            <div><b>Covalent статус:</b> ${(a.covalent_success===false?'FAIL':(a.covalent_success?'OK':'N/A'))} ${a.fail_reason||''}</div>
            <div class="pill">buckets: ${a.buckets_fetched||0}</div>
            <div class="pill">cache hits: ${a.cache_hits||0}</div>
            <div class="pill">429: ${a.http_429_count||0}</div>
            <div class="pill">errors: ${a.errors_count||0}</div>
            <div class="pill">tx: ${a.tx_items_count||0}</div>
            <div class="pill">sleep s: ${(a.sleep_seconds_total||0).toFixed(1)}</div>
            <div class="pill">fees: ${(a.cov_fees_usd||0).toFixed(2)}</div>
          `;

          const actorPumps = a.pumps || [];
          let pnlRows = '';
          actorPumps.forEach(p => {
            pnlRows += `
              <tr>
                <td>${p.symbol||''}</td>
                <td>${(p.pnl_before_usd||0).toFixed(2)}</td>
                <td>${(p.pnl_during_usd||0).toFixed(2)}</td>
                <td>${(p.pnl_after_usd||0).toFixed(2)}</td>
                <td>${(p.pnl_total_usd||0).toFixed(2)}</td>
                <td>${p.tx_count||0}</td>
                <td>${p.buckets_fetched||0}</td>
                <td>${p.pnl_mode||''}</td>
              </tr>
            `;
          });
          document.getElementById('tab-pnl').innerHTML = `
            <table style="width:100%; font-size:12px;">
              <thead><tr>
                <th>Symbol</th><th>Before</th><th>During</th><th>After</th><th>Total</th><th>Tx</th><th>Buckets</th><th>Mode</th>
              </tr></thead>
              <tbody>${pnlRows || '<tr><td colspan="7">Немає даних</td></tr>'}</tbody>
            </table>
          `;

          const holdings = a.current_holdings || [];
          let hRows = '';
          holdings.forEach(h => {
            hRows += `
              <tr>
                <td>${h.symbol||h.contract_address||''}</td>
                <td>${(h.balance||0).toFixed(4)}</td>
                <td>${h.usd_value==null?'—':(h.usd_value||0).toFixed(2)}</td>
              </tr>
            `;
          });
          document.getElementById('tab-holdings').innerHTML = `
            <table style="width:100%; font-size:12px;">
              <thead><tr><th>Token</th><th>Balance</th><th>USD</th></tr></thead>
              <tbody>${hRows || '<tr><td colspan="3">Немає даних</td></tr>'}</tbody>
            </table>
          `;

          const linked = a.linked_addresses || [];
          let lRows = '';
          linked.forEach(l => {
            lRows += `
              <tr>
                <td>${l.address}</td>
                <td>${l.transfers_count||0}</td>
                <td>${(l.total_value_usd||0).toFixed(2)}</td>
                <td>${l.direction||''}</td>
              </tr>
            `;
          });
          document.getElementById('tab-links').innerHTML = `
            <table style="width:100%; font-size:12px;">
              <thead><tr><th>Address</th><th>Transfers</th><th>USD</th><th>Dir</th></tr></thead>
              <tbody>${lRows || '<tr><td colspan="4">Немає даних</td></tr>'}</tbody>
            </table>
          `;
        }

        loadData();
      </script>
    </body>
    </html>
    """

    @app.route("/")
    def index():
        return render_template_string(template)

    @app.route("/api/actors")
    def api_actors():
        merged = []
        for a in actors_data:
            key = f"{a.get('address','').lower()}:{a.get('chain','').lower()}"
            pnl = pnl_actors.get(key, {})
            combined = {**a}
            if "pumps" in a:
                combined["actor_pumps"] = a.get("pumps")
            if pnl:
                combined.update({k: v for k, v in pnl.items() if k not in {"address", "chain", "pumps"}})
                combined["pnl_pumps"] = pnl.get("pumps", [])
                combined["pnl_aggregate"] = pnl.get("pnl_aggregate", {})
            merged.append(combined)
        return jsonify(merged)

    @app.route("/api/watchlist")
    def api_watchlist():
        return jsonify(watchlist_data)

    @app.route("/api/actor/<address>")
    def api_actor(address: str):
        target = None
        for a in actors_data:
            if a.get("address", "").lower() == address.lower():
                target = a
                break
        if target is None:
            return jsonify({}), 404
        key = f"{target.get('address','').lower()}:{target.get('chain','').lower()}"
        pnl = pnl_actors.get(key, {})
        merged = {**target}
        if "pumps" in target:
            merged["actor_pumps"] = target.get("pumps")
        if pnl:
            merged.update({k: v for k, v in pnl.items() if k not in {"address", "chain", "pumps"}})
            merged["pnl_pumps"] = pnl.get("pumps", [])
            merged["pnl_aggregate"] = pnl.get("pnl_aggregate", {})
        return jsonify(merged)

    @app.route("/api/pnl_watchlist")
    def api_pnl_watchlist():
        actors = pnl_data.get("actors") if isinstance(pnl_data, dict) else None
        return jsonify(actors or [])

    webbrowser.open(f"http://{host}:{port}/")
    app.run(host=host, port=port, debug=False)
