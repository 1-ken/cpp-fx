#include "controllers/Dashboard.h"

namespace ctraderplus::controllers {

const char *kDashboardHtml = R"HTML(<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>cTrader Plus - Live Dashboard</title>
<style>
  *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
  :root{--bg:#0f1117;--surface:#1a1d27;--border:#2a2d3e;--text:#e2e8f0;--muted:#94a3b8;
    --green:#22c55e;--yellow:#f59e0b;--red:#ef4444;--blue:#3b82f6}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;font-size:14px;min-height:100vh}
  #banner{padding:14px 24px;font-weight:700;display:flex;align-items:center;gap:10px}
  #banner.ok{background:#052e16;color:var(--green)}
  #banner.degraded{background:#1c1003;color:var(--yellow)}
  #banner.down{background:#1f0202;color:var(--red)}
  #banner.loading{background:var(--surface);color:var(--muted)}
  .dot{width:10px;height:10px;border-radius:50%;background:currentColor}
  .container{max-width:1100px;margin:0 auto;padding:24px 20px}
  h1{font-size:20px;margin-bottom:20px}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px;margin-bottom:24px}
  .card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px 16px;display:flex;flex-direction:column;gap:6px}
  .label{font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-weight:600}
  .value{font-size:18px;font-weight:700}
  table{width:100%;border-collapse:collapse;background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden}
  th{text-align:left;font-size:11px;color:var(--muted);text-transform:uppercase;padding:8px 10px;border-bottom:1px solid var(--border)}
  td{padding:8px 10px;border-bottom:1px solid var(--border);font-size:13px}
  .up{color:var(--green);font-weight:600}.down-pct{color:var(--red);font-weight:600}
  .footer{font-size:11px;color:var(--muted);text-align:center;padding-top:12px}
</style></head>
<body>
<div id="banner" class="loading"><span class="dot"></span><span id="banner-text">Connecting...</span></div>
<div class="container">
  <h1>cTrader Plus - Live Dashboard</h1>
  <div class="grid">
    <div class="card"><span class="label">Overall</span><span id="c-status" class="value">-</span></div>
    <div class="card"><span class="label">cTrader</span><span id="c-observer" class="value">-</span></div>
    <div class="card"><span class="label">Redis</span><span id="c-redis" class="value">-</span></div>
    <div class="card"><span class="label">PostgreSQL</span><span id="c-postgres" class="value">-</span></div>
    <div class="card"><span class="label">Uptime</span><span id="c-uptime" class="value">-</span></div>
    <div class="card"><span class="label">WS Subscribers</span><span id="c-subs" class="value">-</span></div>
    <div class="card"><span class="label">Snapshot Age</span><span id="c-age" class="value">-</span></div>
    <div class="card"><span class="label">Failures</span><span id="c-fail" class="value">-</span></div>
  </div>
  <table><thead><tr><th>Market</th><th>Pair</th><th>Price</th><th>Change %</th></tr></thead>
  <tbody id="tbody"><tr><td colspan="4" style="color:var(--muted)">Waiting for snapshot...</td></tr></tbody></table>
  <div class="footer">Last updated: <strong id="updated">-</strong></div>
</div>
<script>
const POLL=5000;let ws=null,backoff=1000;
function banner(s){const b=document.getElementById('banner'),t=document.getElementById('banner-text');
  b.className=s||'loading';t.textContent=s==='ok'?'All systems operational':s==='degraded'?'Degraded':s==='down'?'Down':'Connecting...';}
function fmtUptime(s){if(s==null)return '-';const h=Math.floor(s/3600),m=Math.floor((s%3600)/60);return h>0?h+'h '+m+'m':m+'m';}
function rows(d){let out=[];const p=d&&d.pairs;if(!p)return out;
  (p.currencies||[]).forEach(r=>out.push({...r,market:'currencies'}));
  (p.commodities||[]).forEach(r=>out.push({...r,market:'commodities'}));return out;}
function render(d){const tb=document.getElementById('tbody');const rs=rows(d);
  if(!rs.length){tb.innerHTML='<tr><td colspan="4" style="color:var(--muted)">No data</td></tr>';return;}
  tb.innerHTML=rs.map(i=>{const pr=(typeof i.price==='number')?i.price:parseFloat(i.price);
    const ch=(i.change==null)?null:(typeof i.change==='number'?i.change:parseFloat(i.change));
    const chc=ch==null?'<td>-</td>':(ch>=0?'<td class="up">+'+ch.toFixed(2)+'%</td>':'<td class="down-pct">'+ch.toFixed(2)+'%</td>');
    return '<tr><td>'+i.market+'</td><td>'+(i.pair||'-')+'</td><td>'+(isNaN(pr)?'-':pr)+'</td>'+chc+'</tr>';}).join('');
  document.getElementById('updated').textContent=new Date().toLocaleTimeString();}
async function resolveWs(){try{const r=await fetch('/client-config');if(r.ok){const c=await r.json();if(c.wsUrl)return c.wsUrl;}}catch(e){}
  return (location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws/observe';}
async function connect(){const url=await resolveWs();try{ws=new WebSocket(url);}catch(e){setTimeout(connect,backoff);backoff=Math.min(backoff*2,10000);return;}
  ws.onopen=()=>backoff=1000;ws.onmessage=e=>{try{render(JSON.parse(e.data));}catch(_){}};
  ws.onclose=()=>{setTimeout(connect,backoff);backoff=Math.min(backoff*2,10000);};ws.onerror=()=>{try{ws.close();}catch(_){}}}
async function poll(){try{const [h,s]=await Promise.all([fetch('/health'),fetch('/stream-health')]);
  const hj=await h.json(),sj=await s.json(),c=hj.checks||{};banner(hj.status);
  document.getElementById('c-status').textContent=hj.status||'-';
  document.getElementById('c-observer').textContent=c.observer||'-';
  document.getElementById('c-redis').textContent=(c.redis||'-').split(':')[0];
  document.getElementById('c-postgres').textContent=(c.postgres||'-').split(':')[0];
  document.getElementById('c-uptime').textContent=fmtUptime(c.uptime_seconds);
  document.getElementById('c-subs').textContent=sj.subscriber_count??'-';
  document.getElementById('c-age').textContent=sj.last_snapshot_age_seconds!=null?sj.last_snapshot_age_seconds.toFixed(1)+' s':'-';
  document.getElementById('c-fail').textContent=sj.consecutive_snapshot_failures??'-';
}catch(e){banner(null);}}
connect();poll();setInterval(poll,POLL);
</script>
</body></html>)HTML";

}  // namespace ctraderplus::controllers
