const NEWS = ["AI","국무부","국방부","텍사스","관세"];
const SOCIAL = ["소셜 (Helberg)","소셜 (Rubio)","소셜 (Hegseth)"];
let state = { active:"AI", boot:null, adminPassword:sessionStorage.getItem("gpa_admin")||"" };

const $ = s => document.querySelector(s);
const esc = v => String(v ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
const api = async (url, opts={}) => {
  const r = await fetch(url, opts);
  let d={}; try{d=await r.json()}catch{}
  if(!r.ok) throw new Error(d.error || `HTTP ${r.status}`);
  return d;
};
const adminHeaders = () => ({ "content-type":"application/json", "x-admin-password":state.adminPassword });

function fmt(dt){
  if(!dt) return "시간 정보 없음";
  return new Intl.DateTimeFormat("ko-KR",{timeZone:"Asia/Seoul",month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",hour12:false}).format(new Date(dt))+" (KST)";
}
function renderTabs(){
  $("#tabs").innerHTML=(state.boot.categories||[]).map(c=>`<button class="tab ${c===state.active?"active":""}" data-cat="${esc(c)}">${esc(c)}</button>`).join("");
  document.querySelectorAll(".tab").forEach(b=>b.onclick=()=>{state.active=b.dataset.cat;renderTabs();loadArticles();});
}
function renderInfo(){
  const box=$("#categoryInfo");
  if(SOCIAL.includes(state.active)){
    box.textContent="무료 공개 X 센싱 대상: "+(state.boot.socialAccounts?.[state.active]||[]).join(" · ")+" · X 공개 임베드 기반 best-effort 방식";
  }else{
    const kws=state.boot.keywordMap?.[state.active]||[];
    box.textContent=kws.length?"현재 키워드: "+kws.map(k=>`"${k}"`).join(" · "):"등록된 키워드가 없습니다.";
  }
}
function articleHtml(a){
  const social=SOCIAL.includes(state.active);
  const tags=(a.tags||[]).map(t=>`<span class="tag">${social?esc(t):"#"+esc(t).replaceAll(" ","_")}</span>`).join("");
  const summary=a.summary?`<div class="summary"><b>🤖 3줄 요약</b>\n${esc(a.summary)}</div>`:`<div class="notice">🤖 아직 3줄 요약이 생성되지 않았습니다.</div>`;
  return `<article class="article" data-id="${a.id}">
    <h2>${social?"🗣️":"⭐"} ${esc(a.source||"Unknown")}${social?"":" | "+esc(a.title||"(제목 없음)")}</h2>
    <div class="tags">${tags}</div>
    ${social?`<p>${esc(a.description||a.title)}</p>`:""}
    <div class="meta"><span>🗞️ ${social?"게시":"기사 발행"}: ${fmt(a.published_at)}</span><span>📡 최초 감지: ${fmt(a.detected_at)}</span></div>
    ${summary}
    <div class="article-actions">
      ${a.summary?"":`<button class="ghost summaryBtn" data-id="${a.id}">🤖 이 ${social?"게시물":"기사"} 3줄 요약 생성</button>`}
      <a class="ghost" href="${esc(a.link)}" target="_blank" rel="noopener">🔗 ${social?"X":"기사"} 원문 보기</a>
    </div>
  </article>`;
}
async function loadArticles(){
  renderInfo(); $("#status").innerHTML='<div class="notice">불러오는 중…</div>'; $("#articles").innerHTML="";
  try{
    const basis=$("#basisSelect").value, period=$("#periodSelect").value;
    const d=await api(`/api/articles?category=${encodeURIComponent(state.active)}&basis=${basis}&period=${period}`);
    const label=SOCIAL.includes(state.active)?"게시물":"기사";
    $("#status").innerHTML=`<div class="result-banner">조건에 맞는 ${label} ${d.articles.length}개</div>`;
    $("#articles").innerHTML=d.articles.map(articleHtml).join("") || '<div class="notice">해당 기간에 저장된 내용이 없습니다.</div>';
    document.querySelectorAll(".summaryBtn").forEach(b=>b.onclick=()=>summarize(Number(b.dataset.id),b));
  }catch(e){$("#status").innerHTML=`<div class="error">${esc(e.message)}</div>`;}
}
async function summarize(id,btn){
  btn.disabled=true;btn.textContent="요약 중…";
  try{await api("/api/summary",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({articleId:id})});await boot();await loadArticles();}
  catch(e){alert(e.message);btn.disabled=false;btn.textContent="🤖 3줄 요약 생성";}
}
async function boot(){
  try{
    state.boot=await api("/api/bootstrap");
    $("#dbStat").textContent=`현재 DB 저장 기사: ${state.boot.totalArticles}개 · 오늘 AI 요약: 자동 ${state.boot.quota.auto_used}/10 · 수동 ${state.boot.quota.manual_used}/10`;
    if(!state.boot.categories.includes(state.active)) state.active=state.boot.categories[0]||"AI";
    renderTabs();
  }catch(e){
    $("#dbStat").textContent="DB 연결 설정이 아직 필요합니다.";
    $("#status").innerHTML=`<div class="error">${esc(e.message)}</div>`;
    throw e;
  }
}
function parseLines(v){return [...new Set(v.split(/[\n,]+/).map(x=>x.trim()).filter(Boolean))];}
async function unlockSettings(){
  const pw=$("#adminPassword").value || state.adminPassword;
  try{
    state.adminPassword=pw;
    const d=await api("/api/settings",{headers:{"x-admin-password":pw}});
    sessionStorage.setItem("gpa_admin",pw);
    $("#loginPane").hidden=true;$("#settingsPane").hidden=false;
    $("#keywordEditors").innerHTML=NEWS.map(name=>`<div class="keyword-block"><label>🔎 ${esc(name)} 키워드<textarea data-keycat="${esc(name)}" rows="6">${esc((d.keywordMap[name]||[]).join("\n"))}</textarea></label></div>`).join("");
    $("#domainsInput").value=(d.domains||[]).join("\n");
  }catch(e){$("#loginMessage").textContent=e.message;}
}
async function saveSettings(){
  const keywordMap={};
  document.querySelectorAll("[data-keycat]").forEach(t=>keywordMap[t.dataset.keycat]=parseLines(t.value));
  const domains=parseLines($("#domainsInput").value);
  try{
    await api("/api/settings",{method:"POST",headers:adminHeaders(),body:JSON.stringify({keywordMap,domains})});
    $("#saveMessage").textContent="설정이 Neon DB에 영구 저장되었습니다.";
    await boot(); await loadArticles();
  }catch(e){$("#saveMessage").textContent=e.message;}
}
async function collectNow(){
  const b=$("#collectBtn");
  b.disabled=true;
  b.textContent="새로고침 중…";
  try{
    await boot();
    await loadArticles();
  }catch(e){
    alert(e.message);
  }finally{
    b.disabled=false;
    b.textContent="🔄 화면 새로고침";
  }
}

$("#basisSelect").onchange=loadArticles;
$("#periodSelect").onchange=loadArticles;
$("#settingsBtn").onclick=()=>{ $("#settingsDialog").showModal(); if(state.adminPassword){$("#adminPassword").value=state.adminPassword;unlockSettings();} };
$("#unlockBtn").onclick=unlockSettings;
$("#saveSettingsBtn").onclick=saveSettings;
$("#collectBtn").onclick=collectNow;

boot().then(loadArticles).catch(()=>{});
