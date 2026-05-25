(() => {
  const grid = document.getElementById('grid');
  const dot = document.getElementById('dot');
  const conn = document.getElementById('conn');
  const count = document.getElementById('count');
  const toast = document.getElementById('toast');

  let snapshot = [];
  let highlightIdx = -1;

  function basename(p) {
    if (!p) return '(no cwd)';
    const parts = p.split('/').filter(Boolean);
    return parts.length ? parts[parts.length - 1] : p;
  }

  function ago(ts) {
    const d = Math.max(0, Math.floor(Date.now() / 1000 - ts));
    if (d < 60) return d + 's';
    if (d < 3600) return Math.floor(d / 60) + 'm';
    return Math.floor(d / 3600) + 'h';
  }

  function esc(s) {
    return String(s).replace(/[&<>"']/g, c => ({
      '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
    }[c]));
  }

  function tmuxLabel(s) {
    if (!s.tmux_socket) return 'no tmux';
    const w = s.tmux_window ? ':' + s.tmux_window : '';
    const p = s.tmux_pane ? ' ' + s.tmux_pane : '';
    return `${s.tmux_session || '?'}${w}${p}`;
  }

  function render() {
    count.textContent = snapshot.length + (snapshot.length === 1 ? ' session' : ' sessions');
    if (!snapshot.length) {
      grid.innerHTML = '<div class="empty-state">No Claude Code sessions running. Start one with <code>claude</code> in any project (inside tmux).</div>';
      highlightIdx = -1;
      return;
    }
    if (highlightIdx >= snapshot.length) highlightIdx = snapshot.length - 1;

    grid.innerHTML = snapshot.map((s, i) => {
      const status = s.status || 'ready';
      const hasTmux = !!(s.tmux_socket && s.tmux_session && s.tmux_window && s.tmux_pane);
      const title = (s.display_name && s.display_name.trim()) || basename(s.cwd);
      const cwdSub = s.display_name ? `<span class="cwd-sub" title="${esc(s.cwd)}">${esc(basename(s.cwd))}</span>` : '';
      const summary = (s.summary || '').trim();
      const summaryHtml = summary
        ? `<div class="summary">${esc(summary)}</div>`
        : `<div class="summary empty">(no prompt yet)</div>`;
      const tool = s.current_tool
        ? `<span class="tool">▸ ${esc(s.current_tool)}</span>`
        : (s.pending_msg ? `<span>${esc(s.pending_msg)}</span>` : '<span></span>');
      const err = s.error ? `<div class="summary" style="color:var(--red)">${esc(s.error)}</div>` : '';
      const idxBadge = i < 9 ? `<span class="idx">${i + 1}</span>` : '';
      const hl = i === highlightIdx ? ' highlight' : '';
      const noTmux = hasTmux ? '' : ' no-tmux';
      return `
        <div class="card ${status}${hl}${noTmux}" data-sid="${esc(s.session_id)}" data-tmux="${hasTmux ? '1' : '0'}" data-idx="${i}" title="${esc(s.cwd)}">
          <div class="row">
            <div class="title">${idxBadge}<span>${esc(title)}</span>${cwdSub}</div>
            <span class="pill ${status}">${status.replace('_', ' ')}</span>
          </div>
          ${summaryHtml}
          ${err}
          <div class="meta">
            ${tool}
            <span>${esc(tmuxLabel(s))} · ${ago(s.last_event_at)} ago</span>
          </div>
        </div>`;
    }).join('');
  }

  function flashError(card, msg) {
    if (card) {
      card.classList.add('flash-error');
      setTimeout(() => card.classList.remove('flash-error'), 1500);
    }
    toast.textContent = msg;
    toast.classList.add('show');
    clearTimeout(flashError._t);
    flashError._t = setTimeout(() => toast.classList.remove('show'), 1800);
  }

  async function switchTo(sid, card) {
    try {
      const resp = await fetch('/switch/' + encodeURIComponent(sid), { method: 'POST' });
      if (!resp.ok) {
        let detail = `HTTP ${resp.status}`;
        try { const j = await resp.json(); detail = j.detail || detail; } catch {}
        flashError(card, detail);
      }
    } catch (e) {
      flashError(card, String(e));
    }
  }

  grid.addEventListener('click', (e) => {
    const card = e.target.closest('.card');
    if (!card) return;
    if (card.dataset.tmux !== '1') {
      flashError(card, 'session not in tmux');
      return;
    }
    switchTo(card.dataset.sid, card);
  });

  document.addEventListener('keydown', (e) => {
    if (e.target && /input|textarea/i.test(e.target.tagName)) return;
    if (!snapshot.length) return;
    // 1-9 quick switch
    if (e.key >= '1' && e.key <= '9') {
      const i = parseInt(e.key, 10) - 1;
      if (i < snapshot.length) {
        const card = grid.querySelector(`.card[data-idx="${i}"]`);
        if (card && card.dataset.tmux === '1') switchTo(card.dataset.sid, card);
        else if (card) flashError(card, 'session not in tmux');
      }
      return;
    }
    if (e.key === 'j' || e.key === 'ArrowDown') {
      highlightIdx = Math.min(snapshot.length - 1, highlightIdx + 1);
      if (highlightIdx < 0) highlightIdx = 0;
      render();
      e.preventDefault();
      return;
    }
    if (e.key === 'k' || e.key === 'ArrowUp') {
      highlightIdx = Math.max(0, highlightIdx - 1);
      render();
      e.preventDefault();
      return;
    }
    if (e.key === 'Enter' && highlightIdx >= 0) {
      const card = grid.querySelector(`.card[data-idx="${highlightIdx}"]`);
      if (card && card.dataset.tmux === '1') switchTo(card.dataset.sid, card);
      else if (card) flashError(card, 'session not in tmux');
    }
  });

  function connect() {
    const es = new EventSource('/events');
    es.addEventListener('snapshot', e => {
      snapshot = JSON.parse(e.data);
      render();
      dot.className = 'dot live';
      conn.textContent = 'live';
    });
    es.addEventListener('ping', () => {
      dot.className = 'dot live';
      conn.textContent = 'live';
    });
    es.onerror = () => {
      dot.className = 'dot dead';
      conn.textContent = 'reconnecting…';
    };
  }
  connect();
  setInterval(render, 5000); // refresh "Xs ago" labels
})();
