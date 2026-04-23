document.addEventListener("DOMContentLoaded", () => {

  // ── Confirm destructive forms ────────────────────────────────────────
  document.querySelectorAll("form[data-confirm]").forEach((form) => {
    form.addEventListener("submit", (e) => {
      if (!confirm(form.dataset.confirm)) e.preventDefault();
    });
  });

  // ── File browser ─────────────────────────────────────────────────────
  initFileBrowsers();

  // ── Filters page ─────────────────────────────────────────────────────
  if (document.getElementById("open-add-btn")) initFiltersPage();

  // ── Library inspector ────────────────────────────────────────────────
  if (document.getElementById("lib-table")) initLibrary();

});

// ─────────────────────────────────────────────────────────────────────────
// FILE BROWSER
// ─────────────────────────────────────────────────────────────────────────
function initFileBrowsers() {
  document.querySelectorAll("[data-file-browser]").forEach((browser) => {
    const targetInput = document.getElementById(browser.dataset.fileBrowser);
    if (!targetInput) return;

    const listEl = browser.querySelector(".fb-list");
    const pathEl = browser.querySelector(".fb-path");
    const formGroup = browser.closest(".form-group");
    const selectedEl = formGroup?.querySelector(".file-browser-selected__path");
    const placeholderEl = formGroup?.querySelector(".file-browser-selected__placeholder");

    async function loadDir(path) {
      if (!listEl) return;
      listEl.innerHTML = '<div class="fb-loading">Loading\u2026</div>';
      try {
        const res = await fetch("/providers/browse?path=" + encodeURIComponent(path));
        if (!res.ok) throw new Error("HTTP " + res.status);
        const data = await res.json();
        if (pathEl) pathEl.textContent = data.current;
        renderDir(data);
      } catch {
        listEl.innerHTML = '<div class="fb-error">Failed to load directory.</div>';
      }
    }

    function renderDir(data) {
      listEl.innerHTML = "";
      if (data.parent) {
        const up = mkItem("\u2B06", "..", "dir");
        up.addEventListener("click", () => loadDir(data.parent));
        listEl.appendChild(up);
      }
      data.dirs.forEach((d) => {
        const el = mkItem("\uD83D\uDCC1", d.name, "dir");
        el.addEventListener("click", () => loadDir(d.path));
        listEl.appendChild(el);
      });
      data.files.forEach((f) => {
        const el = mkItem("\uD83D\uDCC4", f.name, "file");
        el.addEventListener("click", () => selectFile(f.path, el));
        if (targetInput.value === f.path) el.classList.add("fb-item--selected");
        listEl.appendChild(el);
      });
      if (!data.dirs.length && !data.files.length && !data.parent)
        listEl.innerHTML = '<div class="fb-empty">No .m3u files found here.</div>';
    }

    function mkItem(icon, name, type) {
      const div = document.createElement("div");
      div.className = "fb-item fb-item--" + type;
      div.innerHTML = `<span class="fb-icon">${icon}</span><span class="fb-name">${name}</span>`;
      return div;
    }

    function selectFile(path, el) {
      listEl.querySelectorAll(".fb-item--selected").forEach((x) => x.classList.remove("fb-item--selected"));
      el.classList.add("fb-item--selected");
      targetInput.value = path;
      if (selectedEl) { selectedEl.textContent = path; selectedEl.style.display = ""; }
      if (placeholderEl) placeholderEl.style.display = "none";
      const submitBtn = browser.closest("form")?.querySelector("[type=submit]");
      if (submitBtn) submitBtn.removeAttribute("disabled");
    }

    loadDir("");
  });
}

// ─────────────────────────────────────────────────────────────────────────
// FILTERS PAGE
// ─────────────────────────────────────────────────────────────────────────
function initFiltersPage() {
  const openBtn  = document.getElementById("open-add-btn");
  const closeBtn = document.getElementById("close-add-btn");
  const panel    = document.getElementById("add-panel");
  const stepType = document.getElementById("step-type");
  const stepCfg  = document.getElementById("step-config");
  const nextBtn  = document.getElementById("type-next-btn");
  const backBtn  = document.getElementById("config-back-btn");
  const typeHid  = document.getElementById("add-type-hidden");
  const patLabel = document.getElementById("add-pattern-label");

  let chosenType = null;

  // Open / close panel
  openBtn.addEventListener("click", () => {
    panel.style.display = "block";
    openBtn.style.display = "none";
    window.scrollTo({ top: 0, behavior: "smooth" });
  });

  function closePanel() {
    panel.style.display = "none";
    openBtn.style.display = "";
    resetPanel();
  }

  closeBtn.addEventListener("click", closePanel);

  // Type selection
  panel.querySelectorAll(".filter-type-opt").forEach((opt) => {
    opt.addEventListener("click", () => {
      panel.querySelectorAll(".filter-type-opt").forEach((o) => o.classList.remove("filter-type-opt--active"));
      opt.classList.add("filter-type-opt--active");
      opt.querySelector("input").checked = true;
      chosenType = opt.dataset.type;
      nextBtn.disabled = false;
    });
  });

  // Next button
  nextBtn.addEventListener("click", () => {
    if (!chosenType) return;
    typeHid.value = chosenType;
    stepType.style.display = "none";
    stepCfg.style.display = "";
    const list = document.getElementById("add-pattern-list");
    list.dataset.ftype = chosenType;
    patLabel.textContent = chosenType === "replace" ? "Replacement pairs" : "Patterns";
    if (!list.querySelector(".filter-row")) addPatternRow(list, chosenType);
    initScopeGroup(panel);
  });

  // Back button
  backBtn.addEventListener("click", () => {
    stepCfg.style.display = "none";
    stepType.style.display = "";
  });

  function resetPanel() {
    stepType.style.display = "";
    stepCfg.style.display = "none";
    nextBtn.disabled = true;
    chosenType = null;
    typeHid.value = "";
    panel.querySelectorAll(".filter-type-opt").forEach((o) => o.classList.remove("filter-type-opt--active"));
    panel.querySelectorAll("input[type=radio]").forEach((r) => (r.checked = false));
    document.getElementById("add-pattern-list").innerHTML = "";
  }

  // Add pattern row (delegated)
  document.addEventListener("click", (e) => {
    const btn = e.target.closest(".filter-add-row-btn");
    if (!btn) return;
    const listId = btn.dataset.list;
    const ftype  = btn.dataset.ftype || document.getElementById(listId)?.dataset.ftype || "";
    const list   = document.getElementById(listId);
    if (list) addPatternRow(list, ftype);
  });

  // Remove pattern row (delegated)
  document.addEventListener("click", (e) => {
    const btn = e.target.closest(".filter-del-row-btn");
    if (!btn) return;
    const row = btn.closest(".filter-row");
    const list = btn.closest(".filter-pattern-list");
    if (row) row.remove();
    if (list) reindexList(list);
  });

  // Edit open/close (delegated)
  document.addEventListener("click", (e) => {
    const openBtnEl = e.target.closest(".filter-edit-open-btn");
    if (!openBtnEl) return;
    const id = openBtnEl.dataset.id;
    const editPanel = document.getElementById("edit-panel-" + id);
    if (!editPanel) return;
    const isOpen = editPanel.style.display !== "none";
    document.querySelectorAll(".filter-edit-panel").forEach((p) => (p.style.display = "none"));
    if (!isOpen) {
      editPanel.style.display = "";
      initScopeGroup(editPanel);
    }
  });

  document.addEventListener("click", (e) => {
    const cancelBtn = e.target.closest(".filter-edit-cancel-btn");
    if (!cancelBtn) return;
    const editPanel = document.getElementById("edit-panel-" + cancelBtn.dataset.id);
    if (editPanel) editPanel.style.display = "none";
  });

  // Init scope checkbox behaviour for any visible groups on page load
  initScopeGroup(document);
}

function initScopeGroup(container) {
  container.querySelectorAll(".filter-scope-row").forEach((row) => {
    const allBox = row.querySelector("input[data-scope='all']");
    const specifics = Array.from(row.querySelectorAll("input[data-scope='specific']"));
    if (!allBox) return;

    allBox.addEventListener("change", () => {
      if (allBox.checked) specifics.forEach((b) => (b.checked = false));
    });
    specifics.forEach((box) => {
      box.addEventListener("change", () => {
        if (box.checked) allBox.checked = false;
        if (!specifics.some((b) => b.checked)) allBox.checked = true;
      });
    });
  });
}

function addPatternRow(list, ftype) {
  const idx = list.querySelectorAll(".filter-row").length;
  const isReplace = ftype === "replace";
  const row = document.createElement("div");
  row.className = "filter-row";
  row.dataset.idx = idx;
  row.innerHTML = `
    <input type="text" name="pattern_${idx}"
           placeholder="${isReplace ? "Find (literal text)" : "Regex pattern"}"
           class="filter-input" required>
    ${isReplace ? `<span class="filter-arrow">\u2192</span>
    <input type="text" name="replacement_${idx}" placeholder="Replace with"
           class="filter-input filter-input--repl">` : ""}
    <button type="button" class="btn btn-sm btn-danger filter-del-row-btn">&times;</button>
  `;
  list.appendChild(row);
}

// ─────────────────────────────────────────────────────────────────────────
// LIBRARY INSPECTOR — search, sort, expand
// ─────────────────────────────────────────────────────────────────────────
function initLibrary() {
  const table    = document.getElementById("lib-table");
  const tbody    = document.getElementById("lib-tbody");
  const searchEl = document.getElementById("lib-search");
  const countEl  = document.getElementById("lib-search-count");
  const isStreams = table.classList.contains("lib-table--streams");

  // ── Expand/collapse detail rows (streams only) ────────────────────
  if (isStreams) {
    tbody.addEventListener("click", (e) => {
      const btn = e.target.closest(".lib-expand-btn");
      if (!btn) return;
      const mainRow   = btn.closest(".lib-stream-row");
      const idx       = mainRow.dataset.idx;
      const detailRow = document.getElementById("lib-detail-" + idx);
      if (!detailRow) return;
      const open = detailRow.style.display !== "none";
      detailRow.style.display = open ? "none" : "";
      btn.classList.toggle("lib-expand-btn--open", !open);
      btn.innerHTML = open ? "&#x25B6;" : "&#x25BC;";
    });
  }

  // ── Sort ─────────────────────────────────────────────────────────────
  let sortCol = -1;
  let sortAsc = true;

  table.querySelectorAll(".lib-th-sort").forEach((th) => {
    th.addEventListener("click", () => {
      const col = parseInt(th.dataset.col, 10);
      if (sortCol === col) {
        sortAsc = !sortAsc;
      } else {
        sortCol = col;
        sortAsc = true;
      }
      table.querySelectorAll(".lib-th-sort").forEach((h) => {
        h.classList.remove("lib-th--asc", "lib-th--desc");
      });
      th.classList.add(sortAsc ? "lib-th--asc" : "lib-th--desc");
      sortTable(col, sortAsc);
      updateCount();
    });
  });

  function sortTable(col, asc) {
    if (isStreams) {
      // Pair data rows with their detail rows, sort together
      const pairs = [];
      tbody.querySelectorAll(".lib-stream-row").forEach((row) => {
        const idx = row.dataset.idx;
        const detail = document.getElementById("lib-detail-" + idx);
        pairs.push({ row, detail });
      });
      pairs.sort((a, b) => {
        const aVal = cellText(a.row, col);
        const bVal = cellText(b.row, col);
        return compare(aVal, bVal, asc);
      });
      pairs.forEach(({ row, detail }) => {
        tbody.appendChild(row);
        if (detail) tbody.appendChild(detail);
      });
    } else {
      const rows = Array.from(tbody.querySelectorAll("tr"));
      rows.sort((a, b) => compare(cellText(a, col), cellText(b, col), asc));
      rows.forEach((r) => tbody.appendChild(r));
    }
  }

  function cellText(row, col) {
    const cell = row.cells[col];
    return cell ? cell.textContent.trim().toLowerCase() : "";
  }

  function compare(a, b, asc) {
    const numA = parseFloat(a);
    const numB = parseFloat(b);
    const bothNum = !isNaN(numA) && !isNaN(numB);
    const result = bothNum ? numA - numB : a.localeCompare(b);
    return asc ? result : -result;
  }

  // ── Search ────────────────────────────────────────────────────────────
  function updateCount() {
    const visible = tbody.querySelectorAll(
      isStreams ? ".lib-stream-row:not([hidden])" : "tr:not([hidden])"
    ).length;
    const total = tbody.querySelectorAll(
      isStreams ? ".lib-stream-row" : "tr"
    ).length;
    countEl.textContent = searchEl.value ? `${visible} / ${total}` : `${total} rows`;
  }

  searchEl.addEventListener("input", () => {
    const q = searchEl.value.toLowerCase().trim();
    if (isStreams) {
      tbody.querySelectorAll(".lib-stream-row").forEach((row) => {
        const idx       = row.dataset.idx;
        const detail    = document.getElementById("lib-detail-" + idx);
        const text      = row.textContent.toLowerCase();
        const detailTxt = detail ? detail.textContent.toLowerCase() : "";
        const match     = !q || text.includes(q) || detailTxt.includes(q);
        row.hidden   = !match;
        if (detail) detail.hidden = !match;
        if (!match && detail) {
          detail.style.display = "none";
          const btn = row.querySelector(".lib-expand-btn");
          if (btn) { btn.classList.remove("lib-expand-btn--open"); btn.innerHTML = "&#x25B6;"; }
        }
      });
    } else {
      tbody.querySelectorAll("tr").forEach((row) => {
        row.hidden = q ? !row.textContent.toLowerCase().includes(q) : false;
      });
    }
    updateCount();
  });

  updateCount();
}

function reindexList(list) {
  const ftype = list.dataset.ftype || "";
  list.querySelectorAll(".filter-row").forEach((row, idx) => {
    row.dataset.idx = idx;
    const patIn = row.querySelector(".filter-input:not(.filter-input--repl)");
    const repIn = row.querySelector(".filter-input--repl");
    if (patIn) patIn.name = "pattern_" + idx;
    if (repIn) repIn.name = "replacement_" + idx;
  });
}
