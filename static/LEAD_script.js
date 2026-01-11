(function () {

  // =====================================================
  // USER CONTEXT
  // =====================================================
  const deptRaw =
    localStorage.getItem('userDepartment') ||
    localStorage.getItem('userRole') ||
    '';

  const dept = String(deptRaw)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '-')
    .replace(/_/g, '-');

  const role = (localStorage.getItem('userRole') || '').toUpperCase();
  const isLeadUser = role === 'LEAD';
  const isAdmin = localStorage.getItem('userIsAdmin') === 'true';

  if (!dept) {
    window.location.href = 'login.html';
    return;
  }

  let currentFormId = null;

  const LEAD_DEPT_KEYS = [
  'css',
  'materials',
  'technical-operations',
  'quality',
  'operations'
];


  document.addEventListener('DOMContentLoaded', () => {
    // -----------------------------------------
    // GLOBAL LOCKDOWN FOR NON-ADMIN
    // -----------------------------------------
    if (!isAdmin) {
      document.querySelectorAll('input, select, button, textarea').forEach(el => {
        // Always allowed
        if (el.id === 'printBtn' || el.id === 'exportCsv' || el.id === 'exportTxt') return;

        // allow LEAD dept sign checkboxes for non-admin too
        if (el.classList && el.classList.contains('dept-sign-checkbox')) return;

        // Prepared By field must be admin-only
        if (el.id === 'preparedBy') {
          el.disabled = true;
          el.style.opacity = '0.6';
          return;
        }

        // Add / Reset buttons
        if (el.type === 'button' && (el.id === 'addItemBtn' || el.id === 'resetBtn')) {
          el.disabled = true;
          el.style.opacity = '0.5';
          return;
        }

        el.disabled = true;
      });
    }

    // --------------------------------------------------
// LEAD USER RIGHTS: unlock ONLY Remarks & Comments
// --------------------------------------------------
if (!isAdmin && LEAD_DEPT_KEYS.includes(dept)) {

  // Unlock ONLY table remarks column
  document.querySelectorAll('.cell-input.remarks').forEach(inp => {
    inp.disabled = false;
    inp.style.opacity = '1';
  });

  // Unlock ONLY comments textarea + submit button
  const commentBox = document.getElementById('newComment');
  const submitBtn  = document.getElementById('submitComment');

  if (commentBox) {
    commentBox.disabled = false;
    commentBox.style.opacity = '1';
  }

  if (submitBtn) {
    submitBtn.disabled = false;
    submitBtn.style.opacity = '1';
    submitBtn.style.cursor = 'pointer';
  }
}
    // init LEAD verification checkbox logic (same style as CR/PED)
    initLeadDeptVerification();
  });

  // =====================================================
  // DATE HELPERS
  // =====================================================
  function formatDisplay(date) {
    if (!date) return '00-Jan-00';
    if (!(date instanceof Date) || isNaN(date)) return '00-Jan-00';
    const d = String(date.getDate()).padStart(2, '0');
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const m = months[date.getMonth()];
    const yy = String(date.getFullYear()).slice(-2);
    return `${d}-${m}-${yy}`;
  }

  function parseISO(value) {
    if (!value) return null;
    const dt = new Date(value);
    return isNaN(dt) ? null : dt;
  }

  function calcAgreed(custIso, leadDays) {
    const cust = parseISO(custIso);
    if (!cust || isNaN(Number(leadDays))) return null;
    const ag = new Date(cust.getTime());
    ag.setDate(ag.getDate() - Number(leadDays));
    return ag;
  }

  function calcLeadTimeDays(custIso, gtnIso) {
    const custDate = parseISO(custIso);
    const gtnDate = parseISO(gtnIso);
    if (!custDate || !gtnDate) return null;
    const diffTime = custDate.getTime() - gtnDate.getTime();
    const diffDays = Math.round(diffTime / 86400000);
    return diffDays;
  }

function unlockLeadEditableFields() {
  if (isAdmin) return;
  if (!LEAD_DEPT_KEYS.includes(dept)) return;

  // Unlock Remarks column
  document.querySelectorAll('.cell-input.remarks').forEach(inp => {
    inp.disabled = false;
    inp.style.opacity = '1';
  });

  // Unlock Comments
  const commentBox = document.getElementById('newComment');
  const submitBtn = document.getElementById('submitComment');

  if (commentBox) {
    commentBox.disabled = false;
    commentBox.style.opacity = '1';
  }

  if (submitBtn) {
    submitBtn.disabled = false;
    submitBtn.style.cursor = 'pointer';
  }
}

  // =====================================================
  // TABLE ROW LOGIC
  // =====================================================
  function wireRow(row) {
    if (row._wired) return;
    row._wired = true;

    const custInput = row.querySelector('.cust-date');
    const gtnInput = row.querySelector('.gtn-date');
    const leadInput = row.querySelector('.lead-days');

    const dt = row.querySelectorAll('.date-cell .date-text');
    const custDisplay = dt[0];
    const gtnDisplay = dt[1] || dt[0];

    function updateDisplays() {
      const custVal = custInput ? parseISO(custInput.value) : null;
      const gtnVal = gtnInput ? parseISO(gtnInput.value) : null;

      if (custDisplay) {
        custDisplay.textContent = formatDisplay(custVal);
        custDisplay.classList.toggle('empty', !custVal);
      }
      if (gtnDisplay) {
        gtnDisplay.textContent = formatDisplay(gtnVal);
        gtnDisplay.classList.toggle('empty', !gtnVal);
      }
    }

    updateDisplays();

    function autoCalculateLeadTime() {
      const custVal = custInput?.value || '';
      const gtnVal = gtnInput?.value || '';
      if (custVal && gtnVal) {
        const days = calcLeadTimeDays(custVal, gtnVal);
        if (days !== null && leadInput) leadInput.value = days;
      }
      updateDisplays();
      scheduleAutoSave();
    }

    function recalcAndSet(autoSet = true) {
      const custVal = custInput?.value || '';
      const leadVal = leadInput?.value || '';
      const ag = calcAgreed(custVal, leadVal);
      if (ag && autoSet && gtnInput) {
        gtnInput.value = ag.toISOString().slice(0, 10);
      }
      updateDisplays();
      scheduleAutoSave();
    }

    if (custInput) {
      custInput.addEventListener('change', autoCalculateLeadTime);
      const custDisplayEl = custInput.parentElement?.querySelector('.date-text');
      if (custDisplayEl) {
        custDisplayEl.addEventListener('click', () => custInput.showPicker && custInput.showPicker());
      }
    }

    if (gtnInput) {
      const gtnDisplayEl = gtnInput.parentElement?.querySelector('.date-text');
      if (gtnDisplayEl) {
        gtnDisplayEl.addEventListener('click', () => gtnInput.showPicker && gtnInput.showPicker());
      }
      gtnInput.addEventListener('change', autoCalculateLeadTime);
    }

    if (leadInput) {
      leadInput.addEventListener('input', () => recalcAndSet(true));
    }

    const del = row.querySelector('.del-row');
    if (del) {
      del.addEventListener('click', () => {
        if (!isAdmin) {
          alert('Only Admin can delete rows.');
          return;
        }
        if (!confirm('Delete this row?')) return;
        row.remove();
        scheduleAutoSave();
      });
    }

    row.querySelectorAll('input').forEach(inp => inp.addEventListener('input', scheduleAutoSave));

    // Make item number cell editable for admin
    const itemTd = row.querySelector('td.fixed');
    if (itemTd && isAdmin) {
      itemTd.contentEditable = 'true';
      itemTd.addEventListener('input', scheduleAutoSave);
    }
  }

  function getNextItemNumber() {
    const rows = Array.from(document.querySelectorAll('#tbody tr'));
    if (rows.length === 0) return 10;
    let max = 0;
    rows.forEach(r => {
      const td = r.querySelector('td.fixed');
      if (!td) return;
      const v = parseInt(td.textContent.trim(), 10);
      if (!isNaN(v) && v > max) max = v;
    });
    return (max === 0) ? 10 : max + 10;
  }

  function createCellInput(cls, attrs = {}) {
    const td = document.createElement('td');
    const inp = document.createElement('input');
    inp.className = cls;
    for (const k in attrs) inp.setAttribute(k, attrs[k]);
    if (!isAdmin) inp.disabled = true;
    td.appendChild(inp);
    return td;
  }

  function createDateCell(cls, value) {
    const td = document.createElement('td');
    td.className = 'date-cell';

    const input = document.createElement('input');
    input.className = cls + ' cell-date';
    input.type = 'date';
    if (value) input.value = value;
    input.disabled = !isAdmin;

    const span = document.createElement('div');
    span.className = 'date-text';
    span.textContent = '00-Jan-00';
    span.classList.add('empty');

    td.appendChild(input);
    td.appendChild(span);
    return td;
  }

  function addRow(itemNo) {
    const tbody = document.getElementById('tbody');
    if (!tbody) return;

    const tr = document.createElement('tr');

    const tdItem = document.createElement('td');
    tdItem.className = 'fixed';
    tdItem.textContent = itemNo;
    if (isAdmin) tdItem.contentEditable = 'true';
    tr.appendChild(tdItem);

    tr.appendChild(createCellInput('cell-input part'));
    tr.appendChild(createCellInput('cell-input desc'));
    tr.appendChild(createCellInput('cell-input rev'));
    tr.appendChild(createCellInput('cell-input qty', { type: 'number', min: 0 }));

    tr.appendChild(createDateCell('cust-date'));
    tr.appendChild(createCellInput('cell-input lead-days', { type: 'number', min: 0, placeholder: '0' }));
    tr.appendChild(createDateCell('gtn-date'));

    tr.appendChild(createCellInput('cell-input remarks'));

    const act = document.createElement('td');
    act.className = 'row-actions';
    const del = document.createElement('button');
    del.className = 'del-row';
    del.type = 'button';
    del.textContent = 'Delete';
    act.appendChild(del);
    tr.appendChild(act);

    tbody.appendChild(tr);
    wireRow(tr);
    tr.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }

  // =====================================================
  // EXPORT
  // =====================================================
  function exportCSV() {
    const table = document.getElementById('lead-table');
    if (!table) return;

    const rows = Array.from(table.querySelectorAll('tr'));
    const csv = rows.map(r => {
      const cols = Array.from(r.cells).map(cell => {
        const dateText = cell.querySelector('.date-text');
        const inp = cell.querySelector('input');

        if (dateText) return `"${dateText.textContent.trim().replace(/"/g, '""')}"`;
        if (inp) return `"${String(inp.value || '').replace(/"/g, '""')}"`;
        return `"${String(cell.textContent || '').replace(/"/g, '""')}"`;
      });
      return cols.join(',');
    });

    const blob = new Blob([csv.join('\n')], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'lead-time-sheet.csv';
    a.click();
    URL.revokeObjectURL(url);
  }

  // Export TXT (tab-separated), uses displayed formatted dates
  function exportTXT() {
    const table = document.getElementById('lead-table');
    if (!table) return;

    const rows = Array.from(table.querySelectorAll('tr'));
    const lines = rows.map(r => {
      const cols = Array.from(r.cells).map(cell => {
        const dateText = cell.querySelector('.date-text');
        const btn = cell.querySelector('button');
        let txt;

        if (btn) txt = '';
        else if (dateText) txt = dateText.textContent.trim();
        else txt = cell.innerText.trim();

        return txt.replace(/\t/g, ' ').replace(/\r?\n/g, ' ');
      });

      return cols.join('\t');
    });

    const blob = new Blob([lines.join('\n')], { type: 'text/plain' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'lead-time-sheet.txt';
    a.click();
    URL.revokeObjectURL(a.href);
  }

  // =====================================================
  // AUTOSAVE / AUTOREFRESH
  // =====================================================
  let autoSaveTimer = null;
  let autoRefreshTimer = null;
  let isRefreshing = false;
  let isDirty = false;

  function showSaveStatus(msg) {
    const status = document.getElementById('saveStatus');
    if (!status) return;
    status.textContent = msg;

    if (msg.includes('✓')) status.style.color = '#28a745';
    else if (msg.includes('Saving')) status.style.color = '#ffc107';
    else if (msg.includes('Error')) status.style.color = '#dc3545';
    else status.style.color = '';
  }

  function collectFormData() {
    const customer = document.getElementById('customerSelect')?.value || '';
    const bid = document.getElementById('bidDt')?.value || '';
    const po = document.getElementById('poRevDt')?.value || '';
    const cr = document.getElementById('crRevDt')?.value || '';
    const recordNo = document.getElementById('recordNo')?.value || '';
    const recordDate = document.getElementById('recordDate')?.value || '';
    const generalRemarks = document.getElementById('generalRemarksText')?.value || '';
    const preparedBy = document.getElementById('preparedBy')?.value || '';

    const poKey = `${customer}|${bid}|${po}|${cr}`;

    const rows = [];
    document.querySelectorAll('#tbody tr').forEach(tr => {
      const itemNo = tr.querySelector('td.fixed')?.textContent.trim() || '';
      if (!itemNo) return;

      rows.push({
        itemNo,
        part: tr.querySelector('.part')?.value || '',
        desc: tr.querySelector('.desc')?.value || '',
        rev: tr.querySelector('.rev')?.value || '',
        qty: tr.querySelector('.qty')?.value || '',
        customerRequiredDate: tr.querySelector('.cust-date')?.value || '',
        standardLeadTime: tr.querySelector('.lead-days')?.value || '',
        gtnAgreedDate: tr.querySelector('.gtn-date')?.value || '',
        remarks: tr.querySelector('.remarks')?.value || ''
      });
    });

    return { poKey, customer, bid, po, cr, recordNo, recordDate, generalRemarks, preparedBy, rows };
  }

  async function autoSaveForm() {
    if (!isAdmin) return;

    const data = collectFormData();

    // Require at least customer and PO before saving
    if (!data.customer || !data.po) {
      showSaveStatus('Fill Customer and PO to enable auto-save.');
      return;
    }

    showSaveStatus('Saving...');

    try {
      const response = await fetch('/api/lead-form/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
      });

      const result = await response.json().catch(() => ({}));

      if (response.ok && result.success) {
        showSaveStatus(`✓ Auto-saved by ${result.lastModifiedBy || 'you'}`);
        if (result.formId) currentFormId = result.formId;
        isDirty = false;
      } else {
        showSaveStatus('Error saving');
        setTimeout(() => scheduleAutoSave(), 100);
      }
    } catch (error) {
      console.error('autoSaveForm error', error);
      showSaveStatus('Error saving');
      scheduleAutoSave();
    }
  }

  function scheduleAutoSave() {
    if (!isAdmin) return;
    isDirty = true;
    clearTimeout(autoSaveTimer);
    autoSaveTimer = setTimeout(autoSaveForm, 2000);
  }

  async function loadFormData() {
    const customer = document.getElementById('customerSelect')?.value || '';
    const bid = document.getElementById('bidDt')?.value || '';
    const po = document.getElementById('poRevDt')?.value || '';
    const cr = document.getElementById('crRevDt')?.value || '';

    const tbody = document.getElementById('tbody');
    if (!tbody) return;

    tbody.innerHTML = '';

    if (!customer || !bid || !po || !cr) {
      [10, 20, 30, 40, 50, 60, 70, 80, 90, 100].forEach(n => addRow(n));
      return;
    }

    const poKey = `${customer}|${bid}|${po}|${cr}`;

    try {
      const response = await fetch(`/api/lead-form/load?poKey=${encodeURIComponent(poKey)}`);
      const result = await response.json();

      if (response.ok && result.exists) {
        if (result.formId) currentFormId = result.formId;

        const rn = document.getElementById('recordNo');
        const rd = document.getElementById('recordDate');
        if (rn) rn.value = result.recordNo || '';
        if (rd) rd.value = result.recordDate || '';

        const gr = document.getElementById('generalRemarksText');
        if (gr) gr.value = result.generalRemarks || '';

        const pb = document.getElementById('preparedBy');
        if (pb) pb.value = result.preparedBy || '';

        (result.rows || []).forEach(rowData => {
          const tr = document.createElement('tr');

          const tdItem = document.createElement('td');
          tdItem.className = 'fixed';
          tdItem.textContent = rowData.itemNo;
          if (isAdmin) tdItem.contentEditable = 'true';
          tr.appendChild(tdItem);

          const createInput = (cls, val) => {
            const td = document.createElement('td');
            const inp = document.createElement('input');
            inp.className = cls;
            inp.value = val || '';
            if (!isAdmin) inp.disabled = true;
            td.appendChild(inp);
            return td;
          };

          const createDateCell2 = (cls, val) => {
            const td = document.createElement('td');
            td.className = 'date-cell';
            const input = document.createElement('input');
            input.className = cls + ' cell-date';
            input.type = 'date';
            input.value = val || '';
            input.disabled = !isAdmin;

            const span = document.createElement('div');
            span.className = 'date-text';
            td.appendChild(input);
            td.appendChild(span);
            return td;
          };

          tr.appendChild(createInput('cell-input part', rowData.part));
          tr.appendChild(createInput('cell-input desc', rowData.desc));
          tr.appendChild(createInput('cell-input rev', rowData.rev));

          const qtyTd = document.createElement('td');
          const qtyInp = document.createElement('input');
          qtyInp.className = 'cell-input qty';
          qtyInp.type = 'number';
          qtyInp.min = 0;
          qtyInp.value = rowData.qty || '';
          if (!isAdmin) qtyInp.disabled = true;
          qtyTd.appendChild(qtyInp);
          tr.appendChild(qtyTd);

          tr.appendChild(createDateCell2('cust-date', rowData.customerRequiredDate));

          const leadTd = document.createElement('td');
          const leadInp = document.createElement('input');
          leadInp.className = 'cell-input lead-days';
          leadInp.type = 'number';
          leadInp.min = 0;
          leadInp.value = rowData.standardLeadTime || '';
          leadInp.placeholder = '0';
          if (!isAdmin) leadInp.disabled = true;
          leadTd.appendChild(leadInp);
          tr.appendChild(leadTd);

          tr.appendChild(createDateCell2('gtn-date', rowData.gtnAgreedDate));
          tr.appendChild(createInput('cell-input remarks', rowData.remarks));

          const act = document.createElement('td');
          act.className = 'row-actions';
          const del = document.createElement('button');
          del.className = 'del-row';
          del.type = 'button';
          del.textContent = 'Delete';
          act.appendChild(del);
          tr.appendChild(act);

          tbody.appendChild(tr);
          wireRow(tr);
        });

        attachAutoSaveListeners();
        showSaveStatus(`✓ Loaded - last edited by ${result.lastModifiedBy || 'unknown'}`);

        // ensure formatted date display immediately after load
        document.querySelectorAll('#tbody tr').forEach(tr => {
          const cust = tr.querySelector('.cust-date')?.value || '';
          const gtn = tr.querySelector('.gtn-date')?.value || '';
          const dateTexts = tr.querySelectorAll('.date-text');
          if (dateTexts[0]) {
            const custVal = parseISO(cust);
            dateTexts[0].textContent = formatDisplay(custVal);
            dateTexts[0].classList.toggle('empty', !custVal);
          }
          if (dateTexts[1]) {
            const gtnVal = parseISO(gtn);
            dateTexts[1].textContent = formatDisplay(gtnVal);
            dateTexts[1].classList.toggle('empty', !gtnVal);
          }
        });
      } else {
        [10, 20, 30, 40, 50, 60, 70, 80, 90, 100].forEach(n => addRow(n));
      }
    } catch (error) {
      console.error('Error loading form:', error);
      [10, 20, 30, 40, 50, 60, 70, 80, 90, 100].forEach(n => addRow(n));
    }

    unlockLeadEditableFields();
  }


  async function autoRefreshForm() {
    if (isRefreshing) return;
    if (isDirty) return; // do NOT refresh while local edits are unsaved
    isRefreshing = true;

    const customer = document.getElementById('customerSelect')?.value || '';
    const bid = document.getElementById('bidDt')?.value || '';
    const po = document.getElementById('poRevDt')?.value || '';
    const cr = document.getElementById('crRevDt')?.value || '';

    if (!customer || !bid || !po || !cr) {
      isRefreshing = false;
      return;
    }

    const poKey = `${customer}|${bid}|${po}|${cr}`;

    try {
      const response = await fetch(`/api/lead-form/load?poKey=${encodeURIComponent(poKey)}`);
      const result = await response.json();

      if (response.ok && result.exists) {
        const focusedElement = document.activeElement;
        const hasFocus = focusedElement && focusedElement.tagName === 'INPUT';

        const tbody = document.getElementById('tbody');
        if (!tbody) {
          isRefreshing = false;
          return;
        }

        const currentRows = Array.from(tbody.querySelectorAll('tr'));
        const currentRowCount = currentRows.length;
        const newRowCount = (result.rows || []).length;

        if (!hasFocus) {
          const rn = document.getElementById('recordNo');
          const rd = document.getElementById('recordDate');
          if (rn) rn.value = result.recordNo || '';
          if (rd) rd.value = result.recordDate || '';

          const pb = document.getElementById('preparedBy');
          if (pb && document.activeElement !== pb) pb.value = result.preparedBy || '';

          if (newRowCount > currentRowCount) {
            for (let i = currentRowCount; i < newRowCount; i++) {
              const rowData = result.rows[i];
              const tr = document.createElement('tr');

              const tdItem = document.createElement('td');
              tdItem.className = 'fixed';
              tdItem.textContent = rowData.itemNo;
              if (isAdmin) tdItem.contentEditable = 'true';
              tr.appendChild(tdItem);

              const createInput = (cls, val) => {
                const td = document.createElement('td');
                const inp = document.createElement('input');
                inp.className = cls;
                inp.value = val || '';
                if (!isAdmin) inp.disabled = true;
                td.appendChild(inp);
                return td;
              };

              const createDateCell2 = (cls, val) => {
                const td = document.createElement('td');
                td.className = 'date-cell';
                const input = document.createElement('input');
                input.className = cls + ' cell-date';
                input.type = 'date';
                input.value = val || '';
                input.disabled = !isAdmin;

                const span = document.createElement('div');
                span.className = 'date-text';
                td.appendChild(input);
                td.appendChild(span);
                return td;
              };

              tr.appendChild(createInput('cell-input part', rowData.part));
              tr.appendChild(createInput('cell-input desc', rowData.desc));
              tr.appendChild(createInput('cell-input rev', rowData.rev));

              const qtyTd = document.createElement('td');
              const qtyInp = document.createElement('input');
              qtyInp.className = 'cell-input qty';
              qtyInp.type = 'number';
              qtyInp.min = 0;
              qtyInp.value = rowData.qty || '';
              if (!isAdmin) qtyInp.disabled = true;
              qtyTd.appendChild(qtyInp);
              tr.appendChild(qtyTd);

              tr.appendChild(createDateCell2('cust-date', rowData.customerRequiredDate));

              const leadTd = document.createElement('td');
              const leadInp = document.createElement('input');
              leadInp.className = 'cell-input lead-days';
              leadInp.type = 'number';
              leadInp.min = 0;
              leadInp.value = rowData.standardLeadTime || '';
              leadInp.placeholder = '0';
              if (!isAdmin) leadInp.disabled = true;
              leadTd.appendChild(leadInp);
              tr.appendChild(leadTd);

              tr.appendChild(createDateCell2('gtn-date', rowData.gtnAgreedDate));
              tr.appendChild(createInput('cell-input remarks', rowData.remarks));

              const act = document.createElement('td');
              act.className = 'row-actions';
              const del = document.createElement('button');
              del.className = 'del-row';
              del.type = 'button';
              del.textContent = 'Delete';
              act.appendChild(del);
              tr.appendChild(act);

              tbody.appendChild(tr);
              wireRow(tr);
            }
          } else if (newRowCount < currentRowCount) {
            for (let i = currentRowCount - 1; i >= newRowCount; i--) {
              currentRows[i].remove();
            }
          }

          (result.rows || []).forEach((rowData, idx) => {
            const tr = tbody.querySelectorAll('tr')[idx];
            if (tr && !tr.contains(focusedElement)) {
              const item = tr.querySelector('td.fixed');
              if (item) {
                item.textContent = rowData.itemNo;
                if (isAdmin) item.contentEditable = 'true';
              }

              const setIf = (sel, val) => {
                const el = tr.querySelector(sel);
                if (el) el.value = val || '';
              };

              setIf('.part', rowData.part);
              setIf('.desc', rowData.desc);
              setIf('.rev', rowData.rev);
              setIf('.qty', rowData.qty);
              setIf('.cust-date', rowData.customerRequiredDate);
              setIf('.lead-days', rowData.standardLeadTime);
              setIf('.gtn-date', rowData.gtnAgreedDate);
              setIf('.remarks', rowData.remarks);

              const custDate = tr.querySelector('.cust-date');
              const gtnDate = tr.querySelector('.gtn-date');
              const dateTexts = tr.querySelectorAll('.date-text');

              const custVal = parseISO(custDate?.value || '');
              const gtnVal = parseISO(gtnDate?.value || '');

              if (dateTexts[0]) {
                dateTexts[0].textContent = formatDisplay(custVal);
                dateTexts[0].classList.toggle('empty', !custVal);
              }
              if (dateTexts[1]) {
                dateTexts[1].textContent = formatDisplay(gtnVal);
                dateTexts[1].classList.toggle('empty', !gtnVal);
              }
            }
          });
        }
      }
    } catch (error) {
      console.error('Auto-refresh error:', error);
    }

    isRefreshing = false;
  }

  function attachAutoSaveListeners() {
    const fields = ['customerSelect', 'bidDt', 'poRevDt', 'crRevDt', 'recordNo', 'recordDate', 'generalRemarksText', 'preparedBy'];
    fields.forEach(id => {
      const el = document.getElementById(id);
      if (!el) return;
      el.removeEventListener('input', scheduleAutoSave);
      el.addEventListener('input', scheduleAutoSave);
    });
  }

  // =====================================================
  // INITIALIZE (URL params + load + timers)
  // =====================================================
  (function () {
    const p = new URLSearchParams(window.location.search);
    const customer = p.get('customer');
    const bid = p.get('bid');
    const po = p.get('po');
    const cr = p.get('cr');

    if (customer && document.getElementById('customerSelect')) document.getElementById('customerSelect').value = customer;

    // optional alternate customer input
    if (document.getElementById('customerName')) {
      const sel = document.getElementById('customerName');
      if (customer) sel.value = customer;
    }

    if (bid && document.getElementById('bidDt')) document.getElementById('bidDt').value = bid;
    if (po && document.getElementById('poRevDt')) document.getElementById('poRevDt').value = po;
    if (cr && document.getElementById('crRevDt')) document.getElementById('crRevDt').value = cr;

    setTimeout(() => {
      loadFormData();
      attachAutoSaveListeners();

      // update lead verification checkboxes after load
      refreshLeadVerification().catch(() => { });

      if (isAdmin) {
        autoRefreshTimer = setInterval(async () => {
          await autoRefreshForm();
          await refreshLeadVerification().catch(() => { });
        }, 5000);
      }
    }, 100);
  })();

  // =====================================================
  // UI BUTTONS
  // =====================================================
  document.getElementById('addItemBtn')?.addEventListener('click', () => {
    if (!isAdmin) { alert('Only Admin can add rows.'); return; }
    addRow(getNextItemNumber());
    scheduleAutoSave();
  });

  document.getElementById('printBtn')?.addEventListener('click', () => window.print());
  document.getElementById('exportCsv')?.addEventListener('click', exportCSV);
  document.getElementById('exportTxt')?.addEventListener('click', exportTXT);

  document.getElementById('resetBtn')?.addEventListener('click', () => {
    if (!isAdmin) { alert('Only Admin can reset values.'); return; }
    if (!confirm('Clear all input values in the table?')) return;

    document.querySelectorAll('#tbody tr').forEach(tr => {
      tr.querySelectorAll('input').forEach(inp => { inp.value = ''; });
      tr.querySelectorAll('.date-text').forEach(dt => {
        dt.textContent = '00-Jan-00';
        dt.classList.add('empty');
      });
    });

    scheduleAutoSave();
  });

  // =====================================================
  // LEAD Dept verification logic
  // =====================================================
  function leadPoKey() {
    const customer = document.getElementById('customerSelect')?.value || '';
    const bid = document.getElementById('bidDt')?.value || '';
    const po = document.getElementById('poRevDt')?.value || '';
    const cr = document.getElementById('crRevDt')?.value || '';
    if (!customer || !bid || !po || !cr) return null;
    return `${customer}|${bid}|${po}|${cr}`;
  }

  function setSignedUI(deptKey, signed) {
    const chk = document.querySelector(`.dept-sign-checkbox[data-dept="${deptKey}"]`);
    const status = document.querySelector(`.dept-signed-status[data-dept="${deptKey}"]`);
    if (chk) chk.checked = !!signed;
    if (status) status.style.display = signed ? 'block' : 'none';
  }

  async function refreshLeadVerification() {
    const poKey = leadPoKey();
    if (!poKey) return;

    // reset all
    document.querySelectorAll('.dept-sign-checkbox').forEach(c => setSignedUI((c.dataset.dept || '').toLowerCase(), false));

    // enable/disable based on allowed department logic
    await applyAllowedDepartmentRule(poKey);

    // load signed departments
    try {
      const res = await fetch(`/api/forms/lead-signed-departments?poKey=${encodeURIComponent(poKey)}`);
      const data = await res.json();
      (data.signed || []).forEach(s => {
        const d = (s.department || '').toLowerCase();
        setSignedUI(d, true);
      });
    } catch (e) {
      console.warn('lead-signed-departments endpoint missing/error:', e);
    }
  }

  async function applyAllowedDepartmentRule(poKey) {
  const isAdminLS = localStorage.getItem('userIsAdmin') === 'true';

  const userDeptLS = (localStorage.getItem('userDepartment') || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '-')
    .replace(/_/g, '-');

  let allowed = [];

  try {
    const res = await fetch(
      `/api/forms/check-departments?formType=LEAD&poKey=${encodeURIComponent(poKey)}`
    );
    const data = await res.json();

    if (Array.isArray(data.allowedDepartments)) {
      allowed = data.allowedDepartments.map(d =>
        String(d)
          .trim()
          .toLowerCase()
          .replace(/\s+/g, '-')
          .replace(/_/g, '-')
      );
    }
  } catch (e) {
    console.warn('check-departments (LEAD) failed, using fallback:', e);
  }

  // 🔥 FALLBACK: if backend gives nothing, allow all LEAD departments
  if (allowed.length === 0) {
    allowed = [
      'css',
      'materials',
      'technical-operations',
      'quality',
      'operations'
    ];
  }

  document.querySelectorAll('.dept-sign-checkbox').forEach(chk => {
    const deptKey = (chk.dataset.dept || '')
      .trim()
      .toLowerCase()
      .replace(/\s+/g, '-')
      .replace(/_/g, '-');

    const canSign =
      isAdminLS ||
      (allowed.includes(deptKey) && userDeptLS === deptKey);

    chk.disabled = !canSign;
    chk.style.cursor = canSign ? 'pointer' : 'not-allowed';

    if (chk.parentElement) {
      chk.parentElement.style.opacity = canSign ? '1' : '0.5';
    }
  });
}

  async function signLeadDept(deptKey) {
    const poKey = leadPoKey();
    if (!poKey) { alert('Select Customer/BID/PO/CR first.'); return; }

    const res = await fetch('/api/forms/sign-department', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ formType: 'LEAD', poKey, department: deptKey })
    });

    const out = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(out.error || 'Failed to sign');

    setSignedUI(deptKey, true);
  }

  function initLeadDeptVerification() {
    document.querySelectorAll('.dept-sign-checkbox').forEach(chk => {
      chk.addEventListener('change', async () => {
        const deptKey = (chk.dataset.dept || '').toLowerCase();

        // prevent typos / unexpected dept keys
        if (!LEAD_DEPT_KEYS.includes(deptKey)) {
          alert(`Unknown department key: ${deptKey}`);
          chk.checked = false;
          return;
        }

        // prevent uncheck
        if (!chk.checked) {
          chk.checked = true;
          return;
        }

        const ok = confirm(`Are you sure to verify/sign for ${deptKey.toUpperCase()}?`);
        if (!ok) {
          chk.checked = false;
          return;
        }

        try {
          await signLeadDept(deptKey);
          await refreshLeadVerification();
        } catch (e) {
          alert(e.message || 'Sign failed');
          chk.checked = false;
        }
      });
    });

    ['customerSelect', 'bidDt', 'poRevDt', 'crRevDt'].forEach(id => {
      document.getElementById(id)?.addEventListener('change', () => refreshLeadVerification().catch(() => { }));
    });
  }

  // =====================================================
  // COMMENTS
  // =====================================================
  async function loadComments() {
    if (!currentFormId) return;

    try {
      const response = await fetch(`/api/lead-comments/${currentFormId}`);
      if (!response.ok) return;

      const data = await response.json();
      const container = document.getElementById('commentsContainer');
      if (!container) return;

      if (!data.comments || data.comments.length === 0) {
        container.innerHTML = '<p style="color: #999; font-style: italic; padding: 20px; text-align: center;">No comments yet. Be the first to add one!</p>';
        return;
      }

      container.innerHTML = data.comments.map(comment => {
        const date = new Date(comment.createdAt);
        const formattedDate = date.toLocaleString('en-US', {
          year: 'numeric',
          month: 'short',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit'
        });

        return `
          <div style="background: white; padding: 15px; margin-bottom: 15px; border-left: 4px solid #4CAF50; border-radius: 4px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
            <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
              <div>
                <strong style="color: #333; font-size: 15px;">${comment.username}</strong>
                <span style="color: #666; margin-left: 10px; font-size: 13px;">(${String(comment.department || '').toUpperCase()})</span>
              </div>
              <span style="color: #999; font-size: 12px;">${formattedDate}</span>
            </div>
            <div style="color: #555; line-height: 1.6; white-space: pre-wrap;">${comment.text}</div>
          </div>
        `;
      }).join('');
    } catch (err) {
      console.error('Error loading comments:', err);
    }
  }

  async function postComment() {
    if (!currentFormId) return;

    const textarea = document.getElementById('newComment');
    if (!textarea) return;

    const commentText = textarea.value.trim();
    if (!commentText) {
      alert('Please enter a comment before submitting.');
      return;
    }

    try {
      const response = await fetch(`/api/lead-comments/${currentFormId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ comment: commentText })
      });

      if (!response.ok) {
        const error = await response.json().catch(() => ({}));
        alert('Failed to post comment: ' + (error.error || 'Unknown error'));
        return;
      }

      textarea.value = '';
      await loadComments();

      const container = document.getElementById('commentsContainer');
      if (container) container.scrollTop = container.scrollHeight;
    } catch (err) {
      console.error('Error posting comment:', err);
      alert('Failed to post comment. Please try again.');
    }
  }

  const submitCommentBtn = document.getElementById('submitComment');
  if (submitCommentBtn) submitCommentBtn.addEventListener('click', postComment);

  const newCommentTextarea = document.getElementById('newComment');
  if (newCommentTextarea) {
    newCommentTextarea.addEventListener('keydown', (e) => {
      if (e.ctrlKey && e.key === 'Enter') postComment();
    });
  }

  window.addEventListener('DOMContentLoaded', () => {
    loadComments();
    setInterval(loadComments, 10000);
  });
})();