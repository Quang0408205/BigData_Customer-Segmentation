// Web Demo — chỉ ĐỌC output/reports/summary.json (do scripts/build_web_summary.py tạo từ output pipeline).
// Không tính lại RFM, không chạy mô hình, không gọi Spark/HDFS. Biểu đồ SVG vẽ lại từ chính các số trong summary.json.
"use strict";

const SUMMARY_URL = "../output/reports/summary.json";
const ACCENT = "#245B73", ACCENT_LIGHT = "#A9BEC8", GRID = "#E8EBEE", AXIS = "#9AA4AE", MUTED = "#68707A", INK = "#20242A";
// Màu nhóm trùng với các biểu đồ PNG (src/analysis/charts.py) — chỉ dùng làm ô nhận diện nhỏ
const CLUSTER_COLORS = ["#2E86AB", "#E4572E", "#76B041", "#F3A712", "#8E6C8A", "#5C5C5C"];

const RULE_NAMES = {
  invalid_invoice_date: "InvoiceDate không hợp lệ",
  invalid_values: "Giá trị không đọc được (Quantity, UnitPrice…)",
  duplicate_rows: "Dòng trùng lặp",
  cancelled_invoices: "Hóa đơn hủy và dòng mua bị hủy tương ứng",
  non_sale_invoices: "Hóa đơn điều chỉnh (mã A…)",
  quantity_non_positive: "Quantity ≤ 0",
  unit_price_non_positive: "UnitPrice ≤ 0",
  missing_customer_id: "Thiếu CustomerID",
  non_product_stockcode: "Mã không phải sản phẩm (phí, test, voucher…)",
};
const CHART_TITLES = {
  "01_customer_count_by_cluster.png": "Số khách hàng theo nhóm",
  "02_avg_recency_by_cluster.png": "Độ gần đây trung bình theo nhóm",
  "03_avg_frequency_by_cluster.png": "Tần suất mua trung bình theo nhóm",
  "04_avg_monetary_by_cluster.png": "Giá trị mua trung bình theo nhóm",
  "05_customer_vs_revenue_share.png": "Tỉ trọng khách hàng và doanh thu theo nhóm",
  "06_rfm_distribution_by_cluster.png": "Phân phối Recency / Frequency / Monetary trong từng nhóm",
  "07_k_vs_silhouette.png": "Silhouette Score và nhóm nhỏ nhất theo K",
};
const WIDE_CHARTS = new Set(["06_rfm_distribution_by_cluster.png", "07_k_vs_silhouette.png"]);
const VARIANT_LABELS = {
  log1p_standard_scaler: "log1p + StandardScaler (dùng trong pipeline)",
  standard_scaler: "StandardScaler, không log1p (đối chứng)",
};

// Định dạng số giống docs/terminal (1,067,371 · 0.5325)
const fmt = (n, d = 0) => Number(n).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
const $ = (sel) => document.querySelector(sel);
const el = (tag, attrs = {}, html = "") => {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  e.innerHTML = html;
  return e;
};
const kvRows = (tbody, rows) => rows.forEach(([k, v]) => tbody.append(el("tr", {}, `<th>${k}</th><td>${v}</td>`)));

function bindAll(values) {
  document.querySelectorAll("[data-bind]").forEach((node) => {
    const v = values[node.dataset.bind];
    if (v !== undefined) node.textContent = v;
  });
}

// ---------- SVG charts ----------
function niceStep(raw) {
  const p = 10 ** Math.floor(Math.log10(raw)), f = raw / p;
  return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10) * p;
}

function barChart(target, { categories, series, yLabel, valueFmt, tickFmt = (v) => fmt(v) }) {
  const W = 620, H = 300, legendH = series.length > 1 ? 24 : 0;
  const m = { l: 62, r: 12, t: 22 + legendH, b: 40 };
  const max = Math.max(...series.flatMap((s) => s.values));
  const step = niceStep(max / 5), yMax = Math.ceil((max * 1.08) / step) * step;
  const pw = W - m.l - m.r, ph = H - m.t - m.b;
  const y = (v) => m.t + ph - (v / yMax) * ph;
  const gw = pw / categories.length, bw = Math.min(56, (gw * 0.62) / series.length);
  let s = `<svg viewBox="0 0 ${W} ${H}" role="img" font-family="inherit">`;
  for (let v = 0; v <= yMax + 1e-9; v += step) {
    s += `<line x1="${m.l}" x2="${W - m.r}" y1="${y(v)}" y2="${y(v)}" stroke="${GRID}"/>`;
    s += `<text x="${m.l - 8}" y="${y(v) + 4}" text-anchor="end" font-size="12" fill="${MUTED}">${tickFmt(v)}</text>`;
  }
  s += `<line x1="${m.l}" x2="${W - m.r}" y1="${y(0)}" y2="${y(0)}" stroke="${AXIS}"/>`;
  s += `<text transform="translate(14 ${m.t + ph / 2}) rotate(-90)" text-anchor="middle" font-size="12" fill="${MUTED}">${yLabel}</text>`;
  categories.forEach((c, i) => {
    const gx = m.l + gw * i + gw / 2;
    s += `<text x="${gx}" y="${H - m.b + 20}" text-anchor="middle" font-size="12.5" fill="${INK}">${c}</text>`;
    series.forEach((se, j) => {
      const x = gx - (series.length * bw) / 2 + j * bw, v = se.values[i];
      s += `<rect x="${x + 2}" y="${y(v)}" width="${bw - 4}" height="${y(0) - y(v)}" fill="${se.color}"/>`;
      s += `<text x="${x + bw / 2}" y="${y(v) - 5}" text-anchor="middle" font-size="11.5" fill="${INK}">${valueFmt(v)}</text>`;
    });
  });
  if (legendH) {
    let lx = m.l;
    series.forEach((se) => {
      s += `<rect x="${lx}" y="6" width="11" height="11" fill="${se.color}"/>`;
      s += `<text x="${lx + 17}" y="16" font-size="12" fill="${INK}">${se.name}</text>`;
      lx += 26 + se.name.length * 6.6;
    });
  }
  target.innerHTML = s + "</svg>";
  target.classList.add("ready");
}

function silhouetteChart(target, ev) {
  const W = 980, H = 345, m = { l: 60, r: 24, t: 56, b: 46 }, pad = 40; // t: chừa chỗ cho chú thích + nhãn "K được chọn"
  const ks = ev.variants[ev.selected_variant].map((r) => r.k);
  const all = Object.values(ev.variants).flat().map((r) => r.silhouette);
  const yMin = Math.floor((Math.min(...all) - 0.03) * 20) / 20, yMax = Math.ceil((Math.max(...all) + 0.03) * 20) / 20;
  const x = (k) => m.l + pad + ((k - ks[0]) / (ks[ks.length - 1] - ks[0])) * (W - m.l - m.r - 2 * pad);
  const y = (v) => m.t + (1 - (v - yMin) / (yMax - yMin)) * (H - m.t - m.b);
  const names = Object.keys(ev.variants);
  // điểm cao hơn ghi nhãn phía trên, điểm thấp hơn ghi phía dưới -> không đè nhau
  const labelDy = (name, k, v) =>
    names.filter((n) => n !== name).map((n) => ev.variants[n].find((r) => r.k === k)).some((o) => o && o.silhouette > v) ? 18 : -10;
  const styles = { log1p_standard_scaler: { color: ACCENT, dash: "" }, standard_scaler: { color: AXIS, dash: "5 4" } };

  let s = `<svg viewBox="0 0 ${W} ${H}" role="img" font-family="inherit">`;
  const bw = ((W - m.l - m.r - 2 * pad) / (ks.length - 1)) * 0.45;
  s += `<rect x="${x(ev.selected_k) - bw / 2}" y="${m.t}" width="${bw}" height="${H - m.t - m.b}" fill="#EEF1F3"/>`;
  s += `<text x="${x(ev.selected_k)}" y="${m.t - 8}" text-anchor="middle" font-size="12" fill="${ACCENT}" font-weight="600">K được chọn</text>`;
  for (let v = yMin; v <= yMax + 1e-9; v += 0.05) {
    s += `<line x1="${m.l}" x2="${W - m.r}" y1="${y(v)}" y2="${y(v)}" stroke="${GRID}"/>`;
    s += `<text x="${m.l - 8}" y="${y(v) + 4}" text-anchor="end" font-size="12" fill="${MUTED}">${v.toFixed(2)}</text>`;
  }
  s += `<line x1="${m.l}" x2="${W - m.r}" y1="${H - m.b}" y2="${H - m.b}" stroke="${AXIS}"/>`;
  ks.forEach((k) => (s += `<text x="${x(k)}" y="${H - m.b + 19}" text-anchor="middle" font-size="12.5" fill="${INK}">${k}</text>`));
  s += `<text x="${(m.l + W - m.r) / 2}" y="${H - 6}" text-anchor="middle" font-size="12" fill="${MUTED}">Số nhóm K</text>`;
  s += `<text transform="translate(16 ${(m.t + H - m.b) / 2}) rotate(-90)" text-anchor="middle" font-size="12" fill="${MUTED}">Silhouette Score</text>`;
  let lx = m.l;
  for (const [name, rows] of Object.entries(ev.variants)) {
    const st = styles[name] || { color: MUTED, dash: "" };
    s += `<polyline points="${rows.map((r) => `${x(r.k)},${y(r.silhouette)}`).join(" ")}" fill="none" stroke="${st.color}" stroke-width="2" stroke-dasharray="${st.dash}"/>`;
    rows.forEach((r) => {
      const sel = name === ev.selected_variant && r.k === ev.selected_k;
      s += `<circle cx="${x(r.k)}" cy="${y(r.silhouette)}" r="${sel ? 5.5 : 3.5}" fill="${st.color}" stroke="#fff" stroke-width="1.5"/>`;
      s += `<text x="${x(r.k)}" y="${y(r.silhouette) + labelDy(name, r.k, r.silhouette)}" text-anchor="middle" font-size="11.5" fill="${sel ? ACCENT : st.color === AXIS ? MUTED : st.color}" font-weight="${sel ? 700 : 400}">${r.silhouette.toFixed(4)}</text>`;
    });
    s += `<line x1="${lx}" x2="${lx + 24}" y1="12" y2="12" stroke="${st.color}" stroke-width="2" stroke-dasharray="${st.dash}"/>`;
    s += `<text x="${lx + 30}" y="16" font-size="12" fill="${INK}">${VARIANT_LABELS[name] || name}</text>`;
    lx += 330;
  }
  target.innerHTML = s + "</svg>";
  target.classList.add("ready");
}

// ---------- Sections ----------
function renderOverview(s) {
  const d = s.data;
  kvRows($("#dataset-table tbody"), [
    ["Nguồn dữ liệu", "UCI Machine Learning Repository — Online Retail II (giao dịch bán lẻ trực tuyến, Anh)"],
    ["Khoảng thời gian", `${d.date_min.slice(0, 10)} → ${d.date_max.slice(0, 10)}`],
    ["File raw trên HDFS", `<code>${d.raw_hdfs_uri}</code>`],
    ["Kích thước / lưu trữ", `${fmt(d.raw_bytes)} bytes · ${d.raw_blocks} block · replication ${d.raw_replication}`],
    ["Đọc bằng Spark", `${d.raw_partitions} partition song song`],
    ["Sau tiền xử lý", `${fmt(d.processed_records)} dòng · ${fmt(d.invoices)} hóa đơn · ${d.processed_files} file Parquet`],
    ["Tổng giá trị mua (sau xử lý)", fmt(d.amount_total, 2)],
    ["Ngày tham chiếu RFM", s.rfm.analysis_date],
  ]);
}

function renderRules(d) {
  const tbody = $("#rules-table tbody");
  d.rules.forEach((r) => tbody.append(el("tr", {}, `<td class="num">${r.step}</td><td>${RULE_NAMES[r.rule] || r.rule}</td>
    <td><code>${r.rule}</code></td><td class="num">${fmt(r.removed)}</td><td class="num">${fmt(r.after)}</td>`)));
  $("#rules-note").textContent =
    `Tổng cộng loại ${fmt(d.rows_removed)} dòng; giữ lại ${fmt(d.processed_records)} dòng (${fmt(d.pct_rows_kept, 2)}%). ` +
    `Số dòng bị loại của mỗi quy tắc phụ thuộc thứ tự áp dụng; kết quả cuối không đổi.`;
}

function renderRfm(rfm) {
  const meta = [
    ["Recency", "Độ gần đây (Recency)", "Số ngày từ lần mua cuối đến ngày tham chiếu", 0],
    ["Frequency", "Tần suất (Frequency)", "Số giao dịch (hóa đơn khác nhau) của khách", 0],
    ["Monetary", "Tổng giá trị (Monetary)", "Tổng tiền đã mua (Quantity × UnitPrice)", 2],
  ];
  const tbody = $("#rfm-table tbody");
  meta.forEach(([key, name, meaning, d]) => {
    const st = rfm.stats[key];
    tbody.append(el("tr", {}, `<td>${name}</td><td>${meaning}</td><td class="num"><b>${fmt(st.median, d)}</b></td>
      <td class="num">${fmt(st.mean, 2)}</td><td class="num">${fmt(st.p25, d)}</td><td class="num">${fmt(st.p75, d)}</td>
      <td class="num">${fmt(st.min, d)}</td><td class="num">${fmt(st.max, d)}</td><td class="num">${fmt(st.skewness, 2)}</td>`));
  });
  kvRows($("#rfm-def-table tbody"), meta.map(([key, name]) => [name, `<code>${rfm.definitions[key]}</code>`]));
  $("#rfm-note").textContent =
    `${fmt(rfm.customers)} khách hàng. ${fmt(rfm.frequency_eq_1)} khách (${fmt((100 * rfm.frequency_eq_1) / rfm.customers, 1)}%) chỉ mua 1 lần; ` +
    `1% khách chi nhiều nhất chiếm ${fmt(rfm.top_1pct_monetary_share_pct, 2)}% tổng giá trị mua. Frequency và Monetary lệch phải mạnh ` +
    `(trung bình lớn hơn nhiều so với trung vị) nên được biến đổi log1p trước khi chuẩn hóa.`;
}

function renderClusters(cl) {
  const items = cl.items, labels = items.map((c) => `Nhóm ${c.Cluster}`);
  barChart($("#chart-customers"), {
    categories: labels,
    series: [{ name: "Khách hàng", values: items.map((c) => c.CustomerCount), color: ACCENT }],
    yLabel: "Số khách hàng", valueFmt: (v) => fmt(v),
  });
  barChart($("#chart-share"), {
    categories: labels,
    series: [
      { name: "% số khách", values: items.map((c) => c.CustomerPct), color: ACCENT_LIGHT },
      { name: "% doanh thu", values: items.map((c) => c.MonetaryPct), color: ACCENT },
    ],
    yLabel: "%", valueFmt: (v) => `${fmt(v, 1)}%`, tickFmt: (v) => fmt(v),
  });

  const tbody = $("#cluster-table tbody");
  items.forEach((c) => {
    const color = CLUSTER_COLORS[c.Cluster % CLUSTER_COLORS.length], it = c.interpretation;
    tbody.append(el("tr", {}, `<td><span class="swatch" style="background:${color}"></span>Nhóm ${c.Cluster}${it ? `<span class="sub">${it.name}</span>` : ""}</td>
      <td class="num">${fmt(c.CustomerCount)}</td><td class="num">${fmt(c.CustomerPct, 1)}%</td><td class="num">${fmt(c.AvgRecency, 1)}</td>
      <td class="num">${fmt(c.AvgFrequency, 2)}</td><td class="num">${fmt(c.AvgMonetary, 2)}</td><td class="num">${fmt(c.MonetaryPct, 1)}%</td>`));
  });
  const o = cl.overall;
  $("#cluster-table tfoot").append(el("tr", {}, `<td>Toàn bộ</td><td class="num">${fmt(o.CustomerCount)}</td><td class="num">100%</td>
    <td class="num">${fmt(o.AvgRecency, 1)}</td><td class="num">${fmt(o.AvgFrequency, 2)}</td><td class="num">${fmt(o.AvgMonetary, 2)}</td><td class="num">100%</td>`));

  const notes = $("#cluster-notes");
  items.forEach((c) => {
    const it = c.interpretation;
    notes.append(el("div", { class: "note" }, `
      <div class="note-head"><b>Nhóm ${c.Cluster}${it ? ` — ${it.name}` : ""}</b>
        <span>${fmt(c.CustomerCount)} khách · ${fmt(c.MonetaryPct, 1)}% doanh thu · trung vị R / F / M: ${fmt(c.MedianRecency)} ngày / ${fmt(c.MedianFrequency)} lần / ${fmt(c.MedianMonetary, 0)}</span></div>
      ${it ? `<dl><dt>Đặc điểm từ dữ liệu</dt><dd>${it.finding}</dd>
        <dt>Diễn giải kinh doanh</dt><dd>${it.interpretation}</dd>
        <dt>Đề xuất</dt><dd>${it.recommendation}</dd></dl>` : `<p>Chưa có nhận xét cho nhóm này.</p>`}`));
  });
  $("#interp-note").innerHTML = cl.interpretation_source
    ? `Tên nhóm và nhận xét do người phân tích đề xuất sau khi xem số liệu (<code>docs/08_business_analysis.md</code>); ` +
      `đề xuất chưa được kiểm chứng bằng thực nghiệm.`
    : "Nhận xét không khớp với kết quả phân nhóm hiện tại nên không hiển thị.";
}

function renderEvaluation(ev) {
  silhouetteChart($("#chart-silhouette"), ev);
  const other = Object.keys(ev.variants).find((v) => v !== ev.selected_variant);
  const tbody = $("#k-table tbody");
  ev.variants[ev.selected_variant].forEach((r) => {
    const o = other ? ev.variants[other].find((x) => x.k === r.k) : null;
    tbody.append(el("tr", r.k === ev.selected_k ? { class: "selected" } : {}, `<td class="num">${r.k}</td>
      <td class="num">${r.silhouette.toFixed(4)}</td><td class="num">${fmt(r.min_cluster_size)} (${fmt(r.min_cluster_pct, 2)}%)</td>
      <td class="num">${o ? o.silhouette.toFixed(4) : "—"}</td><td class="num">${o ? `${fmt(o.min_cluster_size)} (${fmt(o.min_cluster_pct, 2)}%)` : "—"}</td>`));
  });
  kvRows($("#model-table tbody"), [
    ["K được chọn", String(ev.selected_k)],
    ["Silhouette Score", fmt(ev.silhouette, 4)],
    ["Chuẩn hóa", ev.log_transform ? "log1p + StandardScaler" : "StandardScaler"],
    ["Hội tụ sau", `${ev.num_iter} vòng lặp (tối đa ${ev.max_iter})`],
    ["Seed", String(ev.seed)],
    ["K đã thử", `${ev.k_range[0]} – ${ev.k_range[1]}`],
  ]);
  const total = ev.cluster_sizes.reduce((a, b) => a + b, 0), sizes = $("#size-table tbody");
  ev.cluster_sizes.forEach((n, i) => sizes.append(el("tr", {}, `<td>Nhóm ${i}</td><td class="num">${fmt(n)}</td><td class="num">${fmt((100 * n) / total, 1)}%</td>`)));
}

function renderGallery(charts) {
  const g = $("#gallery");
  charts.forEach((c, i) => {
    const fig = el("figure", WIDE_CHARTS.has(c.file) ? { class: "wide" } : {});
    const a = el("a", { href: `../${c.path}`, target: "_blank", rel: "noopener" });
    const img = el("img", { src: `../${c.path}`, alt: CHART_TITLES[c.file] || c.file, loading: "lazy" });
    img.addEventListener("error", () => a.replaceWith(el("div", { class: "img-missing" }, `Không tìm thấy ảnh <code>${c.path}</code>`)));
    a.append(img);
    fig.append(a, el("figcaption", {}, `<b>Hình ${i + 1}. ${CHART_TITLES[c.file] || c.file}</b><span>${c.question} · <code>${c.file}</code></span>`));
    g.append(fig);
  });
}

function setupScrollSpy() {
  const links = [...document.querySelectorAll(".sidebar a")];
  const byId = new Map(links.map((a) => [a.getAttribute("href").slice(1), a]));
  const visible = new Set();
  const obs = new IntersectionObserver((entries) => {
    entries.forEach((e) => (e.isIntersecting ? visible.add(e.target.id) : visible.delete(e.target.id)));
    const current = [...byId.keys()].find((id) => visible.has(id));
    links.forEach((a) => a.classList.toggle("active", a === byId.get(current)));
  }, { rootMargin: "-80px 0px -55% 0px" });
  byId.forEach((_, id) => { const s = document.getElementById(id); if (s) obs.observe(s); });
}

async function main() {
  setupScrollSpy();
  let s;
  try {
    const res = await fetch(SUMMARY_URL, { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    s = await res.json();
  } catch (e) {
    const box = $("#load-error");
    box.hidden = false;
    box.innerHTML = `Không đọc được <code>output/reports/summary.json</code> (${e.message}). ` +
      `Chạy web bằng <code>python scripts/run_web_demo.py</code> (lệnh này tạo summary rồi mở server).`;
    return;
  }
  const d = s.data, ev = s.evaluation;
  bindAll({
    raw_records: fmt(d.raw_records),
    processed_records: fmt(d.processed_records),
    pct_kept: `giữ lại ${fmt(d.pct_rows_kept, 2)}%`,
    customers: fmt(s.rfm.customers),
    selected_k: String(ev.selected_k),
    k_range: `chọn từ K = ${ev.k_range[0]}..${ev.k_range[1]}`,
    k_range_short: `${ev.k_range[0]}..${ev.k_range[1]}`,
    silhouette: fmt(ev.silhouette, 4),
    analysis_date: s.rfm.analysis_date,
    spark_version: d.spark_version,
    generated_at: s.generated_at,
  });
  renderOverview(s);
  renderRules(d);
  renderRfm(s.rfm);
  renderClusters(s.clusters);
  renderEvaluation(ev);
  renderGallery(s.charts);
  $("#arch-facts").textContent =
    `File raw trên HDFS: ${fmt(d.raw_bytes)} bytes, ${d.raw_blocks} block, replication ${d.raw_replication}; Spark ${d.spark_version} ` +
    `đọc thành ${d.raw_partitions} partition. Dữ liệu sau xử lý: ${d.processed_files} file Parquet.`;
}

main();
