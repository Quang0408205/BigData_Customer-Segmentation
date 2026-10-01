"""Visualization (Phase 10): biểu đồ PNG (Matplotlib) cho phân tích cụm.

Chạy trên HOST (Windows .venv) — host không đọc được HDFS nên chỉ đọc các bảng NHỎ do Spark export:
    output/reports/cluster_profile.csv        (4 dòng: profile từng cụm)   <- src/analysis/cluster_analysis.py
    output/clustering/customer_clusters.csv   (5,8xx khách: R, F, M, Cluster)
    output/evaluation/k_evaluation.csv        (K = 2..6, Silhouette, cluster size) <- src/clustering/evaluate.py
pandas chỉ dùng cho các bảng nhỏ đã aggregate này (không xử lý dataset giao dịch).

Mỗi biểu đồ trả lời MỘT câu hỏi phân tích (ghi trong CHARTS). Biểu đồ chỉ hiển thị số liệu,
không tự sinh kết luận nghiệp vụ; tên cụm giữ dạng "Cluster i".

    python -m src.analysis.charts
"""

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # vẽ ra file, không cần cửa sổ
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config.settings import PROJECT_ROOT, load_config  # noqa: E402

COLORS = ["#2E86AB", "#E4572E", "#76B041", "#F3A712", "#8E6C8A", "#5C5C5C"]
DPI = 150

# file -> câu hỏi phân tích mà biểu đồ trả lời (in ra khi chạy, dùng trong docs)
CHARTS = {
    "01_customer_count_by_cluster.png": "Mỗi cụm có bao nhiêu khách (quy mô từng nhóm)?",
    "02_avg_recency_by_cluster.png": "Các cụm khác nhau thế nào về thời gian kể từ lần mua cuối?",
    "03_avg_frequency_by_cluster.png": "Các cụm khác nhau thế nào về số lần mua?",
    "04_avg_monetary_by_cluster.png": "Các cụm khác nhau thế nào về tổng chi tiêu?",
    "05_customer_vs_revenue_share.png": "Tỉ trọng doanh thu của mỗi cụm so với tỉ trọng số khách?",
    "06_rfm_distribution_by_cluster.png": "Phân phối R/F/M trong từng cụm (trung bình có đại diện không, các cụm chồng lấn bao nhiêu)?",
    "07_k_vs_silhouette.png": "Silhouette và cụm nhỏ nhất thay đổi thế nào theo K — vì sao chọn K = 4?",
}


def _cluster_labels(profile: pd.DataFrame) -> list[str]:
    return [f"Cluster {c}" for c in profile["Cluster"]]


def _save(fig, out_dir: Path, name: str) -> Path:
    fig.tight_layout()
    path = out_dir / name
    fig.savefig(path, dpi=DPI)
    plt.close(fig)
    return path


def chart_customer_count(profile: pd.DataFrame, out_dir: Path) -> Path:
    fig, ax = plt.subplots(figsize=(7, 4.5))
    labels = _cluster_labels(profile)
    bars = ax.bar(labels, profile["CustomerCount"], color=COLORS[: len(profile)])
    for b, n, pct in zip(bars, profile["CustomerCount"], profile["CustomerPct"]):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height(), f"{n:,}\n({pct}%)", ha="center", va="bottom", fontsize=9)
    ax.set_title(f"Số khách hàng theo cụm (tổng {profile['CustomerCount'].sum():,} khách)")
    ax.set_ylabel("Số khách hàng")
    ax.set_ylim(0, profile["CustomerCount"].max() * 1.2)
    return _save(fig, out_dir, "01_customer_count_by_cluster.png")


def chart_metric(profile: pd.DataFrame, overall: dict, metric: str, unit: str, fmt: str, out_dir: Path, name: str) -> Path:
    """Cột = trung bình (đúng yêu cầu 'Average'); chấm đen = trung vị (vì R/F/M lệch, mean bị kéo bởi khách lớn)."""
    fig, ax = plt.subplots(figsize=(7, 4.5))
    labels = _cluster_labels(profile)
    avg, med = profile[f"Avg{metric}"], profile[f"Median{metric}"]
    bars = ax.bar(labels, avg, color=COLORS[: len(profile)], label="Trung bình (mean)")
    ax.scatter(labels, med, color="black", zorder=3, label="Trung vị (median)")
    top = max(avg.max(), med.max())
    for b, v, m in zip(bars, avg, med):
        # đặt nhãn phía trên cả cột lẫn chấm median để không đè nhau
        ax.text(b.get_x() + b.get_width() / 2, max(v, m) + top * 0.02, format(v, fmt), ha="center", va="bottom", fontsize=9)
    ax.axhline(overall[f"Avg{metric}"], color="gray", linestyle="--", linewidth=1,
               label=f"TB toàn bộ khách = {format(overall[f'Avg{metric}'], fmt)}")
    ax.set_title(f"{metric} trung bình theo cụm")
    ax.set_ylabel(unit)
    ax.set_ylim(0, top * 1.22)
    ax.legend(fontsize=8)
    return _save(fig, out_dir, name)


def chart_share(profile: pd.DataFrame, out_dir: Path) -> Path:
    fig, ax = plt.subplots(figsize=(7, 4.5))
    x = range(len(profile))
    w = 0.38
    b1 = ax.bar([i - w / 2 for i in x], profile["CustomerPct"], w, color="#9DB4C0", label="% số khách")
    b2 = ax.bar([i + w / 2 for i in x], profile["MonetaryPct"], w, color="#2E86AB", label="% doanh thu (Monetary)")
    for b in list(b1) + list(b2):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height(), f"{b.get_height():.1f}%", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(list(x), _cluster_labels(profile))
    ax.set_title("Tỉ trọng số khách và tỉ trọng doanh thu theo cụm")
    ax.set_ylabel("%")
    ax.set_ylim(0, max(profile["CustomerPct"].max(), profile["MonetaryPct"].max()) * 1.15)
    ax.legend(fontsize=8)
    return _save(fig, out_dir, "05_customer_vs_revenue_share.png")


def chart_distribution(customers: pd.DataFrame, k: int, out_dir: Path) -> Path:
    fig, axes = plt.subplots(1, 3, figsize=(13, 4.5))
    specs = [("Recency", "ngày", False), ("Frequency", "số hóa đơn (thang log)", True), ("Monetary", "tiền (thang log)", True)]
    for ax, (metric, unit, log) in zip(axes, specs):
        data = [customers.loc[customers["Cluster"] == c, metric] for c in range(k)]
        bp = ax.boxplot(data, patch_artist=True, showfliers=True, flierprops={"markersize": 2, "alpha": 0.4})
        for patch, color in zip(bp["boxes"], COLORS):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        ax.set_xticks(range(1, k + 1), [f"C{c}" for c in range(k)])
        if log:
            ax.set_yscale("log")
        ax.set_title(metric)
        ax.set_ylabel(unit)
    fig.suptitle("Phân phối Recency / Frequency / Monetary trong từng cụm (hộp = p25–p75, vạch = trung vị)")
    return _save(fig, out_dir, "06_rfm_distribution_by_cluster.png")


def chart_k_silhouette(k_eval: pd.DataFrame, selected_k: int, out_dir: Path) -> Path:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
    names = {"log1p_standard_scaler": "log1p + StandardScaler (dùng)", "standard_scaler": "StandardScaler (không log1p)"}
    # 2 đường đặt nhãn lệch nhau (trên / dưới) để không đè lên nhau
    for (variant, label), color, dy in zip(names.items(), ["#2E86AB", "#E4572E"], [-13, 7]):
        d = k_eval[k_eval["variant"] == variant].sort_values("k")
        ax1.plot(d["k"], d["silhouette"], marker="o", color=color, label=label)
        ax2.plot(d["k"], d["min_cluster_pct"], marker="o", color=color, label=label)
        for kk, s in zip(d["k"], d["silhouette"]):
            ax1.annotate(f"{s:.3f}", (kk, s), textcoords="offset points", xytext=(0, dy), ha="center", fontsize=8, color=color)
        for kk, pct, n in zip(d["k"], d["min_cluster_pct"], d["min_cluster_size"]):
            ax2.annotate(f"{n:,} khách", (kk, pct), textcoords="offset points", xytext=(0, 7), ha="center", fontsize=8, color=color)
    for ax in (ax1, ax2):
        ax.axvline(selected_k, color="gray", linestyle="--", linewidth=1)
        ax.set_xticks(sorted(k_eval["k"].unique()))
        ax.set_xlabel("K (số cụm)")
        ax.legend(fontsize=8)
    ax1.set_title("Silhouette Score theo K")
    ax1.set_ylabel("Silhouette")
    ax1.set_ylim(k_eval["silhouette"].min() - 0.03, k_eval["silhouette"].max() + 0.03)
    ax2.set_title("Cụm nhỏ nhất theo K (nhãn = số khách của cụm đó)")
    ax2.set_ylabel("% khách của cụm nhỏ nhất")
    ax2.set_ylim(-2, k_eval["min_cluster_pct"].max() * 1.15)
    fig.suptitle(f"Chọn K (đường đứt = K đã chọn: {selected_k})")
    return _save(fig, out_dir, "07_k_vs_silhouette.png")


def main() -> int:
    cfg = load_config()
    ac = cfg["analysis"]
    import json

    reports = PROJECT_ROOT / cfg["paths"]["output_reports"]
    profile = pd.read_csv(reports / ac["profile_csv"]).sort_values("Cluster")
    overall = json.loads((reports / ac["profile_json"]).read_text(encoding="utf-8"))["overall"]
    customers = pd.read_csv(PROJECT_ROOT / cfg["paths"]["output_clustering"] / ac["customers_csv"],
                            dtype={"CustomerID": str})
    k_eval = pd.read_csv(PROJECT_ROOT / cfg["paths"]["output_evaluation"]
                         / Path(cfg["clustering"]["evaluation_report"]).with_suffix(".csv").name)
    k = cfg["clustering"]["selected_k"]
    out_dir = PROJECT_ROOT / ac["charts_dir"]
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"profile={len(profile)} cụm, customers={len(customers):,}, k_eval={len(k_eval)} dòng -> {out_dir}")

    paths = [
        chart_customer_count(profile, out_dir),
        chart_metric(profile, overall, "Recency", "ngày kể từ lần mua cuối", ",.1f", out_dir, "02_avg_recency_by_cluster.png"),
        chart_metric(profile, overall, "Frequency", "số hóa đơn", ",.2f", out_dir, "03_avg_frequency_by_cluster.png"),
        chart_metric(profile, overall, "Monetary", "tổng tiền mua hàng", ",.0f", out_dir, "04_avg_monetary_by_cluster.png"),
        chart_share(profile, out_dir),
        chart_distribution(customers, k, out_dir),
        chart_k_silhouette(k_eval, k, out_dir),
    ]
    for p in paths:
        print(f"  {p.name:<40} {p.stat().st_size:>8,} bytes  — {CHARTS[p.name]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
