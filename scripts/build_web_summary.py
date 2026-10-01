"""Gom các kết quả pipeline đã có thành 1 file JSON cho Web Demo (presentation layer).

Chạy trên host (.venv), KHÔNG dùng Spark/HDFS, KHÔNG tính lại gì: chỉ đọc các file output đã được
pipeline sinh ra rồi chép các con số cần hiển thị sang output/reports/summary.json.

Nguồn:
    output/reports/hdfs_ingestion_check.json   (raw trên HDFS)
    output/reports/preprocessing_report.json   (9 rule làm sạch)
    output/reports/rfm_report.json             (AnalysisDate, thống kê R/F/M)
    output/evaluation/k_evaluation.json        (K = 2..6, Silhouette)
    output/reports/kmeans_report.json          (model cuối)
    output/reports/cluster_profile.json        (profile từng cụm)
    output/reports/charts/*.png                (biểu đồ, câu hỏi lấy từ src/analysis/charts.py)
    web/cluster_interpretation.json            (chữ diễn giải từ docs/08_business_analysis.md — không chứa số)

Usage:
    python scripts/build_web_summary.py
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config.settings import PROJECT_ROOT, load_config  # noqa: E402

WEB_DIR = PROJECT_ROOT / "web"
OUT_NAME = "summary.json"


def read_json(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError(f"missing pipeline output: {path.relative_to(PROJECT_ROOT)} — chạy pipeline trước")
    return json.loads(path.read_text(encoding="utf-8"))


def chart_questions() -> dict:
    """Câu hỏi phân tích của từng biểu đồ (định nghĩa sẵn trong src/analysis/charts.py)."""
    from src.analysis.charts import CHARTS
    return CHARTS


def main() -> int:
    cfg = load_config()
    reports = PROJECT_ROOT / cfg["paths"]["output_reports"]
    evaluation_dir = PROJECT_ROOT / cfg["paths"]["output_evaluation"]
    cc = cfg["clustering"]

    try:
        ingest = read_json(reports / "hdfs_ingestion_check.json")
        prep = read_json(reports / cfg["preprocessing"]["report_filename"])
        rfm = read_json(reports / cfg["rfm"]["report_filename"])
        k_eval = read_json(evaluation_dir / cc["evaluation_report"])
        km = read_json(reports / cc["kmeans_report"])
        profile = read_json(reports / cfg["analysis"]["profile_json"])
        interp = read_json(WEB_DIR / "cluster_interpretation.json")
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        return 1

    # --- Biểu đồ: chỉ liệt kê PNG đang có, không vẽ lại ------------------------------------
    charts_dir = PROJECT_ROOT / cfg["analysis"]["charts_dir"]
    questions = chart_questions()
    charts = [
        {"file": name, "path": f"{cfg['analysis']['charts_dir']}/{name}", "question": q,
         "exists": (charts_dir / name).is_file()}
        for name, q in questions.items()
    ]

    # --- Diễn giải: chỉ ghép nếu khớp đúng các cụm của model hiện tại -----------------------
    cluster_ids = [c["Cluster"] for c in profile["clusters"]]
    interp_ok = interp.get("k") == km["k"] and sorted(int(i) for i in interp["clusters"]) == cluster_ids
    clusters = []
    for c in profile["clusters"]:
        text = interp["clusters"].get(str(c["Cluster"])) if interp_ok else None
        clusters.append({**c, "interpretation": text})

    variant = "log1p_standard_scaler" if km["log_transform"] else "standard_scaler"
    summary = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "data": {
            "raw_records": ingest["records"],
            "raw_bytes": ingest["hdfs_file"]["bytes"],
            "raw_hdfs_uri": ingest["hdfs_uri"],
            "raw_blocks": len(ingest["hdfs_file"]["blocks"]),
            "raw_replication": ingest["hdfs_file"]["replication"],
            "raw_partitions": ingest["partitions"],
            "processed_records": prep["output_rows"],
            "pct_rows_kept": prep["pct_rows_kept"],
            "rows_removed": prep["rows_removed_total"],
            "invoices": prep["summary"]["invoices"],
            "amount_total": prep["summary"]["amount_total"],
            "date_min": prep["summary"]["invoice_date_min"],
            "date_max": prep["summary"]["invoice_date_max"],
            "processed_files": prep["output"]["parquet_files"],
            "rules": [{"step": r["step"], "rule": r["rule"], "removed": r["records_removed"], "after": r["records_after"]}
                      for r in prep["rules"]],
            "spark_version": ingest["spark_version"],
        },
        "rfm": {
            "analysis_date": rfm["analysis_date"],
            "customers": rfm["customers"],
            "definitions": rfm["definitions"],
            "stats": rfm["stats"],
            "frequency_eq_1": rfm["frequency_eq_1"],
            "top_1pct_monetary_share_pct": rfm["top_1pct_monetary_share_pct"],
            "correlation": rfm["correlation"],
        },
        "evaluation": {
            "k_range": [cc["k_min"], cc["k_max"]],
            "selected_k": km["k"],
            "selected_variant": variant,
            "log_transform": km["log_transform"],
            "silhouette": km["silhouette"],
            "num_iter": km["num_iter"],
            "max_iter": km["max_iter"],
            "seed": km["seed"],
            "cluster_sizes": km["cluster_sizes"],
            "variants": {
                name: [{"k": r["k"], "silhouette": r["silhouette"], "min_cluster_size": r["min_cluster_size"],
                        "min_cluster_pct": r["min_cluster_pct"], "cluster_sizes": r["cluster_sizes"]}
                       for r in v["results"]]
                for name, v in k_eval["variants"].items()
            },
        },
        "clusters": {
            "k": profile["k"],
            "overall": profile["overall"],
            "items": clusters,
            "interpretation_source": interp.get("source") if interp_ok else None,
        },
        "charts": charts,
    }

    # --- Kiểm tra các output có nhất quán với nhau không (không sửa gì, chỉ báo) -------------
    sel = next(r for r in summary["evaluation"]["variants"][variant] if r["k"] == km["k"])
    checks = {
        "raw_records_match_preprocessing_input": ingest["records"] == prep["input_rows"],
        "customers_match_rfm_kmeans_profile": rfm["customers"] == km["customers"] == profile["customers"]
        == prep["summary"]["customers"],
        "selected_k_matches_config": km["k"] == cc["selected_k"] == profile["k"],
        "silhouette_matches_evaluation": km["silhouette"] == sel["silhouette"],
        "cluster_sizes_match_profile": km["cluster_sizes"] == [c["CustomerCount"] for c in profile["clusters"]],
        "all_charts_exist": all(c["exists"] for c in charts),
        "interpretation_matches_clusters": interp_ok,
    }
    summary["checks"] = checks

    out = reports / OUT_NAME
    out.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    for name, ok in checks.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    passed = all(checks.values())
    print(f"summary -> {out.relative_to(PROJECT_ROOT)}")
    print("RESULT:", "PASS" if passed else "FAIL")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
