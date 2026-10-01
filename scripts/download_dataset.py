"""Tải dataset Online Retail II từ UCI và lưu vào data/raw.

Các bước:
    1. Download file zip chính thức (kiểm tra HTTP status + kích thước).
    2. Kiểm tra zip hợp lệ, giải nén lấy online_retail_II.xlsx.
    3. Gộp 2 sheet Excel thành 1 file CSV (giữ nguyên tên cột và giá trị gốc).
       Lý do: Spark đọc CSV native, còn Excel cần thư viện ngoài.
    4. Kiểm tra file CSV tồn tại, không rỗng; ghi metadata (số dòng từng sheet, sha256).

Không download lại nếu CSV đã tồn tại, trừ khi dùng --force.

Usage:
    python scripts/download_dataset.py
    python scripts/download_dataset.py --force
"""

import argparse
import hashlib
import http.client
import json
import logging
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config.settings import PROJECT_ROOT, load_config  # noqa: E402

log = logging.getLogger("download_dataset")

CHUNK_SIZE = 1024 * 1024  # 1 MB
TIMEOUT_SECONDS = 60
PROGRESS_EVERY = 10 * 1024 * 1024  # log mỗi 10 MB
# Giữ các cột định danh ở dạng chuỗi để không bị đổi thành số
# (ví dụ Invoice "C489449", StockCode "85123A", Customer ID "13085").
STRING_COLUMNS = {"Invoice": str, "StockCode": str, "Customer ID": str}


class DownloadError(RuntimeError):
    pass


class PermanentDownloadError(DownloadError):
    """Lỗi không nên retry (HTTP 4xx: sai URL, file không tồn tại, bị chặn)."""


def _download_once(url: str, tmp: Path) -> int:
    """Một lần tải. Trả về số byte nhận được, raise DownloadError nếu lỗi."""
    try:
        with urllib.request.urlopen(url, timeout=TIMEOUT_SECONDS) as resp:
            if resp.status != 200:
                raise DownloadError(f"HTTP status {resp.status} for {url}")
            # UCI trả chunked encoding, thường KHÔNG có Content-Length;
            # khi đó tính toàn vẹn được kiểm tra bằng zipfile.testzip() sau đó.
            expected = int(resp.headers.get("Content-Length") or 0)
            received, next_log = 0, PROGRESS_EVERY
            with open(tmp, "wb") as f:
                while chunk := resp.read(CHUNK_SIZE):
                    f.write(chunk)
                    received += len(chunk)
                    if received >= next_log:
                        log.info("  ... %.0f MB", received / 1e6)
                        next_log += PROGRESS_EVERY
    except urllib.error.HTTPError as e:
        err = PermanentDownloadError if 400 <= e.code < 500 else DownloadError
        raise err(f"HTTP error {e.code} {e.reason}") from e
    except urllib.error.URLError as e:
        raise DownloadError(f"Cannot reach server: {e.reason}") from e
    except http.client.HTTPException as e:  # IncompleteRead khi server cắt kết nối
        raise DownloadError(f"Connection broken: {e!r}") from e
    except (TimeoutError, OSError) as e:
        raise DownloadError(f"Network/IO error: {e}") from e

    if received == 0:
        raise DownloadError("Downloaded file is empty")
    if expected and received != expected:
        raise DownloadError(f"Incomplete download: {received}/{expected} bytes")
    return received


def download_file(url: str, dest: Path, retries: int) -> None:
    """Tải url về dest. Ghi vào file .part trước, chỉ đổi tên khi tải đủ.

    Server không hỗ trợ HTTP Range nên mỗi lần retry phải tải lại từ đầu.
    """
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        log.info("Downloading %s (attempt %d/%d)", url, attempt, retries)
        try:
            received = _download_once(url, tmp)
        except DownloadError as e:
            tmp.unlink(missing_ok=True)
            if isinstance(e, PermanentDownloadError):
                raise
            if attempt == retries:
                raise DownloadError(f"{e} - gave up after {retries} attempts") from e
            wait = 5 * 2 ** (attempt - 1)
            log.warning("Attempt %d failed: %s. Retry in %ds", attempt, e, wait)
            time.sleep(wait)
            continue
        log.info("HTTP 200, received %.1f MB", received / 1e6)
        tmp.replace(dest)
        return


def extract_excel(archive: Path, excel_name: str, dest_dir: Path) -> Path:
    try:
        with zipfile.ZipFile(archive) as zf:
            bad = zf.testzip()
            if bad is not None:
                raise DownloadError(f"Corrupted member in zip: {bad}")
            if excel_name not in zf.namelist():
                raise DownloadError(f"{excel_name} not found in zip: {zf.namelist()}")
            zf.extract(excel_name, dest_dir)
    except zipfile.BadZipFile as e:
        raise DownloadError(f"{archive} is not a valid zip file") from e
    return dest_dir / excel_name


def excel_to_csv(excel_path: Path, csv_path: Path) -> list[dict]:
    """Gộp tất cả sheet thành một CSV. Trả về thông tin từng sheet."""
    import pandas as pd

    log.info("Reading %s (may take a few minutes)...", excel_path.name)
    sheets = pd.read_excel(excel_path, sheet_name=None, dtype=STRING_COLUMNS, engine="openpyxl")

    columns = None
    sheet_info = []
    for name, df in sheets.items():
        if columns is None:
            columns = list(df.columns)
        elif list(df.columns) != columns:
            raise DownloadError(f"Sheet '{name}' has different columns: {list(df.columns)}")
        sheet_info.append({
            "sheet": name,
            "rows": len(df),
            "invoice_date_min": str(df["InvoiceDate"].min()),
            "invoice_date_max": str(df["InvoiceDate"].max()),
        })
        log.info("Sheet '%s': %d rows", name, len(df))

    combined = pd.concat(sheets.values(), ignore_index=True)
    tmp = csv_path.with_suffix(".csv.part")
    combined.to_csv(tmp, index=False, encoding="utf-8", date_format="%Y-%m-%d %H:%M:%S")
    tmp.replace(csv_path)
    log.info("Wrote %s: %d rows, %d columns", csv_path.name, len(combined), len(columns))
    return sheet_info


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Download Online Retail II into data/raw")
    parser.add_argument("--force", action="store_true", help="download lại kể cả khi file đã tồn tại")
    parser.add_argument("--retries", type=int, default=3, help="số lần thử download (mặc định 3)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    cfg = load_config()
    ds = cfg["dataset"]
    raw_dir = PROJECT_ROOT / cfg["paths"]["raw"]
    raw_dir.mkdir(parents=True, exist_ok=True)
    archive = raw_dir / ds["archive_filename"]
    csv_path = raw_dir / ds["raw_filename"]
    meta_path = raw_dir / ds["metadata_filename"]

    if csv_path.exists() and csv_path.stat().st_size > 0 and not args.force:
        log.info("%s already exists (%.1f MB) - skip. Use --force to re-download.",
                 csv_path, csv_path.stat().st_size / 1e6)
        return 0

    try:
        if archive.exists() and zipfile.is_zipfile(archive) and not args.force:
            log.info("Reusing existing archive %s", archive.name)
        else:
            download_file(ds["download_url"], archive, args.retries)
        excel_path = extract_excel(archive, ds["excel_filename"], raw_dir)
        sheet_info = excel_to_csv(excel_path, csv_path)
    except DownloadError as e:
        log.error("Download failed: %s", e)
        return 1
    except OSError as e:
        log.error("File IO error: %s", e)
        return 1

    if not csv_path.exists() or csv_path.stat().st_size == 0:
        log.error("CSV was not created: %s", csv_path)
        return 1

    metadata = {
        "source_url": ds["download_url"],
        "downloaded_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "archive": {"file": archive.name, "bytes": archive.stat().st_size, "sha256": sha256(archive)},
        "csv": {"file": csv_path.name, "bytes": csv_path.stat().st_size,
                "rows": sum(s["rows"] for s in sheet_info)},
        "sheets": sheet_info,
    }
    meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Metadata written to %s", meta_path)
    log.info("Done. Raw dataset: %s", csv_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
