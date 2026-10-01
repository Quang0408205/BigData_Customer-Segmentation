"""Upload raw dataset từ data/raw lên HDFS (thư mục hdfs.raw_dir trong config.yaml).

Chạy trên máy host (Windows .venv). hdfs CLI chỉ có trong container namenode,
nên file được đưa vào container bằng `docker compose cp` rồi `hdfs dfs -put`
lên HDFS (cùng cách gọi với scripts/init_hdfs.py).

Các bước:
    1. Kiểm tra file local: tồn tại, không rỗng, header đúng 8 cột raw; tính sha256.
    2. Kiểm tra namenode đang chạy, chờ HDFS thoát safe mode.
    3. Tạo thư mục HDFS đích nếu chưa có.
    4. Nếu file đã có trên HDFS và giống hệt (cùng size + sha256) -> bỏ qua, không upload trùng.
       Nếu khác -> báo lỗi (dùng --force để ghi đè).
    5. Upload: docker compose cp -> hdfs dfs -put -> xóa file tạm trong container.
    6. Kiểm tra sau upload: size + sha256 trên HDFS phải khớp file local; in thông tin block (fsck).

Raw dataset local không bị sửa; file trên HDFS là bản sao byte-by-byte.

Usage:
    docker compose up -d
    python scripts/init_hdfs.py
    python scripts/upload_to_hdfs.py [--force]
"""

import argparse
import hashlib
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config.settings import PROJECT_ROOT, load_config  # noqa: E402
from src.ingestion.csv_reader import RAW_COLUMNS  # noqa: E402

CONTAINER = "namenode"
SPARK_USER = "spark"


def run(cmd: list[str], check: bool = True, show: bool = True) -> subprocess.CompletedProcess:
    if show:
        print("$", " ".join(cmd))
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}\n{result.stderr.strip()}")
    return result


def container_exec(*args: str, user: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    """Chạy lệnh trong container namenode (mặc định user hadoop = HDFS superuser)."""
    cmd = ["docker", "compose", "exec", "-T"]
    if user:
        cmd += ["-u", user]
    return run([*cmd, CONTAINER, *args], check=check)


def hdfs_file_size(path: str) -> int | None:
    """Kích thước file trên HDFS (bytes), None nếu chưa tồn tại."""
    if container_exec("hdfs", "dfs", "-test", "-f", path, check=False).returncode != 0:
        return None
    return int(container_exec("hdfs", "dfs", "-stat", "%b", path).stdout.strip())


def hdfs_sha256(path: str) -> str:
    """sha256 của file trên HDFS, tính trong container (đọc toàn bộ file từ DataNode).

    Không dùng `hdfs dfs -checksum` vì đó là MD5-of-CRC32 theo block,
    không so sánh được với sha256 của file local.
    """
    out = container_exec("sh", "-c", f"hdfs dfs -cat '{path}' | sha256sum").stdout
    return out.split()[0]


def local_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def check_local_file(path: Path) -> int:
    """Kiểm tra file raw local; trả về size (bytes). Raise ValueError nếu không hợp lệ."""
    if not path.is_file():
        raise ValueError(f"raw dataset not found: {path} (run scripts/download_dataset.py first)")
    size = path.stat().st_size
    if size == 0:
        raise ValueError(f"raw dataset is empty: {path}")
    with open(path, encoding="utf-8") as f:
        header = f.readline().rstrip("\r\n").split(",")
    if header != RAW_COLUMNS:
        raise ValueError(f"unexpected CSV header {header}, expected {RAW_COLUMNS}")
    return size


def upload(local: Path, hdfs_path: str) -> None:
    tmp = f"/tmp/{local.name}"
    run(["docker", "compose", "cp", str(local), f"{CONTAINER}:{tmp}"])
    try:
        # hdfs dfs -put ghi ra <file>._COPYING_ rồi mới rename -> không để lại file dở dang
        container_exec("hdfs", "dfs", "-put", "-f", tmp, hdfs_path)
    finally:
        # docker cp tạo file với owner root -> xóa bằng root
        container_exec("rm", "-f", tmp, user="root", check=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload raw dataset to HDFS")
    parser.add_argument("--force", action="store_true",
                        help="upload lại kể cả khi file trên HDFS đã giống hoặc khác bản local")
    args = parser.parse_args()

    cfg = load_config()
    local = PROJECT_ROOT / cfg["paths"]["raw"] / cfg["dataset"]["raw_filename"]
    hdfs_dir = cfg["hdfs"]["raw_dir"]
    hdfs_path = f"{hdfs_dir}/{local.name}"

    print(f"[1] Check local raw dataset: {local}")
    try:
        size = check_local_file(local)
    except ValueError as e:
        print(f"ERROR: {e}")
        return 1
    sha = local_sha256(local)
    print(f"    OK  size={size:,} bytes  sha256={sha}")

    print(f"\n[2] Check HDFS ({CONTAINER} container)")
    running = run(["docker", "compose", "ps", "--status", "running", "-q", CONTAINER], check=False)
    if running.returncode != 0 or not running.stdout.strip():
        print(f"ERROR: {CONTAINER} container is not running. Start it with: docker compose up -d")
        return 1

    try:
        container_exec("hdfs", "dfsadmin", "-safemode", "wait")

        print(f"\n[3] Ensure HDFS directory: {hdfs_dir}")
        if container_exec("hdfs", "dfs", "-test", "-d", hdfs_dir, check=False).returncode != 0:
            container_exec("hdfs", "dfs", "-mkdir", "-p", hdfs_dir)
            container_exec("hdfs", "dfs", "-chown", f"{SPARK_USER}:supergroup", hdfs_dir)
            print("    created")
        else:
            print("    exists")

        print(f"\n[4] Check existing file: {hdfs_path}")
        remote_size = hdfs_file_size(hdfs_path)
        if remote_size is None:
            print("    not found -> upload")
        else:
            same = remote_size == size and hdfs_sha256(hdfs_path) == sha
            print(f"    found  size={remote_size:,} bytes  identical={same}")
            if same and not args.force:
                print("\nSKIP: HDFS already has an identical copy (use --force to upload again)")
                return 0
            if not same and not args.force:
                print("ERROR: HDFS file differs from local file. Check it, then rerun with --force to overwrite.")
                return 1

        print(f"\n[5] Upload {local.name} -> {hdfs_path}")
        upload(local, hdfs_path)

        print("\n[6] Verify uploaded file")
        remote_size = hdfs_file_size(hdfs_path)
        remote_sha = hdfs_sha256(hdfs_path) if remote_size is not None else None
        print(f"    size   local={size:,}  hdfs={remote_size if remote_size is None else f'{remote_size:,}'}")
        print(f"    sha256 local={sha}\n           hdfs ={remote_sha}")
        if remote_size != size or remote_sha != sha:
            print("ERROR: uploaded file does not match local file")
            return 1
        print(container_exec("hdfs", "dfs", "-ls", hdfs_dir).stdout.rstrip())
        fsck = container_exec("hdfs", "fsck", hdfs_path, "-files", "-blocks", "-locations").stdout
        print("\n".join(line for line in fsck.splitlines()
                        if line.startswith((hdfs_path, "0.", "1.", "2.", "3.", "Status", " Total blocks"))))
    except RuntimeError as e:
        print(f"ERROR: {e}")
        return 1

    print(f"\nOK: {hdfs_path} uploaded and verified ({size:,} bytes, sha256 match)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
