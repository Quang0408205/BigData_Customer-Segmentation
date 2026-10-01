"""Tạo cấu trúc thư mục HDFS cho project (chạy lại nhiều lần vẫn an toàn).

Chạy trên máy host (Windows .venv). Lệnh HDFS được gọi qua
`docker compose exec namenode hdfs ...` vì hdfs CLI chỉ có trong container.

    /data/customer-segmentation/{raw,processed,rfm,output,evaluation}

Các thư mục (không đệ quy vào file bên trong) được chown cho user `spark`
(user chạy Spark trong container), để Spark có quyền ghi kết quả. User `hadoop` (người khởi động NameNode)
là HDFS superuser nên vẫn ghi được mọi nơi.

Usage:
    docker compose up -d
    python scripts/init_hdfs.py
"""

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config.settings import PROJECT_ROOT, load_config  # noqa: E402

SPARK_USER = "spark"
DIR_KEYS = ["raw_dir", "processed_dir", "rfm_dir", "output_dir", "evaluation_dir"]


def hdfs(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", "exec", "-T", "namenode", "hdfs", *args]
    print("$", " ".join(cmd[5:]))
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if result.stdout.strip():
        print(result.stdout.rstrip())
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(cmd)}\n{result.stderr}")
    return result


def main() -> int:
    cfg = load_config()["hdfs"]
    base = cfg["base_dir"]
    dirs = [cfg[k] for k in DIR_KEYS]

    running = subprocess.run(
        ["docker", "compose", "ps", "--status", "running", "-q", "namenode"],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    if running.returncode != 0 or not running.stdout.strip():
        print("ERROR: namenode container is not running. Start it with: docker compose up -d")
        return 1

    try:
        # Chờ NameNode thoát safe mode (lúc mới khởi động HDFS chỉ cho đọc)
        hdfs("dfsadmin", "-safemode", "wait")
        hdfs("dfs", "-mkdir", "-p", *dirs)
        # Chỉ áp quyền cho các THƯ MỤC, không dùng -R: -R sẽ đổi luôn owner/quyền của file raw
        # (raw do hadoop upload, rw-r--r--) mỗi lần chạy lại script (lỗi phát hiện ở Phase 12).
        hdfs("dfs", "-chown", f"{SPARK_USER}:supergroup", base, *dirs)
        hdfs("dfs", "-chmod", "755", base, *dirs)
        hdfs("dfs", "-ls", base)
    except RuntimeError as e:
        print(f"ERROR: {e}")
        return 1

    missing = [d for d in dirs if hdfs("dfs", "-test", "-d", d, check=False).returncode != 0]
    if missing:
        print(f"ERROR: directories not created: {missing}")
        return 1
    print(f"OK: {len(dirs)} HDFS directories ready under {base}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
