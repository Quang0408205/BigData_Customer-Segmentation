"""Đọc cấu hình tập trung của project.

- configs/config.yaml : tham số dùng chung (đường dẫn, HDFS dir, tham số model).
- .env                : giá trị phụ thuộc máy (host, port, memory).

Mọi module khác import từ đây thay vì hard-code đường dẫn.
"""

import os
from pathlib import Path

# src/config/settings.py -> project root là thư mục cha cấp 3
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "configs" / "config.yaml"
ENV_PATH = PROJECT_ROOT / ".env"


def load_config(path: Path = CONFIG_PATH) -> dict:
    """Đọc config.yaml và trả về dict."""
    import yaml

    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_env(path: Path = ENV_PATH) -> None:
    """Nạp biến môi trường từ .env (nếu tồn tại) vào os.environ.

    Biến đã có sẵn (ví dụ do docker-compose đặt) KHÔNG bị ghi đè.
    """
    from dotenv import load_dotenv

    load_dotenv(path)


def hdfs_uri(path: str = "") -> str:
    """Ghép URI HDFS đầy đủ, ví dụ hdfs_uri('/data/x') -> 'hdfs://namenode:8020/data/x'."""
    load_env()
    host = os.getenv("HDFS_NAMENODE_HOST", "namenode")
    port = os.getenv("HDFS_NAMENODE_PORT", "8020")
    return f"hdfs://{host}:{port}{path}"
