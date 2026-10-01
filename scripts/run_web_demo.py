"""Chạy Web Demo (presentation layer) trên máy local.

1. Tạo output/reports/summary.json từ các output pipeline đã có (scripts/build_web_summary.py).
2. Mở web server tĩnh (thư viện chuẩn Python) tại http://127.0.0.1:<port>/web/ — chỉ trên máy này.

Web chỉ ĐỌC file có sẵn: web/ (HTML/CSS/JS) và output/reports/ (summary.json + biểu đồ PNG).
Không gọi Spark, không đọc HDFS, không chạy lại model. Các đường dẫn khác (data/, src/, ...) trả 404.

Usage:
    python scripts/run_web_demo.py [--port 8000]
    Ctrl+C để dừng.
"""

import argparse
import sys
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.build_web_summary import main as build_summary  # noqa: E402
from src.config.settings import PROJECT_ROOT  # noqa: E402

ALLOWED_PREFIXES = ("/web/", "/output/reports/")


class DemoServer(ThreadingHTTPServer):
    # Trên Windows, SO_REUSEADDR cho phép mở trùng port đang bị ứng dụng khác chiếm -> tắt để báo lỗi rõ ràng
    allow_reuse_address = sys.platform != "win32"


class DemoHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path in ("/", "/web"):
            self.send_response(302)
            self.send_header("Location", "/web/")
            self.end_headers()
            return
        if not path.startswith(ALLOWED_PREFIXES) or ".." in path:
            self.send_error(404)
            return
        super().do_GET()

    def end_headers(self):
        # Luôn đọc bản mới nhất của summary.json / PNG khi tải lại trang
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, fmt, *args):
        pass  # giữ console gọn khi demo


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the static Web Demo")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    print("[1] Build summary.json from pipeline outputs")
    if build_summary() != 0:
        print("ERROR: summary checks failed — xem thông báo ở trên")
        return 1

    handler = partial(DemoHandler, directory=str(PROJECT_ROOT))
    try:
        server = DemoServer(("127.0.0.1", args.port), handler)
    except OSError as e:
        print(f"ERROR: không mở được port {args.port} ({e}). Thử: python scripts/run_web_demo.py --port 8001")
        return 1
    print(f"\n[2] Web Demo: http://127.0.0.1:{args.port}/web/   (Ctrl+C để dừng)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
