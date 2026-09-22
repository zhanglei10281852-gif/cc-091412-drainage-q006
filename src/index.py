import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from inspection.app import create_server  # noqa: E402


if __name__ == "__main__":
    server = create_server()
    print(
        "影像采集索引服务已启动 "
        f"(data_dir={os.environ.get('INSPECTION_DATA_DIR', 'data')})",
        flush=True,
    )
    server.serve_forever()
