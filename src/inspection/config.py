"""运行时配置。

数据目录由环境变量 ``INSPECTION_DATA_DIR`` 指定，默认取进程工作目录下的
``data/``；测试通过临时目录隔离，不依赖主机上的隐藏状态。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    db_path: Path
    blob_dir: Path
    service_name: str = "pipeline-inspection-index"

    @classmethod
    def from_env(cls) -> "Settings":
        root = Path(os.environ.get("INSPECTION_DATA_DIR", "data")).resolve()
        return cls(data_dir=root, db_path=root / "index.db", blob_dir=root / "blobs")

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.blob_dir.mkdir(parents=True, exist_ok=True)
