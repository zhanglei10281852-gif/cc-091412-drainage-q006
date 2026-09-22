"""进程入口：复用基线健康检查契约（/health 返回 service 标识）。

业务实现见 inspection 包；默认数据文件位于 .runtime/inspection.db，
可用 INSPECTION_DB 环境变量覆盖。
"""

from inspection.api import create_server

SERVICE_NAME = "pipeline-inspection-index"


def main() -> None:
    server = create_server()
    print("管道影像采集索引服务已启动", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.worker.stop()
        server.server_close()


if __name__ == "__main__":
    main()
