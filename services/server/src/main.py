import os
import signal
import sys

import logger
import server
from service import LotteryService

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
SERVER_STORAGE_DIR = os.environ.get("SERVER_STORAGE_DIR", "/data")
AGENCY_QUORUM_MIN = int(os.environ.get("AGENCY_QUORUM_MIN", "1"))


def main():
    logger.init()
    lottery_service = LotteryService(SERVER_STORAGE_DIR, AGENCY_QUORUM_MIN)
    s = server.Server(SERVER_HOST, SERVER_PORT, lottery_service)
    signal.signal(signal.SIGTERM, lambda *_: s.shutdown())
    try:
        s.run()
    except Exception as e:
        logger.error("server-run", logger.LogResult.fail, "err", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
