import os
import threading

from lottery import Bet, Lottery

BETS_FILE_NAME = "bets.csv"


class ShutdownError(Exception):
    pass


class LotteryService:
    def __init__(self, storage_dir: str, agency_quorum_min: int) -> None:
        os.makedirs(storage_dir, exist_ok=True)
        self._lottery = Lottery(os.path.join(storage_dir, BETS_FILE_NAME))
        self._monitor = threading.Condition()
        self._agency_quorum_min = agency_quorum_min
        self._awaiting_agencies: set[int] = set()
        self._shutting_down = False

    def register_bets(self, bets: list[Bet]) -> None:
        with self._monitor:
            self._lottery.store_bets(bets)

    def winners_for(self, agency_id: int) -> list[Bet]:
        with self._monitor:
            self._wait_for_quorum(agency_id)
            return [
                bet
                for bet in self._lottery.load_bets()
                if self._lottery.has_won(bet) and bet.agency_id == agency_id
            ]

    def shutdown(self) -> None:
        with self._monitor:
            self._shutting_down = True
            self._monitor.notify_all()

    def _wait_for_quorum(self, agency_id: int) -> None:
        self._awaiting_agencies.add(agency_id)
        if self._agency_quorum_reached():
            self._monitor.notify_all()
            return
        self._monitor.wait_for(self._agency_quorum_reached)

    def _agency_quorum_reached(self) -> bool:
        return len(self._awaiting_agencies) >= self._agency_quorum_min or self._shutting_down
