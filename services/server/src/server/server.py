import socket
import threading

import logger
import protocol
from domain.message import Message
from domain.message_header import MessageHeader, MessageType
from domain.parser import parse_agency_id, parse_bets
from service import LotteryService


class Server:
    def __init__(
        self, server_host: str, server_port: int, lottery_service: LotteryService
    ) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery_service = lottery_service
        self._agency_threads: list[threading.Thread] = []
        self._agency_sockets: list[socket.socket] = []
        self._server_socket: socket.socket | None = None
        self._running = True

    def _handle_agency_connection(self, client_socket: socket.socket) -> None:
        action = "handle-client"
        message_amount = 0

        try:
            logger.info(action, logger.LogResult.in_progress)
            with client_socket:
                while True:
                    header, payload = protocol.receive_message(client_socket)
                    message_amount += 1
                    self._dispatch(client_socket, header, payload)
                    if header.type == MessageType.AWAITING_WINNERS:
                        break
            logger.info(
                action, logger.LogResult.success, "messages-amount", message_amount
            )
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e

    def _dispatch(self, client_socket: socket.socket, header: MessageHeader, payload: bytes) -> None:
        match header.type:
            case MessageType.BET:
                self._handle_bet(client_socket, payload)
            case MessageType.AWAITING_WINNERS:
                self._handle_awaiting_winners(client_socket, payload)
            case _:
                raise ValueError(f"unexpected message type: {header.type}")


    def _handle_bet(self, client_socket: socket.socket, payload: bytes) -> None:
        bets = parse_bets(payload)
        self.lottery_service.register_bets(bets)
        protocol.send_message(client_socket, Message.ack())

    def _receive_ack(self, client_socket: socket.socket) -> None:
        header, _ = protocol.receive_message(client_socket)
        if header.type != MessageType.ACK:
            raise ValueError(f"expected ack message type, got {header.type}")

    def _handle_awaiting_winners(self, client_socket: socket.socket, payload: bytes) -> None:
        action = "send-winners"
        agency_id = parse_agency_id(payload)
        winners_amount = 0

        logger.info(action, logger.LogResult.in_progress, "agency-id", agency_id)
        protocol.send_message(client_socket, Message.ack())

        for bet in self.lottery_service.winners_for(agency_id):
            protocol.send_message(client_socket, Message.winner(bet))
            self._receive_ack(client_socket)
            winners_amount += 1

        protocol.send_message(client_socket, Message.finish())
        self._receive_ack(client_socket)
        logger.info(
            action,
            logger.LogResult.success,
            "agency-id",
            agency_id,
            "winners-amount",
            winners_amount,
        )


    def _serve_agency(self, client_socket: socket.socket) -> None:
        try:
            self._handle_agency_connection(client_socket)
        except Exception as e:
            logger.error("drop-client-connection", logger.LogResult.fail, "err", e)
        finally:
            self._agency_sockets.remove(client_socket)

    def _spawn_agency_thread(self, client_socket: socket.socket) -> None:
        thread = threading.Thread(target=self._serve_agency, args=(client_socket,))
        self._agency_sockets.append(client_socket)
        self._agency_threads.append(thread)
        thread.start()

    def _reap_finished_threads(self) -> None:
        alive = []
        for thread in self._agency_threads:
            if thread.is_alive():
                alive.append(thread)
            else:
                thread.join()
        self._agency_threads = alive

    def _join_agency_threads(self) -> None:
        for thread in self._agency_threads:
            thread.join()
        self._agency_threads = []

    def _accept_connection(self, server_socket: socket.socket) -> socket.socket:
        action = "accept-connection"
        try:
            logger.info(action, logger.LogResult.in_progress)
            client_socket, _ = server_socket.accept()
        except Exception:
            logger.error(action, logger.LogResult.fail)
            raise
        logger.info(action, logger.LogResult.success)
        return client_socket

    def run(self) -> None:
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        with self._server_socket as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            try:
                while self._running:
                    try:
                        client_socket = self._accept_connection(server_socket)
                    except OSError:
                        if not self._running:
                            break
                        raise
                    self._spawn_agency_thread(client_socket)
                    self._reap_finished_threads()
            finally:
                self._join_agency_threads()

    def shutdown(self) -> None:
        logger.info("server-shutdown", logger.LogResult.in_progress)
        self._running = False
        self.lottery_service.shutdown()
        for agency_socket in list(self._agency_sockets):
            self._unblock_socket(agency_socket)
        self._unblock_socket(self._server_socket)

    def _unblock_socket(self, sock: socket.socket | None) -> None:
        if sock is None:
            return
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
