#!/usr/bin/env python

import enum
import socket
import threading

from .constants import HEARTBEAT_PORT, LOGGING, MAX_HEARTBEAT_SIZE


class AvailabilityStatus(enum.Enum):
    available = 0
    not_reachable = 1


class HeartbeatClient(object):
    """test the remote host with heartbeat requests"""

    def __init__(self, remote_host, action_handler=None, identifier=None):
        """
        init
        :param action_handler: a function which takes a AvailabilityStatus and returns nothing
        """
        self.remote_host = remote_host
        self.action_handler = action_handler
        self.identifier = identifier
        if self._connect():
            self._stop_event = threading.Event()
            self._run_event = threading.Event()
            self._heartbeat_thread = threading.Thread(target=HeartbeatClient._heartbeat,
                                                      name=self.identifier,
                                                      args=(self))
        else:
            raise ValueError('Could not connect to remote host %s' % (self.remote_host,))

    def _connect(self):
        """Connects to the remote host"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.socket.connect((self.remote_host, HEARTBEAT_PORT))
        except socket.error as error_msg:
            self.socket.close()
            print error_msg
            return False

        return True

    def start(self):
        """Starts with heartbeat"""
        self._run_event.set()
        self._heartbeat_thread.start()

    def pause(self):
        """Pauses the heartbeat"""
        self._run_event.clear()

    def stop(self):
        """Stops the heartbeat"""
        self._stop_event.set()

    @staticmethod
    def _heartbeat(self):
        while not self._stop_event.is_set():
            if not self._run_event:
                self._run_event.wait()
            # TODO make requests and call self.action_handler if the status changes
            pass


class HeartbeatServer(object):
    """The server for the heartbeats"""

    def __init__(self, listen_ip, identifier=None):
        """
        init
        :param action_handler: a function which takes a AvailabilityStatus and returns nothing
        """
        self.listen_ip = listen_ip
        self.identifier = identifier
        self._setup()
        self._stop_event = threading.Event()
        self._heartbeat_thread = threading.Thread(target=HeartbeatServer._heartbeat,
                                                  name=self.identifier,
                                                  args=(self))

    def _setup(self):
        """Connects to the remote host"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.listen_ip, HEARTBEAT_PORT))
        self.socket.listen(1)

    def start(self):
        """Starts with heartbeat"""
        self._heartbeat_thread.start()

    def stop(self):
        """Stops the heartbeat"""
        self._stop_event.set()

    @staticmethod
    def _heartbeat(self):
        while not self._stop_event.is_set():
            conn, addr = self.socket.accept()
            if LOGGING:
                print addr, 'connected to', self.listen_ip
            try:
                while True:
                    if not conn.recv(MAX_HEARTBEAT_SIZE):
                        break
                    # TODO send a response back
            finally:
                conn.close()
            pass


def default_heartbeat_payload():
    return "heartbeat"

