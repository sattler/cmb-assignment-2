#!/usr/bin/env python

import enum
import socket
import threading

from .constants import HEARTBEAT_PORT


class AvailabilityStatus(enum.Enum):
    available = 0
    not_reachable = 1


class Heartbeat(object):
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
            self._heartbeat_thread = threading.Thread(target=Heartbeat._heartbeat,
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
