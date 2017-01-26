#!/usr/bin/env python

import logging
import threading
import time
import ping
import socket


class Heartbeat(object):
    """test the remote host with heartbeat requests"""

    def __init__(self, remote_host, action_handler=None, identifier=None, min_wait_time=0.2):
        """
        init
        :param action_handler: a function which takes a AvailabilityStatus and returns nothing
        """
        self.remote_host = remote_host
        self.action_handler = action_handler
        self.connected = False
        self.identifier = identifier
        self._counter = -1
        self._stop_event = threading.Event()
        self._run_event = threading.Event()
        self._min_wait_time = min_wait_time
        self._heartbeat_thread = threading.Thread(target=_heartbeat,
                                                  name='{}'.format(self.identifier),
                                                  args=(self,))

    def start(self):
        """Starts with heartbeat"""
        self._heartbeat_thread.start()

    def stop(self):
        """Stops the heartbeat"""
        self._stop_event.set()


def _heartbeat(self):
    logging.debug('start pinging {}'.format(self.remote_host))
    while not self._stop_event.is_set():
        try:
            packets_lost, _, _ = ping.quiet_ping(self.remote_host, timeout=0.5, count=1)
        except socket.error:
            continue

        result = True

        if packets_lost:
            result = False

        if result != self.connected:
            if self._counter >= 1 or self._counter == -1:
                self.action_handler(result)
                self.connected = result
                self._counter = 0
            else:
                self._counter += 1

        time.sleep(self._min_wait_time)
