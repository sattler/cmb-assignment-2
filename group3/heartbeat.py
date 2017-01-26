#!/usr/bin/env python

import logging
import threading
import time
import ping
import socket


class Heartbeat(threading.Thread):
    """test the remote host with heartbeat requests"""

    def __init__(self, remote_host, available_event, identifier=None, min_wait_time=0.2):
        """
        init
        :param action_handler: a function which takes a AvailabilityStatus and returns nothing
        """
        self.remote_host = remote_host
        self.available_event = available_event
        self.identifier = identifier
        self._counter = -1
        self._stop_event = threading.Event()
        self._min_wait_time = min_wait_time
        super(Heartbeat, self).__init__()

    def stop(self):
        """Stops the heartbeat"""
        self._stop_event.set()


    def run(self):
        logging.debug('start pinging {}'.format(self.remote_host))
        while not self._stop_event.is_set():
            try:
                packets_lost, _, _ = ping.quiet_ping(self.remote_host, timeout=0.5, count=1)
            except socket.error:
                continue

            result = True

            if packets_lost:
                result = False

            if result != self.available_event.is_set():
                if self._counter >= 2 or self._counter == -1:
                    if result:
                        self.available_event.set()
                    else:
                        self.available_event.clear()

                    self._counter = 0
                else:
                    self._counter += 1
            else:
                self._counter = 0

            time.sleep(self._min_wait_time)
