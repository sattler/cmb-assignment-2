#!/usr/bin/env python

import logging
import threading

import ping


class Heartbeat(object):
    """test the remote host with heartbeat requests"""

    def __init__(self, remote_host, action_handler=None, identifier=None):
        """
        init
        :param action_handler: a function which takes a AvailabilityStatus and returns nothing
        """
        self.remote_host = remote_host
        self.action_handler = action_handler
        self.connected = False
        self.identifier = identifier
        self._stop_event = threading.Event()
        self._run_event = threading.Event()
        self._heartbeat_thread = threading.Thread(target=Heartbeat._heartbeat,
                                                  name='{}'.format(self.identifier),
                                                  args=(self,))

    def start(self):
        """Starts with heartbeat"""
        self._heartbeat_thread.start()

    def stop(self):
        """Stops the heartbeat"""
        self._stop_event.set()

    @staticmethod
    def _heartbeat(self):
        while not self._stop_event.is_set():
            packets_lost, _, _ = ping.quiet_ping(self.remote_host, timeout=0.2, count=1)
            result = True
            if packets_lost:
                result = False

            if result != self.connected:
                logging.debug('connetion status {}'.format(result))
                self.action_handler(result)
                self.connected = result
