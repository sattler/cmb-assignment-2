#!/usr/bin/env python

"""
Our client implementation
"""

import time
import socket
import threading

from group3.constants import SERVER_IP_FAST, SERVER_IP_SLOW, PORT


def main():
    start_time = time.time()
    thread_fast = threading.Thread(target=client_thread, args=(SERVER_IP_FAST, True))
    thread_slow = threading.Thread(target=client_thread, args=(SERVER_IP_SLOW, False))
    thread_fast.start()
    thread_slow.start()

    # TODO  measure how long it took


def client_thread(ip, fast):
    client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_sock.connect((ip, PORT))

        # TODO implement protocol acording to if connection is fast or not
    finally:
        client_sock.close()

if __name__ == '__main__':
    main()
