#!/usr/bin/env python

"""
Our client implementation
"""

import time
import socket
import threading
from group3.constants import SERVER_IP_SLOW, SERVER_IP_FAST, PORT

def main():
    server_sock_fast = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock_fast.bind((SERVER_IP_FAST, PORT))
    server_sock_fast.listen(1)

    server_sock_slow = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock_slow.bind((SERVER_IP_SLOW, PORT))
    server_sock_slow.listen(1)

    thread_fast = threading.Thread(target=server_thread, args=(server_sock_fast, 'fast port'))
    thread_slow = threading.Thread(target=server_thread, args=(server_sock_slow, 'slow port'))


def server_thread(socket, name):
    try:
        while True:
            conn, addr = socket.accept()
            print addr, 'connected to', name
            # TODO implement protocol
            conn.close()

    finally:
        socket.close()


if __name__ == '__main__':
    main()
