#!/usr/bin/env python

"""
Our client implementation
"""

import socket
import threading
import time
import struct

from group3.constants import *


class FileInfoKeys(enum.Enum):
    FileId = 'file_id'
    FileSize = 'file_size'
    FileName = 'file_name'


data = {}
written = 0
file_info = {FileInfoKeys.FileName: 'index.html'}
file_obj = open(file_info[FileInfoKeys.FileName], 'wb')


def main():
    try:
        start_time = time.time()
        data_lock = threading.Lock()
        connected_event = threading.Event()
        connected_event.set()
        finish_event = threading.Event()

        thread_fast = threading.Thread(target=client_thread, args=(SERVER_IP_FAST, True, data_lock,
                                                                   connected_event, finish_event))
        thread_slow = threading.Thread(target=client_thread, args=(SERVER_IP_SLOW, False, data_lock,
                                                                   connected_event, finish_event))
        thread_fast.start()
        time.sleep(0.05)
        thread_slow.start()

        thread_slow.join()
        thread_fast.join()
    finally:
        file_obj.close()

    # TODO  measure how long it took


def client_thread(ip, fast, data_lock, connected_event, finish_event):
    while not finish_event.is_set():
        if FileInfoKeys.FileId not in file_info and not connected_event.is_set():
            connected_event.wait()
        elif FileInfoKeys.FileId not in file_info:
            connected_event.clear()
            print 'connecting (on fast:', fast, ')'
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # Options specific for linux
        client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, TCP_KEEP_ALIVE_IDLE)
        client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, TCP_KEEP_ALIVE_INTERVALL)
        client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, TCP_KEEP_ALIVE_MAX_FAILS)
        try:
            client_sock.connect((ip, PORT))

            if FileInfoKeys.FileId in file_info:
                client_sock.send(ClientMsgs.Reconnect + ',' + file_info[FileInfoKeys.FileId])

                response = client_sock.recv()
                if ServerMsgs.Acknowledge in response:
                    if LOGGING:
                        print 'reconnected; fast', fast
                    start_download(client_sock, data_lock, finish_event)
                else:
                    print 'error occurred', response
                    finish_event.set()
                    return
            else:
                client_sock.send(ClientMsgs.NewFile + ',' + file_info[FileInfoKeys.FileName])
                response = client_sock.recv(128)

                if ServerMsgs.FileErrorMsg in response:
                    print response, ' Aborting!'
                    finish_event.set()
                    connected_event.set()
                    return

                file_size, file_id = response.split(';')
                file_info[FileInfoKeys.FileId] = file_id
                file_info[FileInfoKeys.FileSize] = int(file_size)
                print 'connected', file_id, file_size

                connected_event.set()

                bytes_sent = client_sock.send(ClientMsgs.StartDownload)
                if bytes_sent != len(ClientMsgs.StartDownload):
                    print 'start download connection error (sent:', bytes_sent, ', of:', \
                        len(ClientMsgs.StartDownload)
                    # Probably not the best way to completely reconnect but the connection has no
                    # packet loss so it shouldn't be a Problem
                    continue
                start_download(client_sock, data_lock, finish_event)

        except socket.error as e:
            if LOGGING:
                print e
        finally:
            client_sock.close()


def start_download(sock, data_lock, finish_event):
    while True:
        buf = sock.recv(MSG_LENGTH + 4)

        if len(buf) == 0:
            print 'connection error'
            return

        offset = struct.unpack('I', buf[0:4])[0]
        buf = buf[4:]
        total_len = len(buf)
        msg_len = MSG_LENGTH
        if offset + MSG_LENGTH > file_info[FileInfoKeys.FileSize]:
            msg_len = int(file_info[FileInfoKeys.FileSize]) - offset

        if total_len != msg_len:
            continue

        # while total_len < msg_len:
        #     n_buf = sock.recv(MSG_LENGTH - total_len)
        #     if len(n_buf) == 0:
        #         if LOGGING:
        #             print 'connection error'
        #         return
        #     buf += n_buf
        #     total_len += len(n_buf)

        data[offset] = (buf, total_len)
        global written
        if offset == written:
            while written < int(file_info[FileInfoKeys.FileSize]):
                with data_lock:
                    file_obj.write(data[written][0])
                    written += data[written][1]
                    del data[written]
                if written not in data:
                    break


if __name__ == '__main__':
    main()
