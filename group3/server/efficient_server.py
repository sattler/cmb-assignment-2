#!/usr/bin/env python

"""
Our client implementation
"""

import socket
import threading
import os
import time
import struct
import hashlib

from group3.constants import *

file_infos = {}
file_infos_lock = threading.Lock()


class FileInfoKeys(enum.Enum):
    FileId = 'file_id'
    FileName = 'file_name'
    FileSize = 'file_size'
    MaxOffsetTrans = 'max_offset_trans'
    NextToSend = 'next_to_send'
    OffsetsTrans = 'offsets_trans'
    FileObject = 'file_obj'
    FileLock = 'lock'


def main():
    server_sock_fast = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock_fast.bind((SERVER_IP_FAST, PORT))
    server_sock_fast.listen(1)

    server_sock_slow = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock_slow.bind((SERVER_IP_SLOW, PORT))
    server_sock_slow.listen(1)

    server_sock_fast.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    # Options specific for linux
    server_sock_fast.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, TCP_KEEP_ALIVE_IDLE)
    server_sock_fast.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, TCP_KEEP_ALIVE_INTERVALL)
    server_sock_fast.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, TCP_KEEP_ALIVE_MAX_FAILS)

    server_sock_slow.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    # Options specific for linux
    server_sock_slow.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, TCP_KEEP_ALIVE_IDLE)
    server_sock_slow.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, TCP_KEEP_ALIVE_INTERVALL)
    server_sock_slow.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, TCP_KEEP_ALIVE_MAX_FAILS)

    thread_fast = threading.Thread(target=server_thread, args=(server_sock_fast, 'fast interface'))
    thread_slow = threading.Thread(target=server_thread, args=(server_sock_slow, 'slow interface'))

    thread_fast.start()
    thread_slow.start()

    thread_fast.join()
    thread_slow.join()


def server_thread(serv_socket, thread_name):
    try:
        while True:
            try:
                conn, addr = serv_socket.accept()
                print addr, 'connected to', thread_name
                first_msg = serv_socket.recv()
                if len(first_msg) == 0:
                    print addr, 'disconnected'
                if ClientMsgs.NewFile in first_msg:
                    _, file_name = first_msg.split(',')
                    file_name = file_name.trim()
                    file_path = os.path.join(os.path.curdir, file_name)
                    if not os.path.exists(file_path):
                        conn.send(ServerMsgs.FileErrorMsg + ' Path:' + file_path)
                        continue

                    file_size = os.path.getsize(file_path)
                    file_id = hashlib.sha256(file_path + str(time.time())).hexdigest()
                    with file_infos_lock:
                        file_infos[file_id] = {FileInfoKeys.FileId: file_id,
                                               FileInfoKeys.FileName: file_path,
                                               FileInfoKeys.FileSize: file_size,
                                               FileInfoKeys.MaxOffsetTrans: -1,
                                               FileInfoKeys.NextToSend: 0,
                                               FileInfoKeys.OffsetsTrans: [],
                                               FileInfoKeys.FileObject: open(file_path, 'rb', 4096),
                                               FileInfoKeys.FileLock: threading.Lock()}

                    conn.send(str(file_size) + ',' + file_id)

                    send_file_data(conn, file_infos[file_id], thread_name)
                if ClientMsgs.Reconnect in first_msg:
                    _, file_id = first_msg.split(',')
                    file_id = file_id.trim()
                    if file_id not in file_infos:
                        conn.send(ServerMsgs.FileErrorMsg + file_id + ' is not registered')
                        continue

                    send_file_data(conn, file_infos[file_id], thread_name)
            finally:
                conn.close()

    finally:
        serv_socket.close()


def send_file_data(serv_conn, file_info, thread_name):
    while file_info[FileInfoKeys.NextToSend] > 0:
        with file_infos_lock:
            send_offset = file_info[FileInfoKeys.NextToSend]
            file_info[FileInfoKeys.NextToSend] += MSG_LENGTH
            while file_info[FileInfoKeys.NextToSend] in file_info[FileInfoKeys.OffsetsTrans]:
                file_info[FileInfoKeys.OffsetsTrans].remove(file_info[FileInfoKeys.NextToSend])
                file_info[FileInfoKeys.NextToSend] += MSG_LENGTH

        with file_info[FileInfoKeys.FileLock]:
            file_info[FileInfoKeys.FileObject].seek(send_offset, 0)
            data_to_send = file_info[FileInfoKeys.FileObject].read(MSG_LENGTH)

        send_length = 0
        while send_length < MSG_LENGTH + 4:
            send_length = serv_conn.send(struct.pack(send_offset) + data_to_send)
            if send_length == 0:
                with file_infos_lock:
                    file_info[FileInfoKeys.NextToSend] = send_offset
                return

        with file_infos_lock:
            file_info[FileInfoKeys.OffsetsTrans].append(send_offset)



if __name__ == '__main__':
    main()
