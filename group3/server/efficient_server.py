#!/usr/bin/env python

"""
Our client implementation
"""

import hashlib
import os
import socket
import struct
import threading
import time
import logging

import group3.heartbeat as heartbeat
from group3.constants import *

file_infos = {}
file_infos_lock = threading.Lock()


class FileInfoKeys(enum.Enum):
    FileId = 'file_id'
    FileName = 'file_name'
    FileSize = 'file_size'
    FinishEvent = 'finish_event'
    NextToSend = 'next_to_send'
    TransmittedOffsets = 'offsets_trans'
    FileObject = 'file_obj'
    FileLock = 'lock'


def main():
    setup_logger()

    server_sock_fast = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock_fast.bind((SERVER_IP_FAST, PORT))
    server_sock_fast.listen(1)

    server_sock_slow = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock_slow.bind((SERVER_IP_SLOW, PORT))
    server_sock_slow.listen(1)

    # TCP Keep Alive is to slow in recognizing a connection drop so we replaced it
    # server_sock_fast.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    # # Options specific for linux
    # server_sock_fast.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, TCP_KEEP_ALIVE_IDLE)
    # server_sock_fast.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL,
    # TCP_KEEP_ALIVE_INTERVALL)
    # server_sock_fast.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, TCP_KEEP_ALIVE_MAX_FAILS)
    #
    # server_sock_slow.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    # # Options specific for linux
    # server_sock_slow.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, TCP_KEEP_ALIVE_IDLE)
    # server_sock_slow.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL,
    # TCP_KEEP_ALIVE_INTERVALL)
    # server_sock_slow.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, TCP_KEEP_ALIVE_MAX_FAILS)

    thread_fast = threading.Thread(target=server_thread, args=(server_sock_fast,),
                                   name='fast thread')
    thread_slow = threading.Thread(target=server_thread, args=(server_sock_slow,),
                                   name='slow thread')

    thread_fast.start()
    thread_slow.start()

    thread_fast.join()
    thread_slow.join()
    logging.info('terminated')


def setup_logger():
    logging.basicConfig(filename='server.log', level=LOGGING,
                        format=u'[%(asctime)s][%(levelname)-s][%(threadName)s] '
                               u'%(filename)s:%(lineno)d %(message)s',
                        datefmt='%d.%m %H:%M:%S')


def server_thread(serv_socket):
    logging.info('starting')

    try:
        while True:
            try:
                logging.info('waiting for connection')
                conn, addr = serv_socket.accept()
                conn.settimeout(SOCKET_TIMEOUT)
                logging.info('{} connected'.format(addr))

                available_event = threading.Event()

                def connection_status_changed(available):
                    if available:
                        available_event.set()
                    else:
                        available_event.clear()

                heart_beat = heartbeat.Heartbeat(addr[0], action_handler=connection_status_changed,
                                                 identifier='{} hearbeat'.format(addr[0]))

                heart_beat.start()

                first_msg = conn.recv(1024)

                if len(first_msg) == 0:
                    logging.info('{} disconnected'.format(addr))

                logging.debug('received first msg {}'.format(first_msg))
                time.sleep(0.25)
                if ClientMsgs.NewFile.value in first_msg:
                    logging.debug('first msg is new file')

                    _, file_name = first_msg.split(',')
                    file_name = file_name.strip()
                    file_path = os.path.join(os.path.curdir, file_name)
                    if not os.path.exists(file_path):
                        __send_secure(conn, ServerMsgs.FileErrorMsg + ' Path:' + file_path,
                                      available_event)
                        continue

                    file_size = os.path.getsize(file_path)
                    file_id = hashlib.sha256(file_path + str(time.time())).hexdigest()
                    with file_infos_lock:
                        file_infos[file_id] = {
                            FileInfoKeys.FileId: file_id,
                            FileInfoKeys.FileName: file_path,
                            FileInfoKeys.FileSize: file_size,
                            FileInfoKeys.FinishEvent: threading.Event(),
                            FileInfoKeys.NextToSend: 0,
                            FileInfoKeys.TransmittedOffsets: [],
                            FileInfoKeys.FileObject: open(file_path, 'rb', 4096),
                            FileInfoKeys.FileLock: threading.Lock()
                            }

                    # Wait timeout so that first ping at least is finished
                    time.sleep(0.05)
                    data_sent = __send_secure(conn, str(file_size) + ',' + file_id, available_event)

                    if not data_sent:
                        logging.warning('connection aborted')
                        continue

                    logging.debug('sent back fileId {}'.format(file_id))

                    start_dl_msg = __recv_secure(conn, 1024, available_event)
                    if len(start_dl_msg) == 0:
                        logging.warning('connection aborted')
                        continue

                    if ClientMsgs.StartDownload.value in start_dl_msg:
                        send_file_data(conn, file_infos[file_id], available_event)
                    else:
                        logging.warning('start download msg was not as expected')
                        continue

                elif ClientMsgs.Reconnect.value in first_msg:
                    logging.debug('first msg is reconnect msg')

                    _, file_id = first_msg.split(',')
                    file_id = file_id.strip()

                    if file_id not in file_infos or \
                            file_infos[file_id][FileInfoKeys.FinishEvent].is_set():
                        conn.send(ServerMsgs.FileErrorMsg.value + file_id + ' is not registered')
                        continue

                    send_length = conn.send(ServerMsgs.Acknowledge.value)

                    if send_length == 0:
                        logging.info('connection aborted')

                    send_file_data(conn, file_infos[file_id], available_event)
            except socket.error:
                logging.exception('')
            finally:
                heart_beat.stop()
                conn.close()

    finally:
        logging.info('closing socket')
        serv_socket.close()


def __recv_secure(socket, number_bytes, available_event):
    if available_event.is_set():
        return socket.recv(number_bytes)

    return ''


def __send_secure(socket, data, available_event):
    if available_event.is_set():
        return socket.send(data)

    return None


def send_file_data(conn, file_info, available_event):
    logging.debug('sending data')
    try:
        while file_info[FileInfoKeys.NextToSend] >= 0 or not \
                file_info[FileInfoKeys.FinishEvent].is_set():
            if not available_event.is_set():
                return

            if file_info[FileInfoKeys.NextToSend] < 0:
                file_info[FileInfoKeys.FinishEvent].wait()
                continue
            with file_infos_lock:
                send_offset = file_info[FileInfoKeys.NextToSend]

                if file_info[FileInfoKeys.NextToSend] + MSG_LENGTH >= \
                        file_info[FileInfoKeys.FileSize]:
                    file_info[FileInfoKeys.NextToSend] = -1
                else:
                    file_info[FileInfoKeys.NextToSend] += MSG_LENGTH

                while file_info[FileInfoKeys.NextToSend] in \
                        file_info[FileInfoKeys.TransmittedOffsets]:
                    if file_info[FileInfoKeys.NextToSend] + MSG_LENGTH >= \
                            file_info[FileInfoKeys.FileSize]:
                        file_info[FileInfoKeys.NextToSend] = -1
                    else:
                        file_info[FileInfoKeys.NextToSend] += MSG_LENGTH

            with file_info[FileInfoKeys.FileLock]:
                file_info[FileInfoKeys.FileObject].seek(send_offset, 0)
                data_to_send = file_info[FileInfoKeys.FileObject].read(MSG_LENGTH)

            send_length = __send_secure(conn, struct.pack('>i', send_offset) + data_to_send,
                                        available_event)
            logging.debug('sent for offset {} length {}'.format(send_offset, send_length))
            if not send_length:
                with file_infos_lock:
                    if send_offset + MSG_LENGTH >= file_info[FileInfoKeys.FileSize]:
                        file_info[FileInfoKeys.FinishEvent].set()
                        file_info[FileInfoKeys.FinishEvent].clear()
                    file_info[FileInfoKeys.NextToSend] = send_offset

                logging.info('connection aborted for offset {}'.format(send_offset))
                return

            if send_length < len(data_to_send) + 4:
                with file_infos_lock:
                    if send_offset + MSG_LENGTH >= file_info[FileInfoKeys.FileSize]:
                        file_info[FileInfoKeys.FinishEvent].set()
                        file_info[FileInfoKeys.FinishEvent].clear()
                    file_info[FileInfoKeys.NextToSend] = send_offset

                continue

            if send_length + send_offset >= file_info[FileInfoKeys.FileSize] and \
                    file_info[FileInfoKeys.NextToSend] < 0:
                file_info[FileInfoKeys.FinishEvent].set()

            with file_infos_lock:
                file_info[FileInfoKeys.TransmittedOffsets].append(send_offset)

        logging.info('finished sending data')

    except socket.error:
        logging.exception('')


if __name__ == '__main__':
    main()
