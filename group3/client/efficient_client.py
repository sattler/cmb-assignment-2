#!/usr/bin/env python

"""
Our client implementation
"""

import socket
import threading
import time
import struct
import os
import logging

from group3.constants import *


class FileInfoKeys(enum.Enum):
    FileId = 'file_id'
    FileSize = 'file_size'
    FileName = 'file_name'


data = {}
written = 0
file_info = {FileInfoKeys.FileName: 'index.html'}
file_obj = open(os.path.join('downloads', file_info[FileInfoKeys.FileName]), 'wb')


def main():
    setup_logger()
    try:
        start_time = time.time()
        data_lock = threading.Lock()
        connected_event = threading.Event()
        finish_event = threading.Event()
        connecting_lock = threading.Lock()

        thread_fast = threading.Thread(target=client_thread, args=(SERVER_IP_FAST, data_lock,
                                                                   connected_event, False,
                                                                   finish_event),
                                       name='fast')
        thread_slow = threading.Thread(target=client_thread, args=(SERVER_IP_SLOW, data_lock,
                                                                   connected_event, True,
                                                                   finish_event),
                                       name='slow')
        thread_slow.start()
        time.sleep(0.5)
        thread_fast.start()

        thread_slow.join()
        thread_fast.join()
    finally:
        file_obj.close()

    # TODO  measure how long it took


def setup_logger():
    logging.basicConfig(filename='client.log', level=LOGGING,
                        format=u'[%(asctime)s][%(levelname)-s][%(threadName)s] '
                               u'%(filename)s:%(lineno)d %(message)s',
                        datefmt='%d.%m %H:%M:%S')


def client_thread(ip, data_lock, connected_event, should_connect, finish_event):
    while not finish_event.is_set():
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # Options specific for linux
        client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, TCP_KEEP_ALIVE_IDLE)
        client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, TCP_KEEP_ALIVE_INTERVALL)
        client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, TCP_KEEP_ALIVE_MAX_FAILS)

        if not should_connect:
            if not connected_event.is_set():
                connected_event.wait()
                continue

        try:
            client_sock.connect((ip, PORT))

            logging.debug('after connect')

            if FileInfoKeys.FileId in file_info:
                send_length = client_sock.send(ClientMsgs.Reconnect.value + ',' + file_info[FileInfoKeys.FileId])

                if send_length == 0:
                    logging.warning('server not present anymore')
                    continue

                logging.debug('send reconnect msg')

                response = client_sock.recv(128)

                if ServerMsgs.Acknowledge.value in response:
                    logging.debug('reconnected')
                    connected_event.set()
                    start_download(client_sock, data_lock, finish_event)
                else:
                    logging.info('error occurred'.format(response))
                    connected_event.set()
                    finish_event.set()
                    return
            elif should_connect:
                logging.debug('send newFile msg')
                send_length = client_sock.send(ClientMsgs.NewFile.value + ',' + file_info[FileInfoKeys.FileName])

                if send_length == 0:
                    logging.warning('server not present anymore')
                    continue

                response = client_sock.recv(128)

                if len(response) == 0:
                    logging.warning('connection aborted no response')
                    continue

                logging.debug('newFile msg response was: {}'.format(response))

                if ServerMsgs.FileErrorMsg.value in response:
                    logging.error('Aborting! Serverresponse {}'.format(response))
                    finish_event.set()
                    connected_event.set()
                    return

                file_size, file_id = response.split(',')
                file_info[FileInfoKeys.FileId] = file_id
                file_info[FileInfoKeys.FileSize] = int(file_size)
                logging.info('connected {} {}'.format(file_id, file_size))

                connected_event.set()

                bytes_sent = client_sock.send(ClientMsgs.StartDownload.value)
                if bytes_sent != len(ClientMsgs.StartDownload.value):
                    logging.info('start download connection error (sent: {}, of: {})'.format(
                        bytes_sent, len(ClientMsgs.StartDownload.value)))
                    # Probably not the best way to completely reconnect but the connection has no
                    # packet loss so it shouldn't be a Problem
                    continue
                start_download(client_sock, data_lock, finish_event)
            else:
                logging.warning('strange error')

            if written >= file_info[FileInfoKeys.FileSize]:
                logging.info('finished downloading')

        except socket.error:
            logging.exception('client socket error')
        finally:
            client_sock.close()


def start_download(sock, data_lock, finish_event):
    logging.debug('downloading')
    try:
        while True:
            buf = sock.recv(MSG_LENGTH + 4)

            if len(buf) == 0:
                logging.warning('connection error')
                return

            offset = struct.unpack('I', buf[0:4])[0]
            buf = buf[4:]
            total_len = len(buf)
            msg_len = MSG_LENGTH
            if offset + MSG_LENGTH > file_info[FileInfoKeys.FileSize]:
                msg_len = int(file_info[FileInfoKeys.FileSize]) - offset

            if total_len != msg_len:
                continue

            data[offset] = (buf, total_len)
            global written
            if offset == written:
                while written < int(file_info[FileInfoKeys.FileSize]):
                    write_len = data[written][1]
                    with data_lock:
                        file_obj.write(data[written][0])
                        del data[written]

                    written += write_len
                    if written not in data:
                        break
                logging.debug('written {}'.format(written))
                if written >= file_info[FileInfoKeys.FileSize]:
                    finish_event.set()
                    return

    except socket.error:
        logging.exception('')


if __name__ == '__main__':
    main()
