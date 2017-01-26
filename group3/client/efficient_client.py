#!/usr/bin/env python

"""
Our client implementation
"""

import os
import socket
import struct
import threading
import time
import logging

import group3.heartbeat as heartbeat
from group3.constants import *


class FileInfoKeys(enum.Enum):
    FileId = 'file_id'
    FileSize = 'file_size'
    FileName = 'file_name'


data = {}
acks_sent = set()
acks_lock = threading.Lock()
data_lock = threading.Lock()
finish_event = threading.Event()
written = 0
file_info = {FileInfoKeys.FileName: 'index.html'}
file_obj = open(os.path.join('downloads', file_info[FileInfoKeys.FileName]), 'wb')


def main():
    setup_logger()
    try:
        start_time = time.time()
        connected_event = threading.Event()
        new_file_lock = threading.Lock()

        thread_fast = threading.Thread(target=client_thread, args=(SERVER_IP_FAST,
                                                                   connected_event, new_file_lock),
                                       name='fast')
        thread_slow = threading.Thread(target=client_thread, args=(SERVER_IP_SLOW,
                                                                   connected_event, new_file_lock),
                                       name='slow')
        thread_write = threading.Thread(target=write_thread, name='write thread')

        thread_write.start()
        thread_slow.start()
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


def client_thread(ip, connected_event, new_file_lock):
    available_event = threading.Event()

    def connection_status_changed(available):
        if available:
            available_event.set()
            logging.debug('available {}'.format(time.time()))
        else:
            available_event.clear()
            logging.debug('not available {}'.format(time.time()))

    heart_beat = heartbeat.Heartbeat(ip, action_handler=connection_status_changed,
                                     identifier='{} hearbeat'.format(ip))

    heart_beat.start()

    while not finish_event.is_set():
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.settimeout(SOCKET_TIMEOUT)

        # TCP Keep Alive is to slow in recognizing a connection drop so we replaced it
        # client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # # Options specific for linux
        # client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, TCP_KEEP_ALIVE_IDLE)
        # client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, TCP_KEEP_ALIVE_INTERVALL)
        # client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, TCP_KEEP_ALIVE_MAX_FAILS)

        try:

            if not available_event.is_set():
                logging.debug('before wait')
                available_event.wait()
                logging.debug('after wait')

            if finish_event.is_set():
                break

            logging.debug('connecting')

            client_sock.connect((ip, PORT))

            logging.debug('after connect')

            if FileInfoKeys.FileId in file_info:
                send_length = __send_secure(client_sock,
                                            ClientMsgs.Reconnect.value + ',' +
                                            file_info[FileInfoKeys.FileId],
                                            available_event)

                if send_length == 0:
                    logging.warning('server not present anymore')
                    continue

                logging.debug('reconnect msg sent')

                response = __recv_secure(client_sock, 128, available_event)

                if ServerMsgs.Acknowledge.value in response:
                    logging.debug('reconnected')
                    connected_event.set()
                    start_download(client_sock, available_event)
                else:
                    logging.info('error occurred'.format(response))
                    connected_event.set()
                    return
            else:
                with new_file_lock:
                    if FileInfoKeys.FileId in file_info:
                        continue

                    logging.debug('send newFile msg')

                    send_length = __send_secure(client_sock, ClientMsgs.NewFile.value + ',' +
                                                file_info[FileInfoKeys.FileName],
                                                available_event)

                    if send_length == 0:
                        logging.warning('server not present anymore')
                        continue

                    response = __recv_secure(client_sock, 128, available_event)

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
                    logging.info('connected {} {}'.format(file_id,
                                                          file_info[FileInfoKeys.FileSize]))

                    connected_event.set()

                    bytes_sent = __send_secure(client_sock, ClientMsgs.StartDownload.value,
                                               available_event)
                    if bytes_sent != len(ClientMsgs.StartDownload.value):
                        logging.info('start download connection error (sent: {}, of: {})'.format(
                            bytes_sent, len(ClientMsgs.StartDownload.value)))
                        # Probably not the best way to completely reconnect but the connection
                        # has no
                        # packet loss so it shouldn't be a Problem
                        continue

                    start_download(client_sock, available_event)

                if written >= file_info[FileInfoKeys.FileSize]:
                    while not __send_secure(client_sock, ClientMsgs.FinishedDownload.value, available_event):
                        if not available_event.is_set():
                            available_event.wait()
                    logging.info('finished downloading')

        except socket.error as error:
            if '111' in error.message:
                logging.exception('server not started! Aborting...')
                print('server not started! Aborting')
                finish_event.set()
                break
            logging.exception('client socket error')
        finally:
            client_sock.close()

    heart_beat.stop()


def start_download(sock, available_event):
    logging.debug('downloading')
    stop_event = threading.Event()
    acknowledge_thred = threading.Thread(target=_send_acks, args=(sock, available_event,
                                                                  stop_event))
    acknowledge_thred.daemon = True
    acknowledge_thred.start()

    try:
        while not finish_event.is_set():
            if not available_event.is_set():
                return
            buf = __recv_secure(sock, MSG_LENGTH + 4, available_event)

            if len(buf) == 0:
                logging.warning('connection error')
                return

            offset = struct.unpack('>I', buf[0:4])[0]

            if offset > file_info[FileInfoKeys.FileSize]:
                logging.debug('received strange data! Resync needed, Aborting')
                return

            buf = buf[4:]
            total_len = len(buf)
            msg_len = MSG_LENGTH

            if offset + MSG_LENGTH > file_info[FileInfoKeys.FileSize]:
                msg_len = int(file_info[FileInfoKeys.FileSize]) - offset

            while total_len != msg_len:
                buf += __recv_secure(sock, MSG_LENGTH - total_len, available_event)

                if len(buf) == total_len:
                    logging.warning('connection error')
                    return

                total_len = len(buf)

            if offset in acks_sent:
                with acks_lock:
                    acks_sent.remove(offset)
                continue

            data[offset] = (buf, total_len)
            # logging.debug('received offset {}'.format(offset))

    except socket.error:
        logging.exception('')
    finally:
        stop_event.set()


def _send_acks(conn, available_event, stop_event):
    try:
        while not stop_event.is_set():
            to_send = set(data.keys()).difference(acks_sent)
            data_to_send = ""
            sent_acks = []
            for offset in to_send:
                data_to_send += struct.pack('>I', offset)
                sent_acks.append(offset)

                if len(data_to_send) >= 15 * 4:
                    if not __send_secure(conn, data_to_send, available_event):
                        return
                    data_to_send = ""

                    with acks_lock:
                        for ack in sent_acks:
                            acks_sent.add(ack)

                    sent_acks = []

            time.sleep(0.1)

    except socket.error:
        logging.exception('')


def __recv_secure(sock, number_bytes, available_event):
    if available_event.is_set():
        return sock.recv(number_bytes)

    return ''


def __send_secure(sock, data_to_send, available_event):
    if available_event.is_set():
        return sock.send(data_to_send)

    return None


def write_thread():
    global written
    while True:
        if written in data:
            while written < int(file_info[FileInfoKeys.FileSize]):
                write_len = data[written][1]
                file_obj.write(data[written][0])

                # with data_lock:
                #     del data[written]

                written += write_len
                if written not in data:
                    break
            logging.debug('written {}'.format(written))
            if written >= file_info[FileInfoKeys.FileSize]:
                finish_event.set()
                return

        time.sleep(0.05)


if __name__ == '__main__':
    main()
