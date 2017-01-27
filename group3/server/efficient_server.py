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
import multiprocessing
import logging

import group3.heartbeat as heartbeat
from group3.constants import *

file_infos = {}
file_infos_lock = multiprocessing.Lock()


class FileInfoKeys(enum.Enum):
    FileId = 'file_id'
    FileName = 'file_name'
    FileSize = 'file_size'
    FinishEvent = 'finish_event'
    NextToSend = 'next_to_send'
    TransmittedOffsets = 'offsets_trans'
    FileObject = 'file_obj'
    FileLock = 'lock'
    AcknowledgedOffsets = 'acknowledged_offsets'


def main():
    setup_logger()

    process_fast = multiprocessing.Process(target=server_process, args=(SERVER_IP_FAST,),
                                          name='fast thread')
    process_slow = multiprocessing.Process(target=server_process, args=(SERVER_IP_SLOW,),
                                          name='slow thread')

    process_fast.start()
    process_slow.start()

    process_fast.join()
    process_slow.join()
    logging.info('terminated')


def setup_logger():
    logging.basicConfig(filename='server.log', level=LOGGING,
                        format=u'[%(asctime)s][%(levelname)-s][%(processName)s] '
                               u'%(filename)s:%(lineno)d %(message)s',
                        datefmt='%d.%m %H:%M:%S')


def server_process(ip):
    logging.info('starting')

    serv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serv_socket.bind((ip, PORT))
    serv_socket.listen(1)

    try:
        while True:
            try:
                logging.info('waiting for connection')
                conn, addr = serv_socket.accept()
                conn.settimeout(SOCKET_TIMEOUT)
                logging.info('{} connected'.format(addr))

                available_event = threading.Event()

                heart_beat = heartbeat.Heartbeat(addr[0], available_event=available_event,
                                                 identifier='{} heartbeat'.format(addr[0]),
                                                 min_wait_time=0.02)

                heart_beat.start()

                first_msg = conn.recv(1024)

                if len(first_msg) == 0:
                    logging.info('{} disconnected'.format(addr))
                    continue

                logging.debug('received first msg {}'.format(first_msg))
                time.sleep(0.02)
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
                            FileInfoKeys.FileLock: threading.Lock(),
                            FileInfoKeys.AcknowledgedOffsets: []
                            }

                    # Wait timeout so that first ping at least is finished
                    time.sleep(0.02)
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

                    time.sleep(0.02)
                    send_file_data(conn, file_infos[file_id], available_event)
            except socket.error:
                logging.exception('')
            finally:
                heart_beat.stop()
                conn.close()

    finally:
        logging.info('closing socket')
        serv_socket.close()


def __recv_secure(sock, number_bytes, available_event):
    if available_event.is_set():
        return sock.recv(number_bytes)

    return ''


def __send_secure(sock, data, available_event):
    if available_event.is_set():
        return sock.send(data)

    return None


def send_file_data(conn, file_info, available_event):
    logging.debug('sending data')
    stop_event = threading.Event()
    acknowledge_thred = threading.Thread(target=rec_acks, args=(
        conn.getsockname()[0], file_info, available_event, stop_event,
        file_info[FileInfoKeys.FinishEvent]))
    acknowledge_thred.start()

    try:
        while file_info[FileInfoKeys.NextToSend] >= 0 or not \
                file_info[FileInfoKeys.FinishEvent].is_set():
            if not available_event.is_set():
                return

            if file_info[FileInfoKeys.NextToSend] + MSG_LENGTH >= \
                    file_info[FileInfoKeys.FileSize] or file_info[FileInfoKeys.NextToSend] < 0:

                with file_infos_lock:
                    to_send = set(file_info[FileInfoKeys.TransmittedOffsets]).difference(
                        file_info[FileInfoKeys.AcknowledgedOffsets])

                    for offset in to_send:
                        file_info[FileInfoKeys.TransmittedOffsets].remove(offset)

                if len(to_send) > 0:
                    file_info[FileInfoKeys.NextToSend] = min(to_send)
                else:
                    file_info[FileInfoKeys.NextToSend] = -1

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

            send_length = __send_secure(conn, struct.pack('>I', send_offset) + data_to_send,
                                        available_event)
            time.sleep(0.005)
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
        with file_infos_lock:
            del file_infos[file_info[FileInfoKeys.FileId]]

    except socket.error:
        logging.exception('')
    finally:
        stop_event.set()


def rec_acks(ip, file_info, available_event, stop_event, finish_event):
    logging.debug('start receiving acks for {}'.format(ip))
    while not stop_event.is_set():
        conn = None
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind((ip, ACKS_PORT))
            sock.listen(1)

            conn, addr = sock.accept()
            conn.settimeout(2)

            while not stop_event.is_set():
                ack = __recv_secure(conn, 64, available_event)

                if not ack:
                    return

                if ClientMsgs.FinishedDownload.value in ack:
                    finish_event.set()
                    return

                if len(ack) % 4 != 0:
                    ack = ack[:(len(ack) // 4)*4]

                ack_offsets = struct.unpack('>{}I'.format(len(ack)/4), ack)

                logging.debug('recv ack {}'.format(ack_offsets))
                for ack in ack_offsets:
                    if ack not in file_info[FileInfoKeys.AcknowledgedOffsets]:
                        with file_infos_lock:
                            file_info[FileInfoKeys.AcknowledgedOffsets].append(ack)

        except socket.error:

            logging.exception('')
        finally:
            if conn:
                conn.close()
            sock.close()

    logging.debug('stop receiving acks for {}'.format(ip))


if __name__ == '__main__':
    main()
