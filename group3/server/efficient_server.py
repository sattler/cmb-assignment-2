#!/usr/bin/env python

"""
Our server implementation
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

    serv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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

                    _, file_id, offsets_size = first_msg.split(',')
                    file_id = file_id.strip()

                    offsets_size = struct.unpack('>I', offsets_size)

                    if file_id not in file_infos or \
                            file_infos[file_id][FileInfoKeys.FinishEvent].is_set():
                        conn.send(ServerMsgs.FileErrorMsg.value + file_id + ' is not registered')
                        continue

                    send_length = conn.send(ServerMsgs.Acknowledge.value)

                    if send_length == 0:
                        logging.info('connection aborted')

                    time.sleep(0.02)
                    offsets_to_send_packed = __recv_secure(conn, offsets_size*4, available_event)

                    if len(offsets_to_send_packed) == 0:
                        logging.info('{} disconnected in reconnect'.format(addr))
                        continue

                    offsets_to_send = struct.unpack('>{}I'.format(offsets_size),
                                                    offsets_to_send_packed)

                    with file_infos_lock:
                        for offset in offsets_to_send:
                            file_infos[file_id][FileInfoKeys.TransmittedOffsets].remove(offset)

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


def get_next_to_send(file_info):
    if not file_info[FileInfoKeys.TransmittedOffsets]:
        return 0
    next_offset = None
    for offset in xrange(0, max(file_info[FileInfoKeys.TransmittedOffsets]) + MSG_LENGTH + 1, MSG_LENGTH):
        if offset not in file_info[FileInfoKeys.TransmittedOffsets]:
            next_offset = offset
            break

    if next_offset is None:
        logging.error('error server stuck\n{}'.format(file_info[FileInfoKeys.TransmittedOffsets]))
        raise ValueError('no next offset')

    if next_offset < file_info[FileInfoKeys.FileSize]:
        return next_offset

    return None


def remove_offsets(offsets, file_info):
    with file_infos_lock:
        for offset in offsets:
            file_info[FileInfoKeys.TransmittedOffsets].remove(offset)


def send_file_data(conn, file_info, available_event):
    logging.debug('sending data')
    stop_event = threading.Event()
    status_thread = threading.Thread(target=rec_status_msg, args=(
        conn, file_info, available_event, stop_event, file_info[FileInfoKeys.FinishEvent]))
    status_thread.start()

    try:
        next_to_send = get_next_to_send(file_info)
        while not file_info[FileInfoKeys.FinishEvent].is_set():
            if not available_event.is_set():
                return

            if not next_to_send:
                file_info[FileInfoKeys.FinishEvent].wait()
                next_to_send = get_next_to_send(file_info)
                continue

            with file_info[FileInfoKeys.FileLock]:
                file_info[FileInfoKeys.FileObject].seek(next_to_send, 0)
                data_to_send = file_info[FileInfoKeys.FileObject].read(MSG_LENGTH)

            send_length = __send_secure(conn, struct.pack('>I', next_to_send) + data_to_send,
                                        available_event)
            time.sleep(0.005)
            logging.debug('sent for offset {} length {}'.format(next_to_send, send_length))
            if not send_length:
                with file_infos_lock:
                    if next_to_send + MSG_LENGTH >= file_info[FileInfoKeys.FileSize]:
                        file_info[FileInfoKeys.FinishEvent].set()
                        file_info[FileInfoKeys.FinishEvent].clear()

                logging.info('connection aborted for offset {}'.format(next_to_send))
                return

            if send_length == len(data_to_send) + 4:
                with file_infos_lock:
                    file_info[FileInfoKeys.TransmittedOffsets].append(next_to_send)

                if send_length + next_to_send >= file_info[FileInfoKeys.FileSize] and \
                        len(file_info[FileInfoKeys.TransmittedOffsets]) * MSG_LENGTH >= \
                        file_info[FileInfoKeys.FileSize]:
                    file_info[FileInfoKeys.FinishEvent].wait()

            next_to_send = get_next_to_send(file_info)

        logging.info('finished sending data')
        with file_infos_lock:
            del file_infos[file_info[FileInfoKeys.FileId]]

    except socket.error:
        logging.exception('')
    finally:
        stop_event.set()


def rec_status_msg(conn, file_info, available_event, stop_event, finish_event):
    time.sleep(0.2)
    logging.debug('starting status updates {}'.format(time.time()))
    try:
        while not stop_event.is_set():
            status_msg = __recv_secure(conn, 400, available_event)

            if len(status_msg) == 0:
                logging.debug('connection broke in status thread')
                return

            if status_msg == ClientMsgs.FinishedDownload.value:
                finish_event.set()
                return

            if len(status_msg) % 4 != 0:
                status_msg = status_msg[:(len(status_msg)//4)*4]

            missing_offsets = struct.unpack('>{}I'.format(len(status_msg)//4), status_msg)

            logging.debug('received missing offsets {}'.format(missing_offsets))

            remove_offsets(missing_offsets, file_info)

            if len(missing_offsets) < 100:
                time.sleep(1)

    except socket.error:
        logging.exception('')


if __name__ == '__main__':
    main()
