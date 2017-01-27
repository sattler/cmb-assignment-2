#!/usr/bin/env python

"""
Our client implementation
"""

import os
import socket
import struct
import threading
import multiprocessing
import time
import logging

import group3.heartbeat as heartbeat
from group3.constants import *


class FileInfoKeys(enum.Enum):
    FileId = 'file_id'
    FileSize = 'file_size'
    FileName = 'file_name'
    Data = 'data'
    DataLock = 'data_lock'
    AcksSent = 'acks_sent'
    AcksSentLock = 'acks_sent_lock'
    FinishEvent = 'finish_event'
    Written = 'written'
    FileObj = 'file_obj'
    FileLock = 'file_lock'


file_name = 'index.html'


def main():
    setup_logger()

    file_info = {
        FileInfoKeys.FileName: file_name, FileInfoKeys.Data: {},
        FileInfoKeys.DataLock: multiprocessing.Lock(), FileInfoKeys.AcksSent: set(),
        FileInfoKeys.AcksSentLock: multiprocessing.Lock(),
        FileInfoKeys.FinishEvent: multiprocessing.Event(),
        FileInfoKeys.Written: 0,
        FileInfoKeys.FileObj: open(os.path.join('downloads', file_name), 'wb'),
        FileInfoKeys.FileLock: multiprocessing.Lock()
    }

    try:
        start_time = time.time()

        connected_event = threading.Event()

        fast_thread = ClientProcess(SERVER_IP_FAST, file_info, connected_event, name='fast')
        slow_thread = ClientProcess(SERVER_IP_SLOW, file_info, connected_event, name='slow')

        thread_write = WriteThread(file_info, name='write thread')

        thread_write.start()
        slow_thread.start()
        fast_thread.start()

        slow_thread.join()
        fast_thread.join()
    finally:
        file_info[FileInfoKeys.FileObj].close()

        # TODO  measure how long it took


def setup_logger():
    logging.basicConfig(filename='client.log', level=LOGGING,
                        format=u'[%(asctime)s][%(levelname)-s][%(processName)s] '
                               u'%(filename)s:%(lineno)d %(message)s',
                        datefmt='%d.%m %H:%M:%S')


class ClientProcess(multiprocessing.Process):

    def __init__(self, ip, file_info, connected_event, **kwargs):
        self.ip = ip
        self.file_info = file_info
        self.connected_event = connected_event
        self.available_event = threading.Event()

        super(ClientProcess, self).__init__(**kwargs)

    def run(self):
        heart_beat = heartbeat.Heartbeat(self.ip, available_event=self.available_event,
                                         identifier='{} hearbeat'.format(self.ip))

        heart_beat.start()

        while not self.file_info[FileInfoKeys.FinishEvent].is_set():
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_sock.settimeout(CLIENT_SOCKET_TIMEOUT)

            try:

                if not self.available_event.is_set():
                    logging.debug('before wait')
                    self.available_event.wait()
                    logging.debug('after wait')

                if self.file_info[FileInfoKeys.FinishEvent].is_set():
                    break

                logging.debug('connecting')

                client_sock.connect((self.ip, PORT))

                logging.debug('after connect')

                if FileInfoKeys.FileId in self.file_info:
                    not_received = all_offsets_left(self.file_info)

                    reconnect_msg = ClientMsgs.Reconnect.value + ',' + \
                                    self.file_info[FileInfoKeys.FileId] + ',' + \
                                    struct.pack('>I', len(not_received))
                    send_length = _send_secure(client_sock, reconnect_msg, self.available_event)

                    if send_length == 0:
                        logging.warning('server not present anymore')
                        continue

                    logging.debug('reconnect msg sent')

                    response = _recv_secure(client_sock, 128, self.available_event)

                    if ServerMsgs.Acknowledge.value not in response:
                        logging.info('error occurred'.format(response))
                        return

                    send_length = _send_secure(client_sock, struct.pack(
                        '>{}I'.format(len(not_received)), *not_received), self.available_event)

                    if send_length == 0:
                        logging.warning('server not present anymore')
                        continue

                    logging.debug('reconnected')
                    self.start_download(client_sock)
                else:
                    with self.file_info[FileInfoKeys.FileLock]:
                        if FileInfoKeys.FileId in self.file_info:
                            continue

                        logging.debug('send newFile msg')

                        send_length = _send_secure(client_sock, ClientMsgs.NewFile.value + ',' +
                                                   self.file_info[FileInfoKeys.FileName],
                                                   self.available_event)

                        if send_length == 0:
                            logging.warning('server not present anymore')
                            continue

                        response = _recv_secure(client_sock, 128, self.available_event)

                        if len(response) == 0:
                            logging.warning('connection aborted no response')
                            continue

                        logging.debug('newFile msg response was: {}'.format(response))

                        if ServerMsgs.FileErrorMsg.value in response:
                            logging.error('Aborting! Serverresponse {}'.format(response))
                            self.file_info[FileInfoKeys.FinishEvent].set()
                            self.connected_event.set()
                            return

                        file_size, file_id = response.split(',')
                        self.file_info[FileInfoKeys.FileId] = file_id
                        self.file_info[FileInfoKeys.FileSize] = int(file_size)
                        logging.info('connected {} {}'.format(
                            file_id, self.file_info[FileInfoKeys.FileSize]))

                        self.connected_event.set()

                        bytes_sent = _send_secure(client_sock, ClientMsgs.StartDownload.value,
                                                  self.available_event)

                        if bytes_sent != len(ClientMsgs.StartDownload.value):
                            logging.info('start download connection error (sent: {}, of: {})'
                                         .format(bytes_sent, len(ClientMsgs.StartDownload.value)))
                            # Probably not the best way to completely reconnect but the connection
                            # has no
                            # packet loss so it shouldn't be a Problem
                            continue

                        self.start_download(client_sock)

                    if self.file_info[FileInfoKeys.Written] >= \
                            self.file_info[FileInfoKeys.FileSize]:
                        _send_secure(client_sock, ClientMsgs.FinishedDownload.value,
                                     self.available_event)
                        logging.info('finished downloading')

            except socket.error as error:
                if error.errno == 111:
                    logging.exception('server not started! Aborting...')
                    print('server not started! Aborting')
                    self.file_info[FileInfoKeys.FinishEvent].set()
                    break
                logging.exception('client socket error')
            finally:
                client_sock.close()

        heart_beat.stop()

    def start_download(self, sock):
        logging.debug('downloading')
        stop_event = threading.Event()

        acknowledge_thred = StatusThread(sock, self.available_event, self.file_info, stop_event)
        acknowledge_thred.start()

        try:
            while not self.file_info[FileInfoKeys.FinishEvent].is_set():
                if not self.available_event.is_set():
                    return
                buf = _recv_secure(sock, MSG_LENGTH + 4, self.available_event)

                if len(buf) == 0:
                    logging.warning('connection error')
                    return

                offset = struct.unpack('>I', buf[0:4])[0]

                if offset > self.file_info[FileInfoKeys.FileSize]:
                    logging.debug('received strange data! Resync needed, Aborting')
                    return

                buf = buf[4:]
                total_len = len(buf)
                msg_len = MSG_LENGTH

                if offset + MSG_LENGTH > self.file_info[FileInfoKeys.FileSize]:
                    msg_len = int(self.file_info[FileInfoKeys.FileSize]) - offset

                while total_len != msg_len:
                    buf += _recv_secure(sock, MSG_LENGTH - total_len, self.available_event)

                    if len(buf) == total_len:
                        logging.warning('connection error')
                        return

                    total_len = len(buf)

                # if offset in self.file_info[FileInfoKeys.AcksSent]:
                #     with self.file_info[FileInfoKeys.AcksSentLock]:
                #         self.file_info[FileInfoKeys.AcksSent].remove(offset)
                #     continue

                self.file_info[FileInfoKeys.Data][offset] = (buf, total_len)
                # logging.debug('received offset {}'.format(offset))

        except socket.error:
            logging.exception('')
        finally:
            stop_event.set()


def all_offsets_left(file_info):
    if len(file_info[FileInfoKeys.Data]):
        offsets = list(file_info[FileInfoKeys.Data].keys())
        left_offsets = []
        for offset in xrange(0, max(offsets), MSG_LENGTH):
            if offset not in offsets:
                left_offsets.append(offset)

        left_offsets.append(max(offsets) + MSG_LENGTH)
        return left_offsets
    return []


class StatusThread(threading.Thread):

    def __init__(self, conn, available_event, file_info, stop_event, **kwargs):
        self.conn = conn
        self.available_event = available_event
        self.file_info = file_info
        self.stop_event = stop_event
        super(StatusThread, self).__init__(**kwargs)

    def run(self):
        logging.debug('starting status updates {}'.format(time.time()))
        try:
            while not self.stop_event.is_set():
                offsets_left = all_offsets_left(self.file_info)[:100]
                send_number = _send_secure(self.conn,
                                           struct.pack('>{}I'.format(len(offsets_left)),
                                                       *offsets_left),
                                           self.available_event)
                if send_number == 0:
                    logging.debug('socket disconnected')

                if len(offsets_left) < 100:
                    time.sleep(1)
        except socket.error:
            logging.exception('')


def _recv_secure(sock, number_bytes, available_event):
    if available_event.is_set():
        return sock.recv(number_bytes)

    return ''


def _send_secure(sock, data_to_send, available_event):
    if available_event.is_set():
        return sock.send(data_to_send)

    return None


class WriteThread(threading.Thread):

    def __init__(self, file_info, **kwargs):
        self.file_info = file_info
        super(WriteThread, self).__init__(**kwargs)

    def run(self):
        while True:
            if self.file_info[FileInfoKeys.Written] in self.file_info[FileInfoKeys.Data]:
                while self.file_info[FileInfoKeys.Written] < \
                        int(self.file_info[FileInfoKeys.FileSize]):
                    write_len = self.file_info[FileInfoKeys.Data][
                        self.file_info[FileInfoKeys.Written]][1]
                    self.file_info[FileInfoKeys.FileObj].write(
                        self.file_info[FileInfoKeys.Data][self.file_info[FileInfoKeys.Written]][0])

                    # with data_lock:
                    #     del data[written]

                    self.file_info[FileInfoKeys.Written] += write_len
                    if self.file_info[FileInfoKeys.Written] not in \
                            self.file_info[FileInfoKeys.Data]:
                        break
                logging.debug('written {}'.format(self.file_info[FileInfoKeys.Written]))
                if self.file_info[FileInfoKeys.Written] >= self.file_info[FileInfoKeys.FileSize]:
                    self.file_info[FileInfoKeys.FinishEvent].set()
                    return

            time.sleep(0.05)


if __name__ == '__main__':
    main()
