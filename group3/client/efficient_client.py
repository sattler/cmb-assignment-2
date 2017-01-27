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
        FileInfoKeys.DataLock: threading.Lock(), FileInfoKeys.AcksSent: set(),
        FileInfoKeys.AcksSentLock: threading.Lock(), FileInfoKeys.FinishEvent: threading.Event(),
        FileInfoKeys.Written: 0,
        FileInfoKeys.FileObj: open(os.path.join('downloads', file_name), 'wb'),
        FileInfoKeys.FileLock: threading.Lock()
    }

    try:
        start_time = time.time()

        connected_event = threading.Event()

        fast_thread = ClientThread(SERVER_IP_FAST, file_info, connected_event, name='fast')
        slow_thread = ClientThread(SERVER_IP_SLOW, file_info, connected_event, name='slow')

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
                        format=u'[%(asctime)s][%(levelname)-s][%(threadName)s] '
                               u'%(filename)s:%(lineno)d %(message)s',
                        datefmt='%d.%m %H:%M:%S')


class ClientThread(threading.Thread):

    def __init__(self, ip, file_info, connected_event, **kwargs):
        self.ip = ip
        self.file_info = file_info
        self.connected_event = connected_event
        self.available_event = threading.Event()

        super(ClientThread, self).__init__(**kwargs)

    def run(self):
        heart_beat = heartbeat.Heartbeat(self.ip, available_event=self.available_event,
                                         identifier='{} hearbeat'.format(self.ip))

        heart_beat.start()

        while not self.file_info[FileInfoKeys.FinishEvent].is_set():
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
                    send_length = _send_secure(client_sock,
                                               ClientMsgs.Reconnect.value + ',' +
                                               self.file_info[FileInfoKeys.FileId],
                                               self.available_event)

                    if send_length == 0:
                        logging.warning('server not present anymore')
                        continue

                    logging.debug('reconnect msg sent')

                    response = _recv_secure(client_sock, 128, self.available_event)

                    if ServerMsgs.Acknowledge.value in response:
                        logging.debug('reconnected')
                        self.connected_event.set()
                        self.start_download(client_sock)
                    else:
                        logging.info('error occurred'.format(response))
                        self.connected_event.set()
                        return
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
                            logging.error('Aborting! Server response {}'.format(response))
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
                if '111' in error.message:
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

        acknowledge_thred = AcksThread(sock.getpeername()[0], self.available_event, self.file_info, stop_event)
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

                if offset in self.file_info[FileInfoKeys.AcksSent]:
                    with self.file_info[FileInfoKeys.AcksSentLock]:
                        self.file_info[FileInfoKeys.AcksSent].remove(offset)
                    continue

                self.file_info[FileInfoKeys.Data][offset] = (buf, total_len)
                # logging.debug('received offset {}'.format(offset))

        except socket.error:
            logging.exception('')
        finally:
            stop_event.set()


class AcksThread(threading.Thread):

    def __init__(self, ip, available_event, file_info, stop_event, **kwargs):
        self.ip = ip
        self.available_event = available_event
        self.file_info = file_info
        self.stop_event = stop_event
        super(AcksThread, self).__init__(**kwargs)

    def _send_acks(self):
        while not self.stop_event.is_set():
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            conn.connect((self.ip, ACKS_PORT))
            try:
                while not self.stop_event.is_set():
                    to_send = set(self.file_info[FileInfoKeys.Data].keys()).difference(
                        self.file_info[FileInfoKeys.AcksSent])
                    data_to_send = ""
                    sent_acks = []
                    for offset in to_send:
                        data_to_send += struct.pack('>I', offset)
                        sent_acks.append(offset)

                        if len(data_to_send) >= 15 * 4:
                            if not _send_secure(conn, data_to_send, self.available_event):
                                return
                            data_to_send = ""

                            with self.file_info[FileInfoKeys.AcksSentLock]:
                                for ack in sent_acks:
                                    self.file_info[FileInfoKeys.AcksSent].add(ack)

                            sent_acks = []

                    time.sleep(0.1)

            except socket.error:
                logging.exception('')
            finally:
                conn.close()


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
