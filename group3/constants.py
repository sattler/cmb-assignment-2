#!/usr/bin/env python

import enum
import logging

PORT = 12345
SERVER_IP_FAST = '10.1.0.3'
SERVER_IP_SLOW = '10.0.0.3'
LOGGING = logging.DEBUG

TCP_KEEP_ALIVE_IDLE = 1
TCP_KEEP_ALIVE_INTERVALL = 1
TCP_KEEP_ALIVE_MAX_FAILS = 2

SOCKET_TIMEOUT = 0.2

MSG_LENGTH = 1400


class ServerMsgs(enum.Enum):
    FileErrorMsg = 'File does not exist!'
    Acknowledge = 'Reconnect to file successful'


class ClientMsgs(enum.Enum):
    NewFile = 'NewFile'
    StartDownload = 'StartDownload'
    Reconnect = 'Reconnect;'
