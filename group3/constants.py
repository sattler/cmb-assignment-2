#!/usr/bin/env python

import enum
import logging

PORT = 12345
SERVER_IP_FAST = '10.1.0.3'
SERVER_IP_SLOW = '10.0.0.3'
LOGGING = logging.DEBUG

STATUS_PORT = 23456

SOCKET_TIMEOUT = 0.5
CLIENT_SOCKET_TIMEOUT = 0.5

MSG_LENGTH = 1400


class ServerMsgs(enum.Enum):
    FileErrorMsg = 'File does not exist!'
    Acknowledge = 'Reconnect to file successful'


class ClientMsgs(enum.Enum):
    NewFile = 'NewFile'
    StartDownload = 'StartDownload'
    Reconnect = 'Reconnect'
    NoMissing = 'NoMissing'
    FinishedDownload = 'F;'
