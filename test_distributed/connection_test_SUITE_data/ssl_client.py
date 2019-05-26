#!/usr/bin/env python

import socket
import ssl
import argparse
import time
import struct

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Connect to ssl server and send custom messages.')

parser.add_argument(
    '--message',
    action='store',
    help='path to message that will be sent',
    dest='message')

parser.add_argument(
    '--count',
    action='store',
    default=1,
    help='message count',
    dest='count')

parser.add_argument(
    '--host',
    action='store',
    help='server host',
    dest='host')

parser.add_argument(
    '--port',
    action='store',
    default=443,
    help='server port',
    dest='port')

parser.add_argument(
    '--handshake-message',
    action='store',
    help='path to first message that will be send to server',
    dest='handshake')

# get args
args = parser.parse_args()
port = int(args.port)
count = int(args.count)
handshake = open(args.handshake, "rb").read()
handshake_length = socket.htonl(len(handshake))
handshake_bytes = struct.pack("I", handshake_length) + handshake
message = open(args.message, "rb").read()
message_length = socket.htonl(len(message))
message_bytes = struct.pack("I", message_length) + message

proto_upgrade_msg = "GET /clproto HTTP/1.1\r\n" \
                    "Host: " + args.host + "\r\n" \
                    "Connection: upgrade\r\n" \
                    "Upgrade: clproto\r\n" \
                    "\r\n"

# prepare socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sslSocket = ssl.wrap_socket(s)

# connect
sslSocket.connect((args.host, port))
sslSocket.write(proto_upgrade_msg)
sslSocket.read()
sslSocket.write(handshake_bytes)
sslSocket.read()

# send messages
for i in range(0, count):
    sslSocket.write(message_bytes)

# wait for kill by ct test
time.sleep(30)
