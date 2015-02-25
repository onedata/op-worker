#!/usr/bin/env python

import socket
import ssl
import argparse

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Connect to ssl server and send custom messages.')

parser.add_argument(
    '-m', '--message',
    action='store',
    help='message to be sent',
    dest='message')

parser.add_argument(
    '-c', '--count',
    action='store',
    default=1,
    help='message count',
    dest='count')

parser.add_argument(
    '-h', '--host',
    action='store',
    help='server host',
    dest='host')

parser.add_argument(
    '-p', '--port',
    action='store',
    default=5555,
    help='server port',
    dest='port')

args = parser.parse_args()

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sslSocket = ssl.wrap_socket(s)
sslSocket.connect((args.host, args.port))
print repr(sslSocket.server())
print repr(sslSocket.issuer())
for i in range(0, args.count):
    sslSocket.write(args.message)
sslSocket.close()