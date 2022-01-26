
from email import header
import socket
import sys
import struct
from collections import namedtuple
import time

def main():
    dest = sys.argv[1]
    UDP_IP, UDP_PORT = dest.split(":")
    UDP_PORT = int(UDP_PORT)
    MESSAGE = "testmessage"
    print("UDP target IP:", UDP_IP)
    print("UDP target port:", UDP_PORT)
    print("message:", MESSAGE)

    # Pack the xrootd header
    Header = namedtuple("header", ["code", "pseq", "plen", "server_start"])
    header = Header(code=0, pseq=0, plen=len(MESSAGE) + 8, server_start=int(time.time()))
    buf = struct.pack("!cBHI", header.code.to_bytes(1, byteorder="big"), header.pseq, header.plen, header.server_start) + MESSAGE.encode('utf-8')

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
    for i in range(10):
        sock.sendto(buf, (UDP_IP, UDP_PORT))


if __name__ == "__main__":
    main()
