
import socket
import sys

def main():
    dest = sys.argv[1]
    UDP_IP, UDP_PORT = dest.split(":")
    UDP_PORT = int(UDP_PORT)
    MESSAGE = "testmessage"
    print("UDP target IP:", UDP_IP)
    print("UDP target port:", UDP_PORT)
    print("message:", MESSAGE)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
    for i in range(100000):
        sock.sendto(bytes(MESSAGE + str(i), "utf-8"), (UDP_IP, UDP_PORT))


if __name__ == "__main__":
    main()
