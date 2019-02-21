import SocketServer
import time

class MyTCPHandler(SocketServer.BaseRequestHandler):
    """
    Provider data for update_state_by_key.sh.
    """

    def handle(self):
        while True:
            data_file = open("/root/test_data/part-00000",'r')
            for l in data_file:
                self.request.sendall(l)
                print l
            #self.request.sendall('i love python'.upper())
            #time.sleep(1)


if __name__ == "__main__":
    HOST, PORT = "127.0.0.1", 7777
    server = SocketServer.TCPServer((HOST, PORT), MyTCPHandler)
    server.serve_forever()
