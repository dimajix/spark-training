#!/usr/bin/python

import sys
import socket
import optparse
from datetime import datetime
import select



def parse_options():
    """
    Parses all command line options and returns an approprate Options object
    :return:
    """
    parser = optparse.OptionParser(description='Python NetCat Server.')
    parser.add_option('-P','--port', action='store', dest='port', nargs=1, default=9977, help='Port to listen on')
    parser.add_option('-H','--host', action='store', dest='host', nargs=1, default='0.0.0.0', help='Host to listen on')
    parser.add_option('-I','--interval', action='store', dest='interval', nargs=1, default=1, help='Interval between batches')
    parser.add_option('-B','--batch', action='store', dest='batch', nargs=1, default=5, help='Batch size (number of lines)')
    parser.add_option('-T','--timestamp', action='store_const', dest='timestamp', const=True, default=False, help='Add Timestamp Column')

    (opts, args) = parser.parse_args()

    return opts



def run_server_loop(serversocket, opts):
    clients = []
    input = [serversocket, sys.stdin]
    timeout = int(opts.interval)
    max_batch = int(opts.batch)
    timestamp = opts.timestamp
    current_batch = 0

    while 1:
        input_ready,output_ready,except_ready = select.select(input + clients,[],[],timeout)

        # Add input when timeout happened
        if not input_ready:
            # Only add input if not already in input list
            if not sys.stdin in input:
                input += [sys.stdin]
                current_batch = 0
        elif current_batch > max_batch and sys.stdin in input:
            input.remove(sys.stdin)

        for s in input_ready:
            if s == serversocket:
                # handle the server socket
                client, address = serversocket.accept()
                clients.append(client)
                print("Accepted connection from " + str(address))

            elif s == sys.stdin:
                line = sys.stdin.readline()
                if not line:
                    return
                if timestamp:
                    line = str(datetime.now()) + "\t" + line
                current_batch = current_batch + 1
                for c in clients:
                    c.sendall(line)

            else:
                # handle all other sockets
                data = s.recv(4096)
                if not data:
                    s.close()
                    clients.remove(s)


def run_server(opts):
    host = opts.host
    port = int(opts.port)
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind((host, port))
    serversocket.listen(5)

    try:
        run_server_loop(serversocket, opts)
    except KeyboardInterrupt:
        pass
    finally:
        serversocket.close()


def main():
    """Main method of wordcount start script"""
    opts = parse_options()

    run_server(opts)



main()
