#!/usr/bin/python
from __future__ import print_function

import socket
import boto3
import optparse
import time
import select
import logging
import tempfile
from urlparse import urlparse
from datetime import datetime


def parse_options():
    """
    Parses all command line options and returns an approprate Options object
    :return:
    """
    parser = optparse.OptionParser(description='S3 Cat.')
    parser.add_option('-I','--interval', action='store', dest='interval', nargs=1, default=1, help='Interval between batches')
    parser.add_option('-B','--batch', action='store', dest='batch', nargs=1, default=10, help='Batch size (number of lines)')
    parser.add_option('-T','--timestamp', action='store_const', dest='timestamp', const=True, default=False, help='Add Timestamp Column')

    (opts, args) = parser.parse_args()

    return (opts, args)


class Server(object):
    logger = logging.getLogger("Server")

    def __init__(self, interval, max_batchsize, timestamp):
        self.interval = interval
        self.max_batchsize = max_batchsize
        self.timestamp = timestamp
        self.s3client = boto3.resource('s3')

    def _process_lines(self, source):
        current_batch = 0

        for line in source:
            if self.timestamp:
                line = str(datetime.now()) + "\t" + line

            current_batch += 1
            print(line, end='')

            if current_batch > self.max_batchsize:
                current_batch = 0
                time.sleep(self.interval)

    def _process_file(self, s3object):
        self.logger.info("Reading file s3://%s/%s", s3object.bucket_name, s3object.key)
        with tempfile.SpooledTemporaryFile() as source:
            s3object.download_fileobj(source)
            source.seek(0)
            self._process_lines(source)

    def _process_dir(self, dir):
        url = urlparse(dir)
        bucket_name = url.hostname
        prefix = url.path[1:]

        self.logger.info("Processing bucket %s prefixes %s", bucket_name, prefix)

        s3bucket = self.s3client.Bucket(bucket_name)
        files = s3bucket.objects.filter(Prefix=prefix)
        for file in files:
            s3object = self.s3client.Object(file.bucket_name, file.key)
            self._process_file(s3object)

    def run(self, files):
        for dir in files:
            self._process_dir(dir)


def run_server(opts, files):
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)

    interval = int(opts.interval)
    max_batch = int(opts.batch)
    timestamp = opts.timestamp

    # boto3.set_stream_logger('boto3.resources', logging.INFO)

    server = Server(interval, max_batch, timestamp)
    try:
        server.run(files)
    except KeyboardInterrupt:
        pass
    except RuntimeError as error:
        print(error)


def main():
    """Main method of wordcount start script"""
    opts, files = parse_options()

    run_server(opts, files)


main()
