#
# Copyright (c) 2021 Vasile Vilvoiu <vasi@vilvoiu.ro>
#
# This is free software; you can redistribute it and/or modify
# it under the terms of the MIT license. See LICENSE for details.
#

import re
import urllib.request as req
import gzip
import json
import sys
import struct


def fetch_archive_urls(corpus_url, regex):
    archives = []
    if corpus_url[-1] != '/':
        corpus_url += '/'
    manifest = req.urlopen(corpus_url + "manifest.txt")
    for line in manifest:
        arfn = line.decode('utf-8').strip()
        if re.search(regex, arfn):
            archives.append(corpus_url + arfn)

    return archives


class ObjectIterator:
    def __init__(self, archive_urls):
        self.archive_urls = archive_urls

    def process(self, obj, archive_url, object_no):
        raise NotImplementedError("Abstract method process() called")

    def iterate(self):
        for arfn in self.archive_urls:
            try:
                print('Iterating over {} ...'.format(arfn), file=sys.stderr)
                archive = req.urlopen(arfn).read()
                lines = gzip.decompress(archive).decode('utf-8').split('\n')
                # For each object
                line_no = 0
                for line in lines:
                    if line.strip() == '':
                        continue
                    try:
                        self.process(json.loads(line), arfn, line_no)
                    except:
                        print('Failed to process object: {}'.format(line), file=sys.stderr)
                    line_no += 1
            except Exception as e:
                print('Unexpected failure in archive {}: {}'.format(arfn, str(e)), file=sys.stderr)


class SearchableIndex:
    def __init__(self, filename):
        # Read whole file
        file = open(filename, 'rb')
        self.data = file.read()
        file.close()
        # Check size
        assert len(self.data) % 28 == 0
        self.count = len(self.data) // 28

    def lookup(self, long_id):
        location = None
        # Binary search
        lower_bound = 0
        upper_bound = self.count - 1
        while True:
            if lower_bound > upper_bound:
                break
            current = (lower_bound + upper_bound) // 2
            key = bytes(self.data[(current*28):(current*28+20)])
            if key == long_id:
                location = bytes(self.data[(current*28+20):(current*28+28)])
                break
            elif key < long_id:
                lower_bound = current+1
            else:
                upper_bound = current-1
        # Parse location
        if location is not None:
            an, on = struct.unpack("!II", location)
            return an, on
        else:
            return None, None
