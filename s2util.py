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
import sortedcontainers as sc


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


class ObjectProvider:
    def __init__(self):
        pass

    def next(self):
        raise NotImplementedError('call to abstract method next()')


class SequentialObjectProvider(ObjectProvider):
    def __init__(self, archive_urls):
        super().__init__()
        self.archive_urls = archive_urls
        self.current_archive = None
        self.current_lines = []
        self.current_line_no = 0

    def load_archive(self, url):
        print('Iterating over {} ...'.format(url), file=sys.stderr)
        self.current_archive = url
        archive = req.urlopen(url).read()
        self.current_lines = gzip.decompress(archive).decode('utf-8').split('\n')
        if self.current_lines[-1] == '':
            self.current_lines.pop()
        self.current_line_no = 0

    def next(self):
        if self.current_line_no >= len(self.current_lines):
            if len(self.archive_urls) > 0:
                try:
                    self.load_archive(self.archive_urls[0])
                    self.archive_urls = self.archive_urls[1:]
                except Exception as e:
                    print('Unexpected failure in archive {}: {}'.format(self.current_archive, str(e)),
                          file=sys.stderr)
                    return None, None, None
            else:
                return None, None, None
        if self.current_line_no < len(self.current_lines):
            line = self.current_lines[self.current_line_no]
            self.current_line_no += 1
            return json.loads(line), self.current_archive, self.current_line_no-1
        else:
            # End of archives
            return None, None, None


class IndexObjectProvider(ObjectProvider):
    def __init__(self, index, corpus_url):
        super().__init__()
        self.index = index
        self.corpus_url = corpus_url
        if self.corpus_url[-1] != '/':
            self.corpus_url += '/'
        self.span = list(self.index.span().keys())
        self.object_offsets = []
        self.lines = []
        self.current_archive = None
        self.current_an = -1

    def load_archive(self, archive_no):
        self.object_offsets = self.index.all_offsets(self.current_an)

        if archive_no < 1000:
            url = self.corpus_url + 's2-corpus-{:03}.gz'.format(archive_no)
        else:
            url = self.corpus_url + 's2-corpus-{:04}.gz'.format(archive_no)
        self.current_archive = url
        print('Picking objects from {} ...'.format(url), file=sys.stderr)

        archive = req.urlopen(url).read()
        self.lines = gzip.decompress(archive).decode('utf-8').split('\n')
        if self.lines[-1] == '':
            self.lines.pop()

    def next(self):
        # Next archive?
        if len(self.object_offsets) == 0:
            if len(self.span) == 0:
                return None, None, None
            self.current_an = self.span[0]
            self.span = self.span[1:]
            self.load_archive(self.current_an)
        # Load object
        current_on = self.object_offsets.pop()
        return json.loads(self.lines[current_on]), self.current_archive, current_on


class ObjectIterator:
    def __init__(self, provider):
        self.provider = provider
        pass

    def process(self, obj, archive_url, object_no):
        raise NotImplementedError("Abstract method process() called")

    def iterate(self):
        while True:
            obj, ar, on = self.provider.next()
            if obj is not None:
                try:
                    self.process(obj, ar, on)
                except Exception as e:
                    print('Failed to process object {}@{}'.format(ar, on),
                          file=sys.stderr)
            else:
                break


class SearchableIndex:
    def __init__(self, filename):
        # Read whole file
        file = open(filename, 'rb')
        self.data = file.read()
        file.close()
        # Check size
        assert len(self.data) % 28 == 0
        self.count = len(self.data) // 28

    def size(self):
        return len(self.data) // 28

    def span(self):
        archs = sc.SortedDict()
        for current in range(self.size()):
            # Fetch location
            location = bytes(self.data[(current * 28 + 20):(current * 28 + 28)])
            an, _ = struct.unpack("!II", location)
            # Add to span
            if an in archs.keys():
                archs[an] += 1
            else:
                archs[an] = 1
        return archs

    def all_offsets(self, archive_no):
        offsets = []
        for current in range(self.size()):
            # Fetch location
            location = bytes(self.data[(current * 28 + 20):(current * 28 + 28)])
            an, on = struct.unpack("!II", location)
            if an == archive_no:
                offsets.append(on)
        return offsets

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
