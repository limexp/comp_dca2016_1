#!/usr/bin/env python
# -*- coding: utf-8 -*-
# User hashes resolver of data preparation script for
# "CIKM Cup 2016 Track 1: Cross-Device Entity Linking Challenge"
# https://competitions.codalab.org/competitions/11171
#
# Copyright (c) 2016 Boris Kostenko
# https://github.com/limexp/comp_dca2016_1/

from optparse import OptionParser
import pandas as pd
from datetime import datetime

datadir = '../data/'

def parse_options():
    usage = "usage: %prog [indices] [hashes]"
    parser = OptionParser(usage)
    (options, args) = parser.parse_args()

    hashes = []
    indices = []

    for v in args:
        if len(v) == 32:
            hashes.append(v.encode('ascii'))
        else:
            indices.append(int(v))

    if not hashes and not indices:
        raise Exception('input', 'hashes or indices are needed')

    return options, hashes, indices

if __name__ == '__main__':

    options, hashes, indices = parse_options()

    print(datetime.now().strftime("%H:%M:%S start"))

    h5store = pd.HDFStore(datadir + "hashes.hdf5")

    if hashes:
        hdb = h5store['userhashes']
        for v in hashes:
            print('%s => %d' % (v, hdb[v]))
        hdb = None

    if indices:
        hdb = h5store['touserhash']
        for v in indices:
            print('%d => %s' % (v, hdb[v]))
        hdb = None

    h5store.close()

    print(datetime.now().strftime("%H:%M:%S end"))
