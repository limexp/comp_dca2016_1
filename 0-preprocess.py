#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Data preparation script for
# "CIKM Cup 2016 Track 1: Cross-Device Entity Linking Challenge"
# https://competitions.codalab.org/competitions/11171
#
# Copyright (c) 2016 Boris Kostenko
# https://github.com/limexp/comp_dca2016_1/

import numpy as np
import pandas as pd
import json
from tqdm import tqdm
import h5py
from datetime import datetime

inputdir = '../input/'
datadir = '../data/'

def get_pairs(u):
    r = [u]
    if u in train:
        for i in train[u]:
            r += get_pairs(i)
        del train[u]
    return r

print(datetime.now().strftime("%H:%M:%S start"))

# 1. Find ids for users from train
print(datetime.now().strftime("%H:%M:%S processing users from train"))
train = {}
with open(inputdir + 'train.csv') as f_in:
    for line in tqdm(f_in):
        l, h = line.strip().split(',')
        if not h:
            continue
        l = l.encode('ascii')
        h = h.encode('ascii')
        if l in train:
            train[l] += [h]
        else:
            train[l] = [h]

uids = []
for u in tqdm(sorted(train.keys())):
    if u in train:
        p = list(set(get_pairs(u)))
        uids.append([u, p, len(p)])

uids.sort(key=lambda x: x[2], reverse=True)

train = None

user_labels = []
user_groups = []
istrain = []
user_dict = {}

y = None
i = 0
g = 0
for u in uids:
    y = i
    g += 1
    for j in sorted(u[1]):
        user_labels.append(j)
        user_groups.append(y)
        user_dict[j] = i
        istrain.append(1)
        i += 1

print('There are %d users in %d groups' % (i, g))

uids = {}


# 2. Find ids for all other users
print(datetime.now().strftime("%H:%M:%S processing users from facts"))
facts_count = 0
with open(inputdir + 'facts.json') as f_in:
    for line in tqdm(f_in):
        j = json.loads(line.strip())
        u = j.get('uid').encode('ascii')
        if not u in user_dict:
            uids[u] = True
        facts_count += len(j.get('facts'))

for u in sorted(uids.keys()):
    user_labels.append(u)
    user_groups.append(i)
    user_dict[u] = i
    istrain.append(0)
    i += 1

uids = None

# Saving hashes for users
ar = np.array(user_labels)
h5hashes = pd.HDFStore(datadir + "hashes.hdf5", complib='zlib')
h5hashes['userhashes'] = pd.Series(data=np.reshape(np.arange(ar.shape[0]), ar.shape), index=ar)
h5hashes['touserhash'] = pd.Series(ar)
ar = None

print(datetime.now().strftime('%H:%M:%S user hashes saved'))


h5data = pd.HDFStore(datadir + 'data.hdf5', complib='zlib')
h5data['groups'] = pd.DataFrame({ 'gid': pd.Series(user_groups),
                                   'istrain': pd.Series(istrain) })



h5file = h5py.File(datadir + "tempdata.hdf5", "w")

print('Found %d facts' % facts_count)

# 3. Process facts
print(datetime.now().strftime("%H:%M:%S processing facts"))
dset = h5file.create_dataset("facts", (facts_count,3), dtype='int64', compression="gzip")
start = 0
i = 0
facts = []
with open(inputdir + 'facts.json') as f_in:
    for line in tqdm(f_in):
        j = json.loads(line.strip())
        u = j.get('uid').encode('ascii')
        uid = user_dict[u]
        for f in j.get('facts'):
            if f['ts'] > 1.0e+13:
                f['ts'] = f['ts'] / 1000
            facts.append([uid, f['fid'], f['ts']])
            i += 1
            if i % 5000000 == 0:
                print('\nSaving at %d' % i)
                dset[start:i] = np.asarray(facts)
                facts = []
                start = i
            
print('\nLast saving at %d' % i)
dset[start:i] = np.asarray(facts)
facts = None

# convert facts to dataframe
ar = np.array(dset)
df = pd.DataFrame({'uid': ar[:, 0],
                   'fid': ar[:, 1],
                   'ts': ar[:, 2]})
df['uid'] = df['uid'].values.astype(np.int32)
df['fid'] = df['fid'].values.astype(np.int32)
h5data['facts'] = df
df = None
ar = None


######################################

hashes = {}
next_hash = 0


def get_hash(h):
    global next_hash
    if not h in hashes:
        hashes[h] = next_hash
        next_hash += 1
    return hashes[h]


# fake first hash!
get_hash('')

# counters
n_queries = 0
n_domains = 0
n_paths = 0
n_titles = 0

# 4. First pass over urls
print(datetime.now().strftime("%H:%M:%S pass 1 over urls"))
with open(inputdir + 'urls.csv') as f_in:
    for line in tqdm(f_in):
        fid, url = line.strip().split(',')
        m = url.split('?')
        if len(m) > 1:
            n_queries += 1
            get_hash(m[1])

        m = m[0].split('/')
        n_domains += 1
        get_hash(m[0])

        for j in range(1, len(m)):
            n_paths += 1
            get_hash(m[j])
print('Totals:')
print('%d domains' % n_domains)
print('%d queries' % n_queries)
print('%d paths' % n_paths)

# 5. First pass over titles
print(datetime.now().strftime("%H:%M:%S pass 1 over titles"))
with open(inputdir + 'titles.csv') as f_in:
    for line in tqdm(f_in):
        fid, title = line.strip().split(',')
        m = title.split(' ')
        for j in m:
            n_titles += 1
            get_hash(j)
print('Totals:')
print('%d titles' % n_titles)
# we have one fake hash
print('%d hashes' % (len(hashes) - 1))

# saving hashes for debugging and cross checking
h5hashes['hashes'] = pd.Series(hashes)

inv_hashes = {v: k for k, v in hashes.items()}
h5hashes['tohash'] = pd.Series(inv_hashes)
h5hashes.close()

print(datetime.now().strftime('%H:%M:%S hashes saved'))


# prepare output datasets

dset_domains = h5file.create_dataset("domains", (n_domains, 2), dtype='i', compression="gzip")
dset_paths = h5file.create_dataset("paths", (n_paths, 3), dtype='i', compression="gzip")
dset_queries = h5file.create_dataset("queries", (n_queries, 2), dtype='i', compression="gzip")
dset_titles = h5file.create_dataset("titles", (n_titles, 3), dtype='i', compression="gzip")

titles = []
paths = []
queries = []
domains = []

# domains, paths, queries
start_rows = [0, 0, 0]
next_rows = [0, 0, 0]

# 6. Second pass over urls
print(datetime.now().strftime("%H:%M:%S pass 2 over urls"))
with open(inputdir + 'urls.csv') as f_in:
    for line in tqdm(f_in):
        fid, url = line.strip().split(',')
        fid = int(fid)
        m = url.split('?')
        if len(m) > 1:
            queries.append([fid, get_hash(m[1])])
            next_rows[2] += 1
            if next_rows[2] % 3000000 == 0:
                print('\nSaving queries at %d' % next_rows[2])
                dset_queries[start_rows[2]:next_rows[2]] = queries
                queries = []
                start_rows[2] = next_rows[2]

        m = m[0].split('/')
        domains.append([fid, get_hash(m[0])])
        next_rows[0] += 1
        if next_rows[0] % 3000000 == 0:
            print('\nSaving domains at %d' % next_rows[0])
            dset_domains[start_rows[0]:next_rows[0]] = domains
            domains = []
            start_rows[0] = next_rows[0]

        for j in range(1, len(m)):
            paths.append([fid, j - 1, get_hash(m[j])])
            next_rows[1] += 1
            if next_rows[1] % 3000000 == 0:
                print('\nSaving paths at %d' % next_rows[1])
                dset_paths[start_rows[1]:next_rows[1]] = paths
                paths = []
                start_rows[1] = next_rows[1]

print('\nSaving queries at %d' % next_rows[2])
dset_queries[start_rows[2]:next_rows[2]] = queries
queries = []

print('\nSaving domains at %d' % next_rows[0])
dset_domains[start_rows[0]:next_rows[0]] = domains
domains = []

print('\nSaving paths at %d' % next_rows[1])
dset_paths[start_rows[1]:next_rows[1]] = paths
paths = []

ar = np.array(dset_queries)
h5data['queries'] = pd.Series(data=ar[:, 0], index=ar[:, 1])
dset_queries = None

ar = np.array(dset_domains)
h5data['domains'] = pd.Series(data=ar[:, 0], index=ar[:, 1])
dset_domains = None

ar = np.array(dset_paths)
df = pd.DataFrame({'fid': ar[:, 0],
                   'pos': ar[:, 1],
                   'wid': ar[:, 2]})
h5data['paths'] = df
dset_paths = None
df = None
ar = None




# 7. Second pass over titles
start_row = 0
next_row = 0
titles = []
print(datetime.now().strftime("%H:%M:%S pass 2 over titles"))
with open(inputdir + 'titles.csv') as f_in:
    for line in tqdm(f_in):
        fid, title = line.strip().split(',')
        fid = int(fid)
        m = title.split(' ')
        i = 0
        for j in m:
            get_hash(j)
            titles.append([fid, i, get_hash(j)])
            i += 1
            next_row += 1
            if next_row % 3000000 == 0:
                print('\nSaving titles at %d' % next_row)
                dset_titles[start_row:next_row] = titles
                titles = []
                start_row = next_row

print('\nSaving titles at %d' % next_row)
dset_titles[start_row:next_row] = titles
titles = []

ar = np.array(dset_titles)
df = pd.DataFrame({'fid': ar[:, 0],
                   'pos': ar[:, 1],
                   'wid': ar[:, 2]})
h5data['titles'] = df
dset_titles = None
df = None
ar = None


h5file.close()
h5data.close()

print(datetime.now().strftime("%H:%M:%S end"))

