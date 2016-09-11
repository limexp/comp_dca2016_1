
Data preparation script for 
[CIKM Cup 2016 Track 1: Cross-Device Entity Linking Challenge](https://competitions.codalab.org/competitions/11171)


## Introduction
Input files for a challenge are 3.8 Gb in size and it is not easy and fast to deal with them, so I created a script that do some preprocessing and save all data to hdf5 files (0.7 Gb + hashes indices 0.3 Gb). 

## How to run
Clone this repo:

    git clone https://github.com/limexp/comp_dca2016_1.git
    
By default script uses *input* and *data* folders in parent folder, so create them or change paths in script source.

Copy challenge data files (facts.json, titles.csv, train.csv, urls.csv) to *input* folder.

Run script:

    python 0-preprocess.py 
    
It takes about 10 minutes on my computer to finish.
 
Delete temporary file tempdata.hdf5 in *data* folder.

In *data* folder two main files are created: data.hdf5 and hashes.hdf5.

## Algorithm

To save memory this is a 2-passes script. On the first pass target sizes are calculated and then this information used for memory allocation. 

I started this script development on my laptop, so memory saving was one of priorities. That's why temporary hdf5 file is used to store data during processing. 

### Users

All user hashes are sequentially encoded with int32 starting with 0. They are sorted in the following way:

 * Users from train.csv (rows 0 - 240731), grouped by same userid, ordered by hash inside the group, groups are ordered by size descending.
 * All other users (from facts.json) ordered by hashes.

Groups are labeled with the first user id (the lowest one).
 
With such encoding it's very easy to know if two user ids are from the same group - just check if their indices are close. 

Groups are saved in pandas Dataframe **groups** in file data.hdf5:

 * Datafame *index* - int32 new encoded user id
 * Column *gid* - group id
 * Column *istrain* - 1 if present in train and 0 otherwise

Hashes dictionary is saved in pandas DataFrames **userhashes** and **touserhash** in hashes.hdf5. With 2-getuser.py one can check them.

```
  python 2-getuser.py 508056d397769af3033212b965ec6f12 \
  22f4792041c02683994c17fbfd448015 03e403781aedce98a0ff73aac0a8b674 1 2 
  08:31:18 start
  b'508056d397769af3033212b965ec6f12' => 44044
  b'22f4792041c02683994c17fbfd448015' => 11127
  b'03e403781aedce98a0ff73aac0a8b674' => 11122
  1 => b'0525deb3bdd7ccf3e8d66f313abbfd97'
  2 => b'08b8294eaac6a2ad5eed9d2727cf161d'
  08:31:18 end
```

### Facts

All facts are saved in pandas DataFrame **facts** in file data.hdf5:

 * Column *uid* - int32 encoded user id
 * Column *fid* - fid from facts.json
 * Column *ts* - int64 timestamp from facts.json

### Urls and Titles

All hashes from urls and titles are sequentially encoded with int32 starting with 1. The zero-index is reserved for NA in merges and so on.

Urls are splited into domain, query and path.

Hashes dictionary is saved in pandas DataFrames **hashes** and **tohash** in hashes.hdf5. With 1-gethash.py one can check them.

```
  python 1-gethash.py 1 5162fc6a223f248d 2  64036905ebb32b6e \
  415607cbd9de5983
  08:29:23 start
  5162fc6a223f248d => 2
  64036905ebb32b6e => 73
  415607cbd9de5983 => 28
  1 => ed95a9a5be30e4c8
  2 => 5162fc6a223f248d
  08:29:37 end

```


### Domains

All domains are extracted from urls.csv and saved in pandas Series **domains** in file data.hdf5:
  * Series *index* - fact id (fid)
  * value - domain hash int32 index 

### Paths

All paths are extracted from urls.csv and saved in pandas DataFrame **paths** in file data.hdf5:

 * Column *fid* - fact id
 * Column *pos* - 0-based position in path
 * Column *wid* - hash index (int32)

If there is no path - no row for that fact id. 
 
### Queries

All queries are extracted from urls.csv and saved in pandas Series **queries** in file data.hdf5:
  * Series *index* - fact id (fid)
  * value - query hash int32 index 

If there is no query - no row for that fact id. 

### Titles 

All titles are extracted from titles.csv and saved in pandas DataFrame **titles** in file data.hdf5:

 * Column *fid* - fact id
 * Column *pos* - 0-based position in title
 * Column *wid* - hash index (int32)
 
## Disclaimer
I use this script myself. But there is no warranty that there are no errors.   
