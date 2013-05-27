#!/usr/bin/env python
# track_lookup.py
# quickly lookup the metadata + bag of words for a specific track

import sys
import sqlite3

METADATA_DB = "track_metadata.db"
MXM_DB = "mxm_dataset.db"

md_fields = [ 'artist_name', 'title', 'release', 'year', 'duration' ]
field_len = max(map(lambda x: len(x), md_fields))

def get_metadata(track_id, mdd):
    query = "SELECT " + ", ".join(md_fields) + " FROM songs WHERE track_id = ?"
    c = mdd.cursor()
    c.execute(query, (track_id,))
    if (c.rowcount == 0):
        return []
    else:
        md = c.fetchone()
        return zip(md_fields,md)

def get_mxmdata(track_id, mxm):
    # get mxm data
    query = "SELECT count, word FROM lyrics WHERE track_id = ? ORDER BY count DESC"
    c = mxm.cursor()
    c.execute(query, (track_id,))
    return c.fetchall()


def main(track_id):

    mdd = sqlite3.connect(METADATA_DB)
    mxm = sqlite3.connect(MXM_DB)

    metadata = get_metadata(track_id, mdd)
    if metadata:
        for field, value in metadata:
            print("{:{field_len}}   {}".format(field, value, field_len=field_len))
    else:
        sys.stderr.write("track_id <{}> not found in metadata database\n".format(track_id))

    print
    mxmdata = get_mxmdata(track_id, mxm)
    if mxmdata:
        for entry in mxmdata:
            print("{:5}   {}".format(*entry))
    else:
        sys.stderr.write("track_id <{}> not found in mxm database\n".format(track_id))

    mxm.close()
    mdd.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        sys.stderr.write("USAGE: {} track_id\n".format(sys.argv[0]))
        sys.exit(64)
