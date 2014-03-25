# read_tfidf.py
# utilities for working with the tfidf database (generate with get_tfidf.py)

"""
# Sample script to print highest tfidf for each track

import read_tfidf

tdb = read_tfidf.TFIDFDb("mxm_tfidf.db")
all_tracks = tdb.track_ids()
for track_id in all_tracks:
    tfidf_scores = tdb.tf_idf_by_track(track_id)
    max = 0
    max_word = None
    for word, tfidf in tfidf_scores.iteritems():
        if tfidf > max:
            max_word = word
            max = tfidf
    print("Track: {}\tWord: {}\tTfidf: {}".format(track_id, max_word.encode("utf-8"), max))
"""

from math import log
import sqlite3
import sys

class TFIDFDb(object):
    def __init__(self, db_file):
        self.db = sqlite3.connect(db_file)
    def tf_idf_by_track(self, track_id):
        """Return a dictionary of word->tfidf for the given track_id"""
        c = self.db.execute("SELECT word, tfidf FROM tfidf WHERE track_id = ?", (track_id,))
        return dict(c.fetchall())
    def tf_idf_all(self):
        """ Return a dictionary of dictionaries of word->tfidf for all track ids 
            in the database.

            Could be trivially implemented by
                { tid : self.tf_idf_by_track(tid) for tid in self.track_ids() }

            But this way is another way, which is useful with a large dataset
        """
        output = {}
        c = self.db.execute("SELECT track_id, word, tfidf FROM tfidf")

        
        # row-by-row fetching loop
        # useful when you expect the query to return a lot of data
        row = c.fetchone()      # fetch the first row
        while row:
            track_id, word, tfidf = row[0], row[1], row[2]
            if track_id not in output:
                # add empty tfidf dictionary if we're encountering track_id for the first time 
                output[track_id] = {}
            output[track_id][word] = tfidf
            row = c.fetchone()  # fetch the next row
        return output
    def track_ids(self):
        """ Return a list of all track_id in the database. Strip the 1-tuple
            off each bare track_id that is returned """
        c = self.db.execute("SELECT DISTINCT(track_id) FROM tfidf")
        return [row[0] for row in c.fetchall()]
