# get_tfidf.py
# create a tfidf cooefficient for each row in the lyrics database

import sqlite3
from IPython.core.debugger import Tracer; trace = Tracer()
from math import log
import sys

class TFIDFCounter(object):
    def __init__(self, dbh):
        self.dbh = dbh
        self.words = {}
        self.totaldocs = 0
        self._init_totals_for_words()

    def _init_totals_for_words(self):
        dbg("Initializing document frequency totals...")
        c = self.dbh.cursor()
        for row in c.execute("SELECT word, COUNT(track_id) FROM lyrics GROUP BY word"):
            self.words[row[0]] = row[1]
        c.execute("SELECT COUNT(DISTINCT(track_id)) from lyrics")
        self.totaldocs = float(c.fetchone()[0])

    def calc_tfidf(self, track_id):

        tfidf = []
        words_in_song = []
        total_words = 0

        # get all words in a given song
        c = self.dbh.cursor()
        for row in c.execute("SELECT word, count FROM lyrics WHERE track_id = ?", (track_id,)):
            words_in_song.append(row)
            total_words += row[1]
        total_words = float(total_words)
        # calculate tfidf
        for word,count in words_in_song:
            tf = count / total_words
            idf = log ( self.totaldocs / self.words[word] )
            tfidf.append((word,tf * idf))

        return tfidf

def init_output_db(dbh):
    # create the tfidf table
    c = dbh.cursor()
    c.execute('''CREATE TABLE tfidf
              (track_id text,
               word text,
               tfidf real)''')
    dbh.commit()


def main(input_db="mxm_dataset.db", output_db="mxm_tfidf.db"):
    # load the databases
    mxm = sqlite3.connect(input_db)
    out = sqlite3.connect(output_db)

    dbg("Creating output tables in {}".format(output_db))
    init_output_db(out) 
    tdc = TFIDFCounter(mxm)

    # calculate the tfidf for all documents
    dbg("Begin calculating TFIDF...")
    compl = 0
    tenpcnt = int(tdc.totaldocs / 100)
    c = mxm.cursor()
    d = out.cursor()
    query = "INSERT INTO tfidf VALUES ( ?, ?, ? )"
    for row in c.execute ("SELECT DISTINCT(track_id) FROM lyrics"):
        track_id = row[0]
        for word, tfidf in tdc.calc_tfidf(track_id):
            d.execute(query, (track_id, word, tfidf))
        compl += 1
        if (compl % tenpcnt == 0):
            print ("{:.2f}% complete".format(compl / tdc.totaldocs * 100))
    out.commit()
    print ("Processed {} tracks".format(compl))

def dbg(message):
    sys.stderr.write(message + "\n")

if __name__ == "__main__":
    main(input_db="minidb.db")
