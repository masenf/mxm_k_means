# get_tdidf.py
# create a tdidf cooefficient for each row in the lyrics database

import sqlite3

class TDIDFCounter(object):
    def __init__(self, dbh):
        self.dbh = dbh
        self.words = {}
    def _init_totals_for_words(self):
        c = self.dbh.cursor()
        for row in c.execute("SELECT word, COUNT(track_id) FROM lyrics GROUP BY word"):
            self.words[row[0]] = row[1]
    def _get_total_words_for_track(self, track_id):
        c = self.dbh.cursor()
        c.execute("SELECT SUM(count) FROM lyrics WHERE track_id = ?", (track_id,))
    def calc_tdidf(self, track_id, word):
        pass

def init_output_db(dbh):
    # create the tdidf table
    c = dbh.cursor()
    c.execute('''CREATE TABLE tdidf
              (track_id text,
               word text,
               tdidf real)''')
    dbh.commit()


def main(input_db="mxm_dataset.db", output_db="mxm_tdidf.db"):
    # load the databases
    mxm = sqlite3.connect(input_db)
    out = sqlite3.connect(output_db)

    init_output_db(out) 



if __name__ == "__main__":
    main()
