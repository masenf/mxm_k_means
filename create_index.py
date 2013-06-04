import sqlite3

mxm = sqlite3.connect("mxm_tfidf.db")
c = mxm.cursor()
c.execute("CREATE INDEX idx_track_id ON tfidf (track_id)")
c.execute("CREATE INDEX idx_word ON tfidf (word)")
mxm.commit()
mxm.close()
