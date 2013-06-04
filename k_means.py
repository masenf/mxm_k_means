# k_means.py
# calculate k-means over the lyric database
# using tfidf scaling + cosine similarity

import sys
import sqlite3
from math import sqrt
import numpy as np

trace = None
try:
    from IPython.core.debugger import Tracer; trace = Tracer()
except ImportError:
    def disabled(): dbg("Tracepoint disabled -- IPython not found")
    trace = disabled

MXM_DB = "mxm_dataset.db"
MXM_TFIDF = "mxm_tfidf_small.db"

tfidf = None
num_means = 6
total_docs = 0
modpct = 1

centroids = [ ]

def bow_av_merge (src, dest, cursz):
    for key,val in src.iteritems():
        if key not in dest:
            dest[key] = val
        else:
            dest[key] = ((dest[key] * cursz) + val) / (cursz + 1)

def bow_dotproduct (t1, t2):
    common_keys = set(t1.keys()).intersection(t2)
    #return reduce(lambda dp, key: dp + t1[key]*t2[key], common_keys, 0)
    v1 = np.array([t1[k] for k in common_keys])
    v2 = np.array([t2[k] for k in common_keys])
    return np.sum(v1*v2)

def bow_magnitude (t):
    """ compute the euiclidian length of the vector """
    v = np.array(t.values())
    return np.sqrt(np.sum(np.square(v)))
    #return sqrt(reduce(lambda acc,v: acc + v ** 2, t.values(),0))

def cosine (t1, t2):

    result = bow_dotproduct(t1,t2) / (bow_magnitude(t1) * bow_magnitude(t2))
#    if t1 == t2:
#        trace()
    return result

def get_vectory (track_id):
    c = tfidf.cursor()
    c.execute("SELECT word, tfidf FROM tfidf WHERE track_id = ?", (track_id,))
    return dict(c.fetchall())

def make_sense_of_cluster(cluster):
    # fetch the tfidf scores for every song in the cluster to determine
    # the top 30 (hopefully) word-genre defining tokens

    c = tfidf.cursor()
    query = """SELECT word, SUM(tfidf) AS s FROM tfidf 
               WHERE track_id IN ( '{}' ) 
               GROUP BY word ORDER BY s DESC LIMIT 30""".format("','".join(cluster))
    c.execute(query)
    return c.fetchall()

def update_clustering(npass, nproc, ndocs, last_track, last_cluster):
    # move up two lines and spit out the pass number, and percentage done
    update_text("Pass# {:3}       {}  --->  {}\033[K".format(npass, last_track, last_cluster))
    update_progress(nproc,ndocs)

def update_text(message):
    # move up two lines and spit out the pass number, and percentage done
    sys.stderr.write("\033[2A\r{}\033[K\033[2B\r".format(message))

def update_progress(comp, total, length=40):
    sys.stderr.write("\033[1A\r")
    perc_done = comp / float(total)
    progress = "#" * int((comp / float(total)) * length)
    sys.stderr.write("{:6.2f}%  [{:{length}}] {}/{}\033[K\n".format(perc_done * 100, 
                                                progress, comp, total, 
                                                length=length))
    sys.stderr.write("\033[1B\r")

def init():
    global tfidf, centroids, total_docs, modpct
    tfidf = sqlite3.connect(MXM_TFIDF)

    dbg("Beginning k-means clustering with K={}".format(num_means))

    dbg("Initializing values...")

    c = tfidf.cursor()
    c.execute("SELECT COUNT(DISTINCT(track_id)) FROM tfidf")
    total_docs = c.fetchone()[0]
    modpct = total_docs / 2000
    if (modpct < 1): modpct = 1

    update_text("Picking random centroids K={}".format(num_means))
    # find some random centroids
    for t_id in c.execute("SELECT DISTINCT(track_id) FROM tfidf ORDER BY RANDOM() LIMIT {}".format(num_means)): 
        centroids.append(get_vectory(t_id[0]))
        update_progress(len(centroids),num_means) 

def main():
    global centroids
    npass = 0
    clusters = None
    cluster_counts = [0] * num_means
    old_cluster_counts = None

    c = tfidf.cursor()

    update_text("Caching tracks, n={}".format(total_docs))
    c.execute("SELECT DISTINCT(track_id) FROM tfidf")
    all_tracks = c.fetchall()

    update_text("Set up is complete, starting k-means. modpct = {}".format(modpct))

    # keep going until we converge
    while tuple(cluster_counts) != old_cluster_counts:
        print("{} != {}\n\n".format(cluster_counts, old_cluster_counts))
        update_progress(0,total_docs)
        nprocs = 0
        old_cluster_counts  = tuple(cluster_counts)
        cluster_counts = [0] * num_means
        clusters = [[] for x in xrange(0,num_means)]
        new_centroids = [{} for x in xrange(0,num_means)]

        # for each track, find nearest cluster
        for row in all_tracks:
            t_id = row[0]
            trackVec = get_vectory(t_id)
            similarities = [float('inf')] * num_means
            for i,centr  in enumerate(centroids):
                similarities[i] = cosine (trackVec, centr)
            mindex = reduce(lambda x, y: x if x[1] > y[1] else y, enumerate(similarities))[0]
            clusters[mindex].append(t_id)
#            bow_av_merge(trackVec, new_centroids[mindex], cluster_counts[mindex])
            cluster_counts[mindex] += 1
            nprocs += 1
            if nprocs % modpct == 0:
                update_clustering(npass, nprocs, total_docs, t_id, mindex)

#        centroids = new_centroids
        update_text("Recomputing centroids...{}".format(repr(cluster_counts)))
        
        for cluster_id in range(num_means):
            for t_id in clusters[cluster_id]:
                query = """SELECT word, AVG(tfidf) FROM tfidf 
                           WHERE track_id IN ( '{}' ) 
                           GROUP BY word""".format("','".join(clusters[cluster_id]))
                c.execute(query)
                centroids[cluster_id] = dict(c.fetchall())
                update_progress(cluster_id,num_means)

#            if centroids[cluster_id] != new_centroids[cluster_id]:
#                trace()

        npass += 1

    return clusters, cluster_counts

def dump_centroids(centroids, cluster_counts):
    for i, centroid in enumerate(centroids):
        print("Cluster {} ({})\n{}".format(i, cluster_counts[i], "="*31))
        words = centroid.items()
        words.sort(key=lambda x: x[1], reverse=True)
        for word, tfidf in words[:30]:
            print("{:20} {:10f}".format(word.encode('utf-8'), tfidf))
        print

def dump_clusters(clusters, cluster_counts):
    for i, cluster in enumerate(clusters):
        print("Cluster {} ({})\n{}".format(i, cluster_counts[i], "="*31))
        for t_id in cluster:
            print(t_id)
        print

def dbg(message):
    sys.stderr.write(message + "\n")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            num_means = int(sys.argv[1])
        except ValueError:
            dbg("Error, argument 1 must be an integer")
    init()
    clusters, cluster_counts = main()
    dbg("Process complete, cluster counts={}".format(cluster_counts))
    dump_centroids()
    dump_clusters(clusters)
