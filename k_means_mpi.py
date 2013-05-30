# k_means.py
# calculate k-means over the lyric database
# using tfidf scaling + cosine similarity

import sys
import sqlite3
from math import sqrt, ceil
from time import sleep
from random import random
from mpi4py import MPI
from k_means import cosine

#trace = None
#try:
#    from IPython.core.debugger import Tracer; trace = Tracer()
#except ImportError:
#    def disabled(): dbg("Tracepoint disabled -- IPython not found")
#    trace = disabled

MXM_DB = "mxm_dataset.db"
MXM_TFIDF = "mxm_tfidf_small.db"

tfidf = None
num_means = 6
total_docs = 0
modpct = 1

centroids = [ ]
track_cache = [ ]

myrank = None
size = None
comm = None

def build_cache(tracks):
    c = tfidf.cursor()
    track_table = " ( '{}' ) ".format("','".join(tracks))
    c.execute("SELECT COUNT(word) FROM tfidf WHERE track_id IN {}".format(track_table))
    nrecs = c.fetchone()[0]
    c.execute("SELECT track_id, word, tfidf FROM tfidf WHERE track_id IN {}".format(track_table))
    update_text("Caching {} words in {} songs".format(nrecs, len(tracks)))
    update_progress(0,nrecs)
    result = {}
    for i in xrange(nrecs):
        row = c.fetchone()
        if row[0] not in result:
            result[row[0]] = {}
        result[row[0]][row[1]] = row[2]
        if i % modpct == 0:
            update_progress(i+1, nrecs)

    return result

def get_vector (track_id):
    c = tfidf.cursor()
    c.execute("SELECT word, tfidf FROM tfidf WHERE track_id = ?", (track_id,))
    return dict(c.fetchall())

def cluster_mean(tracks, callback=None):
    sofar = 0
    ntracks = len(tracks)
    cluster_totals = {}
    for t_id in tracks:
        vec = track_cache[t_id]
        for word, tfidf in vec.items():
            if word not in cluster_totals:
                cluster_totals[word] = 0
            cluster_totals[word] += tfidf
        if callback is not None:
            sofar += 1
            if sofar % modpct == 0:
                callback(sofar)
                sofar = 0
    if cluster_totals:
        for word in cluster_totals:
            cluster_totals[word] /= ntracks
    return cluster_totals

def update_clustering(npass, nproc, ndocs, last_track, last_cluster):
    update_text("Pass# {:3}       {}  --->  {}".format(npass, last_track, last_cluster))
    update_progress(nproc,ndocs)

def update_text(message):
    # move up two lines and spit out the pass number, and percentage done
    sys.stderr.write("\033[{lines}A\r{}\033[K\033[{lines}B\r".format(message, lines=str((size-myrank)*2)))

def update_progress(comp, total, length=40):
    perc_done = comp / float(total)
    progress = "#" * int((comp / float(total)) * length)
    sys.stderr.write("\033[{lines}A\r{:6.2f}%  [{:{length}}] {}/{}\033[K\n\033[{lines}B\r".format(perc_done * 100, 
                                                progress, comp, total,
                                                length=length,
                                                lines=str(((size-myrank)*2)-1)))

def init():
    global tfidf, centroids, total_docs, modpct, track_cache
    tfidf = sqlite3.connect(MXM_TFIDF)

    rcentroids = []

    if myrank == 0:
        update_text("Counting tracks...")
        update_progress(0,1)

        c = tfidf.cursor()
        c.execute("SELECT COUNT(DISTINCT(track_id)) FROM tfidf")
        total_docs = c.fetchone()[0]
        modpct = total_docs / 2000
        if (modpct < 1): modpct = 1

        update_text("Picking random centroids K={}".format(num_means))
        update_progress(1,2)

        # find some random centroids
        for t_id in c.execute("SELECT DISTINCT(track_id) FROM tfidf ORDER BY RANDOM() LIMIT {}".format(num_means)): 
            rcentroids.append(get_vector(t_id[0]))
            update_progress(len(rcentroids),num_means) 

        update_text("Transmitting initial values".format(num_means))
        update_progress(2,2)
    else:
        update_text("Waiting for initial centroids...")
        update_progress(0,1)
    total_docs = comm.bcast(total_docs, 0)
    modpct = comm.bcast(modpct, 0)
    centroids = comm.bcast(rcentroids, root=0)
    update_progress(1,1)

    if myrank == 0:
        c = tfidf.cursor()
        c.execute("SELECT DISTINCT(track_id) FROM tfidf")
        chunksz = int(ceil(total_docs / float(size)))
        update_text("Caching track_ids, chunksz={}".format(chunksz))
        update_progress(0,size)
        alltracks = [x[0] for x in c.fetchmany(chunksz)]
        for r in xrange(1,size):
            chunk = [x[0] for x in c.fetchmany(chunksz)]
            comm.send(chunk,  dest=r, tag=1)
            update_progress(r+1,size)
    else:
        update_text("Waiting for tracks...")
        alltracks = comm.recv(source=0, tag=1)
    track_cache = build_cache(alltracks)

def main():
    global centroids
    npass = 0
    clusters = None
    cluster_counts = [0] * num_means
    old_cluster_counts = None
    ntracks = len(track_cache)

    update_text("Set up is complete, starting k-means. modpct = {} ntracks={}".format(modpct,ntracks))

    # keep going until we converge
    while tuple(cluster_counts) != old_cluster_counts:
        update_progress(0,ntracks)
        nprocs = 0
        old_cluster_counts  = tuple(cluster_counts)
        cluster_counts = [0] * num_means
        clusters = [[] for x in xrange(0,num_means)]
        new_centroids = [{} for x in xrange(0,num_means)]

        # for each track, find nearest cluster
        for t_id, vec in track_cache.iteritems():
            similarities = [float('inf')] * num_means
            for i,centr  in enumerate(centroids):
                similarities[i] = cosine (vec, centr)
            mindex = reduce(lambda x, y: x if x[1] > y[1] else y, enumerate(similarities))[0]
            clusters[mindex].append(t_id)
#            bow_av_merge(trackVec, new_centroids[mindex], cluster_counts[mindex])
            cluster_counts[mindex] += 1
            nprocs += 1
            if nprocs % modpct == 0:
                update_clustering(npass, nprocs, ntracks, t_id, mindex)

#        centroids = new_centroids
        update_text("Recomputing centroids...{}".format(repr(cluster_counts)))
        
        nprocs = [0]
        def cb(sofar):
            nprocs[0] += sofar     # i <3 lexical scoping
            update_progress(nprocs[0], ntracks)
        for cluster_id in range(num_means):
            centroids[cluster_id] = cluster_mean(clusters[cluster_id], cb)

        npass += 1

    return clusters, cluster_counts

def init_mpi():
    global myrank, size, comm

    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    myrank = comm.Get_rank()

    sys.stderr.write("\n\n")

    comm.Barrier()

    update_text("{} checking in!".format(str(myrank)))
    update_progress(0,1)

if __name__ == "__main__":
    init_mpi()
    init()
    main()
