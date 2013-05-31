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
from progress import ProgressManager

#trace = None
#try:
#    from IPython.core.debugger import Tracer; trace = Tracer()
#except ImportError:
#    def disabled(): dbg("Tracepoint disabled -- IPython not found")
#    trace = disabled

MXM_DB = "mxm_dataset.db"
MXM_TFIDF = "mxm_tfidf.db"

tfidf = None
num_means = 6
total_docs = 0
modpct = 1

centroids = [ ]
track_cache = [ ]

progressmgr = None
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
    update_progress(nrecs,nrecs)

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
def vector_mean(vectors):
    nvecs = len(vectors)
    cluster_totals = {}
    for vec in vectors:
        for word, tfidf in vec.items():
            if word not in cluster_totals:
                cluster_totals[word] = 0
            cluster_totals[word] += tfidf
    if cluster_totals:
        for word in cluster_totals:
            cluster_totals[word] /= nvecs
    return cluster_totals

def update_clustering(npass, nproc, ndocs, last_track, last_cluster):
    update_text("Pass# {:3}       {}  --->  {}".format(npass, last_track, last_cluster))
    update_progress(nproc,ndocs)

def update_text(message):
    progressmgr.update_text(message)

def update_progress(comp, total, length=40):
    progressmgr.update_progress(comp, total)

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

    requests = []
    if myrank == 0:
        c = tfidf.cursor()
        c.execute("SELECT DISTINCT(track_id) FROM tfidf")
        chunksz = int(ceil(total_docs / float(size)))
        update_text("Caching track_ids, chunksz={}".format(chunksz))
        update_progress(0,size)
        alltracks = [x[0] for x in c.fetchmany(chunksz)]
        for r in xrange(1,size):
            chunk = [x[0] for x in c.fetchmany(chunksz)]
            requests.append(comm.isend(chunk, dest=r, tag=1))
            update_progress(r+1,size)
    else:
        update_text("Waiting for tracks...")
        alltracks = comm.recv(source=0, tag=1)
    track_cache = build_cache(alltracks)

    if myrank == 0:
        MPI.Request.waitall(requests)

    comm.Barrier()

def main(centroid_file, cluster_file):
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
        update_text("Recomputing centroids...")
        
        nprocs = [0]
        def cb(sofar):
            nprocs[0] += sofar     # i <3 lexical scoping
            update_progress(nprocs[0], ntracks)
        for cluster_id in range(num_means):
            centroids[cluster_id] = cluster_mean(clusters[cluster_id], cb)

        # reconcile clusters
        comm.Barrier()
        centroid_groups = [[] for x in xrange(0,num_means)]
        if myrank == 0:
            update_text("Reconcile centroids...")
            for i, cd in enumerate(centroids):
                centroid_groups[i].append(cd)
            update_progress(1,size)
            for r in xrange(1,size):
                in_centroids = comm.recv(source=MPI.ANY_SOURCE, tag=68)
                in_cluster_counts = comm.recv(source=MPI.ANY_SOURCE, tag=69)
                for i in xrange(num_means):
                    centroid_groups[i].append(in_centroids[i])
                    cluster_counts[i] += in_cluster_counts[i]
                update_progress(r+1,size)
            for cluster_id in xrange(num_means):
                centroids[cluster_id] = vector_mean(centroid_groups[cluster_id])
        else:
            update_text("Sending centroids to root")
            comm.send(centroids, dest=0, tag=68)
            update_text("Sending cluster_counts to root")
            comm.send(cluster_counts, dest=0, tag=69)
            update_text("[BLOCKED] Waiting for centroids and counts...")
        centroids = comm.bcast(centroids, root=0)
        cluster_counts = comm.bcast(cluster_counts, root=0)

        if myrank == 0:
            update_text("Accumulating clusters")
            for r in xrange(1,size):
                in_clusters = comm.recv(source=MPI.ANY_SOURCE, tag=70)
                for i in xrange(num_means):
                    for t_id in in_clusters[i]:
                        clusters[i].append(t_id)
                update_progress(r+1,size)
        else:
            update_text("Transferring clusters to root")
            comm.send(clusters, dest=0, tag=70)
        if myrank == 0:
            update_text("Writing current state to file")
            dump_clusters(clusters, cluster_counts, cluster_file)
            dump_centroids(cluster_counts, centroid_file)
        npass += 1
        comm.Barrier()

    comm.Barrier()
    progressmgr.running = False

    return clusters, cluster_counts

def init_mpi():
    global myrank, size, comm, progressmgr

    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    myrank = comm.Get_rank()

    progressmgr = ProgressManager(comm, myrank, size)
    progressmgr.start_handling()

    comm.Barrier()

    update_text("{} checking in!".format(str(myrank)))
    update_progress(0,1)

def dump_centroids(cluster_counts, filename):
    with open(filename, 'w') as f:
        for i, centroid in enumerate(centroids):
            f.write("Cluster {} ({})\n{}\n".format(i, cluster_counts[i], "="*31))
            words = centroid.items()
            words.sort(key=lambda x: x[1], reverse=True)
            for word, tfidf in words[:30]:
                f.write("{:20} {:10f}\n".format(word.encode('utf-8'), tfidf))
            f.write("\n")

def dump_clusters(clusters, cluster_counts, filename):
    with open(filename,'w') as f:
        for i, cluster in enumerate(clusters):
            f.write("Cluster {} ({})\n{}\n".format(i, cluster_counts[i], "="*31))
            for t_id in cluster:
                f.write(t_id + "\n")
            f.write("\n")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            num_means = int(sys.argv[1])
        except ValueError:
            dbg("Error, argument 1 must be an integer")
    if len(sys.argv) > 2:
        out_prefix = sys.argv[2]
    else:
        out_prefix = "{}means_output".format(num_means)
    cluster_file = out_prefix + "_clusters"
    centroid_file = out_prefix + "_centroids"

    init_mpi()
    init()
    clusters, cluster_counts = main(centroid_file,cluster_file)
    if myrank == 0:
        print("Process complete, cluster counts={}".format(cluster_counts))
        dump_centroids(cluster_counts, centroid_file)
        dump_clusters(clusters, cluster_counts, cluster_file)
