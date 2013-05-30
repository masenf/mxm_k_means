from mpi4py import MPI
from time import sleep
from signal import signal, alarm, SIGALRM
import sys
from progress import ProgressUpdater
from random import random, randint

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

class MsgReceiver(object):

    def __init__(self):
        self.req1 = None
        self.req2 = None
        self.st = MPI.Status()

    def get_handler(self):
        def handler(signo, st):
            handle()
        def handle(rec=0):
            if self.req1 is None:
                self.req1 = comm.irecv(dest=MPI.ANY_SOURCE, tag=90)
            if self.req2 is None:
                self.req2 = comm.irecv(dest=MPI.ANY_SOURCE, tag=91)
            res = MPI.Request.testany((self.req1,self.req2), status=self.st)
            if res[1]:
                print(res)
                print("Received: {} from {} with tag {}".format(res[2], self.st.Get_source(), self.st.Get_tag()))
                if self.st.Get_tag() == 90:
                    self.req1 = None
                else:
                    self.req2 = None
            elif rec > 1:
                alarm(1)
                return
            else:
                sleep(0.06)
                rec += 1

            # try to call again recursively
            handle(rec)
        return handler

p = ProgressUpdater(comm, rank, size)

if rank == 0:
    handler = p._get_handler()
    signal(SIGALRM,handler)
    handler(None,None)
    p.update_text("I'm root bitch!")
    while 1:
        sys.stdin.readline()
else:
    p.update_text("Hello from #{}".format(rank))
    prog = randint(10,30)
    for i in xrange(prog):
        p.update_progress(i,prog-1)
        sleep(random() * 2)

#
#
#if rank == 0:
#	data = [(i+1)**2 for i in range(2*size)]
#	print(data)
#else:
#	data = None
#data = comm.scatter(data, root=0)
#assert data == (rank+1)**2
#
#data *= -1
#print("my rank={}/{}".format(rank,size))
#
#data = comm.gather(data, root=0)
#if rank == 0:
#	print(data)
#	for i in range(size*2):
#		assert data[i] == -(i+1)**2
#else:
#	assert data is None
