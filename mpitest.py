from mpi4py import MPI
from time import sleep
from threading import Timer
import sys
from progress import ProgressManager
from random import random, randint

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

class MsgReceiver(object):

    def __init__(self):
        self.req1 = None
        self.req2 = None
        self.st = MPI.Status()

    def start_handler(self):
        def handle():
            attempts = 0
            while attempts < 1:
                if self.req1 is None:
                    self.req1 = comm.irecv(dest=MPI.ANY_SOURCE, tag=90)
                if self.req2 is None:
                    self.req2 = comm.irecv(dest=MPI.ANY_SOURCE, tag=91)
                res = MPI.Request.testany((self.req1,self.req2), status=self.st)
                if res[1]:
                    print("Received: {} from {} with tag {}".format(res[2], self.st.Get_source(), self.st.Get_tag()))
                    if self.st.Get_tag() == 90:
                        self.req1 = None
                    else:
                        self.req2 = None
                else:
                    sleep(0.06)
                    attempts += 1

            Timer(0.3, handle).start()
        handle()

def testMsgReceiver():

    if rank == 0:
        m = MsgReceiver()
        m.start_handler()
    else:
        print("Sending messages!")
        comm.send("Hello from #{}".format(rank), dest=0, tag=90)
        prog = randint(10,30)
        for i in xrange(prog):
            comm.send((i,prog-1), dest=0, tag=91)
            sleep(random())


def testProgressManager():
    p = ProgressManager(comm, rank, size)
    p.start_handling()

    if rank == 0:
        p.update_text("I'm root bitch!")
        for i in xrange(0,35):
            p.update_progress(i,34)
            sleep(random())
        p.update_text("Done")
    else:
        p.update_text("Hello from #{}".format(rank))
        prog = randint(10,30)
        for i in xrange(prog):
            p.update_progress(i,prog-1)
            sleep(random())
        p.update_text("Done")
        p.client_send()

    p.running = False
if __name__ == '__main__':
    testProgressManager()

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
