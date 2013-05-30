import os
from time import sleep
from signal import signal, alarm, SIGALRM
from mpi4py import MPI
import sys

BORDER_H = "-"
BORDER_V = "|"
BORDER_T = "+"
ROWS = 5

MESSAGE_TAG = 98
PROGRESS_TAG = 99

SEC_CHANCE = 0.06

class ProgressUpdater(object):
    """ 256 threads tryin to blast updates gets out of hand! """

    def __init__(self, comm, myrank, size, maxwidth=40):
        self.comm = comm
        self.myrank = myrank
        self.size = size
        self.maxwidth = maxwidth
        self.messages = ['' for x in xrange(size)]
        self.progress = [[0,1] for x in xrange(size)]

        # these are for node 0
        self.request_m = None
        self.request_p = None
        self.status = None
        if myrank == 0:
            #self.rows, self.columns = os.popen('stty size', 'r').read().split()
            self.rows = 61
            self.columns = 105

    def _get_handler(self):
        def handler(signo, st):
            handle()
            self._redraw()
        def handle(rec=0):
            # get a new request if we're not already waiting for one
            if self.request_m is None:
                self.request_m = self.comm.irecv(dest=MPI.ANY_SOURCE, tag=98)
            if self.request_p is None:
                self.request_p = self.comm.irecv(dest=MPI.ANY_SOURCE, tag=99)
            # check the status
            res = MPI.Request.testany((self.request_m, self.request_p), status=self.status)
            if res[1]:
                if res[0] == 0:
                    self.messages[self.status.Get_source()] = res[2]
                    self.request_m = None
                elif res[0] == 1:
                    self.progress[self.status.Get_source()] = res[2]
                    self.request_p = None
            elif rec > 1:
                alarm(1)
                return
            else:
                sleep(0.06)
                rec += 1

            # try to call again recursively
            handle(rec)
        return handler

    def _redraw(self):
        try:
            self.rows, self.columns = os.popen('stty size', 'r').read().split()
        except ValueError:
            pass

        pcols = self.columns / self.maxwidth
        prows = self.rows / ROWS

        def _blit_status(i, j):
            rank = i*prows + j
            sys.stderr.write("\033[{};{}H".format(i*ROWS,j*self.maxwidth))
            sys.stderr.write("{c}{h:-^{len}}".format(c=BORDER_T,h=BORDER_H,len=self.maxwidth-1))
            sys.stderr.write("\033[{};{}H".format(i*ROWS+1,j*self.maxwidth))
            sys.stderr.write("{v}".format(v=BORDER_V))
            sys.stderr.write("\033[{};{}H".format(i*ROWS+2,j*self.maxwidth))
            sys.stderr.write("{v}{message:^{len}}".format(message=self.messages[rank],v=BORDER_V,len=self.maxwidth-1))
            sys.stderr.write("\033[{};{}H".format(i*ROWS+3,j*self.maxwidth))
            sys.stderr.write("{v}{progress:^{len}}".format(progress=self._get_progress_string(*self.progress[rank]),
                                            len=self.maxwidth-1,
                                            v=BORDER_V))
            sys.stderr.write("\033[{};{}H".format(i*ROWS+4,j*self.maxwidth))
            sys.stderr.write("{v}".format(v=BORDER_V))

        sys.stderr.write("\033[2J")     # clear the screen
        for i in xrange(prows):
            for j in xrange(pcols):
                rank = i*prows + j
                if rank > self.size - 1:
                    break
                _blit_status(i,j)

    def _get_progress_string(self, comp, total):
        length = self.maxwidth - 30
        perc_done = comp / float(total)
        progress = "#" * int((comp / float(total)) * length)
        output = "{:6.2f}%  [{:{length}}] {}/{}".format(perc_done * 100, 
                                                    progress, comp, total,
                                                    length=length)
        return output

        

    def update_text(self, message):
        """ format a message for printing and send it to the master node """
        # move up two lines and spit out the pass number, and percentage done
        if self.myrank > 0:
            self.comm.isend(message, dest=0, tag=98)
        else:
            self.messages[0] = message

    def update_progress(self, comp, total):
        if self.myrank > 0:
            self.comm.isend((comp,total), dest=0, tag=99)
        else:
            self.progress[0] = (comp, total)
