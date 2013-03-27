#!/usr/bin/env python
from IPython.parallel import Client
rc = Client(); lv = rc.load_balanced_view()

import os
import time
import sys
import curses
from datetime import datetime, timedelta

def main(ss):
    curses.curs_set(0)
    ss.addstr(1,1,"IPYTHON CLUSTER MONITOR")
    ss.hline(2,1,'-',30)
    ss.refresh()
    curses.halfdelay(5)
    
    while True:
        scrupdate(ss)
        try: 
            ss.refresh()
            c = ss.getch()
            if c == ord('q'):
                sys.exit()
            elif c == ord('p'):
                purgemenu(ss)
        except Exception,e:
            raise e

def purgemenu(ss):
    ss.addstr(12,3,"Purge? ([r]esults, [e]verything, [c]ancel)")
    ss.refresh()
    q=True
    while q:
        scrupdate(ss)
        ss.refresh()
        c = ss.getch()
        if c == ord('r'):
            rc.purge_results('all')
            q=False
        elif c == ord('e'):
            rc.purge_everything()
            q=False
        elif c == ord('c'):
            q=False  
    ss.addstr(12,2,"                                            ")

def scrupdate(ss):
    ss.addstr(4,3,"Time: {}               ".format(time.ctime()))    
    ss.addstr(6,3,"# of engines: {}            ".format(len(rc.ids)))
    ss.addstr(8,3,"Jobs: {} done / {} tasked / {} unassigned          ".format(*jobsrem()))
    ss.addstr(10,3,"Done in last 1/5/10/60 minutes: {} / {} / {} / {}            ".format(*[doneintime(x) for x in [1,5,10,60]]))
    ss.addstr(12,3,"[p]urge / [q]uit")
    ss.refresh()
   
  
def doneintime(mins):
   since = datetime.now() - timedelta(1.0/24/60*mins)
   nrecent = len(rc.db_query({'completed' : {'$ne': None, '$gte' : since }}))
   return nrecent
  

def jobsrem():
   qs = lv.queue_status()
   u = qs[u'unassigned']
   del(qs[u'unassigned'])
   t = sum( x[u'tasks'] for x in qs.values() )
   d = sum( x[u'completed'] for x in qs.values() )
   return d,t,u

curses.wrapper(main)