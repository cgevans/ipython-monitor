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
    ss.nodelay(True)
    
    while True:
        ss.addstr(5,3,"# of engines: {}".format(len(rc.ids)))
        ss.addstr(7,3,"Jobs: {} done / {} tasked / {} unassigned".format(*jobsrem()))
        ss.addstr(9,3,"Done in last 1/5/10/60 minutes: {} / {} / {} / {}".format(*[doneintime(x) for x in [1,5,10,60]]))
        #ss.addstr(11,3,"HI {}".format(ss.getkey()))
        #lastcomp = mostrecent
        ss.refresh()
        time.sleep(5)

# def mostrecent(since):
#   if since:
#     recent = len(rc.db_query({'completed' : {'$gte' : since }}))
#   else:
#     recent = len(rc.db_query({'completed' : {'$ne' : None }}))
#   
  
  
def doneintime(mins):
   since = datetime.now() - timedelta(1.0/24/60*mins)
   nrecent = len(rc.db_query({'completed' : {'$gte' : since }}))
   return nrecent
  

def jobsrem():
   qs = lv.queue_status()
   u = qs[u'unassigned']
   del(qs[u'unassigned'])
   t = sum( x[u'tasks'] for x in qs.values() )
   d = sum( x[u'completed'] for x in qs.values() )
   #q = sum( x[u'queue'] for x in qs.values() )
   #n = sum( x[u'tasks'] for x in qs.values() )
   return d,t,u

curses.wrapper(main)
