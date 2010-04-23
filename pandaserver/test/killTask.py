import time
import sys

import userinterface.Client as Client

aSrvID = None

from taskbuffer.OraDBProxy import DBProxy
# password
from config import panda_config

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

jobs = []

varMap = {}
varMap[':prodSourceLabel']  = 'managed'
varMap[':taskID'] = sys.argv[1]
sql = "SELECT PandaID FROM %s WHERE prodSourceLabel=:prodSourceLabel AND taskID=:taskID ORDER BY PandaID"
for table in ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsWaiting4','ATLAS_PANDA.jobsDefined4']:
    status,res = proxyS.querySQLS(sql % table,varMap)
    if res != None:
        for id, in res:
            if not id in jobs:
                jobs.append(id)

print 'The number of jobs to be killed : %s' % len(jobs)            
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        print 'kill %s' % str(jobs[iJob:iJob+nJob])
        Client.killJobs(jobs[iJob:iJob+nJob])
        iJob += nJob
        time.sleep(1)
                        
