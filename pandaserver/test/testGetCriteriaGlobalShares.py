from pandaserver.taskbuffer.OraDBProxy import DBProxy
from config import panda_config

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

#proxyS.getCriteriaForGlobalShares('BNL-OSG')

print proxyS.getJobs(1, 'BNL_PROD_MCORE', 'managed', None, 1000,
               0, 'aipanda081.cern.ch', 20, None, None,
               None, None, None, None, None)
