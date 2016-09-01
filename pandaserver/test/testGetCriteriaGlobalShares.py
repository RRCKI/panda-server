from pandaserver.taskbuffer.OraDBProxy import DBProxy
from config import panda_config

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

proxyS.getCriteriaForGlobalShares('BNL-OSG')