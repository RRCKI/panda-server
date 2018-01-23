"""
WrappedCursor for a generic database connection proxy

"""

import re
import os
import sys
import warnings
from pandalogger.PandaLogger import PandaLogger
from config import panda_config

warnings.filterwarnings('ignore')

# logger
_logger = PandaLogger().getLogger('WrappedCursor')

# proxy
class WrappedCursor(object):

    # constructor
    def __init__(self, connection):
        # connection object
        self.conn = connection
        # cursor object
        self.cur = self.conn.cursor()
        # backend
        self.backend = panda_config.backend
        # statement
        self.statement = None


    # __iter__
    def __iter__(self):
        return iter(self.cur)


    # serialize
    def __str__(self):
        return 'WrappedCursor[%(conn)s]' % ({'conn': self.conn})

    # initialize
    def initialize(self):
        hostname = None
        if self.backend == 'oracle':
            # get hostname
            self.execute("SELECT SYS_CONTEXT('USERENV','HOST') FROM dual")
            res = self.fetchone()
            if res != None:
                hostname = res[0]
            # set TZ
            self.execute("ALTER SESSION SET TIME_ZONE='UTC'")
            # set DATE format
            self.execute("ALTER SESSION SET NLS_DATE_FORMAT='YYYY/MM/DD HH24:MI:SS'")
        else:
            # get hostname
            self.execute("SELECT SUBSTRING_INDEX(USER(),'@',-1)")
            res = self.fetchone()
            if res != None:
                hostname = res[0]
            # set TZ
            self.execute("SET @@SESSION.TIME_ZONE = '+00:00'")
            # set DATE format
            #self.execute("SET @@SESSION.DATETIME_FORMAT='%%Y/%%m/%%d %%H:%%i:%%s'")
            # disable autocommit
            self.execute("SET autocommit=0")
        return hostname
    # execute query on cursor
    def execute(self, sql, varDict=None, cur=None  # , returningInto=None
                ):
        if varDict is None:
            varDict = {}
        if cur is None:
            cur = self.cur
        ret = None
        if self.backend == 'oracle':
            # schema names
            sql = re.sub('ATLAS_PANDA\.',     panda_config.schemaPANDA + '.',     sql)
            sql = re.sub('ATLAS_PANDAMETA\.', panda_config.schemaMETA + '.',      sql)
            sql = re.sub('ATLAS_GRISLI\.',    panda_config.schemaGRISLI + '.',    sql)
            sql = re.sub('ATLAS_PANDAARCH\.', panda_config.schemaPANDAARCH + '.', sql)

            # remove `
            sql = re.sub('`','',sql)
            ret = cur.execute(sql, varDict)
        elif self.backend == 'mysql':
           
            #print "DEBUG execute : original SQL     %s " % sql
            #print "DEBUG execute : original varDict %s " % varDict
            # CURRENT_DATE interval
             #__KI for case like _DATE-3/24; here 1/24 means 1 hour
            sql = re.sub("CURRENT_DATE\s*-\s*(\d+)/24", "DATE_SUB(CURRENT_TIMESTAMP,INTERVAL \g<1> HOUR)", sql)
            sql = re.sub("CURRENT_DATE\s*-\s*(\d+|:[^\s\)]+)", "DATE_SUB(CURRENT_TIMESTAMP,INTERVAL \g<1> DAY)", sql)
            #CURRENT_DATE _mod up to 3 times in line
            sql = re.sub('CURRENT_DATE', 'CURRENT_TIMESTAMP', sql,3)
            # SYSDATE interval
             #__KI for case like SYSDATE-3/24; here 1/24 means 1 hour
            sql = re.sub("SYSDATE\s*-\s*(\d+)/24", "DATE_SUB(SYSDATE,INTERVAL \g<1> HOUR)", sql)
            sql = re.sub("SYSDATE\s*-\s*(\d+|:[^\s\)]+)", "DATE_SUB(SYSDATE,INTERVAL \g<1> DAY)", sql)
            # SYSDATE _mod up to 3 times in line
            sql = re.sub('SYSDATE', 'SYSDATE()', sql,3)
            # EMPTY_CLOB()
            sql = re.sub('EMPTY_CLOB\(\)', "''", sql)
            # ROWNUM
            sql = re.sub("(?i)(AND)*\s*ROWNUM.*(\d+)", " LIMIT \g<2>", sql)
            sql = re.sub("(?i)(WHERE)\s*LIMIT\s*(\d+)", " LIMIT \g<2>" , sql)
            # NOWAIT
            sql = re.sub('NOWAIT', "", sql)
            # RETURNING INTO
            returningInto = None
            #Moved to INSERT section
            #m = re.search("RETURNING ([^\s]+) INTO ([^\s]+)", sql, re.I)
            #if m is not None:
            #    returningInto = [{'returning': m.group(1), 'into': m.group(2)}]
            #    self._returningIntoMySQLpre(returningInto, varDict, cur)
            #    sql = re.sub(m.group(0), '', sql)
            # Addressing sequence
            seq_name = ""
            sql_update_returning = False
            tecd=0
            try:
             sql=sql.lstrip() #remove all the leading spaces 
             if sql.startswith("INSERT"):
            #if "INSERT" in sql:
                #sql = re.sub('[a-zA-Z\._]+\.nextval','NULL',sql)
                tecd=100
                tmpstr = re.search('[a-zA-Z0-9\._]+\.nextval',sql)
                if tmpstr:
                        tecd=101
                        schema_name = tmpstr.group(0).split('.')[0]
                        seq_name = tmpstr.group(0).split('.')[1] #this value is used for returning into
                        sql = re.sub('[a-zA-Z0-9\._]+\.nextval','{0}.nextval("{1}")'.format(schema_name,seq_name),sql)
                m1 = re.search('(\w+)\.(\w+)\.currval',sql)
                if m1: #note that mysql function to add is curval()
                    tecd=131
                    sql = re.sub('\w+\.\w+\.currval',m1.group(1)+'.curval("'+m1.group(2)+'")',sql)
                m2 = re.search("RETURNING ([^\s]+) INTO ([^\s]+)", sql, re.I)
                if m2 is not None:
                    tecd=151
                    returningInto = [{'returning': m2.group(1), 'into': m2.group(2)}]
                    self._returningIntoMySQLpre(returningInto, varDict, cur)
                    sql = re.sub(m2.group(0), '', sql)

             elif sql.startswith("SELECT"):
            #if "SELECT" in sql:
                tecd=200
                #look for "(SELECT ...)" - must change to "(SELECT ...) AS tempT"
                if not 'MEDIAN' in sql:
                  m=re.search('FROM \([ ]?SELECT ([^)]+)\)',sql)
                  if m:
                    tecd=202
                    if '(' in m.group(0)[1:]:
                        tecd=212
                        # look for "(SELECT ... IN (..) [ AND (...OR...) AND  (SYSDATE-2)]...)" up to 3 (...) inside main (SELECT ...), include complex case of (SELECT ...IN (...) AND (...(...OR...)...(...AND...)...)) with intra () up to 2
                        #m1 = re.search('\([ ]?SELECT ([^)(]+)(\(([^)]+)\)([^)(]+)?){1,2}([^)]+)?\)',sql)
                        m1 = re.search('FROM \([ ]?SELECT [^)(]+(\([^)(]+((\([^()]+\)([^()]+)?){1,2})?\)([^()]+)?){1,3}([^()]+)?\)',sql)
                        sql = re.sub(re.escape(m1.group(0)), m1.group(0)+' AS tmpSelect ',sql)
                    else:
                        sql = re.sub(re.escape(m.group(0)), m.group(0)+' AS tmpSelect ',sql)
                    _logger.debug("SELECT FROM (SELECT) AS tablename")
                  else: # look for nextval (suppose not in complex select) 
                    tecd=206
                    m2 = re.search('(\w+)\.(\w+)\.nextval',sql)
                    if m2:
                        tecd=232
                        sql = re.sub('\w+\.\w+\.nextval',m2.group(1)+'.nextval("'+m2.group(2)+'")',sql)

            #sql_update_returning = False
             elif sql.startswith("UPDATE"):
                tecd=300
            #if "UPDATE" in sql:
                m = re.search("RETURNING ([^\s]+) INTO ([^\s]+)", sql, re.I)
                if m is not None:
                    tecd=311
                    returningInto = [{'returning': m.group(1), 'into': m.group(2)}]
                    self._returningIntoMySQLpre(returningInto, varDict, cur)
                    sql = re.sub(m.group(0), '', sql)
                    #In case of UPDATE in JEDI, RETURNING variables cannot be acquired via sequences - additional select is required.
                    #__mod can not find such cases for code of 07.2017 (Ora+JedDBProxy)
                    sql_update_returning = True
                    _logger.debug("UPDATE_RETURNING_INTO HERE")
            except:
             #_logger.debug("some except")
             _logger.debug("some except in " +str(tecd) +" in "+sql)
             raise

            # schema names
            # use re.compile('ATLAS_PANDA\.',re.I) #for python 2.6 only - re.sub not understand flags (re.IGNORECASE)
            sql = re.sub(re.compile('ATLAS_PANDA\.',re.I),     panda_config.schemaPANDA + '.',     sql)
            sql = re.sub(re.compile('ATLAS_PANDAMETA\.',re.I), panda_config.schemaMETA + '.',      sql)
            sql = re.sub(re.compile('ATLAS_GRISLI\.',re.I),    panda_config.schemaGRISLI + '.',    sql)
            sql = re.sub(re.compile('ATLAS_PANDAARCH\.',re.I), panda_config.schemaPANDAARCH + '.', sql)
#            sql = re.sub('ATLAS_PANDA\.',     panda_config.schemaPANDA + '.',     sql)
#            sql = re.sub('ATLAS_PANDAMETA\.', panda_config.schemaMETA + '.',      sql)
#            sql = re.sub('ATLAS_GRISLI\.',    panda_config.schemaGRISLI + '.',    sql)
#            sql = re.sub('ATLAS_PANDAARCH\.', panda_config.schemaPANDAARCH + '.', sql)
            # bind variables
            newVarDict = {}
            # make sure that :prodDBlockToken will not be replaced by %(prodDBlock)sToken
            keys = sorted(varDict.keys(), key=lambda s:-len(str(s)))
            for key in keys:
                val = varDict[key]
                if key[0] == ':':
                    newKey = key[1:]
                    sql = sql.replace(key, '%(' + newKey + ')s')
                else:
                    newKey = key
                    sql = sql.replace(':' + key, '%(' + newKey + ')s')
                newVarDict[newKey] = val
            try:
                # from PanDA monitor it is hard to log queries sometimes, so let's debug with hardcoded query dumps
                import time
                if os.path.exists('/data/atlpan/oracle/panda/monitor/logs/write_queries.txt'):
                    f = open('/data/atlpan/oracle/panda/monitor/logs/mysql_queries_WrappedCursor.txt', 'a')
                    f.write('mysql|%s|%s|%s\n' % (str(time.time()), str(sql), str(newVarDict)))
                    f.close()
            except:
                pass
            _logger.debug("execute : SQL     %s " % sql)
            _logger.debug("execute : varDict %s " % newVarDict)
            #print "DEBUG execute : SQL     %s " % sql
            #print "DEBUG execute : varDict %s " % newVarDict
            try:
                ret = cur.execute(sql, newVarDict)
            except:
                _logger.debug("EXCEPT im mysql"+str(sys.exc_info()[1])+"=="+sql)
                raise
            if returningInto is not None:
                #ret = self._returningIntoMySQLpost(returningInto, varDict, cur)
                #Operate as select last_inserted_id for sequences
                if seq_name == "" and not sql_update_returning:
                    ret = self._returningIntoMySQLpost(returningInto, varDict, cur)
                elif sql_update_returning: # perform a select to retrieve needed variables. Select is constructed from update.
                    n = re.search("UPDATE ([^\s]+) SET ([^\s]+) WHERE", sql, re.I)
                    if n is not None:
                        sql_update = " SELECT %s FROM %s WHERE" % (returningInto[0]['returning'], n.group(1)) + sql.replace(n.group(0), "")
                    ret1 = self.cur.execute(sql_update, newVarDict)
                    vs = returningInto[0]['into'].split(",")
                    ret = self.cur.fetchone()
                    for i in xrange(len(vs)):
                        varDict[vs[i]] = long(ret[i])
                    _logger.debug("VARDICT AFTER SELECT:" + str(varDict))
                else:
                    ret1 = self.cur.execute(" SELECT curval('%s') " % seq_name)
                    ret, = self.cur.fetchone()
                    try:
                        varDict[returningInto[0]['into']] = long(ret)
                        #_logger.debug("for %s manage sequence %s valued %s" % (returningInto[0]['into'],seq_name,ret))
                    except KeyError:
                        pass
        return ret


    def _returningIntoOracle(self, returningInputData, varDict, cur, dryRun=False):
        # returningInputData=[{'returning': 'PandaID', 'into': ':newPandaID'}, {'returning': 'row_ID', 'into': ':newRowID'}]
        result = ''
        if returningInputData is not None:
            try:
                valReturning = str(',').join([x['returning'] for x in returningInputData])
                listInto = [x['into'] for x in returningInputData]
                valInto = str(',').join(listInto)
                # assuming that we use RETURNING INTO only for PandaID or row_ID columns
                if not dryRun:
                    for x in listInto:
                        varDict[x] = cur.var(cx_Oracle.NUMBER)
                result = ' RETURNING %(returning)s INTO %(into)s ' % {'returning': valReturning, 'into': valInto}
            except:
                pass
        return result


    def _returningIntoMySQLpre(self, returningInputData, varDict, cur):
        # returningInputData=[{'returning': 'PandaID', 'into': ':newPandaID'}, {'returning': 'row_ID', 'into': ':newRowID'}]
        if returningInputData is not None:
            try:
                # get rid of "returning into" items in varDict
                listInto = [x['into'] for x in returningInputData]
                for x in listInto:
                    try:
                        del varDict[x]
                    except KeyError:
                        pass
                if len(returningInputData) == 1:
                    # and set original value in varDict to null, let auto_increment do the work
                    listReturning = [x['returning'] for x in returningInputData]
                    for x in listReturning:
                        varDict[':' + x] = None
            except:
                pass

    def _returningIntoMySQLpost(self, returningInputData, varDict, cur):
        # returningInputData=[{'returning': 'PandaID', 'into': ':newPandaID'}, {'returning': 'row_ID', 'into': ':newRowID'}]
        result = long(0)
        if len(returningInputData) == 1:
            ret = self.cur.execute(""" SELECT LAST_INSERT_ID() """)
            result, = self.cur.fetchone()
            if returningInputData is not None:
                try:
                    # update of "returning into" items in varDict
                    listInto = [x['into'] for x in returningInputData]
                    for x in listInto:
                        try:
                            varDict[x] = long(result)
                        except KeyError:
                            pass
                except:
                    pass
        return result


    # fetchall
    def fetchall(self):
        return self.cur.fetchall()


    # fetchmany
    def fetchmany(self, arraysize=1000):
        self.cur.arraysize = arraysize
        return self.cur.fetchmany()


    # fetchall
    def fetchone(self):
        return self.cur.fetchone()


    # var
    def var(self, dataType, *args, **kwargs):
        if self.backend == 'mysql':
            return apply(dataType,[0])
        else:
            return self.cur.var(dataType, *args, **kwargs)


    # get value
    def getvalue(self,dataItem):
        if self.backend == 'mysql':
            return dataItem
        else:
            return dataItem.getvalue()


    # next
    def next(self):
        if self.backend == 'mysql':
            return self.cur.fetchone()
        else:
            return self.cur.next()


    # close
    def close(self):
        return self.cur.close()

    # prepare
    def prepare(self, statement):
        self.statement = statement

    # executemany
    def executemany(self, sql, params):
        if sql is None:
            sql = self.statement
        if self.backend == 'oracle':
            self.cur.executemany(sql,params)
        else: 
            for paramsItem in params:
                self.execute(sql, paramsItem)

    # get_description
    @property
    def description(self):
        return self.cur.description

    # rowcount
    @property
    def rowcount(self):
        return self.cur.rowcount

    # arraysize
    @property
    def arraysize(self):
        return self.cur.arraysize

    @arraysize.setter
    def arraysize(self,val):
        self.cur.arraysize = val




