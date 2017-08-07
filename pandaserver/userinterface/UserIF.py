'''
provide web interface to users

'''

import re
import sys
import time
import json
import types
import datetime
import cPickle as pickle
import jobdispatcher.Protocol as Protocol
import brokerage.broker
import taskbuffer.ProcessGroups
from config import panda_config
from taskbuffer.JobSpec import JobSpec
from taskbuffer.WrappedPickle import WrappedPickle
from brokerage.SiteMapper import SiteMapper
from pandalogger.PandaLogger import PandaLogger
from taskbuffer import PrioUtil
from dataservice.DDM import rucioAPI

# logger
_logger = PandaLogger().getLogger('UserIF')


# main class     
class UserIF:
    # constructor
    def __init__(self):
        self.taskBuffer = None
        

    # initialize
    def init(self,taskBuffer):
        self.taskBuffer = taskBuffer


    # submit jobs
    def submitJobs(self,jobsStr,user,host,userFQANs,prodRole=False,toPending=False):
        try:
            # deserialize jobspecs
            jobs = WrappedPickle.loads(jobsStr)
            _logger.debug("submitJobs %s len:%s prodRole=%s FQAN:%s" % (user,len(jobs),prodRole,str(userFQANs)))
            maxJobs = 5000
            if len(jobs) > maxJobs:
                _logger.error("submitJobs: too many jobs more than %s" % maxJobs)
                jobs = jobs[:maxJobs]
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("submitJobs : %s %s" % (type,value))
            jobs = []
        # check prodSourceLabel
        try:
            goodProdSourceLabel = True
            for tmpJob in jobs:
                # prevent internal jobs from being submitted from outside
                if tmpJob.prodSourceLabel in taskbuffer.ProcessGroups.internalSourceLabels:
                    _logger.error("submitJobs %s wrong prodSourceLabel=%s" % (user,tmpJob.prodSourceLabel))
                    goodProdSourceLabel = False
                    break
                # check production role
                if tmpJob.prodSourceLabel in ['managed']:
                    if not prodRole:
                        _logger.error("submitJobs %s missing prod-role for prodSourceLabel=%s" % (user,tmpJob.prodSourceLabel))
                        goodProdSourceLabel = False
                        break
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("submitJobs : checking goodProdSourceLabel %s %s" % (errType,errValue))
            goodProdSourceLabel = False
        # reject injection for bad prodSourceLabel
        if not goodProdSourceLabel:
            return "ERROR: production role is required for production jobs"
        job0 = None
        # get user VO
        userVO = 'atlas'
        try:
            job0 = jobs[0]
            if not job0.VO in [None,'','NULL']:
                userVO = job0.VO
        except (IndexError, AttributeError) as e:
            _logger.error("submitJobs : checking userVO. userVO not found, defaulting to %s. (Exception %s)" %(userVO, e))
        # atlas jobs require FQANs
        if userVO == 'atlas' and userFQANs == []:
            _logger.error("submitJobs : VOMS FQANs are missing in your proxy. They are required for {0}".format(userVO))
            #return "ERROR: VOMS FQANs are missing. They are required for {0}".format(userVO)
        # get LSST pipeline username
        if userVO.lower() == 'lsst':
            try:
                if job0.prodUserName and job0.prodUserName.lower() != 'none':
                    user = job0.prodUserName
            except AttributeError:
                _logger.error("submitJobs : checking username for userVO[%s]: username not found, defaulting to %s. %s %s" % (userVO, user))
        # store jobs
        ret = self.taskBuffer.storeJobs(jobs,user,forkSetupper=True,fqans=userFQANs,
                                        hostname=host, toPending=toPending, userVO=userVO)
        _logger.debug("submitJobs %s ->:%s" % (user,len(ret)))
        # serialize 
        return pickle.dumps(ret)


    # logger interface
    def sendLogInfo(self,user,msgType,msgListStr):
        try:
            # deserialize message
            msgList = WrappedPickle.loads(msgListStr)
            # short user name
            cUID = self.taskBuffer.cleanUserID(user)
            # logging
            iMsg = 0
            for msgBody in msgList:
                # make message
                message = "dn='%s' %s" % (cUID,msgBody)
                # send message to logger
                if msgType in ['analy_brokerage']:
                    brokerage.broker.sendMsgToLogger(message)
                # get logger
                _pandaLogger = PandaLogger()            
                _pandaLogger.lock()
                _pandaLogger.setParams({'Type':msgType})
                logger = _pandaLogger.getHttpLogger(panda_config.loggername)
                # add message
                logger.info(message)
                # release HTTP handler
                _pandaLogger.release()
                # sleep
                iMsg += 1
                if iMsg % 5 == 0:
                    time.sleep(1)
        except:
            pass
        # return
        return True


    # run task assignment
    def runTaskAssignment(self,jobsStr):
        try:
            # deserialize jobspecs
            jobs = WrappedPickle.loads(jobsStr)
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("runTaskAssignment : %s %s" % (type,value))
            jobs = []
        # run
        ret = self.taskBuffer.runTaskAssignment(jobs)
        # serialize 
        return pickle.dumps(ret)


    # get serial number for group job
    def getSerialNumberForGroupJob(self,name):
        # get
        ret = self.taskBuffer.getSerialNumberForGroupJob(name)
        # serialize 
        return pickle.dumps(ret)


    # change job priorities
    def changeJobPriorities(self,user,prodRole,newPrioMapStr):
        # check production role
        if not prodRole:
            return False,"production role is required"
        try:
            # deserialize map
            newPrioMap = WrappedPickle.loads(newPrioMapStr)
            _logger.debug("changeJobPriorities %s : %s" % (user,str(newPrioMap)))
            # change
            ret = self.taskBuffer.changeJobPriorities(newPrioMap)
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("changeJobPriorities : %s %s" % (errType,errValue))
            return False,'internal server error' 
        # serialize 
        return ret


    # run rebrokerage
    def runReBrokerage(self,dn,jobID,cloud,excludedSite,forceRebro):
        returnVal = "True"
        # return
        return returnVal


    # retry failed subjobs in running job
    def retryFailedJobsInActive(self,dn,jobID):
        returnVal = False
        try:
            _logger.debug("retryFailedJobsInActive %s JobID:%s" % (dn,jobID))
            cUID = self.taskBuffer.cleanUserID(dn)            
            tmpRet = self.taskBuffer.retryJobsInActive(cUID,jobID)
            returnVal = True
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("retryFailedJobsInActive: %s %s" % (errType,errValue))
            returnVal = "ERROR: server side crash"
        # return
        return returnVal


    # set debug mode
    def setDebugMode(self,dn,pandaID,prodManager,modeOn,workingGroup):
        ret = self.taskBuffer.setDebugMode(dn,pandaID,prodManager,modeOn,workingGroup)
        # return
        return ret


    # insert sandbox file info
    def insertSandboxFileInfo(self,userName,hostName,fileName,fileSize,checkSum):
        ret = self.taskBuffer.insertSandboxFileInfo(userName,hostName,fileName,fileSize,checkSum)
        # return
        return ret


    # check duplicated sandbox file
    def checkSandboxFile(self,userName,fileSize,checkSum):
        ret = self.taskBuffer.checkSandboxFile(userName,fileSize,checkSum)
        # return
        return ret


    # get job status
    def getJobStatus(self,idsStr):
        try:
            # deserialize jobspecs
            ids = WrappedPickle.loads(idsStr)
            _logger.debug("getJobStatus len   : %s" % len(ids))
            maxIDs = 5500
            if len(ids) > maxIDs:
                _logger.error("too long ID list more than %s" % maxIDs)
                ids = ids[:maxIDs]
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getJobStatus : %s %s" % (type,value))
            ids = []
        _logger.debug("getJobStatus start : %s" % ids)       
        # peek jobs
        ret = self.taskBuffer.peekJobs(ids)
        _logger.debug("getJobStatus end")
        # serialize 
        return pickle.dumps(ret)


    # get PandaID with jobexeID
    def getPandaIDwithJobExeID(self,idsStr):
        try:
            # deserialize jobspecs
            ids = WrappedPickle.loads(idsStr)
            _logger.debug("getPandaIDwithJobExeID len   : %s" % len(ids))
            maxIDs = 5500
            if len(ids) > maxIDs:
                _logger.error("too long ID list more than %s" % maxIDs)
                ids = ids[:maxIDs]
        except:
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("getPandaIDwithJobExeID : %s %s" % (errtype,errvalue))
            ids = []
        _logger.debug("getPandaIDwithJobExeID start : %s" % ids)       
        # peek jobs
        ret = self.taskBuffer.getPandaIDwithJobExeID(ids)
        _logger.debug("getPandaIDwithJobExeID end")
        # serialize 
        return pickle.dumps(ret)



    # get PandaIDs with TaskID
    def getPandaIDsWithTaskID(self,jediTaskID):
        # get PandaIDs
        ret = self.taskBuffer.getPandaIDsWithTaskID(jediTaskID)
        # serialize 
        return pickle.dumps(ret)


    # get assigned cloud for tasks
    def seeCloudTask(self,idsStr):
        try:
            # deserialize jobspecs
            ids = WrappedPickle.loads(idsStr)
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("seeCloudTask : %s %s" % (type,value))
            ids = []
        _logger.debug("seeCloudTask start : %s" % ids)       
        # peek jobs
        ret = {}
        for id in ids:
            tmpRet = self.taskBuffer.seeCloudTask(id)
            ret[id] = tmpRet
        _logger.debug("seeCloudTask end")
        # serialize 
        return pickle.dumps(ret)


    # get active datasets
    def getActiveDatasets(self,computingSite,prodSourceLabel):
        # run
        ret = self.taskBuffer.getActiveDatasets(computingSite,prodSourceLabel)
        # return
        return ret


    # get assigning task
    def getAssigningTask(self):
        # run
        ret = self.taskBuffer.getAssigningTask()
        # serialize 
        return pickle.dumps(ret)


    # set task by user
    def setCloudTaskByUser(self,user,tid,cloud,status):
        # run
        ret = self.taskBuffer.setCloudTaskByUser(user,tid,cloud,status)
        return ret

    
    # get job statistics
    def getJobStatistics(self,sourcetype=None):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsForExtIF(sourcetype)
        # serialize 
        return pickle.dumps(ret)


    # get highest prio jobs
    def getHighestPrioJobStat(self,perPG=False,useMorePG=False):
        # get job statistics
        ret = self.taskBuffer.getHighestPrioJobStat(perPG,useMorePG)
        # serialize 
        return pickle.dumps(ret)


    # get queued analysis jobs at a site
    def getQueuedAnalJobs(self,site,dn):
        # get job statistics
        ret = self.taskBuffer.getQueuedAnalJobs(site,dn)
        # serialize 
        return pickle.dumps(ret)


    # get job statistics for Bamboo
    def getJobStatisticsForBamboo(self,useMorePG=False):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsForBamboo(useMorePG)
        # serialize 
        return pickle.dumps(ret)
        

    # get job statistics per site
    def getJobStatisticsPerSite(self,predefined=False,workingGroup='',countryGroup='',jobType='',
                                minPriority=None,readArchived=True):
        # get job statistics
        ret = self.taskBuffer.getJobStatistics(readArchived,predefined,workingGroup,countryGroup,jobType,
                                               minPriority=minPriority)
        # serialize 
        return pickle.dumps(ret)


    # get the number of waiting jobs per site and use
    def getJobStatisticsPerUserSite(self):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsPerUserSite()
        # serialize 
        return pickle.dumps(ret)


    # get job statistics per site with label
    def getJobStatisticsWithLabel(self,site):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsWithLabel(site)
        # serialize 
        return pickle.dumps(ret)


    # query PandaIDs
    def queryPandaIDs(self,idsStr):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        # query PandaIDs 
        ret = self.taskBuffer.queryPandaIDs(ids)
        # serialize 
        return pickle.dumps(ret)


    # get number of analysis jobs per user  
    def getNUserJobs(self,siteName):
        # get 
        ret = self.taskBuffer.getNUserJobs(siteName)
        # serialize 
        return pickle.dumps(ret)


    # query job info per cloud
    def queryJobInfoPerCloud(self,cloud,schedulerID):
        # query PandaIDs 
        ret = self.taskBuffer.queryJobInfoPerCloud(cloud,schedulerID)
        # serialize 
        return pickle.dumps(ret)

    
    # query PandaIDs at site
    def getPandaIDsSite(self,site,status,limit):
        # query PandaIDs 
        ret = self.taskBuffer.getPandaIDsSite(site,status,limit)
        # serialize 
        return pickle.dumps(ret)


    # get PandaIDs to be updated in prodDB
    def getJobsToBeUpdated(self,limit,lockedby):
        # query PandaIDs 
        ret = self.taskBuffer.getPandaIDsForProdDB(limit,lockedby)
        # serialize 
        return pickle.dumps(ret)


    # update prodDBUpdateTimes
    def updateProdDBUpdateTimes(self,paramsStr):
        # deserialize IDs
        params = WrappedPickle.loads(paramsStr)
        # get jobs
        ret = self.taskBuffer.updateProdDBUpdateTimes(params)
        # serialize 
        return pickle.dumps(True)


    # query last files in datasets
    def queryLastFilesInDataset(self,datasetStr):
        # deserialize names
        datasets = WrappedPickle.loads(datasetStr)
        # get files
        ret = self.taskBuffer.queryLastFilesInDataset(datasets)
        # serialize 
        return pickle.dumps(ret)


    # get input files currently in used for analysis
    def getFilesInUseForAnal(self,outDataset):
        # get files
        ret = self.taskBuffer.getFilesInUseForAnal(outDataset)
        # serialize 
        return pickle.dumps(ret)


    # get list of dis dataset to get input files in shadow
    def getDisInUseForAnal(self,outDataset):
        # get files
        ret = self.taskBuffer.getDisInUseForAnal(outDataset)
        # serialize 
        return pickle.dumps(ret)


    # get input LFNs currently in use for analysis with shadow dis
    def getLFNsInUseForAnal(self,inputDisListStr):
        # deserialize IDs
        inputDisList = WrappedPickle.loads(inputDisListStr)
        # get files
        ret = self.taskBuffer.getLFNsInUseForAnal(inputDisList)
        # serialize 
        return pickle.dumps(ret)


    # kill jobs
    def killJobs(self,idsStr,user,host,code,prodManager,useMailAsID,fqans,killOpts=[]):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        if not isinstance(ids,types.ListType):
            ids = [ids]
        _logger.info("killJob : %s %s %s %s %s" % (user,code,prodManager,fqans,ids))
        try:
            if useMailAsID:
                _logger.debug("killJob : getting mail address for %s" % user)
                realDN = re.sub('/CN=limited proxy','',user)
                realDN = re.sub('(/CN=proxy)+','',realDN)
                nTry = 3
                for iDDMTry in range(nTry):
                    status,userInfo = rucioAPI.finger(realDN)
                    if status:
                        _logger.debug("killJob : %s is converted to %s" % (user,userInfo['email']))
                        user = userInfo['email']
                        break
                    time.sleep(1)
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("killJob : failed to convert email address %s : %s %s" % (user,errType,errValue))
        # get working groups with prod role
        wgProdRole = []
        for fqan in fqans:
            tmpMatch = re.search('/atlas/([^/]+)/Role=production',fqan)
            if tmpMatch != None:
                # ignore usatlas since it is used as atlas prod role
                tmpWG = tmpMatch.group(1) 
                if not tmpWG in ['','usatlas']+wgProdRole:
                    wgProdRole.append(tmpWG)
                    # group production
                    wgProdRole.append('gr_%s' % tmpWG)
        # kill jobs
        ret = self.taskBuffer.killJobs(ids,user,code,prodManager,wgProdRole,killOpts)
        # serialize 
        return pickle.dumps(ret)


    # reassign jobs
    def reassignJobs(self,idsStr,user,host,forPending,firstSubmission):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        # reassign jobs
        ret = self.taskBuffer.reassignJobs(ids,forkSetupper=True,forPending=forPending,
                                           firstSubmission=firstSubmission)
        # serialize 
        return pickle.dumps(ret)
        

    # resubmit jobs
    def resubmitJobs(self,idsStr):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        # kill jobs
        ret = self.taskBuffer.resubmitJobs(ids)
        # serialize 
        return pickle.dumps(ret)


    # get list of site spec
    def getSiteSpecs(self,siteType='analysis'):
        # get analysis site list
        specList = {}
        siteMapper = SiteMapper(self.taskBuffer)
        for id,spec in siteMapper.siteSpecList.iteritems():
            if siteType == 'all' or spec.type == siteType:
                # convert to map
                tmpSpec = {}
                for attr in spec._attributes:
                    if attr in ['ddm_endpoints']:
                        continue
                    tmpSpec[attr] = getattr(spec,attr)
                specList[id] = tmpSpec
        # serialize
        return pickle.dumps(specList)


    # get list of cloud spec
    def getCloudSpecs(self):
        # get cloud list
        siteMapper = SiteMapper(self.taskBuffer)
        # serialize
        return pickle.dumps(siteMapper.cloudSpec)


    # get list of cache prefix
    def getCachePrefixes(self):
        # get
        ret = self.taskBuffer.getCachePrefixes()
        # serialize 
        return pickle.dumps(ret)


    # get list of cmtConfig
    def getCmtConfigList(self,relaseVer):
        # get
        ret = self.taskBuffer.getCmtConfigList(relaseVer)
        # serialize 
        return pickle.dumps(ret)


    # get nPilots
    def getNumPilots(self):
        # get nPilots
        ret = self.taskBuffer.getCurrentSiteData()
        numMap = {}
        for siteID,siteNumMap in ret.iteritems():
            nPilots = 0
            # nPilots = getJob+updateJob
            if siteNumMap.has_key('getJob'):
                nPilots += siteNumMap['getJob']
            if siteNumMap.has_key('updateJob'):
                nPilots += siteNumMap['updateJob']
            # append
            numMap[siteID] = {'nPilots':nPilots}
        # serialize
        return pickle.dumps(numMap)


    # run brokerage
    def runBrokerage(self,sitesStr,cmtConfig,atlasRelease,trustIS=False,processingType=None,
                     dn=None,loggingFlag=False,memorySize=None,workingGroup=None,fqans=[],
                     nJobs=None,preferHomeCountry=False,siteReliability=None,maxCpuCount=None):
        if not loggingFlag:
            ret = 'NULL'
        else:
            ret = {'site':'NULL','logInfo':[]}
        try:
            # deserialize sites
            sites = WrappedPickle.loads(sitesStr)
            # instantiate siteMapper
            siteMapper = SiteMapper(self.taskBuffer)
            # instantiate job
            job = JobSpec()
            job.AtlasRelease = atlasRelease
            job.cmtConfig    = cmtConfig
            if processingType != None:
                job.processingType = processingType
            if memorySize != None:
                job.minRamCount = memorySize
            if workingGroup != None:
                userDefinedWG = True
                validWorkingGroup = True
                job.workingGroup = workingGroup
            else:
                userDefinedWG = False
                validWorkingGroup = False
            if maxCpuCount != None:
                job.maxCpuCount = maxCpuCount
            # get parameters related to priority
            withProdRole,workingGroup,priorityOffset,serNum,weight = self.taskBuffer.getPrioParameters([job],dn,fqans,
                                                                                                       userDefinedWG,
                                                                                                       validWorkingGroup)
            # get min priority using nJobs
            try:
                nJobs = long(nJobs)
            except:
                # use 200 as a default # of jobs
                nJobs =200
            minPrio = PrioUtil.calculatePriority(priorityOffset,serNum+nJobs,weight)
            # get countryGroup
            prefCountries = []
            if preferHomeCountry:
                for tmpFQAN in fqans:
                    match = re.search('^/atlas/([^/]+)/',tmpFQAN)
                    if match != None:
                        tmpCountry = match.group(1)
                        # use country code or usatlas
                        if len(tmpCountry) == 2:
                            prefCountries.append(tmpCountry)
                            break
                        # usatlas
                        if tmpCountry in ['usatlas']:
                            prefCountries.append('us')
                            break
            # run brokerage
            _logger.debug("runBrokerage for dn=%s FQAN=%s minPrio=%s preferred:%s:%s" % (dn,str(fqans),minPrio,
                                                                                         preferHomeCountry,
                                                                                         str(prefCountries)))
            brokerage.broker.schedule([job],self.taskBuffer,siteMapper,True,sites,trustIS,dn,
                                      reportLog=loggingFlag,minPriority=minPrio,preferredCountries=prefCountries,
                                      siteReliability=siteReliability)
            # get computingSite
            if not loggingFlag:
                ret = job.computingSite
            else:
                ret = pickle.dumps({'site':job.computingSite,'logInfo':job.brokerageErrorDiag})
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("runBrokerage : %s %s" % (type,value))
        return ret


    # get script for offline running
    def getScriptOfflineRunning(self,pandaID,days=None):
        # register
        ret = self.taskBuffer.getScriptOfflineRunning(pandaID,days)
        # return
        return ret


    # register proxy key
    def registerProxyKey(self,params):
        # register
        ret = self.taskBuffer.registerProxyKey(params)
        # return
        return ret

    
    # get client version
    def getPandaClientVer(self):
        # get
        ret = self.taskBuffer.getPandaClientVer()
        # return
        return ret


    # get proxy key
    def getProxyKey(self,dn):
        # get files
        ret = self.taskBuffer.getProxyKey(dn)
        # serialize 
        return pickle.dumps(ret)


    # get slimmed file info with PandaIDs
    def getSlimmedFileInfoPandaIDs(self,pandaIDsStr,dn):
        try:
            # deserialize IDs
            pandaIDs = WrappedPickle.loads(pandaIDsStr)
            # truncate
            maxIDs = 5500
            if len(pandaIDs) > maxIDs:
                _logger.error("getSlimmedFileInfoPandaIDs: too long ID list more than %s" % maxIDs)
                pandaIDs = pandaIDs[:maxIDs]
            # get
            _logger.debug("getSlimmedFileInfoPandaIDs start : %s %s" % (dn,len(pandaIDs)))            
            ret = self.taskBuffer.getSlimmedFileInfoPandaIDs(pandaIDs)
            _logger.debug("getSlimmedFileInfoPandaIDs end")            
        except:
            ret = {}
        # serialize 
        return pickle.dumps(ret)


    # get JobIDs in a time range
    def getJobIDsInTimeRange(self,dn,timeRange):
        # get IDs
        ret = self.taskBuffer.getJobIDsInTimeRange(dn,timeRange)
        # serialize 
        return pickle.dumps(ret)


    # get active JediTasks in a time range
    def getJediTasksInTimeRange(self,dn,timeRange):
        # get IDs
        ret = self.taskBuffer.getJediTasksInTimeRange(dn,timeRange)
        # serialize 
        return pickle.dumps(ret)


    # get details of JediTask
    def getJediTaskDetails(self,jediTaskID,fullFlag,withTaskInfo):
        # get IDs
        ret = self.taskBuffer.getJediTaskDetails(jediTaskID,fullFlag,withTaskInfo)
        # serialize 
        return pickle.dumps(ret)


    # get PandaIDs for a JobID
    def getPandIDsWithJobID(self,dn,jobID,nJobs):
        # get IDs
        ret = self.taskBuffer.getPandIDsWithJobID(dn,jobID,nJobs)
        # serialize 
        return pickle.dumps(ret)


    # check merge job generation status
    def checkMergeGenerationStatus(self,dn,jobID):
        # check
        ret = self.taskBuffer.checkMergeGenerationStatus(dn,jobID)
        # serialize 
        return pickle.dumps(ret)


    # get full job status
    def getFullJobStatus(self,idsStr,dn):
        try:
            # deserialize jobspecs
            ids = WrappedPickle.loads(idsStr)
            # truncate
            maxIDs = 5500
            if len(ids) > maxIDs:
                _logger.error("getFullJobStatus: too long ID list more than %s" % maxIDs)
                ids = ids[:maxIDs]
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getFullJobStatus : %s %s" % (type,value))
            ids = []
        _logger.debug("getFullJobStatus start : %s %s" % (dn,str(ids)))
        # peek jobs
        ret = self.taskBuffer.getFullJobStatus(ids)
        _logger.debug("getFullJobStatus end")
        # serialize 
        return pickle.dumps(ret)


    # add account to siteaccess
    def addSiteAccess(self,siteID,dn):
        # add
        ret = self.taskBuffer.addSiteAccess(siteID,dn)
        # serialize 
        return pickle.dumps(ret)


    # list site access
    def listSiteAccess(self,siteID,dn,longFormat=False):
        # list
        ret = self.taskBuffer.listSiteAccess(siteID,dn,longFormat)
        # serialize 
        return pickle.dumps(ret)


    # update site access
    def updateSiteAccess(self,method,siteid,requesterDN,userName,attrValue):
        # list
        ret = self.taskBuffer.updateSiteAccess(method,siteid,requesterDN,userName,attrValue)
        # serialize 
        return str(ret)


    # insert task params
    def insertTaskParams(self,taskParams,user,prodRole,fqans,properErrorCode):
        # register
        ret = self.taskBuffer.insertTaskParamsPanda(taskParams,user,prodRole,fqans,properErrorCode=properErrorCode)
        # return
        return ret


    # kill task
    def killTask(self,jediTaskID,user,prodRole,properErrorCode):
        # kill
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'kill',properErrorCode=properErrorCode)
        # return
        return ret


    # finish task
    def finishTask(self,jediTaskID,user,prodRole,properErrorCode,qualifier):
        # kill
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'finish',
                                                   properErrorCode=properErrorCode,
                                                   comQualifier=qualifier)
        # return
        return ret


    # retry task
    def retryTask(self,jediTaskID,user,prodRole,properErrorCode,newParams):
        # retry with new params
        if newParams != None:
            try:
                # convert to dict
                newParams = PrioUtil.decodeJSON(newParams)
                # get original params
                taskParams = self.taskBuffer.getTaskPramsPanda(jediTaskID)
                taskParamsJson = PrioUtil.decodeJSON(taskParams)
                # replace with new values
                for newKey,newVal in newParams.iteritems():
                    taskParamsJson[newKey] = newVal
                taskParams = json.dumps(taskParamsJson)
                # retry with new params
                ret = self.taskBuffer.insertTaskParamsPanda(taskParams,user,prodRole,[],properErrorCode=properErrorCode,
                                                            allowActiveTask=True)
            except:
                errType,errValue = sys.exc_info()[:2]
                ret = 1,'server error with {0}:{1}'.format(errType,errValue)
        else:
            # normal retry
            ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'retry',properErrorCode=properErrorCode)
        if properErrorCode == True and ret[0] == 5:
            # retry failed analysis jobs
            jobdefList = self.taskBuffer.getJobdefIDsForFailedJob(jediTaskID)
            cUID = self.taskBuffer.cleanUserID(user)
            for jobID in jobdefList:
                self.taskBuffer.retryJobsInActive(cUID,jobID,True)
            self.taskBuffer.increaseAttemptNrPanda(jediTaskID,5)
            retStr  = 'retry has been triggered for failed jobs '
            retStr += 'while the task is still {0}'.format(ret[1])
            if newParams == None:
                ret = 0,retStr
            else:
                ret = 3,retStr
        # return
        return ret


    # reassign task
    def reassignTask(self,jediTaskID,user,prodRole,comComment):
        # reassign
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'reassign',
                                                   comComment=comComment,properErrorCode=True)
        # return
        return ret


    # pause task
    def pauseTask(self,jediTaskID,user,prodRole):
        # exec
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'pause',properErrorCode=True)
        # return
        return ret


    # resume task
    def resumeTask(self,jediTaskID,user,prodRole):
        # exec
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'resume',properErrorCode=True)
        # return
        return ret


    # force avalanche for task
    def avalancheTask(self,jediTaskID,user,prodRole):
        # exec
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'avalanche',properErrorCode=True)
        # return
        return ret


    # get retry history
    def getRetryHistory(self,jediTaskID,user):
        # get
        _logger.debug("getRetryHistory jediTaskID={0} start {1}".format(jediTaskID,user))
        ret = self.taskBuffer.getRetryHistoryJEDI(jediTaskID)
        _logger.debug("getRetryHistory jediTaskID={0} done".format(jediTaskID))
        # return
        return ret


    # change task priority 
    def changeTaskPriority(self,jediTaskID,newPriority):
        # kill
        ret = self.taskBuffer.changeTaskPriorityPanda(jediTaskID,newPriority)
        # return
        return ret


    # increase attempt number for unprocessed files
    def increaseAttemptNrPanda(self,jediTaskID,increasedNr):
        # exec
        ret = self.taskBuffer.increaseAttemptNrPanda(jediTaskID,increasedNr)
        # return
        return ret


    # change task attribute
    def changeTaskAttributePanda(self,jediTaskID,attrName,attrValue):
        # kill
        ret = self.taskBuffer.changeTaskAttributePanda(jediTaskID,attrName,attrValue)
        # return
        return ret


    # change split rule for task
    def changeTaskSplitRulePanda(self,jediTaskID,attrName,attrValue):
        # exec
        ret = self.taskBuffer.changeTaskSplitRulePanda(jediTaskID,attrName,attrValue)
        # return
        return ret


    # reactivate task
    def reactivateTask(self,jediTaskID):
        # update datasets and task status
        ret = self.taskBuffer.reactivateTask(jediTaskID)
        return ret


    # get task status
    def getTaskStatus(self,jediTaskID):
        # update task status
        ret = self.taskBuffer.getTaskStatus(jediTaskID)
        return ret[0]


    # reassign share
    def reassignShare(self, jedi_task_ids, share_dest):
        return self.taskBuffer.reassignShare(jedi_task_ids, share_dest)


    # list tasks in share
    def listTasksInShare(self, gshare, status):
        return self.taskBuffer.listTasksInShare(gshare, status)


    # get taskParamsMap
    def getTaskParamsMap(self, jediTaskID):
        # get taskParamsMap
        ret = self.taskBuffer.getTaskParamsMap(jediTaskID)
        return ret

    # update workers
    def updateWorkers(self,user,host,harvesterID,data):
        ret = self.taskBuffer.updateWorkers(harvesterID,data)
        if ret is None:
            retVal = (False,'database error')
        else:
            retVal = (True,ret)
        # serialize 
        return json.dumps(retVal)

    # heartbeat for harvester
    def harvesterIsAlive(self,user,host,harvesterID,data):
        ret = self.taskBuffer.harvesterIsAlive(user,host,harvesterID,data)
        if ret is None:
            retVal = (False,'database error')
        else:
            retVal = (True,ret)
        # serialize 
        return json.dumps(retVal)

    # report stat of workers
    def reportWorkerStats(self, harvesterID, siteName, paramsList):
        return self.taskBuffer.reportWorkerStats(harvesterID, siteName, paramsList)



# Singleton
userIF = UserIF()
del UserIF


# get FQANs
def _getFQAN(req):
    fqans = []
    for tmpKey,tmpVal in req.subprocess_env.iteritems():
        # compact credentials
        if tmpKey.startswith('GRST_CRED_'):
            # VOMS attribute
            if tmpVal.startswith('VOMS'):
                # FQAN
                fqan = tmpVal.split()[-1]
                # append
                fqans.append(fqan)
        # old style         
        elif tmpKey.startswith('GRST_CONN_'):
            tmpItems = tmpVal.split(':')
            # FQAN
            if len(tmpItems)==2 and tmpItems[0]=='fqan':
                fqans.append(tmpItems[-1])
    # return
    return fqans


# get DN
def _getDN(req):
    realDN = ''
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        realDN = req.subprocess_env['SSL_CLIENT_S_DN']
        # remove redundant CN
        realDN = re.sub('/CN=limited proxy','',realDN)
        realDN = re.sub('/CN=proxy(/CN=proxy)+','/CN=proxy',realDN)
    return realDN
                                        

# check role
def _isProdRoleATLAS(req):
    # check role
    prodManager = False
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in ['/atlas/usatlas/Role=production','/atlas/Role=production']:
            if fqan.startswith(rolePat):
                return True
    return False


# get primary working group with prod role
def _getWGwithPR(req):
    try:
        fqans = _getFQAN(req)
        for fqan in fqans:
            tmpMatch = re.search('/[^/]+/([^/]+)/Role=production',fqan)
            if tmpMatch != None:
                # ignore usatlas since it is used as atlas prod role
                tmpWG = tmpMatch.group(1)
                if not tmpWG in ['','usatlas']:
                    return tmpWG.split('-')[-1].lower()
    except:
        pass
    return None


    
"""
web service interface

"""

# security check
def isSecure(req):
    # check security
    if not Protocol.isSecure(req):
        return False
    # disable limited proxy
    if '/CN=limited proxy' in req.subprocess_env['SSL_CLIENT_S_DN']:
        _logger.warning("access via limited proxy : %s" % req.subprocess_env['SSL_CLIENT_S_DN'])
        return False
    return True


# submit jobs
def submitJobs(req,jobs,toPending=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)
    # get FQAN
    fqans = _getFQAN(req)
    # hostname
    host = req.get_remote_host()
    # production Role
    prodRole = _isProdRoleATLAS(req)
    # to pending
    if toPending == 'True':
        toPending = True
    else:
        toPending = False
    return userIF.submitJobs(jobs,user,host,fqans,prodRole,toPending)


# run task assignment
def runTaskAssignment(req,jobs):
    # check security
    if not isSecure(req):
        return "False"
    return userIF.runTaskAssignment(jobs)


# get job status
def getJobStatus(req,ids):
    return userIF.getJobStatus(ids)


# get PandaID with jobexeID
def getPandaIDwithJobExeID(req,ids):
    return userIF.getPandaIDwithJobExeID(ids)


# get queued analysis jobs at a site
def getQueuedAnalJobs(req,site):
    # check security
    if not isSecure(req):
        return "ERROR: SSL is required"
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)
    return userIF.getQueuedAnalJobs(site,user)


# get active datasets
def getActiveDatasets(req,computingSite,prodSourceLabel='managed'):
    return userIF.getActiveDatasets(computingSite,prodSourceLabel)


# get assigning task
def getAssigningTask(req):
    return userIF.getAssigningTask()


# get assigned cloud for tasks
def seeCloudTask(req,ids):
    return userIF.seeCloudTask(ids)


# set task by user
def setCloudTaskByUser(req,tid,cloud='',status=''):
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "ERROR: SSL connection is required"
    user = _getDN(req)
    # check role
    if not _isProdRoleATLAS(req):
        return "ERROR: production role is required"
    return userIF.setCloudTaskByUser(user,tid,cloud,status)


# set debug mode
def setDebugMode(req,pandaID,modeOn):
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "ERROR: SSL connection is required"
    user = _getDN(req)
    # check role
    prodManager = _isProdRoleATLAS(req)
    # mode
    if modeOn == 'True':
        modeOn = True
    else:
        modeOn = False
    # get the primary working group with prod role
    workingGroup = _getWGwithPR(req)
    # exec    
    return userIF.setDebugMode(user,pandaID,prodManager,modeOn,workingGroup)


# insert sandbox file info
def insertSandboxFileInfo(req,userName,fileName,fileSize,checkSum):
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "ERROR: SSL connection is required"
    user = _getDN(req)
    # check role
    prodManager = _isProdRoleATLAS(req)
    if not prodManager:
        return "ERROR: missing role"
    # hostname
    hostName = req.get_remote_host()
    # exec    
    return userIF.insertSandboxFileInfo(userName,hostName,fileName,fileSize,checkSum)


# check duplicated sandbox file
def checkSandboxFile(req,fileSize,checkSum):
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "ERROR: SSL connection is required"
    user = _getDN(req)
    # exec    
    return userIF.checkSandboxFile(user,fileSize,checkSum)


# query PandaIDs
def queryPandaIDs(req,ids):
    return userIF.queryPandaIDs(ids)


# query job info per cloud
def queryJobInfoPerCloud(req,cloud,schedulerID=None):
    return userIF.queryJobInfoPerCloud(cloud,schedulerID)


# get PandaIDs at site
def getPandaIDsSite(req,site,status,limit=500):
    return userIF.getPandaIDsSite(site,status,limit)


# get PandaIDs to be updated in prodDB
def getJobsToBeUpdated(req,limit=5000,lockedby=''):
    limit = int(limit)
    return userIF.getJobsToBeUpdated(limit,lockedby)


# update prodDBUpdateTimes
def updateProdDBUpdateTimes(req,params):
    # check security
    if not isSecure(req):
        return False
    return userIF.updateProdDBUpdateTimes(params)


# get job statistics
def getJobStatistics(req,sourcetype=None):
    return userIF.getJobStatistics(sourcetype)


# get highest prio jobs
def getHighestPrioJobStat(req,perPG=None,useMorePG=None):
    if perPG == 'True':
        perPG = True
    else:
        perPG = False
    if useMorePG == 'True':
        useMorePG = taskbuffer.ProcessGroups.extensionLevel_1
    elif useMorePG in ['False',None]:
        useMorePG = False
    else:
        try:
            useMorePG = int(useMorePG)
        except:
            useMorePG = False
    return userIF.getHighestPrioJobStat(perPG,useMorePG)


# get job statistics for Babmoo
def getJobStatisticsForBamboo(req,useMorePG=None):
    if useMorePG == 'True':
        useMorePG = taskbuffer.ProcessGroups.extensionLevel_1
    elif useMorePG in ['False',None]:
        useMorePG = False
    else:
        try:
            useMorePG = int(useMorePG)
        except:
            useMorePG = False
    return userIF.getJobStatisticsForBamboo(useMorePG)


# get the number of waiting jobs per site and user
def getJobStatisticsPerUserSite(req):
    return userIF.getJobStatisticsPerUserSite()


# get job statistics per site
def getJobStatisticsPerSite(req,predefined='False',workingGroup='',countryGroup='',jobType='',
                            minPriority=None,readArchived=None):
    if predefined=='True':
        predefined=True
    else:
        predefined=False
    if minPriority != None:
        try:
            minPriority = int(minPriority)
        except:
            minPriority = None
    if readArchived=='True':
        readArchived = True
    elif readArchived=='False':
        readArchived = False
    else:
        host = req.get_remote_host()
        # read jobsArchived for panglia
        if re.search('panglia.*\.triumf\.ca$',host) != None or host in ['gridweb.triumf.ca']:
            readArchived = True
        else:
            readArchived = False
    return userIF.getJobStatisticsPerSite(predefined,workingGroup,countryGroup,jobType,
                                          minPriority,readArchived)


# get job statistics per site with label
def getJobStatisticsWithLabel(req,site=''):
    return userIF.getJobStatisticsWithLabel(site)


# query last files in datasets
def queryLastFilesInDataset(req,datasets):
    return userIF.queryLastFilesInDataset(datasets)


# get input files currently in used for analysis
def getFilesInUseForAnal(req,outDataset):
    return userIF.getFilesInUseForAnal(outDataset)


# get list of dis dataset to get input files in shadow
def getDisInUseForAnal(req,outDataset):
    return userIF.getDisInUseForAnal(outDataset)


# get input LFNs currently in use for analysis with shadow dis
def getLFNsInUseForAnal(req,inputDisList):
    return userIF.getLFNsInUseForAnal(inputDisList)


# kill jobs
def killJobs(req,ids,code=None,useMailAsID=None,killOpts=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodManager = False
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in ['/atlas/usatlas/Role=production','/atlas/Role=production']:
            if fqan.startswith(rolePat):
                prodManager = True
                break
        # escape
        if prodManager:
            break
    # use email address as ID
    if useMailAsID == 'True':
        useMailAsID = True
    else:
        useMailAsID = False
    # hostname
    host = req.get_remote_host()
    # options
    if killOpts == None:
        killOpts = []
    else:
        killOpts = killOpts.split(',')
    return userIF.killJobs(ids,user,host,code,prodManager,useMailAsID,fqans,killOpts)


# reassign jobs
def reassignJobs(req,ids,forPending=None,firstSubmission=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)
    # hostname
    host = req.get_remote_host()
    # for pending
    if forPending == 'True':
        forPending = True
    else:
        forPending = False
    # first submission
    if firstSubmission == 'False':
        firstSubmission = False
    else:
        firstSubmission = True
    return userIF.reassignJobs(ids,user,host,forPending,firstSubmission)


# resubmit jobs
def resubmitJobs(req,ids):
    # check security
    if not isSecure(req):
        return False
    return userIF.resubmitJobs(ids)


# change job priorities
def changeJobPriorities(req,newPrioMap=None):
    # check security
    if not isSecure(req):
        return pickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    ret = userIF.changeJobPriorities(user,prodRole,newPrioMap)
    return pickle.dumps(ret)


# get list of site spec
def getSiteSpecs(req,siteType=None):
    if siteType != None:
        return userIF.getSiteSpecs(siteType)
    else:
        return userIF.getSiteSpecs()

# get list of cloud spec
def getCloudSpecs(req):
    return userIF.getCloudSpecs()

# get list of cache prefix
def getCachePrefixes(req):
    return userIF.getCachePrefixes()

# get list of cmtConfig
def getCmtConfigList(self,relaseVer):
    return userIF.getCmtConfigList(relaseVer)

# get client version
def getPandaClientVer(req):
    return userIF.getPandaClientVer()
    
# get nPilots
def getNumPilots(req):
    return userIF.getNumPilots()

# run brokerage
def runBrokerage(req,sites,cmtConfig=None,atlasRelease=None,trustIS=False,processingType=None,
                 loggingFlag=False,memorySize=None,workingGroup=None,nJobs=None,
                 siteGroup=None,maxCpuCount=None):
    if trustIS=='True':
        trustIS = True
    else:
        trustIS = False
    if loggingFlag=='True':
        loggingFlag = True
    else:
        loggingFlag = False
    if memorySize != None:   
        try:
            memorySize = long(memorySize)
        except:
            pass
    if siteGroup != None:
        try:
            siteGroup = int(siteGroup)
        except:
            siteGroup = None
    if maxCpuCount != None:
        try:
            maxCpuCount = int(maxCpuCount)
        except:
            maxCpuCount = None
    preferHomeCountry = True
    dn = _getDN(req)
    fqans = _getFQAN(req)
    return userIF.runBrokerage(sites,cmtConfig,atlasRelease,trustIS,processingType,dn,
                               loggingFlag,memorySize,workingGroup,fqans,nJobs,preferHomeCountry,
                               siteGroup,maxCpuCount)

# run rebrokerage
def runReBrokerage(req,jobID,libDS='',cloud=None,excludedSite=None,forceOpt=None):
    # check SSL
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "ERROR: SSL connection is required"
    # get DN
    dn = _getDN(req)
    if dn == '':
        return "ERROR: could not get DN"
    # convert jobID to long
    try:
        jobID = long(jobID)
    except:
        return "ERROR: jobID is not an integer"
    # force option
    if forceOpt == 'True':
        forceOpt = True
    else:
        forceOpt = False
    return userIF.runReBrokerage(dn,jobID,cloud,excludedSite,forceOpt)


# retry failed subjobs in running job
def retryFailedJobsInActive(req,jobID):
    # check SSL
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "ERROR: SSL connection is required"
    # get DN
    dn = _getDN(req)
    if dn == '':
        return "ERROR: could not get DN"
    # convert jobID to long
    try:
        jobID = long(jobID)
    except:
        return "ERROR: jobID is not an integer"
    return userIF.retryFailedJobsInActive(dn,jobID)


# logger interface
def sendLogInfo(req,msgType,msgList):
    # check SSL
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "ERROR: SSL connection is required"
    # get DN
    dn = _getDN(req)
    if dn == '':
        return "ERROR: could not get DN"
    return userIF.sendLogInfo(dn,msgType,msgList)


# get serial number for group job
def getSerialNumberForGroupJob(req):
    # check SSL
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "ERROR: SSL connection is required"
    # get DN
    dn = _getDN(req)
    if dn == '':
        return "ERROR: could not get DN"
    return userIF.getSerialNumberForGroupJob(dn)


# get script for offline running
def getScriptOfflineRunning(req,pandaID,days=None):
    try:
        if days != None:
            days = int(days)
    except:
        days=None
    return userIF.getScriptOfflineRunning(pandaID,days)


# register proxy key
def registerProxyKey(req,credname,origin,myproxy):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    # get expiration date
    if not req.subprocess_env.has_key('SSL_CLIENT_V_END'):
        return False
    params = {}
    params['dn'] = _getDN(req)
    # set parameters
    params['credname'] = credname
    params['origin']   = origin
    params['myproxy']  = myproxy
    # convert SSL_CLIENT_V_END
    try:
        expTime = req.subprocess_env['SSL_CLIENT_V_END']
        # remove redundant white spaces
        expTime = re.sub('\s+',' ',expTime)
        # convert to timestamp
        expTime = time.strptime(expTime,'%b %d %H:%M:%S %Y %Z')
        params['expires']  = time.strftime('%Y-%m-%d %H:%M:%S',expTime)
    except:
        _logger.error("registerProxyKey : failed to convert %s" % \
                      req.subprocess_env['SSL_CLIENT_V_END'])
    # execute
    return userIF.registerProxyKey(params)


# register proxy key
def getProxyKey(req):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    dn = _getDN(req)
    # execute
    return userIF.getProxyKey(dn)


# get JobIDs in a time range
def getJobIDsInTimeRange(req,timeRange,dn=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    if dn == None:
        dn = _getDN(req)
    _logger.debug("getJobIDsInTimeRange %s %s" % (dn,timeRange))
    # execute
    return userIF.getJobIDsInTimeRange(dn,timeRange)


# get active JediTasks in a time range
def getJediTasksInTimeRange(req,timeRange,dn=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    if dn == None:
        dn = _getDN(req)
    _logger.debug("getJediTasksInTimeRange %s %s" % (dn,timeRange))
    # execute
    return userIF.getJediTasksInTimeRange(dn,timeRange)


# get details of JediTask
def getJediTaskDetails(req,jediTaskID,fullFlag,withTaskInfo):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    # option
    if fullFlag == 'True':
        fullFlag = True
    else:
        fullFlag = False
    if withTaskInfo == 'True':
        withTaskInfo = True
    else:
        withTaskInfo = False
    _logger.debug("getJediTaskDetails %s %s %s" % (jediTaskID,fullFlag,withTaskInfo))
    # execute
    return userIF.getJediTaskDetails(jediTaskID,fullFlag,withTaskInfo)


# get PandaIDs for a JobID
def getPandIDsWithJobID(req,jobID,nJobs,dn=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    if dn == None:
        dn = _getDN(req)
    _logger.debug("getPandIDsWithJobID %s JobID=%s nJobs=%s" % (dn,jobID,nJobs))
    # execute
    return userIF.getPandIDsWithJobID(dn,jobID,nJobs)


# check merge job generation status
def checkMergeGenerationStatus(req,jobID,dn=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    if dn == None:
        dn = _getDN(req)
    _logger.debug("checkMergeGenerationStatus %s JobID=%s" % (dn,jobID))
    # execute
    return userIF.checkMergeGenerationStatus(dn,jobID)


# get slimmed file info with PandaIDs
def getSlimmedFileInfoPandaIDs(req,ids):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    dn = _getDN(req)
    return userIF.getSlimmedFileInfoPandaIDs(ids,dn)


# get full job status
def getFullJobStatus(req,ids):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    dn = _getDN(req)
    return userIF.getFullJobStatus(ids,dn)


# get a list of DN/myproxy pass phrase/queued job count at a site
def getNUserJobs(req,siteName):
    # check security
    prodManager = False
    if not isSecure(req):
        return "Failed : HTTPS connection is required"
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in ['/atlas/usatlas/Role=production',
                        '/atlas/Role=production',
                        '/atlas/usatlas/Role=pilot',                        
                        '/atlas/Role=pilot',
                        ]:
            if fqan.startswith(rolePat):
                prodManager = True
                break
        # escape
        if prodManager:
            break
    # only prod managers can use this method
    if not prodManager:
        return "Failed : VOMS authorization failure. production or pilot role required"
    # execute
    return userIF.getNUserJobs(siteName)


# add account to siteaccess
def addSiteAccess(req,siteID):
    # check security
    if not isSecure(req):
        return "False"        
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "False"        
    dn = req.subprocess_env['SSL_CLIENT_S_DN']
    return userIF.addSiteAccess(siteID,dn)


# list site access
def listSiteAccess(req,siteID=None,longFormat=False):
    # check security
    if not isSecure(req):
        return "False"
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "False"
    # set DN if siteID is none
    dn = None
    if siteID==None:
        dn = req.subprocess_env['SSL_CLIENT_S_DN']
    # convert longFormat option
    if longFormat == 'True':
        longFormat = True
    else:
        longFormat = False
    return userIF.listSiteAccess(siteID,dn,longFormat)


# update site access
def updateSiteAccess(req,method,siteid,userName,attrValue=''):
    # check security
    if not isSecure(req):
        return "non HTTPS"
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "invalid DN"
    # set requester's DN
    requesterDN = req.subprocess_env['SSL_CLIENT_S_DN']
    # update
    return userIF.updateSiteAccess(method,siteid,requesterDN,userName,attrValue)


# insert task params
def insertTaskParams(req,taskParams=None,properErrorCode=None):
    if properErrorCode == 'True':
        properErrorCode = True
    else:
        properErrorCode = False
    # check security
    if not isSecure(req):
        return pickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check format
    try:
        json.loads(taskParams)
    except:
        return pickle.dumps((False,'failed to decode json'))        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # get FQANs
    fqans = _getFQAN(req)
    ret = userIF.insertTaskParams(taskParams,user,prodRole,fqans,properErrorCode)
    return pickle.dumps(ret)



# kill task
def killTask(req,jediTaskID=None,properErrorCode=None):
    if properErrorCode == 'True':
        properErrorCode = True
    else:
        properErrorCode = False
    # check security
    if not isSecure(req):
        if properErrorCode:
            return pickle.dumps((100,'secure connection is required'))
        else:
            return pickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except:
        if properErrorCode:
            return pickle.dumps((101,'jediTaskID must be an integer'))        
        else:
            return pickle.dumps((False,'jediTaskID must be an integer'))
    ret = userIF.killTask(jediTaskID,user,prodRole,properErrorCode)
    return pickle.dumps(ret)



# retry task
def retryTask(req,jediTaskID,properErrorCode=None,newParams=None):
    if properErrorCode == 'True':
        properErrorCode = True
    else:
        properErrorCode = False
    # check security
    if not isSecure(req):
        if properErrorCode:
            return pickle.dumps((100,'secure connection is required'))
        else:
            return pickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except:
        if properErrorCode:
            return pickle.dumps((101,'jediTaskID must be an integer'))        
        else:
            return pickle.dumps((False,'jediTaskID must be an integer'))
    ret = userIF.retryTask(jediTaskID,user,prodRole,properErrorCode,newParams)
    return pickle.dumps(ret)



# reassign task to site/cloud
def reassignTask(req,jediTaskID,site=None,cloud=None,nucleus=None,soft=None,mode=None):
    # check security
    if not isSecure(req):
        return pickle.dumps((100,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except:
        return pickle.dumps((101,'jediTaskID must be an integer'))        
    # site or cloud
    if site != None:
        # set 'y' to go back to oldStatus immediately
        comComment = 'site:{0}:y'.format(site)
    elif nucleus != None:
        comComment = 'nucleus:{0}:n'.format(nucleus)
    else:
        comComment = 'cloud:{0}:n'.format(cloud)
    if mode == 'nokill':
        comComment += ':nokill reassign'
    elif mode == 'soft' or soft == 'True':
        comComment += ':soft reassign'
    ret = userIF.reassignTask(jediTaskID,user,prodRole,comComment)
    return pickle.dumps(ret)



# finish task
def finishTask(req,jediTaskID=None,properErrorCode=None,soft=None):
    if properErrorCode == 'True':
        properErrorCode = True
    else:
        properErrorCode = False
    qualifier = None
    if soft == 'True':
        qualifier = 'soft'
    # check security
    if not isSecure(req):
        if properErrorCode:
            return pickle.dumps((100,'secure connection is required'))
        else:
            return pickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except:
        if properErrorCode:
            return pickle.dumps((101,'jediTaskID must be an integer'))        
        else:
            return pickle.dumps((False,'jediTaskID must be an integer'))
    ret = userIF.finishTask(jediTaskID,user,prodRole,properErrorCode,
                            qualifier)
    return pickle.dumps(ret)



# get retry history
def getRetryHistory(req,jediTaskID=None):
    # check security
    if not isSecure(req):
        return pickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    try:
        jediTaskID = long(jediTaskID)
    except:
        return pickle.dumps((False,'jediTaskID must be an integer'))        
    ret = userIF.getRetryHistory(jediTaskID,user)
    return pickle.dumps(ret)



# change task priority
def changeTaskPriority(req,jediTaskID=None,newPriority=None):
    # check security
    if not isSecure(req):
        return pickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # only prod managers can use this method
    if not prodRole:
        return "Failed : production or pilot role required"
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except:
        return pickle.dumps((False,'jediTaskID must be an integer'))        
    # check priority
    try:
        newPriority = long(newPriority)
    except:
        return pickle.dumps((False,'newPriority must be an integer'))        
    ret = userIF.changeTaskPriority(jediTaskID,newPriority)
    return pickle.dumps(ret)



# increase attempt number for unprocessed files
def increaseAttemptNrPanda(req,jediTaskID,increasedNr):
    # check security
    if not isSecure(req):
        return pickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # only prod managers can use this method
    ret = None
    if not prodRole:
        ret = 3,"production or pilot role required"
    # check jediTaskID
    if ret == None:
        try:
            jediTaskID = long(jediTaskID)
        except:
            ret = 4,'jediTaskID must be an integer'
    # check increase
    if ret == None:
        wrongNr = False
        try:
            increasedNr = long(increasedNr)
        except:
            wrongNr = True
        if wrongNr or increasedNr<0:
            ret = 4,'increase must be a positive integer'
    # exec
    if ret == None:
        ret = userIF.increaseAttemptNrPanda(jediTaskID,increasedNr)
    return pickle.dumps(ret)



# change task attribute
def changeTaskAttributePanda(req,jediTaskID,attrName,attrValue):
    # check security
    if not isSecure(req):
        return pickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # only prod managers can use this method
    if not prodRole:
        return pickle.dumps((False,"production or pilot role required"))
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except:
        return pickle.dumps((False,'jediTaskID must be an integer'))        
    # check attribute
    if not attrName in ['ramCount','wallTime','cpuTime','coreCount']:
        return pickle.dumps((2,"disallowed to update {0}".format(attrName)))
    ret = userIF.changeTaskAttributePanda(jediTaskID,attrName,attrValue)
    return pickle.dumps((ret,None))



# change split rule for task
def changeTaskSplitRulePanda(req,jediTaskID,attrName,attrValue):
    # check security
    if not isSecure(req):
        return pickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # only prod managers can use this method
    if not prodRole:
        return pickle.dumps((False,"production or pilot role required"))
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except:
        return pickle.dumps((False,'jediTaskID must be an integer'))        
    # check attribute
    if not attrName in ['TW','EC','ES','MF','NG']:
        return pickle.dumps((2,"disallowed to update {0}".format(attrName)))
    ret = userIF.changeTaskSplitRulePanda(jediTaskID,attrName,attrValue)
    return pickle.dumps((ret,None))



# pause task
def pauseTask(req,jediTaskID):
    # check security
    if not isSecure(req):
        return pickle.dumps((False, 'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except:
        return pickle.dumps((False, 'jediTaskID must be an integer'))
    ret = userIF.pauseTask(jediTaskID,user,prodRole)
    return pickle.dumps(ret)



# resume task
def resumeTask(req,jediTaskID):
    # check security
    if not isSecure(req):
        return pickle.dumps((False, 'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except:
        return pickle.dumps((False, 'jediTaskID must be an integer'))
    ret = userIF.resumeTask(jediTaskID,user,prodRole)
    return pickle.dumps(ret)



# force avalanche for task
def avalancheTask(req,jediTaskID):
    # check security
    if not isSecure(req):
        return pickle.dumps((False, 'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except:
        return pickle.dumps((False, 'jediTaskID must be an integer'))
    ret = userIF.avalancheTask(jediTaskID,user,prodRole)
    return pickle.dumps(ret)



# kill unfinished jobs
def killUnfinishedJobs(req,jediTaskID,code=None,useMailAsID=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodManager = False
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in ['/atlas/usatlas/Role=production','/atlas/Role=production']:
            if fqan.startswith(rolePat):
                prodManager = True
                break
        # escape
        if prodManager:
            break
    # use email address as ID
    if useMailAsID == 'True':
        useMailAsID = True
    else:
        useMailAsID = False
    # hostname
    host = req.get_remote_host()
    # get PandaIDs
    ids = userIF.getPandaIDsWithTaskID(jediTaskID)
    # kill
    return userIF.killJobs(ids,user,host,code,prodManager,useMailAsID,fqans)



# change modificationTime for task
def changeTaskModTimePanda(req,jediTaskID,diffValue):
    # check security
    if not isSecure(req):
        return pickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # only prod managers can use this method
    if not prodRole:
        return pickle.dumps((False,"production or pilot role required"))
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except:
        return pickle.dumps((False,'jediTaskID must be an integer'))
    try:
        diffValue = int(diffValue)
        attrValue = datetime.datetime.now() + datetime.timedelta(hours=diffValue)
    except:
        return pickle.dumps((False,'failed to convert {0} to time diff'.format(diffValue)))
    ret = userIF.changeTaskAttributePanda(jediTaskID,'modificationTime',attrValue)
    return pickle.dumps((ret,None))



# get PandaIDs with TaskID
def getPandaIDsWithTaskID(req,jediTaskID):
    try:
        jediTaskID = long(jediTaskID)
    except:
        return pickle.dumps((False,'jediTaskID must be an integer'))
    idsStr = userIF.getPandaIDsWithTaskID(jediTaskID)
    # deserialize
    ids = WrappedPickle.loads(idsStr)

    return pickle.dumps(ids)



# reactivate Task
def reactivateTask(req,jediTaskID):
    # check security
    if not isSecure(req):
        return pickle.dumps((False,'secure connection is required'))

    try:
        jediTaskID = long(jediTaskID)
    except:
        return pickle.dumps((False,'jediTaskID must be an integer'))
    ret = userIF.reactivateTask(jediTaskID)

    return pickle.dumps(ret)



# get task status
def getTaskStatus(req,jediTaskID):
    try:
        jediTaskID = long(jediTaskID)
    except:
        return pickle.dumps((False,'jediTaskID must be an integer'))
    ret = userIF.getTaskStatus(jediTaskID)
    return pickle.dumps(ret)


# reassign share
def reassignShare(req, jedi_task_ids_pickle, share):
    # check security
    if not isSecure(req):
        return pickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)
    # check role
    prod_role = _isProdRoleATLAS(req)
    if not prod_role:
        return pickle.dumps((False,"production or pilot role required"))

    jedi_task_ids = WrappedPickle.loads(jedi_task_ids_pickle)
    _logger.debug('reassignShare: jedi_task_ids: {0}, share: {1}'.format(jedi_task_ids, share))

    if not ((isinstance(jedi_task_ids, list) or (isinstance(jedi_task_ids, tuple)) and isinstance(share, str))):
        return pickle.dumps((False, 'jedi_task_ids must be tuple/list and share must be string'))

    ret = userIF.reassignShare(jedi_task_ids, share)
    return pickle.dumps(ret)


# list tasks in share
def listTasksInShare(req, gshare, status):
    # check security
    if not isSecure(req):
        return pickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)
    # check role
    prod_role = _isProdRoleATLAS(req)
    if not prod_role:
        return pickle.dumps((False,"production or pilot role required"))

    _logger.debug('listTasksInShare: gshare: {0}, status: {1}'.format(gshare, status))

    if not ((isinstance(gshare, str) and isinstance(status, str))):
        return pickle.dumps((False, 'gshare and status must be of type string'))

    ret = userIF.listTasksInShare(gshare, status)
    return pickle.dumps(ret)

# get taskParamsMap with TaskID
def getTaskParamsMap(req,jediTaskID):
    try:
        jediTaskID = long(jediTaskID)
    except:
        return pickle.dumps((False,'jediTaskID must be an integer'))
    ret = userIF.getTaskParamsMap(jediTaskID)
    return pickle.dumps(ret)

# update workers
def updateWorkers(req,harvesterID,workers):
    # check security
    if not isSecure(req):
        return json.dump((False,"SSL is required"))
    # get DN
    user = _getDN(req)        
    # hostname
    host = req.get_remote_host()
    # convert
    try:
        data = json.loads(workers)
    except:
        return json.dumps((False,"failed to load JSON"))
    # update
    return userIF.updateWorkers(user,host,harvesterID,data)


# heartbeat for harvester
def harvesterIsAlive(req,harvesterID,data=None):
    # check security
    if not isSecure(req):
        return json.dump((False,"SSL is required"))
    # get DN
    user = _getDN(req)        
    # hostname
    host = req.get_remote_host()
    # convert
    try:
        if data is not None:
            data = json.loads(data)
        else:
            data = dict()
    except:
        return json.dumps((False,"failed to load JSON"))
    # update
    return userIF.harvesterIsAlive(user,host,harvesterID,data)


# report stat of workers
def reportWorkerStats(req, harvesterID, siteName, paramsList):
    # check security
    if not isSecure(req):
        return json.dumps((False,"SSL is required"))
    # update
    ret = userIF.reportWorkerStats(harvesterID, siteName, paramsList)
    return json.dumps(ret)
