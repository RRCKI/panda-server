import re
import time
import datetime
from threading import Lock

from config import panda_config
from pandalogger.PandaLogger import PandaLogger
f = open('/var/log/panda/debuggin', 'a+')
f.write('test1')
print 'test1''
f.close()
from taskbuffer.TaskBuffer import taskBuffer
f = open('/var/log/panda/debuggin', 'a+')
f.write('test2')
f.close()
# Definitions
EXECUTING = 'executing'
QUEUED = 'queued'
PLEDGED = 'pledged'
IGNORE = 'ignore'


class Singleton(type):
    """
    Meta class singleton implementation, as described here:
    https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)

        return cls._instances[cls]


class Node(object):

    def __init__(self):
        self.children = []

    def add_child(self, node):
        self.children.append(node)

    def get_leaves(self, leaves=[]):

        # If the node has no leaves, return the node in a list
        if not self.children:
            leaves.append(self)
            return leaves

        # Recursively get to the bottom
        for child in self.children:
            child.get_leaves(leaves)

        return leaves


class Share(Node):
    """
    Implement the share node
    """
    def __str__(self, level=0):
        """
        Print the tree structure
        """
        ret = "{0} name: {1}, value: {2}\n".format('\t' * level, self.name, self.value)
        for child in self.children:
            ret += child.__str__(level + 1)
        return ret

    def __repr__(self):
        return self.__str__()

    def __mul__(self, other):
        """
        If I multiply a share object by a number, multiply the value field
        """
        self.value *= other
        return self.value

    def __rmul__(self, other):
        return self.__mul__

    def __imul__(self, other):
        return self.__mul__

    def __init__(self, name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype):
        Node.__init__(self)
        self.name = name
        self.value = value
        self.parent = parent
        self.prodsourcelabel = prodsourcelabel
        self.workinggroup = workinggroup
        self.campaign = campaign
        self.processingtype = processingtype

    def normalize(self, multiplier=100, divider=100):
        """
        Will run down the branch and normalize values beneath
        """
        self.value *= (multiplier * 1.0 / divider)
        if not self.children:
            return

        divider = 0
        for child in self.children:
            divider += child.value

        multiplier = self.value

        for child in self.children:
            child.normalize(multiplier=multiplier, divider=divider)

        return

    def sort_branch_by_current_hs_distribution(self, hs_distribution):
        """
        Runs down the branch in order of under-pledging. It returns a list of sorted leave shares
        """
        sorted_shares = []

        # If the node has no leaves, return the node in a list
        if not self.children:
            sorted_shares = [self]
            return sorted_shares

        # If the node has leaves, sort the children
        children_sorted = []
        for child1 in self.children:
            loop_index = 0
            insert_index = len(children_sorted)  # insert at the end, if not deemed otherwise

            # Calculate under-pledging
            child1_under_pledge = hs_distribution[child1.name][PLEDGED] - hs_distribution[child1.name][EXECUTING]
            for child2 in children_sorted:
                try:
                    # Calculate under-pledging
                    child2_under_pledge = hs_distribution[child2.name][PLEDGED] \
                                          - hs_distribution[child2.name][EXECUTING]
                except KeyError:
                    continue

                if child1_under_pledge > child2_under_pledge:
                    insert_index = loop_index
                    break

                loop_index += 1

            # Insert the child into the list
            children_sorted.insert(insert_index, child1)

        # Go recursively and sort the grand* children
        for child in children_sorted:
            sorted_shares.extend(child.sort_branch_by_current_hs_distribution(hs_distribution))

        return sorted_shares

    def aggregate_hs_distribution(self, hs_distribution):
        """
        We have the current HS distribution values for the leaves, but want to propagate it updwards to the parents.
        We will traverse the tree from top to bottom and bring up the aggregated values.
        """
        executing, queued, pledged = 0, 0, 0

        # If the node has no children, it's a leave and should have an entry in the hs_distribution
        if not self.children:
            try:
                executing = hs_distribution[self.name][EXECUTING]
                queued = hs_distribution[self.name][QUEUED]
                pledged = hs_distribution[self.name][PLEDGED]
            except KeyError:
                pass

            return executing, queued, pledged

        # If the node has children, sum up the values of the children
        executing = 0
        queued = 0
        pledged = 0

        for child in self.children:
            executing_child, queued_child, pledged_child = child.aggregate_hs_distribution(hs_distribution)
            executing += executing_child
            queued += queued_child
            pledged += pledged_child

        # Add the aggregated value to the map
        hs_distribution[self.name] = {
                                       EXECUTING: executing,
                                       QUEUED: queued,
                                       PLEDGED: pledged
                                     }

        # Return the aggregated values
        return executing, queued, pledged


class GlobalShares:
    """
    Class to manage the tree of shares
    """
    __metaclass__ = Singleton

    def __init__(self):

        global _logger
        _logger = PandaLogger().getLogger('GlobalShares')

        # task buffer is imported here to avoid circular import between global shares and oradbproxy
        self.lock = Lock()

        # TODO: Ask Tadashi for advise, whether I need a lock here as well
        t_before = time.time()
        # Initialize DB connection

        if not (hasattr(taskBuffer, 'proxyPool') and taskBuffer.proxyPool):
            taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd)
        self.__task_buffer = taskBuffer
        t_after = time.time()
        total = t_after - t_before
        _logger.debug('Getting a taskbuffer instance took {0}s'.format(total))

        self.tree = None # Pointer to the root of the global shares tree
        self.leave_shares = None # Pointer to the list with leave shares
        self.__t_update_shares = None # Timestamp when the shares were last updated
        self.__hs_distribution = None # HS06s distribution of sites
        self.__t_update_distribution = None  # Timestamp when the HS06s distribution was last updated

        self.__reload_shares()
        self.__reload_hs_distribution()

    def __get_hs_leave_distribution(self, leave_shares):
        """
        Get the current HS06 distribution for running and queued jobs
        """
        comment = ' /* GlobalShares.get_hs_leave_distribution */'

        sql_hs_distribution = """
            SELECT gshare, jobstatus_grouped, SUM(HS)
            FROM
                (SELECT gshare, HS,
                     CASE
                         WHEN jobstatus IN('activated') THEN 'queued'
                         WHEN jobstatus IN('sent', 'starting', 'running', 'holding') THEN 'executing'
                         ELSE 'ignore'
                     END jobstatus_grouped
                 FROM ATLAS_PANDA.JOBS_SHARE_STATS JSS)
            GROUP BY gshare, jobstatus_grouped
            """

        proxy = self.__task_buffer.proxyPool.getProxy()
        hs_distribution_raw = proxy.querySQL(sql_hs_distribution + comment)
        self.__task_buffer.proxyPool.putProxy(proxy)

        # get the hs distribution data into a dictionary structure
        hs_distribution_dict = {}
        hs_queued_total = 0
        hs_executing_total = 0
        hs_ignore_total = 0
        for hs_entry in hs_distribution_raw:
            gshare, status_group, hs = hs_entry
            hs_distribution_dict.setdefault(gshare, {PLEDGED: 0, QUEUED: 0, EXECUTING: 0})
            hs_distribution_dict[gshare][status_group] = hs
            # calculate totals
            if status_group == QUEUED:
                hs_queued_total += hs
            elif status_group == EXECUTING:
                hs_executing_total += hs
            else:
                hs_ignore_total += hs

        # Calculate the ideal HS06 distribution based on shares.
        for share_node in leave_shares:
            share_name, share_value = share_node.name, share_node.value
            hs_pledged_share = hs_executing_total * share_value / 100.0

            hs_distribution_dict.setdefault(share_name, {PLEDGED: 0, QUEUED: 0, EXECUTING: 0})
            # Pledged HS according to global share definitions
            hs_distribution_dict[share_name]['pledged'] = hs_pledged_share
        return hs_distribution_dict

    # retrieve global shares
    def get_shares(self, parents=''):
        comment = ' /* GlobalShares.get_shares */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        tmpLog = LogWrapper(_logger, methodName)
        tmpLog.debug('start')


        sql  = """
               SELECT NAME, VALUE, PARENT, PRODSOURCELABEL, WORKINGGROUP, CAMPAIGN, PROCESSINGTYPE
               FROM ATLAS_PANDA.GLOBAL_SHARES
               """
        var_map = None

        if parents == '':
            # Get all shares
            pass
        elif parents is None:
            # Get top level shares
            sql += "WHERE parent IS NULL"

        elif type(parents) == str:
            # Get the children of a specific share
            var_map = {':parent': parents}
            sql += "WHERE parent = :parent"

        elif type(parents) in (list, tuple):
            # Get the children of a list of shares
            i = 0
            var_map = {}
            for parent in parents:
                key = ':parent{0}'.format(i)
                var_map[key] = parent
                i += 1

            parentBindings = ','.join(':parent{0}'.format(i) for i in xrange(len(parents)))
            sql += "WHERE parent IN ({0})".format(parentBindings)

        proxy = self.__task_buffer.proxyPool.getProxy()
        resList = proxy.querySQL(sql + comment, var_map)
        self.__task_buffer.proxyPool.putProxy(proxy)

        tmpLog.debug('done')
        return resList

    def __reload_shares(self, force = False):
        """
        Reloads the shares from the DB and recalculates distributions
        """

        # Acquire lock to prevent parallel reloads
        self.lock.acquire()

        # Don't reload shares every time
        if (self.__t_update_shares is not None and self.__t_update_shares > datetime.datetime.now() - datetime.timedelta(hours=1))\
                or force:
            self.lock.release()
            return

        # Root dummy node
        t_before = time.time()
        tree = Share('root', 100, None, None, None, None, None)
        t_after = time.time()
        total = t_after - t_before
        _logger.debug('Root dummy tree took {0}s'.format(total))

        # Get top level shares from DB
        t_before = time.time()
        shares_top_level = self.get_shares(parents=None)
        t_after = time.time()
        total = t_after - t_before
        _logger.debug('Getting shares took {0}s'.format(total))

        # Load branches
        t_before = time.time()
        for (name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype) in shares_top_level:
            share = Share(name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype)
            tree.children.append(self.__load_branch(share))
        t_after = time.time()
        total = t_after - t_before
        _logger.debug('Loading the branches took {0}s'.format(total))

        # Normalize the values in the database
        t_before = time.time()
        tree.normalize()
        t_after = time.time()
        total = t_after - t_before
        _logger.debug('Normalizing the values took {0}s'.format(total))

        # get the leave shares (the ones not having more children)
        t_before = time.time()
        leave_shares = tree.get_leaves()
        t_after = time.time()
        total = t_after - t_before
        _logger.debug('Getting the leaves took {0}s'.format(total))

        self.leave_shares = leave_shares
        self.__t_update_shares = datetime.datetime.now()

        # get the distribution of shares
        t_before = time.time()
        # Retrieve the current HS06 distribution of jobs from the database and then aggregate recursively up to the root
        hs_distribution = self.__get_hs_leave_distribution(leave_shares)
        tree.aggregate_hs_distribution(hs_distribution)
        t_after = time.time()
        total = t_after - t_before
        _logger.debug('Aggregating the hs distribution took {0}s'.format(total))

        self.tree = tree
        self.__hs_distribution = hs_distribution
        self.__t_update_distribution = datetime.datetime.now()
        self.lock.release()
        return

    def __reload_hs_distribution(self):
        """
        Reloads the HS distribution
        """

        # Acquire lock to prevent parallel reloads
        _logger.debug('lock')
        self.lock.acquire()

        _logger.debug(self.__t_update_distribution)
        _logger.debug(self.__hs_distribution)
        # Reload HS06s distribution every 10 seconds
        if self.__t_update_distribution is not None \
                and self.__t_update_distribution > datetime.datetime.now() - datetime.timedelta(seconds=10):
            self.lock.release()
            _logger.debug('release')
            return

        # Retrieve the current HS06 distribution of jobs from the database and then aggregate recursively up to the root
        _logger.debug('get dist')
        hs_distribution = self.__get_hs_leave_distribution(self.leave_shares)
        _logger.debug('aggr dist')
        self.tree.aggregate_hs_distribution(hs_distribution)
        t_after = time.time()
        total = t_after - t_before
        _logger.debug('Reloading the hs distribution took {0}s'.format(total))

        self.__hs_distribution = hs_distribution
        self.__t_update_distribution = datetime.datetime.now()

        self.lock.release()

        # log the distribution for debugging purposes
        _logger.info('Current HS06 distribution is {0}'.format(hs_distribution))

        return

    def get_sorted_leaves(self):
        """
        Re-loads the shares, then returns the leaves sorted by under usage
        """
        self.__reload_shares()
        _logger.debug('going to call reload dist')
        self.__reload_hs_distribution()
        _logger.debug('back from call')
        return self.tree.sort_branch_by_current_hs_distribution(self.__hs_distribution)

    def __load_branch(self, share):
        """
        Recursively load a branch
        """
        node = Share(share.name, share.value, share.parent, share.prodsourcelabel,
                     share.workinggroup, share.campaign, share.processingtype)

        children = self.__task_buffer.get_shares(parents=share.name)
        if not children:
            return node

        for (name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype) in children:
            child = Share(name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype)
            node.children.append(self.__load_branch(child))

        return node

    def compare_share_task(self, share, task):
        """
        Logic to compare the relevant fields of share and task
        """

        if share.prodsourcelabel is not None and re.match(share.prodsourcelabel, task.prodSourceLabel) is None:
            return False

        if share.workinggroup is not None and re.match(share.workinggroup, task.workingGroup) is None:
            return False

        if share.campaign is not None and re.match(share.campaign, task.campaign) is None:
            return False

        if share.processingtype is not None and re.match(share.processingtype, task.processingtype) is None:
            return False

        return True

    def get_share_for_task(self, task):
        """
        Return the share based on a task specification
        """

        selected_share_name = 'Undefined'

        for share in self.leave_shares:
            if self.compare_share_task(share, task):
                selected_share_name = share.name
                break

        if selected_share_name == 'Undefined':
            _logger.warning("No share matching jediTaskId={0} (prodSourceLabel={1} workingGroup={2} campaign={3} )".
                           format(task.jediTaskID, task.prodSourceLabel, task.workingGroup, task.campaign))

        return selected_share_name

    def is_valid_share(self, share_name):
        """
        Checks whether the share is a valid leave share
        """
        for share in self.leave_shares:
            if share_name == share.name:
                # Share found
                return True

        # Share not found
        return False