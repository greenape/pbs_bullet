import logging, sys
logging.basicConfig()
logger = logging.getLogger('pbs_bullet.watcher')
try:
    from subprocess32 import check_output, call
except:
    logger.error("pbs_bullet uses the check_output command, added in python 2.7.")
    sys.exit(1)
from notify import Notifier


class Watcher(object):
    def __init__(self, jobid, qstat, qdel, showstart, events, lowmem=0., pb_token=None):
        self.jobid = jobid
        self.qstat_cmd = qstat + [jobid]
        self.qdel_cmd = qdel + [jobid]
        self.showstart_cmd = showstart + [jobid]
        self.lowmem = lowmem
        self.events = events

        self.started = False
        self.finished = False
        self.notifier = None
        self.update()
        self.started = False  # Need to reset after qstat to get job details
        self.jobname = self.jobdetails['Job_Name']
        if pb_token:
            self.notifier = self.set_notifier(pb_token)

    def set_notifier(self, pb_token, name=None):
        """
        Add a pushbullet notifier to the job.
        There can only be one notifier.

        """
        self.remove_notifier()
        if name is None:
            name = "%s - %s" % (self.jobname, self.jobid)
        self.notifier = Notifier(name, pb_token)

    def remove_notifier(self):
        """
        Remove pushbullet notifier if it exists.
        """
        if self.notifier:
            self.notifier.delete_listener()
        self.notifier = None

    def update(self):
        """
        Poll this job.
        Checks run status, updates details, sends any relevant notifications,
        checks for and acts on pushes, and kills itself if required.
        """
        self.jobdetails = self.qstat()
        # Check and update run/finish status
        if self.jobdetails['job_state'] == 'R':
            if self.started:
                # Check memory use
                self.memory_safety()
            else:
                self.started = True
                self.nodes = self.get_nodes()
                if self.notifier and "start" in self.events:
                    self.start_notify()
        elif self.jobdetails['job_state'] != 'R' and self.started:
            # Job finished. Notify if appropriate
            self.finished = True
            if self.notifier and "finish" in self.events:
                self.finish_notify()
        # Check for and act on pushes
        if self.notifier:
            map(self.parse_push, self.notifier.check_pushes())

    def memory_safety(self):
        """
        Run a check on available node memory, kill the 
        job if it falls below threshold on any one node.
        """
        try:
            logger.debug("Checking memory on %s" % ", ".join(self.nodes))
            self.freemem = self.check_free()
            logger.debug(self.make_free_str())
            if filter(lambda (node, mem): float(mem) < self.lowmem, self.freemem):
                logger.debug("Free memory below threshold. Killing the job.")
                try:
                    self.kill_job()
                except Exception as e:
                    logger.error("qdel command failed.")
                    logger.error('Error was:')
                    logger.error(e)
                if self.notifier and "kill" in self.events:
                    self.kill_notify()
        except Exception as e:
            logger.error("Freemem check failed.")
            logger.error(e)


    @staticmethod
    def parse_job(jobdetails):
        """
        Turn the output of qstat -f into a dictionary.
        """
        lines = jobdetails.replace("\n\t", "").splitlines()[1:-1]
        return dict(map(lambda line: tuple(line.strip().split(" = ")), lines))

    def qstat(self):
        """
        Output of qstat command munged into a dictionary.
        """
        try:
            logger.debug("Checking status for job %s" % self.jobid)
            jobdetails = self.parse_job(check_output(self.qstat_cmd, timeout=60))
        except Exception as e:
            logger.error('qstat command failed. Bailing out.')
            logger.error('Error was:')
            logger.error(e)
            if self.notifier and "error" in self.events:
                self.error_notify(e)
            raise
        return jobdetails


    def get_nodes(self):
        """
        Return a list of the nodes in use from a job dictionary.
        """

        nodes = self.jobdetails['exec_host']
        return list(set(map(lambda x: x.split('/')[0], nodes.split('+'))))

    def kill_job(self):
        """
        Attempt to kill a job using qdel.
        """

        return call(self.qdel_cmd)


    def check_free(self):
        """
        Return a list of tuples where each one is a node, and the % free memory.
        """
        return zip(self.nodes, map(self._check_free, self.nodes))

    @staticmethod
    def _check_free(node):
        """
        Use rsh and free to get the percentage of free memory on a node.
        """

        return check_output(["rsh", node, "free", "|",  "awk",  "'FNR == 3 {print $4/($3+$4)*100}'"], timeout=60)

    def make_free_str(self):
        return "Free memory - %s" % ", ".join(map(lambda (node, free): "%s: %s%%" % (node, free.strip()), self.freemem))

    def start_notify(self):
        """
        Send a notification that the job has started.
        """
        title = "%s, id: %s, started." % (self.jobname, str(self.jobid))
        body = "Running on nodes %s, and started %s." % (", ".join(self.nodes), self.jobdetails['start_time']) 
        self.notifier.send_notification(title, body)

    def error_notify(self, error):
        """
        Send a notification that the watcher has hit an error.
        """
        title = "%s, id: %s, watcher error." % (self.jobname, str(self.jobid))
        body = str(error)
        self.notifier.send_notification(title, body)

    def finish_notify(self):
        """
        Send a notification that the job has completed.
        """
        title = "%s, id: %s, finished." % (self.jobname, str(self.jobid))
        body = ""
        self.notifier.send_notification(title, body)

    def kill_notify(self):
        """
        Send a notification that the job is being killed.
        """
        title = "Attempting to kill job %s, id: %s." % (self.jobname, str(self.jobid))
        body = self.make_free_str()
        self.notifier.send_notification(title, body)

    @staticmethod
    def mkwalltime(seconds):
        """
        Make an hours:minutes:seconds string from seconds.
        """
        seconds = int(seconds)
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        return "%d:%02d:%02d" % (h, m, s)

    def parse_push(self, push):
        """
        Take a push, and execute some commands.
        This is very primitive - we look for preset strings
        in the body of the push.
        """
        logger.debug("Parsing push msg.")

        try:
            cmd = push['body'].lower()
            logger.debug(cmd)
            logger.debug(push)
            try:
                target = push['source_device_iden']
            except KeyError:
                logger.debug("No specific device to send to.")
                target = None
            commands = []
            if 'showstart' in cmd:
                # Return the starttime for this job.
                try:
                    body = check_output(self.showstart_cmd, timeout=60)
                    title = "Job %s (%s) Start Time" % (self.jobname, self.jobid)
                except Exception as e:
                    body = str(e)
                    title = "Showstart failed."
                self.notifier.send_notification(title, body, target=target)
                commands.append('showstart')
            if 'walltime' in cmd:
                # Return the starttime for this job.
                try:
                    remaining = self.mkwalltime(self.jobdetails['Walltime.Remaining'])
                    requested = self.jobdetails['Resource_List.walltime']
                    body = "%s walltime left of %s" % (remaining, requested)
                    title = "Job %s (%s) Remaining walltime" % (self.jobname, self.jobid)
                except Exception as e:
                    body = str(e)
                    title = "Walltime failed."
                self.notifier.send_notification(title, body, target=target)
                commands.append('walltime')
            if 'cancel' in cmd:
                # Cancel the job
                try:
                    self.kill_job()
                    self.kill_notify()
                except Exception as e:
                    body = str(e)
                    title = "qdel failed."
                    self.notifier.send_notification(title, body, target=target)
                commands.append('cancel')
            if 'freemem' in cmd:
                # Get the free memory for nodes
                try:
                    body = self.make_free_str()
                    title = "Job %s (%s) Free Memory" % (self.jobname, self.jobid)
                except Exception as e:
                    body = str(e)
                    title = "Freemem check failed."
                self.notifier.send_notification(title, body, target=target)
                commands.append('freemem')
            if 'killwatcher' in cmd:
                #Kill this watcher.
                raise Exception('Killed by Push command.')
            assert commands
        except KeyError as e:
            logger.debug(e)
            logger.debug("No body in this push.")
        except AssertionError:
            logger.debug("No commands in this push.")