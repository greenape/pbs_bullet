"""
pbs_bullet

A script to keep an eye on HPC jobs using the PBS system, and
send pushbullet notifications on a few events.

I have not tested this. Use with extreme caution.

Jono Gray
j.gray@soton.ac.uk
"""

import sys
import argparse
import logging
from time import sleep

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('pbs_bullet')

from watcher import Watcher

def arguments():
    parser = argparse.ArgumentParser(
        description="Watch a PBS job. Can optionally send notifications by pushbullet, and kill the job if low on memory.")
    parser.add_argument('jobid', help='Job id to monitor.')
    parser.add_argument('--pushbullet-token', type=str, dest="pb_token", default=None)
    parser.add_argument('--notify-on', type=str, nargs='*', dest="notify_on",
        choices=["start", "finish", "killed"], default=["start", "finish", "killed"],
        help="Events to send a notification on.")
    parser.add_argument('--low-mem', type=float, dest="kill_threshold", default=0.,
        help="Kill the job using qdel if free memory on any one node drops below this \%.")
    parser.add_argument('--poll-interval', type=int, dest="poll_interval", default=300,
        help="Number of seconds to wait between each check. Defaults to 5 minutes.")
    parser.add_argument('--log-level', dest='log_level', type=str, choices=['debug',
        'info', 'warning', 'error'], default='info', nargs="?")
    parser.add_argument('--submit', dest='submit', action="store_true",
        help="If set, assumes a pbs script has been passed and attempt to submit it.")
    parser.add_argument('--qsub-cmd', dest='qsub_cmd', default=['qsub'],
        help="Specifies the command to use to submit a pbs job.")
    parser.add_argument('--qstat-cmd', dest='qstat_cmd', default=['qstat', '-f'],
        help="Specifies the qstat command to use for checking status.")
    parser.add_argument('--qdel-cmd', dest='qdel_cmd', default=['qdel'],
        help="Specifies the command to use to delete a pbs job.")
    args, extras = parser.parse_known_args()
    return args

def main():
    args = arguments()
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % log_level)

    #Get commands

    logger.setLevel(numeric_level)
    jobid = args.jobid
    if args.submit:
        try:
            jobin = jobid
            jobid = check_output(args.qsub + jobid)
            jobid = jobid.strip().split(".")[0]
            logger.info("Submitted %s, got id %s" % (jobin, jobid))
        except Exception as e:
            logger.error("Failed to submit %s" % jobin)
            logger.error("Bailing out.")
            raise

    pb_token = args.pb_token
    sleep_time = args.poll_interval
    events = args.notify_on
    lowmem = args.kill_threshold

    try:
        # Create the job object
        job = Watcher(jobid, args.qstat, args.qdel, args.showstart, events, lowmem=lowmem)
        # Set a notifier for it
        if pb_token:
            job.set_notifier(pb_token)
        while not finished:
            job.update()
            logger.debug("Sleeping for %ds" % sleep_time)
            sleep(sleep_time)
    except Exception:
        raise
    finally:
        job.remove_notifier()

if __name__ == "__main__":
    main()