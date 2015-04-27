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
import json
import urllib2, urllib
import logging
from time import sleep

logging.basicConfig()
logger = logging.getLogger(__name__)

try:
    from subprocess import check_output, call
except:
    logger.error("pbs_bullet uses the check_output command, added in python 2.7.")
    sys.exit(1)

def arguments():
    parser = argparse.ArgumentParser(
        description="Watch a PBS job. Can optionally send notifications by pushbullet, and kill the job if low on memory.")
    parser.add_argument('jobid', help='Job id to monitor.')
    parser.add_argument('--pushbullet-token', type=str, dest="pb_token", default=None)
    parser.add_argument('--notify-on', type=str, nargs='*', dest="notify_on",
        choices=["start", "finish", "killed", "fail"], default=["start", "finish", "killed", "fail"],
        help="Events to send a notification on.")
    parser.add_argument('--low-mem', type=float, dest="kill_threshold", default=0.,
        help="Kill the job using qdel if free memory on any one node drops below this \%.")
    parser.add_argument('--poll-interval', type=int, dest="poll_interval", default=300,
        help="Number of seconds to wait between each check. Defaults to 5 minutes.")
    parser.add_argument('--log-level', dest='log_level', type=str, choices=['debug',
        'info', 'warning', 'error'], default='info', nargs="?")
    args, extras = parser.parse_known_args()
    return args


def parse_job(jobdetails):
    """
    Turn the output of qstat -f into a dictionary.
    """
    lines = jobdetails.replace("\n\t", "").splitlines()[1:-1]
    return dict(map(lambda line: tuple(line.strip().split(" = ")), lines))

def get_nodes(job_dict):
    """
    Return a list of the nodes in use from a job dictionary.
    """

    nodes = job_dict['exec_host']
    return set(map(lambda x: x.split('/')[0], nodes.split('+')))

def kill_job(jobid):
    """
    Attempt to kill a job using qdel.
    """

    return call(['qdel', jobid])

def check_free(node):
    """
    Use rsh and free to get the percentage of free memory on a node.
    """

    return check_output(["rsh", node, "free", "|",  "awk",  "'FNR == 3 {print $4/($3+$4)*100}'"])

def start_notify(jobid, jobdetails, nodes, pb_token):
    """
    Send a notification that the job has started.
    """
    title = "%s, id: %s, started." % (jobdetails['Job_Name'], str(jobid))
    body = "Running on nodes %s, and due to finish %s." % (", ".join(nodes), jobdetails['etime']) 
    send_notification(title, body, pb_token)

def finish_notify(jobid, jobdetails, pb_token):
    """
    Send a notification that the job has completed.
    """
    title = "%s, id: %s, finished." % (jobdetails['Job_Name'], str(jobid))
    body = ""
    send_notification(title, body, pb_token)

def kill_notify(jobid, jobdetails, nodes, freemem, pb_token):
    """
    Send a notification that the job is being killed.
    """
    title = "Attempting to kill job %s, id: %s." % (jobdetails['Job_Name'], str(jobid))
    body = ["Free memory on nodes: "]
    body += map(lambda (node, mem): "%s - %f\%", zip(nodes, freemem))
    send_notification(title, "\n".join(body), pb_token)

def send_notification(title, body, token, target=None):
    """
    Send a pushbullet notification.
    """
    data = {"type":"note", "title": title, "body": body}
    if target is not None:
        data['device_iden'] = target
    note = json.dumps(data)
    logger.debug("Sending %s to pushbullet." % note)
    request = urllib2.Request('https://api.pushbullet.com/v2/pushes', note, headers={
        'Authorization':"Bearer %s" % token,
        'Content-Type':'application/json',
        'Accept':'*/*'
    })
    try:
        resp = urllib2.urlopen(request)
    except urllib2.HTTPError as e:
        logger.error("Pushbullet notify error.")
        logger.error(e.read())

def create_listener(name, token):
    """
    Register this device to receive pushbullet notifications,
    and if successful return the identifier.
    """
    data = urllib.urlencode({'nickname':name, 'type':'streaming'})
    logger.debug("Adding %s to pushbullet." % name)
    request = urllib2.Request('https://api.pushbullet.com/v2/devices', data, headers={
        'Authorization':"Bearer %s" % token,
        'Accept':'*/*'
    })
    try:
        resp = urllib2.urlopen(request)
        return json.load(resp)['iden']
    except urllib2.HTTPError as e:
        logger.error("Pushbullet register error.")
        logger.error(e.read())

def delete_listener(iden, token):
    """
    Unregister this device as a listener.
    """
    logger.debug("Deleting %s from pushbullet." % iden)
    request = urllib2.Request('https://api.pushbullet.com/v2/devices/%s' % iden, headers={
        'Authorization':"Bearer %s" % token,
        'Accept':'*/*'
    })
    request.get_method = lambda: 'DELETE'
    try:
        resp = urllib2.urlopen(request)
        return json.load(resp)
    except urllib2.HTTPError as e:
        logger.error("Pushbullet delete error.")
        logger.error(e.read())

def delete_push(push, token):
    """
    Delete a push.
    """
    logger.debug("Deleting push id %s from pushbullet." % push['iden'])
    request = urllib2.Request('https://api.pushbullet.com/v2/pushes/%s' % push['iden'], headers={
        'Authorization':"Bearer %s" % token,
        'Accept':'*/*'
    })
    request.get_method = lambda: 'DELETE'
    try:
        resp = urllib2.urlopen(request)
        return json.load(resp)
    except urllib2.HTTPError as e:
        logger.error("Pushbullet delete error.")
        logger.error(e.read())


def check_pushes(iden, token):
    """
    Return any undismissed pushes for this device, and dismiss
    them.
    """
    data = urllib.urlencode({'active':'0'})
    logger.debug("Checking pushes for %s." % iden)
    request = urllib2.Request('https://api.pushbullet.com/v2/pushes?%s' % data, headers={
        'Authorization':"Bearer %s" % token,
        'Accept':'*/*'
    })
    try:
        resp = urllib2.urlopen(request)
        pushes = json.load(resp)['pushes']
        pushes = filter(lambda push: 'target_device_iden' in push.keys() and push['target_device_iden'] == iden, pushes)
        logger.debug("Got %d pushes." % len(pushes))
        #Delete them from the server
        map(lambda push: delete_push(push, token), pushes)
        pushes.reverse()
        return pushes

    except urllib2.HTTPError as e:
        logger.error("Pushbullet check error.")
        logger.error(e.read())

def parse_push(push, token, jobid, jobdetails):
    """
    Take a push, and execute some commands.
    This is very primitive - we look for preset strings
    in the body of the push.
    """
    logger.debug("Parsing push msg.")

    try:
        cmd = push['body'].lower()
        logger.debug(cmd)
        target = push['source_device_iden']
        commands = []
        if 'showstart' in cmd:
            # Return the starttime for this job.
            body = check_output(['showstart', jobid])
            title = "Job %s (%s) Start Time" % (jobdetails['Job_Name'], jobid)
            send_notification(title, body, token, target=target)
            commands.append('showstart')
        if 'cancel' in cmd:
            # Cancel the job
            kill_job(jobid)
            commands.append('cancel')
        if 'freemem' in cmd:
            # Get the free memory for nodes
            nodes = get_nodes(jobdetails)
            freemem = map(free, nodes)
            body = "Free memory - %s" % ", ".join(map(lambda (node, free): "%s: %f/%" % (node, free), zip(nodes, freemem)))
            title = "Job %s (%s) Free Memory" % (jobdetails['Job_Name'], jobid)
            send_notification(title, body, token, target=target)
            commands.append('freemem')
        assert not commands.empty()
    except KeyError:
        logger.debug("No body in this push.")
    except AssertionError:
        logger.debug("No commands in this push.")


def main():
    args = arguments()
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % log_level)

    logger.setLevel(numeric_level)
    jobid = args.jobid
    pb_token = args.pb_token
    sleep_time = args.poll_interval
    notify_on = args.notify_on
    lowmem = args.kill_threshold

    started = False
    finished = False

    if pb_token is not None:
        try:
            logger.debug("Checking status for job %s" % jobid)
            jobdetails = parse_job(check_output(['qstat', '-f', jobid]))
        except Exception as e:
            logger.error('qstat command failed. Bailing out.')
            logger.error('Error was:')
            logger.error(e)
            raise
        name = "%s - %s" % (jobdetails['Job_Name'], jobid)
        iden = create_listener(name, pb_token)

    while not finished:
        try:
            logger.debug("Checking status for job %s" % jobid)
            jobdetails = parse_job(check_output(['qstat', '-f', jobid]))
        except Exception as e:
            logger.error('qstat command failed. Bailing out.')
            logger.error('Error was:')
            logger.error(e)
            if pb_token is not None:
                delete_listener(iden, pb_token)
            break
        if jobdetails['job_state'] == 'R':
            logger.debug("Job %s is running." % jobid)
            nodes = get_nodes(jobdetails)
            if not started:
                started = True

                if pb_token is not None and "start" in notify_on:
                    start_notify(jobid, jobdetails, nodes, pb_token)

            #Check memory use
            try:
                logger.debug("Checking memory on %s" % ", ".join(nodes))
                freemem = map(free, nodes)
                logger.debug("Free memory - %s" % ", ".join(map(lambda (node, free): "%s: %f/%" % (node, free), zip(nodes, freemem))))
                if not filter(lambda x: float(x) < lowmem, freemem).empty():
                    logger.debug("Free memory below threshold. Killing the job.")
                    try:
                        kill_job(jobid)
                    except Exception as e:
                        logger.error("qdel command failed.")
                        logger.error('Error was:')
                        logger.error(e)
                    if pb_token is not None and "kill" in notify_on:
                        kill_notify(jobid, jobdetails, nodes, freemem, pb_token)
            except Exception as e:
                logger.error("Freemem check failed.")
                logger.error(e)

        elif jobdetails['job_state'] != 'R' and started:
            #Job finished. Notify if appropriate
            finished = True
            if pb_token is not None and "finish" in notify_on:
                finish_notify(jobid, jobdetails, pb_token)
            break

        #Check for pushed commands 
        if pb_token is not None:
            map(lambda push: parse_push(push, pb_token, jobid, jobdetails), check_pushes(iden, pb_token))
        logger.debug("Sleeping for %ds" % sleep_time)
        sleep(sleep_time)
    if pb_token is not None:
        delete_listener(iden, pb_token)

if __name__ == "__main__":
    main()