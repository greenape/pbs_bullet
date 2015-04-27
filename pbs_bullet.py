"""
pbs_bullet

A script to keep an eye on HPC jobs using the PBS system, and
send pushbullet notifications on a few events.

I have not tested this. Use with extreme caution.

Jono Gray
j.gray@soton.ac.uk
"""

import sys
import subprocess
import argparse
import json
import urllib2, urllib
from time import sleep

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

    return subprocess.call(['qdel', jobid])

def check_free(node):
    """
    Use rsh and free to get the percentage of free memory on a node.
    """

    return subprocess.check_output(["rsh", node, "free", "|",  "awk",  "'FNR == 3 {print $4/($3+$4)*100}'"])

def start_notify(jobid, jobdetails, nodes, pb_token):
    title = "%s, id: %s, started." % (jobdetails['Job_Name'], str(jobid))
    body = "Running on nodes %s, and due to finish %s." % (", ".join(nodes), jobdetails['etime']) 
    send_notification(title, body, pb_token)

def finish_notify(jobid, jobdetails, pb_token):
    title = "%s, id: %s, finished." % (jobdetails['Job_Name'], str(jobid))
    body = ""
    send_notification(title, body, pb_token)

def kill_notify(jobid, jobdetails, nodes, freemem, pb_token):
    title = "Attempting to kill job %s, id: %s." % (jobdetails['Job_Name'], str(jobid))
    body = ["Free memory on nodes: "]
    body += map(lambda (node, mem): "%s - %f\%", zip(nodes, freemem))
    send_notification(title, "\n".join(body), pb_token)

def send_notification(title, body, token):
    """
    Send a pushbullet notification.
    """
    note = json.dumps({"type":"note", "title": title, "body": body})

    request = urllib2.Request('https://api.pushbullet.com/v2/pushes', note, headers={
        'Authorization':"Bearer %s" % token,
        'Content-Type':'application/json',
        'Accept':'*/*'
    })
    urllib2.urlopen(request)

if __name__ == "__main__":
    args = arguments()
    jobid = args.jobid
    pb_token = args.pb_token
    sleep_time = args.poll_interval
    notify_on = args.notify_on
    lowmem = args.kill_threshold

    started = False
    finished = False

    while not finished:
        jobdetails = parse_job(subprocess.check_output(['qstat', '-f', jobid]))
        if jobdetails['job_state'] == 'R':
            nodes = get_nodes(jobdetails)
            if not started:
                started = True

                if pb_token is not None and "start" in notify_on:
                    start_notify(jobid, jobdetails, nodes, pb_token)

            #Check memory use
            freemem = map(free, nodes)
            if not filter(lambda x: float(x) < lowmem, freemem).empty():
                kill_job(jobid)
                if pb_token is not None and "kill" in notify_on:
                    kill_notify(jobid, jobdetails, nodes, freemem, pb_token)

        elif jobdetails['job_state'] != 'R' and started:
            #Job finished. Notify if appropriate
            finished = True
            if pb_token is not None and "finish" in notify_on:
                finish_notify(jobid, jobdetails, pb_token)
        sleep(sleep_time)