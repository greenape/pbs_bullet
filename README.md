# PBSBullet

A simple python module for monitoring PBS jobs, and killing them if they run low on free memory.
By default, the script will check on the job every 5 minutes.

If provided with a Pushbullet API token, it can also send notifications on job status.

Can be installed with pip -

    pip install git+https://github.com/greenape/pbs_bullet.git

(Although you may need to use the --user switch, depending on your setup.)

Usage:

```
#Use pushbullet to notify on start, kill, and finish. Terminate if there is less
#than 10% free memory on any one node.
pbs-bullet --pushbullet-token <your api token> --low-mem 10 <jobnumber>

#Notify, but only on kill
pbs-bullet --pushbullet-token <your api token> --low-mem 10 --notify-on kill <jobnumber>

#Don't notify, and check every minute.
pbs-bullet --low-mem 10 --poll-interval 60 <jobnumber>
```

(You can also run the python module directly with python -m pbsbullet.pbs_bullet.)

Use this at your own risk: I have tested this only briefly, and only on Southampton's IRIDIS. I have no idea if it will
work in other PBS environments, and I'd strongly suggest exercising extreme caution.