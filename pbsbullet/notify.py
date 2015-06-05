import json
import urllib2, urllib
import logging
logging.basicConfig()
logger = logging.getLogger('pbs_bullet.notifier')


class Notifier(object):
    def __init__(self, name, pb_token):
        self.token = pb_token
        self.name = name
        self.iden = self.create_listener()

    def create_listener(self):
        """
        Register this device to receive pushbullet notifications,
        and if successful return the identifier.
        """
        data = urllib.urlencode({'nickname': self.name, 'type': 'stream'})
        logger.debug("Adding %s to pushbullet." % self.name)
        request = urllib2.Request('https://api.pushbullet.com/v2/devices', data, headers={
            'Authorization': "Bearer %s" % self.token,
            'Accept': '*/*'
        })
        try:
            resp = urllib2.urlopen(request)
            return json.load(resp)['iden']
        except urllib2.HTTPError as e:
            logger.error("Pushbullet register error.")
            logger.error(e.read())
        except urllib2.URLError as e:
            logger.error("Pushbullet register error.")
            logger.error(e)

    def delete_listener(self):
        """
        Unregister this device as a listener.
        """
        logger.debug("Deleting %s from pushbullet." % self.iden)
        request = urllib2.Request('https://api.pushbullet.com/v2/devices/%s' % self.iden, headers={
            'Authorization': "Bearer %s" % self.token,
            'Accept': '*/*'
        })
        request.get_method = lambda: 'DELETE'
        try:
            resp = urllib2.urlopen(request)
            return json.load(resp)
        except urllib2.HTTPError as e:
            logger.error("Pushbullet delete listener error.")
            logger.error(e.read())
        except urllib2.URLError as e:
            logger.error("Pushbullet delete listener error.")
            logger.error(e)

    def delete_push(self, push):
        """
        Delete a push.
        """
        logger.debug("Deleting push id %s from pushbullet." % push['iden'])
        request = urllib2.Request('https://api.pushbullet.com/v2/pushes/%s' % push['iden'], headers={
            'Authorization': "Bearer %s" % self.token,
            'Accept': '*/*'
        })
        request.get_method = lambda: 'DELETE'
        try:
            resp = urllib2.urlopen(request)
            return json.load(resp)
        except urllib2.HTTPError as e:
            logger.error("Pushbullet delete error.")
            logger.error(e.read())
        except urllib2.URLError as e:
            logger.error("Pushbullet delete error.")
            logger.error(e)

    def check_pushes(self):
        """
        Return any undismissed pushes for this device, and dismiss
        them.
        """
        data = urllib.urlencode({'active': '0'})
        logger.debug("Checking pushes for %s." % self.iden)
        request = urllib2.Request('https://api.pushbullet.com/v2/pushes?%s' % data, headers={
            'Authorization': "Bearer %s" % self.token,
            'Accept': '*/*'
        })
        pushes = []
        try:
            resp = urllib2.urlopen(request)
            pushes = json.load(resp)['pushes']
            pushes = filter(lambda push: 'target_device_iden' in push.keys() and push['target_device_iden'] == self.iden, pushes)
            logger.debug("Got %d pushes." % len(pushes))
            # Delete them from the server
            map(lambda push: self.delete_push(push), pushes)
            pushes.reverse()

        except urllib2.HTTPError as e:
            logger.error("Pushbullet check error.")
            logger.error(e.read())
        except urllib2.URLError as e:
            logger.error("Pushbullet check error.")
            logger.error(e)
        finally:
            return pushes

    def send_notification(self, title, body, target=None):
        """
        Send a pushbullet notification.
        """
        data = {"type":"note", "title": title, "body": body, "source_device_iden":self.iden}
        if target is not None:
            data['device_iden'] = target
        note = json.dumps(data)
        logger.debug("Sending %s to pushbullet." % note)
        request = urllib2.Request('https://api.pushbullet.com/v2/pushes', note, headers={
            'Authorization':"Bearer %s" % self.token,
            'Content-Type':'application/json',
            'Accept':'*/*'
        })
        try:
            resp = urllib2.urlopen(request)
        except urllib2.HTTPError as e:
            logger.error("Pushbullet notify error.")
            logger.error(e.read())
        except urllib2.URLError as e:
            logger.error("Pushbullet notify error.")
            logger.error(e)