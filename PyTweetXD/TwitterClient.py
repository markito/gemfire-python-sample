# -*- coding: utf-8 -*-
'''
Created on Nov 27, 2013
TwitterClient script that reads current twitter stream for a given 'subject' and persist into GemFireXD
@see: check config file for twitter OAuth key settings
@author: markito
'''
from TwitterAPI import TwitterAPI
from GfxdClient import GfxdClient
from ConfigParser import ConfigParser
import time
import signal
import sys

successCount = 0
errorCount = 0

'''
Print some stats after script is stopped
'''
def signal_handler(signal, frame):
        print '\n# Stats'
        print 'Saved tweets: %d' % successCount
        print 'Errors: %d' % errorCount
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

### Read config file
config = ConfigParser()
config.read('config')

# get twitter keys
consumer_key        = config.get('Twitter', 'consumer_key')
consumer_secret     = config.get('Twitter', 'consumer_secret')
access_token_key    = config.get('Twitter', 'access_token_key')
access_token_secret = config.get('Twitter', 'access_token_secret')

# the subject you want to track on twitter
subject = 'NBA'

# twitter streaming api
api = TwitterAPI(consumer_key,consumer_secret,access_token_key,access_token_secret)
stream = api.request('statuses/filter', {'track': subject})

# Connect to GemFireXD
gfxd = GfxdClient()
gfxd.connect()

print "Reading Twitter stream for subject: %s (hit ctrl+c to stop)" % subject

# read streaming data and persist into GemFireXD
for tweet in stream.get_iterator():
    sql = "insert into tweets values (?,?,?,?,?,?,?,?,?)"
    try:
        params = (tweet['id'],time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')),str(tweet['favorited']), \
              str(tweet['lang']),tweet['retweet_count'],str(tweet['retweeted']),str(tweet['source']),unicode(tweet['text']).encode('latin-1', errors='ignore'),tweet['user']['id'])
    
        gfxd.insert(sql, params)
        successCount += 1
    except Exception as e:
        errorCount += 1
        print e

if __name__ == '__main__':
    pass
