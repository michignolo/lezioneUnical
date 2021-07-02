#!/usr/bin/python
# Listen to a Twitter stream and save data on disk and create a network using Redis
from twython import TwythonStreamer
from twython import Twython

## a better solution
from argparse import ArgumentParser
from yaml import safe_load, YAMLError
import json
import sys


from logging.handlers import TimedRotatingFileHandler
import logging

import redis

class Work():
    """
    This class is used to log-rotate the data output (saving every minute  a log file)
    """
    def __init__(self,jsonp):
        self.logger = self.create_rotating_log(jsonp)

    def create_rotating_log(self,path):
        """
        Creates a rotating log
        """
        logger = logging.getLogger("Rotating Log")
        logger.setLevel(logging.INFO)
     
        handler = TimedRotatingFileHandler(path, when='M')     #  M stands for minute
        logger.addHandler(handler)

        return logger

def get_params(parfile):
    """
    read parameters from external file (read from YAML)
    """
    with open(parfile, 'r') as stream:

        try:
            data = safe_load(stream) ## read yaml file
        except YAMLError as exc:
            print(exc)
            pass
    return data


class MyStreamer(TwythonStreamer):
    
    """
    this class from Twython is the one that allows to download a stream of tweets
    """

    
    def saveData(self,data):
        if('truncated' in data):
            if(data['truncated']):
                text = data['extended_tweet']['full_text']  ## full text 
                
            if('text' in data and  not data['truncated']): ## ordinary text 
                text = data['text']
            ##print(text) 
            ## save to rotating log
            jout = json.dumps(data)
            wk.logger.info(jout)    

    def createNetwork(self,data):
        """
        create a mention network (uses, REDIS)
        """
        if not hasattr(self, 'red_conn'):
            self.red_conn = redis.Redis(host='localhost', port=6379, db=0)

        if('in_reply_to_screen_name' in data ):
            if(data['in_reply_to_screen_name']):
                
                

                n = "link_%s_-_%s"%(data['user']['screen_name'], data['in_reply_to_screen_name']) 
                k = "%s_-_%s"%(data['user']['screen_name'], data['in_reply_to_screen_name'])
                
                vv = self.red_conn.hget(n,k)
                
                if(vv):
                    try:
                        val = int(vv)
                    except:
                        val = 0
                    self.red_conn.hset(n,k, val +1)
                    print(data['user']['screen_name'], data['in_reply_to_screen_name'], val+1)
                else:
                    self.red_conn.hset(n,k, 1)
                    print(data['user']['screen_name'], data['in_reply_to_screen_name'],1)
                self.red_conn.expire(n,36000)
                    
                


    def on_success(self, data):
        ## solving the problem of truncated tweet 
        ## https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/object-model/tweet
        
        self.saveData(data)
        self.createNetwork(data)

            

    def on_error(self, status_code, data):
        print(status_code)

        self.disconnect()

# ---- configuration -----
parser = ArgumentParser()
parser.add_argument("-c", "--configfile", help="yaml file containing the global parameters")
parser.add_argument("-q", "--querystring", help="specify the \" querystring \" ")
args = parser.parse_args()
if(args.configfile):
    global_params = get_params(parfile = args.configfile)
else:
    print("Please enter the configuration. For more info add --help to the command line")
    sys.exit()
    
if(args.querystring):
    filt = args.querystring
    if(len(filt) < 4):
        print("Use a longer query string")
        sys.exit()
else:
    print("Please enter the querystring")
    sys.exit()
    

# ----- rotating log -----

output_dir = global_params['output_dir']
output_file = global_params['output_dir'] + "/" + global_params['output_file']

wk = Work(output_file)


# ---- authentication ----
APP_KEY = global_params['APP_KEY']
APP_SECRET = global_params['APP_SECRET']
OAUTH_TOKEN =  global_params['OAUTH_TOKEN']
OAUTH_TOKEN_SECRET = global_params['OAUTH_TOKEN_SECRET']

global cnt
cnt  = 0
stream = MyStreamer(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
stream.statuses.filter(track=filt)
