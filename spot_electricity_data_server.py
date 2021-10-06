#!/usr/bin/env python

import sys, pathlib 
# Import library for fetching Elspot data
import base64
import traceback
import sys
from pprint import pprint
import pickle
import os, json
import time as Time
from dateutil.relativedelta import *
from dateutil.easter import *
from dateutil.rrule import *
from dateutil.parser import *
from datetime import *

try: COMPILED
except NameError: COMPILED = False

import common

if not COMPILED:
    import utils
    import noteServer
    import noteZmq
    import noteClient
    noteSrv = noteServer.noteServer(common.ZMQ_HOST_DATA_SERVER, common.ZMQ_PORT_DATA_SERVER)
else:
    noteSrv = noteServer(common.ZMQ_HOST_DATA_SERVER, common.ZMQ_PORT_DATA_SERVER)

def publish_on_zmq(topic, title, msgdata):
    global noteSrv;
    #print("DATASERVE:", title)
    noteSrv.publish_on_zmq(topic, title, msgdata)
    #print("DATASERVED.")

def get_time_in_seconds ( dt ):
    return dt.now()

def get_time_diff ( last, now ):
    diff = get_time_in_seconds(now.now()) - get_time_in_seconds(last.now())
    return diff

def update_on_every(data):
    cnt=1
    last_info_encoded = ""
    time_last = False
    time_now  = datetime.now()
    interval_in_seconds = data['publishInterval']
    last_hour = int(time_now.strftime("%H"))
    while data['update']:
        nation          = data['country']
        
        print("Updater published ( %d times )-------------------------" % cnt)
        cnt += 1;
        info_dict = utils.get_fresh_country_data(nation)
        publish_on_zmq("elspot", nation, info_dict)

        time_now        = datetime.now()
        current_sec     = int(time_now.strftime("%S"))
        wait_till_next_event = interval_in_seconds - current_sec
                    
        print("Waiting to sync to next interval of {} seconds ... {} seconds to go....".format(interval_in_seconds, wait_till_next_event))
        
        Time.sleep(wait_till_next_event);

    noteSrv.term()

be_looping = True

# Updater data
updater_data = {}

# 5min update interval
updater_data['publishInterval']         = common.SAHKOPIHI_PUBLISH_INTERVAL
updater_data['subscriberReadTimeout']   = common.SAHKOPIHI_UPS_CONTROLLER_MESSAGE_READ_TIMEOUT
updater_data['country']                 = common.SAHKOPIHI_ELECTRICITY_USED_IN_COUNTRY
updater_data['config_min_charge']       = common.SAHKOPIHI_ELECTRICITY_MIN_CHARGE_PERCENT
updater_data['config_charge_to']        = common.SAHKOPIHI_ELECTRICITY_CUTOFF_CHARGE_PERCENT
updater_data['config_min_timetolive']   = common.SAHKOPIHI_ELECTRICITY_MIN_TIME_TO_LIVE
updater_data['config_min_timeleft']     = common.SAHKOPIHI_ELECTRICITY_MIN_TIME_LEFT
updater_data['update']                  = be_looping

update_on_every(updater_data)
