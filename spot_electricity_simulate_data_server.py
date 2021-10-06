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
import common
from dateutil.relativedelta import *
from dateutil.easter import *
from dateutil.rrule import *
from dateutil.parser import *
from datetime import *
from dateutil.parser import parse as parse_dt

try: COMPILED
except NameError: COMPILED = False

if not COMPILED:
    import common
    import utils
    import noteServer
    import noteZmq
    import noteClient
    noteSrv = noteServer.noteServer(common.ZMQ_HOST_DATA_SERVER, common.ZMQ_PORT_DATA_SERVER)
else:
    noteSrv = noteServer(common.ZMQ_HOST_DATA_SERVER, common.ZMQ_PORT_DATA_SERVER)

def publish_on_zmq(topic, title, msgdata):
    global noteSrv;
    print("DATASERVE:", title, msgdata)
    noteSrv.publish_on_zmq(topic, title, msgdata)
    print("DATASERVED.")



def update_on_every(data):
    cnt=1
    last_info_encoded = ""
    time_last = False
    time_start_str  = data['start_date']
    time_end_str    = data['end_date']

    time_now_date = parse_dt(time_start_str)
    time_end_date = parse_dt(time_end_str  )
    
    interval_in_seconds = data['publishInterval']
    last_hour = 0

    total_day_min = 24 * 60
    total_day_sec = total_day_min * 60
    time_year_delta = timedelta(days=365).total_seconds()
    time_total_delta = (time_end_date - time_now_date).total_seconds()

    total_seconds = 365 * interval_in_seconds * (time_total_delta/time_year_delta)


    while data['update']:

        print("Updater published ( %d times, %d loops remain (%.02f minutes until ready) )-------------------------" % (cnt, 365 - cnt, total_seconds/60.0))
        print("Updater working on day {} till {} ....".format(time_now_date, time_end_date))

        daily_data = utils.simulate_get_fresh_country_data('FI', start_date=time_now_date, fromfile = data['sourceDatasFrom'])

        cnt += 1;
        total_seconds -= interval_in_seconds;

        (topic, title) = ("dailydata", "simulator")
        publish_on_zmq(topic, title, daily_data)
        
        Time.sleep(interval_in_seconds);
        
        time_now_date += timedelta(seconds=24*60*60)
        
        if time_now_date > time_end_date:
            print("END....")
            exit(0)
   

    noteSrv.term()

be_looping = True

# Updater data
updater_data = {}


# 5min update interval
updater_data['publishInterval']         = 3
#updater_data['publishInterval']         = 15
updater_data['subscriberReadTimeout']   = common.SAHKOPIHI_UPS_CONTROLLER_MESSAGE_READ_TIMEOUT
updater_data['country']                 = common.SAHKOPIHI_ELECTRICITY_USED_IN_COUNTRY
updater_data['config_min_charge']       = common.SAHKOPIHI_ELECTRICITY_MIN_CHARGE_PERCENT
updater_data['config_charge_to']        = common.SAHKOPIHI_ELECTRICITY_CUTOFF_CHARGE_PERCENT
updater_data['config_min_timetolive']   = common.SAHKOPIHI_ELECTRICITY_MIN_TIME_TO_LIVE
updater_data['config_min_timeleft']     = common.SAHKOPIHI_ELECTRICITY_MIN_TIME_LEFT
updater_data['update']                  = be_looping

# Simulation dates, start & end
updater_data['start_date']              = common.SIMULATION_DATA_INPUT_DATE_START      # Ex. "1.1.2020"
updater_data['end_date']                = common.SIMULATION_DATA_INPUT_DATE_END        # Ex. "31.12.2020"
updater_data['sourceDatasFrom']         = common.SIMULATION_DATA_INPUT_FILE            # Ex. "datain.xls"

update_on_every(updater_data)
