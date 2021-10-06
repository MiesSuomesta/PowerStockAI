#!/usr/bin/env python3 

import sys,os

# Import library for fetching Elspot data
import base64
import traceback
import sys
import pickle
import random
import os, json
import time as Time
from dateutil.relativedelta import *
from dateutil.easter import *
from dateutil.rrule import *
from dateutil.parser import *
from datetime import *
from dateutil.parser import parse as parse_dt

import common

try: COMPILED
except NameError: COMPILED = False

if not COMPILED:
    import utils
    import DateInfo
    import noteServer
    import noteZmq
    import noteClient

    if common.SIMULATE_UPS:
        import UPSSimulator
        UPS = UPSSimulator.UPSSimulator()
    else:
        import UPSDevice as UPSDevice
        UPS = UPSDevice.UPSDevice()
else:
    if common.SIMULATE_UPS:
        UPS = UPSSimulator()
    else:
        UPS = UPSDevice()

UPS_STATUS_NO_CHANGE        = common.UPS_STATUS_NO_CHANGE
UPS_STATUS_RUN_ON_BATTERY   = common.UPS_STATUS_RUN_ON_BATTERY
UPS_STATUS_RUN_ON_MAINS     = common.UPS_STATUS_RUN_ON_MAINS

if COMPILED:
    noteClnt = noteClient(common.ZMQ_HOST_DATA_SERVER, common.ZMQ_PORT_DATA_SERVER)
else:
    noteClnt = noteClient.noteClient(common.ZMQ_HOST_DATA_SERVER, common.ZMQ_PORT_DATA_SERVER)

mlog = utils.get_logger("SähköPIHI DataLogger")

mlog.debug("Starting logger.....................")

def get_dayinfo(day, data):
    global COMPILED

    if COMPILED:
        rv = DateInfo(day,data)
    else:
        rv = DateInfo.DateInfo(day,data)

    return rv

def get_message_from_zmq(tout):
    global noteClnt
    global mlog
    (topic, title, msgdata) = noteClnt.recv_from_zmq(tout)
    print("Got data: {} // {} // {}".format(topic, title, msgdata))
    return topic, title, msgdata

last_upsvaraus     = 0
last_upstimetolive = 0
last_upsstatus     = 0
last_upstimeleft   = 0
last_upsnompower   = 0

def init_data_values(data):
    global mlog
    data['smart_plug_state_soft_change']    = True
    data['smart_plug_state_hard_change']    = False
    data['smart_plug_state_soft_to']        = True
    data['smart_plug_state_hard_to']        = False
    data['battery_charge_status']           = False
    data['charge_till_cutoff']              = False

def get_ups_current_status():
    global mlog
    global UPS
    apcdata = {};
    
    apcdata['upsvaraus']        = UPS.get_current_battery_used_percentage()
    apcdata['upstimetolive']    = UPS.get_time_to_live()
    apcdata['upsstatus']        = UPS.get_currently_on_battery_txt()
    apcdata['upstimeleft_min']  = UPS.get_time_to_live_min()
    apcdata['upsnompower']      = UPS.get_max_battery_capacity()

    #mlog.debug("APC Data:", apcdata)
    return apcdata


def control_ups_mains(mains_on, forced_mains, timeNow):
    global mlog
    global UPS

    conn = True

    current_on = get_ups_mains_status()

    cur_onoff = utils.get_onoff(current_on)
    set_onoff = utils.get_onoff(mains_on)

    mlog.debug("UPS CONTROL: UPS power change from {} to {} {}".format(cur_onoff,  set_onoff, forced_mains))
    if forced_mains or (current_on != mains_on) :
        to_battery = not mains_on
        mlog.debug("UPS CONTROL: UPS plug power set: {} -> To Batt: {}".format(set_onoff, to_battery))
        UPS.set_currently_on_battery(to_battery, timeNow)
    else:
        mlog.debug("UPS CONTROL: UPS power already %s, no actions." % cur_onoff)
    
    current_on = get_ups_mains_status()
    return current_on

def get_ups_mains_status():
    global mlog
    global UPS

    if UPS.get_currently_on_battery():
        rv = "off"
    else:
        rv = "on"
        
    return rv

def ups_data_log(data):
    global mlog
    global UPS_STATUS_RUN_ON_BATTERY
    global UPS_STATUS_RUN_ON_MAINS
    global UPS_STATUS_NO_CHANGE
    global cumulative_total_price_orginal
    global cumulative_total_price_applied

    glipse_change = 0;
    last_recorded_day = False
    last_recorded_hour = False
    last_recorded_maainfo = False

    init_data_values(data)

    time_start_str  = data['logging_start_at']

    logging_run_at = logging_start_at = parse_dt(time_start_str)

    last_glimpse_change = 0
    now, future, glimpse_change = 0, 0, 0
    timeout                              = data['publishInterval']
    price_when_battery_state_change_end  = now
    battery_state_on_change              = 0
    saved_price_on_change                = 0
    price_on_change                      = 0
    prices_of_battery_state_change       = []

    ups_status = get_ups_current_status()
    # Initial data set
    upsstatus                 =  ups_status['upsstatus']
    upsnompower               =  ups_status['upsnompower']
    upsvaraus                 =  ups_status['upsvaraus']
    ups_currently_on_battery  =  (upsstatus == "ONBATT")

    date_when_battery_state_change_start = logging_start_at
    date_when_battery_state_change_end   = logging_start_at

    # Viimeksi olleet tiedot talteen
    last_price                              = now
    last_power                              = ups_status['upsnompower']
    last_charge_level                       = ups_status['upsvaraus']
    last_ups_status                         = ups_status['upsstatus']
    last_ups_currently_on_battery           = (last_ups_status == "ONBATT" )

    current_hour = 0
    period_seconds = 60*60

    while updater_data['update']:

        mlog.debug("ZMQ receive (TO:{} millisekunttia)......".format(timeout));
        (topic, title, maainfo) = get_message_from_zmq(timeout)

        if maainfo == False:
            mlog.debug("ZMQ received NO data.");
            continue

        mlog.debug("ZMQ received data.");

        for hour in range(0, 23):


            DI = get_dayinfo(logging_run_at, maainfo)
            mlog.debug(DI)

            now, future, glimpse_change, told_to_set_mains_to = \
                         DI.get_future_glimpse(hour)

            mlog.debug("Price now     {}".format(now))
            mlog.debug("Price Future  {}".format(future))

            ups_status = get_ups_current_status()
            # Updated data set
            upsstatus                 =  ups_status['upsstatus']
            upsnompower               =  ups_status['upsnompower']
            upsvaraus                 =  ups_status['upsvaraus']
            ups_currently_on_battery  =  (upsstatus == "ONBATT")

            has_change         = utils.is_ups_change(glimpse_change, 
                                      UPS_STATUS_RUN_ON_MAINS | UPS_STATUS_RUN_ON_BATTERY)
            soft_mains         = utils.is_ups_change(glimpse_change, UPS_STATUS_RUN_ON_MAINS)
            no_change_in_price = utils.is_ups_change(glimpse_change, UPS_STATUS_NO_CHANGE)

            mlog.debug("Soft control glimpse_change              : {}".format(glimpse_change))
            mlog.debug("Soft control has_change                  : {}".format(has_change))
            mlog.debug("Soft control told_to_set_mains_to        : {}".format(told_to_set_mains_to))
            mlog.debug("Soft control ups_currently_on_battery    : {}".format(ups_currently_on_battery))

            data['ups_currently_on_battery'] = ups_currently_on_battery
            data['smart_plug_state_soft_change'] = has_change
            data['smart_plug_state_soft_to']     = told_to_set_mains_to

            (data, prefixi, muutos, critical) = utils.check_critical(
			        data, ups_status
		        )

            # Shot codes -------------------
            chargin_till_cutoff    = data['charge_till_cutoff']

            # Jos on cutoff tilanne, niin forcetetaan lataus 
            forced_mains           = data['smart_plug_state_hard_change'] or chargin_till_cutoff
            set_plug_state_hard_to = data['smart_plug_state_hard_to']     or chargin_till_cutoff

            soft_mains             = data['smart_plug_state_soft_change']
            set_plug_state_soft_to = data['smart_plug_state_soft_to']

            # Jokin pakko, eli ei dryrun ja muutos pakotetaan....
            if forced_mains:
                dryrun = False
                has_change = True

            mlog.debug("UPS HW currently on battery              : {}".format(ups_currently_on_battery))
            mlog.debug("PRC no_change_in_price                   : {}".format(no_change_in_price))
            mlog.debug("UPS HW has_change                        : {}".format(has_change))
            mlog.debug("UPS HW chargin_till_cutoff               : {}".format(chargin_till_cutoff))
            mlog.debug("UPS HW forced_mains                      : {}".format(forced_mains))
            mlog.debug("UPS HW soft_mains                        : {}".format(soft_mains))
            mlog.debug("UPS HW H setting on battery              : {}".format(set_plug_state_hard_to))
            mlog.debug("UPS HW S setting on battery              : {}".format(set_plug_state_soft_to))
            
            if has_change:
                if forced_mains:
                    mlog.debug("UPS HW Control: HARD FORCE plug {}".format(utils.get_onoff(set_plug_state_hard_to)))
                    control_ups_mains(  set_plug_state_hard_to,           True, logging_run_at)
                else:
                    mlog.debug("UPS HW Control: SOFT FORCE plug {}".format(utils.get_onoff(set_plug_state_soft_to)))
                    control_ups_mains( set_plug_state_soft_to,       True, logging_run_at)
            else:
                mlog.debug("UPS HW Control: Soft setting plug {}".format(utils.get_onoff(set_plug_state_soft_to) ))
                control_ups_mains( set_plug_state_soft_to, False, logging_run_at)

            random_secs = common.SIMULATE_UPS_CONSUME_PERIOD_PER_CYCLE_RANDOM_BASE
            period_len_sec = common.SIMULATE_UPS_PERIOD_IN_SECONDS #.total_seconds()

            # Tunnin sisällä muutos .. stämpätään jakson loppu
            if common.SIMULATE_UPS_CONSUME_PERIOD_PER_CYCLE_RANDOM:
                 rnd = random.random()
                 random_secs += (common.SIMULATE_UPS_CONSUME_PERIOD_PER_CYCLE_RANDOM_PART/2) - \
                                (common.SIMULATE_UPS_CONSUME_PERIOD_PER_CYCLE_RANDOM_PART * rnd) 

            has_price_change          = (last_price != now)
            is_hour_changed           = (current_hour != last_recorded_hour)
            has_battery_status_change = \
                (last_ups_currently_on_battery != ups_currently_on_battery)

            # Joku muuttunut, joko tasatunti, tai tuntitiedon muutos tai patterin status tai hinnanmuutos
            period_change = has_battery_status_change or has_price_change or is_hour_changed or has_change

            mlog.debug("UPS Logger: has_change                 {}".format(has_change))
            mlog.debug("UPS Logger: is_hour_changed            {}".format(is_hour_changed))
            mlog.debug("UPS Logger: has_battery_status_change  {}".format(has_battery_status_change))
            mlog.debug("UPS Logger: has_price_change           {}".format(has_price_change))

            charging_time_max_mins = UPS.get_time_to_live_min()
            
            period_seconds = charging_time_max_mins
            
            if period_seconds > (60 * 60):
                mlog.debug("Period change MAXed %d .........................." % period_seconds)
                period_seconds = 60 * 60

            if period_change:
                mlog.debug("Period changed ..........................")

                date_when_battery_state_change_end = logging_run_at


                date_battery_state_change_start = date_when_battery_state_change_start
                date_battery_state_change_end   = date_when_battery_state_change_end

                if common.SIMULATE_UPS:
                    tmp  = date_battery_state_change_start
                    tmp += timedelta(seconds=period_seconds)
                    date_battery_state_change_end = tmp

                mlog.debug("Period: {} - {} // Len: {} sec".format(
                                        date_battery_state_change_start,
                                        date_battery_state_change_end,
                                        period_seconds
                                        ))

                (last_price, last_power, last_charge_level, \
                            last_ups_status, last_ups_currently_on_battery,\
                            last_recorded_hour, period_seconds) = utils.log_period_change(                                             \
                               logging_run_at, date_battery_state_change_start, date_battery_state_change_end, ups_status,             \
                               last_ups_currently_on_battery, last_charge_level, last_power,                                           \
                               last_price, now, upsvaraus, upsnompower,                                                                \
                               ups_currently_on_battery, glimpse_change)

                mlog.debug("Chargin Period changed diff ..... {}".format(period_seconds))
                mlog.debug("Chargin Period changed from ..... {}".format(date_battery_state_change_start))
                mlog.debug("Chargin Period changed to   ..... {}".format(date_battery_state_change_end))

                date_when_battery_state_change_start    = logging_run_at

                # Viimeksi olleet tiedot talteen
                last_price                              = now
                last_power                              = ups_status['upsnompower']
                last_charge_level                       = ups_status['upsvaraus']
                last_ups_status                         = ups_status['upsstatus']
                last_ups_currently_on_battery           = (last_ups_status == "ONBATT" )

            last_recorded_hour  = current_hour
            last_recorded_maainfo = maainfo

            if period_seconds < 1:
                mlog.debug("Time change diff : hardcoded 3600")
                period_seconds = 60*60

            mlog.debug("Time change diff : {}".format(period_seconds))
            mlog.debug("Time change from : {}".format(logging_run_at))
            logging_run_at += timedelta(seconds=(60*60))
            mlog.debug("Time change to   : {}".format(logging_run_at))

            UPS.run_cycle()



be_looping = True

# Updater data
updater_data = {}
updater_data['publishInterval']         = common.SAHKOPIHI_LOGGER_MESSAGE_READ_TIMEOUT
updater_data['country']                 = common.SAHKOPIHI_ELECTRICITY_USED_IN_COUNTRY
updater_data['config_min_charge']       = common.SAHKOPIHI_ELECTRICITY_MIN_CHARGE_PERCENT
updater_data['config_charge_to']        = common.SAHKOPIHI_ELECTRICITY_CUTOFF_CHARGE_PERCENT
updater_data['config_min_timetolive']   = common.SAHKOPIHI_ELECTRICITY_MIN_TIME_TO_LIVE
updater_data['config_min_timeleft']     = common.SAHKOPIHI_ELECTRICITY_MIN_TIME_LEFT
updater_data['config_min_charge']       = common.SAHKOPIHI_ELECTRICITY_MIN_CHARGE_PERCENT
updater_data['update']                  = be_looping
updater_data['logging_start_at']              = "1.1.2020"

mlog.debug("Config:");
mlog.debug(updater_data);

ups_data_log(updater_data)
