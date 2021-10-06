
import sys,os
# Import library for fetching Elspot data
import base64
import traceback
import sys
import pickle
import os, json
import time as Time
from dateutil.relativedelta import *
from dateutil.easter import *
from dateutil.rrule import *
from dateutil.parser import *
from datetime import *
from dateutil.parser import parse as parse_dt
import random

try: COMPILED
except NameError: COMPILED = False

import common

if not COMPILED:
    import utils
    import DateInfo
    import huepwr as huepwr
    import noteServer
    import noteZmq
    import noteClient

    try:
        from nordpool import elspot, elbas
    except:
        import elspot, elbas
        pass

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

if not COMPILED:
    noteClnt = noteClient.noteClient(common.ZMQ_HOST_DATA_SERVER, common.ZMQ_PORT_DATA_SERVER)
else:
    noteClnt = noteClient(common.ZMQ_HOST_DATA_SERVER, common.ZMQ_PORT_DATA_SERVER)

mlog = utils.get_logger("SähköPIHI UPS controller")

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
    mlog.debug("Got data: {} // {} // ".format(topic, title, msgdata))
    return topic, title, msgdata


def get_ups_mains_status():
    global mlog
    global UPS

    try:
        rv = UPS.get_currently_on_battery()

        if rv:
            rv = False
        else:
            rv = True

    except:
        mlog.debug("Getting UPS status failed......")
        traceback.mlog.debug_exc(file=sys.stdout)
        pass

    return rv

def get_ups_current_status_simulate():
    global mlog
    global UPS
    apcdata = {};
    
    apcdata['upsvaraus']        = UPS.get_current_battery_used_percentage()
    apcdata['upstimetolive']    = UPS.get_time_to_live()
    apcdata['upsstatus']        = UPS.get_currently_on_battery_txt()
    apcdata['upstimeleft_min']  = UPS.get_time_to_live_min()
    apcdata['upsnompower']      = UPS.get_max_battery_capacity()

    mlog.debug("Simulated APC Data: {}".format(apcdata));
    return apcdata

def get_ups_current_status_production():
    global mlog
    apcdata = APCups.apc_get_dict(strip_units=True)

    apcdata['upsvaraus']          = float(apcdata["BCHARGE"])
    apcdata['upstimetolive']      = float(apcdata["MINTIMEL"])
    apcdata['upsstatus']          = apcdata["STATUS"]
    apcdata['upstimeleft_min']    = float(apcdata["TIMELEFT"]) - 1
    apcdata['upsnompower']        = float(apcdata["NOMPOWER"])



    mlog.debug("Real APC Data: {}".format(apcdata));

    return apcdata


def get_ups_current_status():

    if common.SIMULATE_UPS:
        rv = get_ups_current_status_simulate()
    else:
        rv = get_ups_current_status_production()

    return rv



def get_tasatunnilla_str(tasatuntiin_min):
    tasatunnilla="%d minuutin päästä " % (tasatuntiin_min)
    if tasatuntiin_min == 0:
        tasatunnilla="nyt "
    return tasatunnilla


def control_ups_mains(mains_on, forced_mains, timeNow):
    global mlog
    global UPS

    conn = True

    current_on = get_ups_mains_status()

    cur_onoff = utils.get_onoff(current_on)
    set_onoff = utils.get_onoff(mains_on)

    mlog.debug("UPS CONTROL: UPS power change from {} to {} {}".format(cur_onoff,  set_onoff, forced_mains))
    if forced_mains or (current_on != mains_on) :
        to_battery = True
        if mains_on:
            to_battery = False

        mlog.debug("UPS CONTROL: UPS plug power set: {}/{} -> To Batt: {}".format(mains_on, set_onoff, to_battery))
        UPS.set_currently_on_battery(to_battery, timeNow)
    else:
        mlog.debug("UPS CONTROL: UPS power already %s, no actions." % cur_onoff)
    
    current_on = get_ups_mains_status()
    return current_on

def init_data_values(data):
    global mlog
    data['smart_plug_state_soft_change']    = True
    data['smart_plug_state_hard_change']    = False
    data['smart_plug_state_soft_to']        = True
    data['smart_plug_state_hard_to']        = False
    data['battery_charge_status']           = False 
    data['charge_till_cutoff']              = False
    
def is_dryrun(last_record_hour, current_hour, maainfo):
    global mlog
    dryrun = False
    # Jos tunti vaihtunut ja ei ole dataa -> kuiva ajo
    if last_record_hour != current_hour:
            if maainfo is False:
                dryrun = True

    return dryrun
    
def ups_health_checker(data):
    global mlog

    global UPS_STATUS_RUN_ON_BATTERY
    global UPS_STATUS_RUN_ON_MAINS
    global UPS_STATUS_NO_CHANGE

    if not COMPILED:
        noteSrv = noteServer.noteServer(common.ZMQ_HOST_USER_SERVER,
                                    common.ZMQ_PORT_USER_SERVER)
    else:
        noteSrv = noteServer(common.ZMQ_HOST_USER_SERVER,
                         common.ZMQ_PORT_USER_SERVER)

    glipse_change = 0;

    last_glimpse_change = 0

    init_data_values(data)

    timeout = updater_data['subscriberReadTimeout']
    logging_run_at = False

    last_recorded_day     = None
    last_recorded_hour    = None
    last_recorded_maainfo = False
    last_ups_currently_on_battery = False
    current_hour          = None
    current_min           = None
    current_day           = None

    ups_status = get_ups_current_status()
    # Updated data set
    upsstatus                 =  ups_status['upsstatus']
    upsnompower               =  ups_status['upsnompower']
    upsvaraus                 =  ups_status['upsvaraus']
    ups_currently_on_battery  =  (upsstatus == "ONBATT")

    while True:
        mlog.debug("ZMQ receive (TO: {} millisekunttia)......".format(timeout));
        (topic, title, maainfo) = get_message_from_zmq(timeout)
        #mlog.debug("UPS CONTROL: message recv: ", title)
        mlog.debug(maainfo)

        if maainfo == False:
            mlog.debug("ZMQ received NO data 1.");
            continue

        if len(maainfo) < 1:
            mlog.debug("ZMQ received NO data 2.");
            continue

        mlog.debug("ZMQ received data.");

        for hour in range(0, 23):

            if logging_run_at == False:
                logging_run_at        = parse_dt(maainfo[0][0])
                mlog.debug("Log start at {}".format(logging_run_at))
                current_hour          = int(logging_run_at.strftime("%H"))
                current_min           = int(logging_run_at.strftime("%M"))
                current_day           = int(logging_run_at.strftime("%d"))
                date_when_battery_state_change_start = logging_run_at
                last_recorded_day     = current_day
                last_recorded_hour    = current_hour
                last_recorded_maainfo = False
                last_ups_currently_on_battery = False

            DI = get_dayinfo(logging_run_at, maainfo)
            now, future, glimpse_change, told_to_set_mains_to = DI.get_future_glimpse(current_hour)
            mlog.debug("DI glimpse: {} // {} // {} // {}".format(now, future, glimpse_change, told_to_set_mains_to))

            current_hour    = int(logging_run_at.strftime("%H"))
            current_min     = int(logging_run_at.strftime("%M"))
            current_day     = int(logging_run_at.strftime("%d"))
            tasatuntiin_min = 59 - current_min

            tasatunnilla = get_tasatunnilla_str(tasatuntiin_min)

            dryrun = is_dryrun(last_recorded_hour, current_hour, maainfo)
            mlog.debug("DruRun set: {}".format(dryrun))

            # Get user defined minimums
            config_min_charge       = data['config_min_charge']
            config_charge_to        = data['config_charge_to']
            config_min_timetolive   = data['config_min_timetolive']
            config_min_timeleft     = data['config_min_timeleft']

            # default period len
            period_len_sec = common.SIMULATE_UPS_PERIOD_IN_SECONDS
            (last_charge_level, last_power, last_price) = (upsvaraus, upsnompower, now)

            # Simulated or realtime data 
            ups_status = get_ups_current_status()
            # Updated data set
            upsstatus                 =  ups_status['upsstatus']
            upsnompower               =  ups_status['upsnompower']
            upsvaraus                 =  ups_status['upsvaraus']
            ups_currently_on_battery  =  (upsstatus == "ONBATT")

            # Pehmeä kontrolli
            if maainfo is False:
                glimpse_change = last_glimpse_change
            else:
                last_glimpse_change = glimpse_change


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
            period_change = has_battery_status_change or has_price_change or is_hour_changed

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

                # Viimeksi olleet tiedot talteen
                last_price                              = now
                last_power                              = ups_status['upsnompower']
                last_charge_level                       = ups_status['upsvaraus']
                last_ups_status                         = ups_status['upsstatus']
                last_ups_currently_on_battery           = (last_ups_status == "ONBATT" )
                date_when_battery_state_change_start    = logging_run_at

            last_recorded_hour  = current_hour
            last_recorded_maainfo = maainfo

            

            # Logic to create msg for user
            pakko = ""
            onoff = "purkamaan"
            endi = "a"

            ups_status = get_ups_current_status()
            # Updated data set
            upsstatus                 =  ups_status['upsstatus']
            upsnompower               =  ups_status['upsnompower']
            upsvaraus                 =  ups_status['upsvaraus']
            ups_currently_on_battery  =  (upsstatus == "ONBATT")
            ups_currently_on_mains    =  (upsstatus != "ONBATT")

            mlog.debug("UPS plug state at {}: (TOM: {} ==  COM:_{}) Period chage:{}".format(
                    logging_run_at,
                    told_to_set_mains_to,
                    ups_currently_on_mains,
                    period_change,
                ))

#
#            if period_change:
#                UPS.set_currently_on_battery(turn_on_mains, logging_run_at)
#

            # Plugi on pois päältä jos ups_currently_on_battery == True
            # has_change == True  -> tilanmuutos on pakotettu
            # has_change == False -> tilanmuutos on pehmyt
            if has_change:
                if ups_currently_on_battery:
                    onoff = "olemaan"
                    endi  = "lla"
                else:
                    onoff = "purkamaan"

            if critical or data['charge_till_cutoff']:
                pakko = "pakolla "
                

            inputdata = {}
            inputdata['charge_level_low']   = config_min_charge
            inputdata['charge_level_curr']  = upsvaraus
            inputdata['charge_level_high']  = config_charge_to
            inputdata['ttl']                = config_min_timetolive
            inputdata['change_by_reason']   = muutos
            inputdata['charging_set_to']    = onoff
            

            inputdata['dryrun']             = dryrun

            dryrunmsg = ""
            if dryrun:
                dryrunmsg = "[DR] "
            
            dynamic_message = dryrunmsg + prefixi + "Kytketään "+ tasatunnilla + pakko + onoff +" patteri" + endi
            inputdata['charging_off_msg']           = dynamic_message
            inputdata['charging_on_msg']            = dynamic_message
            inputdata['ups_currently_on_battery']   = ups_currently_on_battery

            inputdata['price_now']          = now
            inputdata['price_future']       = future
            inputdata['upsnompower']        = upsnompower

            # Publish on ZMQ
#            if not dryrun:
#                if not maainfo is False:
#                    rdata = utils.generate_user_message(inputdata)
#                    noteSrv.publish_on_zmq("elspot", rdata['title'], rdata['msg'] )
#            mlog.debug("Last inputdata:", inputdata)

            last_recorded_maainfo = maainfo
            # Viimeksi olleet tiedot talteen
            last_price                              = now
            last_power                              = ups_status['upsnompower']
            last_charge_level                       = ups_status['upsvaraus']
            last_ups_status                         = ups_status['upsstatus']
            last_ups_currently_on_battery           = (last_ups_status == "ONBATT" )

            date_when_battery_state_change_start    = logging_run_at

            mlog.debug("Time change diff : {}".format(period_seconds))
            mlog.debug("Time change from : {}".format(logging_run_at))
            logging_run_at += timedelta(seconds=(60*60))
            mlog.debug("Time change to   : {}".format(logging_run_at))

            UPS.run_cycle()

            if maainfo is False:
                st = int(data['publishInterval'] / 4)
                mlog.debug("Sleeping %d seconds" % st);
                Time.sleep(st);
                mlog.debug("Slept %d seconds" % st);



be_looping = True

# Updater data
updater_data = {}
updater_data['publishInterval']         = common.SAHKOPIHI_PUBLISH_INTERVAL
updater_data['subscriberReadTimeout']   = common.SAHKOPIHI_UPS_CONTROLLER_MESSAGE_READ_TIMEOUT
updater_data['country']                 = common.SAHKOPIHI_ELECTRICITY_USED_IN_COUNTRY
updater_data['config_min_charge']       = common.SAHKOPIHI_ELECTRICITY_MIN_CHARGE_PERCENT
updater_data['config_charge_to']        = common.SAHKOPIHI_ELECTRICITY_CUTOFF_CHARGE_PERCENT
updater_data['config_min_timetolive']   = common.SAHKOPIHI_ELECTRICITY_MIN_TIME_TO_LIVE
updater_data['config_min_timeleft']     = common.SAHKOPIHI_ELECTRICITY_MIN_TIME_LEFT
updater_data['update']                  = True

print("Starting UPS controller ......")
ups_health_checker(updater_data)
