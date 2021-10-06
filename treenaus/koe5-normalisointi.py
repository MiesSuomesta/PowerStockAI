import numpy as np
import pandas as pd
import datetime, time
from matplotlib import pyplot as plt
import seaborn as sns
import math, random
from sklearn.model_selection import train_test_split
from sklearn.datasets import make_regression
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sklearn.feature_extraction import DictVectorizer 
from sklearn.utils import assert_all_finite
from sklearn import metrics
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_squared_error
import pickle


import WeatherMonitor.Ilmatieteenlaitos as IL
import elspot



COUNTRY                             = 'FI'
GENERATE_MAIN_SOURCES               = False
GENERATE_COMBINED_SOURCES           = False
GENERATE_MODELS                     = False

GENERATE_MAX_SAMPLE_COUNT           = 0 # Off == 0, set = samplecount

DF_NAME_NORDPOOL_HISTORY_IN             = "porssisahko.ods"
DF_NAME_WEATHER_HISTORY_IN              = "saadata.csv"

DF_NAME_CUSTOM_HISTORY_IN               = "custom.csv"
DF_NAME_CUSTOM_HISTORY_OUT              = "custom.csv"

DF_NAME_NORDPOOL_HISTORY_OUT            = "df_nordpool_historical.bin"
DF_NAME_WEATHER_HISTORY_OUT             = "df_weather_historical.bin"

DF_NAME_COMBINED_HISTORY                = "df_weather_nordpool_historical_combined.bin"


itl = IL.Ilmatieteenlaitos()

def check_inf(X):
    ass = False
    try:
        assert_all_finite(X)
    except:
        ass = True
        pass

    print("INF check: ", ass)
    return ass

def apply_drift(old):
    rnd = 1 + ((0.5 - random.random()) * 0.1)
    uus = old * rnd;
    return uus


def make_custom_item(   var_huone_lampotila                ,
                        var_kuumanveden_lampotila          ,
                        var_porssisahkon_hinta_mwh         ,
                        var_saatoarvo_pumpulle             ,
                        var_saatoarvo_lammitysvastus       ,
                        var_patterille_lataamaan           ,
                        var_porssisahko_paivan_ylin        ,
                        var_porssisahko_paivan_alin        ,
                        var_porssisahkon_osto_hinta        ,
                        var_porssisahkon_osto_kokonaishinta):
             

    d = 1 + int(30 * random.random())
    m = 1 + int(11 * random.random())
    y = 2020
    H = 1 + int(23 * random.random())
    M = int(5 * random.random()) * 10 # 10 min välein

    datestr = "%02d-%02d-%04d" % (d, m, y)
    klostr = "%02d:%02d" % (H, M)

    item = {}
    item['datestr'] = datestr
    item['klo'] = klostr
    item['huone_lampotila']                = int(var_huone_lampotila * 100)                   # Celsius
    item['kuumanveden_lampotila']          = int(var_kuumanveden_lampotila * 100)             # Celsius
    item['porssisahkon_hinta_mwh']         = int(var_porssisahkon_hinta_mwh * 100)            # Eur
    item['saatoarvo_pumpulle']             = int(var_saatoarvo_pumpulle * 100)                # %
    item['saatoarvo_lammitysvastus']       = int(var_saatoarvo_lammitysvastus * 100)          # %
    item['patterille_lataamaan']           = int(var_patterille_lataamaan)                    # T/F
    item['porssisahko_paivan_ylin']        = int(var_porssisahko_paivan_ylin * 100)           # Eur
    item['porssisahko_paivan_alin']        = int(var_porssisahko_paivan_alin * 100)           # Eur
    item['porssisahkon_osto_hinta']        = int(var_porssisahkon_osto_hinta * 100)           # Eur
    item['porssisahkon_osto_kokonaishinta']= int(var_porssisahkon_osto_kokonaishinta * 100)   # Eur

    #print("ITEMI:", item)
    return item

def make_custom_items(cnt):

    total_price = 0
    thelist = []
    for index in range(cnt):

        price = 15 + int(30 * random.random())
        water = 30 + int(10 * random.random())
        tmp1 = apply_drift(price)
        tmp2 = apply_drift(price)
        
        hprice,lprice = max(tmp1, tmp2), min(tmp1, tmp2)
        
        total_price += price

        item = make_custom_item(    apply_drift(23),    # var_huone_lampotila                
                                    water,              # var_kuumanveden_lampotila          
                                    apply_drift(26),    # var_porssisahkon_hinta_mwh         
                                    apply_drift(1),     # var_saatoarvo_pumpulle             
                                    apply_drift(1),     # var_saatoarvo_lammitysvastus       
                                    int((index % 5) == 0),      # var_patterille_lataamaan           
                                    hprice,             # var_porssisahko_paivan_ylin        
                                    lprice,             # var_porssisahko_paivan_alin        
                                    price,              # var_porssisahkon_osto_hinta        
                                    total_price         # var_porssisahkon_osto_kokonaishinta
                                )

        #print("Adding custom item: ", item)
        thelist.append(item)
    
    return thelist


def root_mean_squared_error(y, y_pred, divide = False):

    y = y.astype(float)
    y_pred = y_pred.astype(float)
    error = y_pred - y
    
    if divide:
        error /= 100
        
    mse = (error ** 2).mean()
    rv = np.sqrt(mse)
    return rv

def cleanup_names(DF):
    DF.columns = DF.columns.str.lower().str.replace('(', '')
    DF.columns = DF.columns.str.lower().str.replace(')', '')
    DF.columns = DF.columns.str.lower().str.replace('/', '')
    DF.columns = DF.columns.str.lower().str.replace('ä', 'a')
    DF.columns = DF.columns.str.lower().str.replace('ö', 'o')
    DF.columns = DF.columns.str.lower().str.replace(' ', '_')
    DF.columns = DF.columns.str.lower().str.replace('_%', '')
    DF = DF.fillna(0)
    
    if GENERATE_MAX_SAMPLE_COUNT > 0:
        return DF[:GENERATE_MAX_SAMPLE_COUNT]
    
    return DF

def getds():
    ds = datetime.datetime.now().strftime("%H:%M:%S %d-%m-%Y")
    return ds
    
def printds(out):
    ds = getds()
    print (ds, out)

def get_fresh_spot_electricity_country_data(contry_code):

    prices_spot = elspot.Prices()
    hourly = prices_spot.hourly()
    maa = hourly['areas'][contry_code]
    print(prices_spot)
    return maa

def put_cache_object(obj, fname):
    print ("Putting model: ", fname);
    with open(fname, "wb") as fout:
        pickle.dump(obj, fout)

def get_cache_object(fname):
    obj = None
    print ("Getting model: ", fname);
    
    with open(fname, "rb") as fin:
        obj = pickle.load(fin)

    return obj

def load_from_cache(fname):
    DF_src = get_cache_object(fname)
    DF = pd.DataFrame(DF_src)
    print("LOAD: ", DF.info());
    
    if GENERATE_MAX_SAMPLE_COUNT > 0:
        return DF[:GENERATE_MAX_SAMPLE_COUNT]

    print ("fname:", fname, "Nulls: ", check_inf(DF))
    return DF

cache_spot_last_ds = None
cache_spot_last_ds_selections = None
def find_electricity_stock_info_by_date(df_from, ds, klo):
    global cache_spot_last_ds
    global cache_spot_last_ds_selections

    #print("Custom from: ", df_from)
    if cache_spot_last_ds == ds:
        selected = cache_spot_last_ds_selections
    else:
        selected = df_from[ df_from['datestr'] == ds ]
        cache_spot_last_ds = ds

    cache_spot_last_ds_selections = selected

    hs = DS_klo_hour
    he = DS_klo_hour + 1
    if he > 23: he = 0
    sel_hours = "%02d - %02d" % (hs, he)

    selected = selected[ selected['hours'] == sel_hours ]
    #print("FESIBD: ", selected)
    return selected

cache_custom_last_ds = None
cache_custom_last_ds_selections = None
def find_custom_info_by_date(df_from, ds, klo_hour, klo_min):
    global cache_custom_last_ds
    global cache_custom_last_ds_selections

    selected = None
    select = False
    selected2 = None
    #print("Custom from: ", df_from)
    if cache_custom_last_ds == ds:
        selected = cache_custom_last_ds_selections
    else:

        selected = df_from[ df_from['datestr'] == ds ]
        
        if (selected.all().empty == True):
            idx = int(len(df_from) * random.random())
            selected = df_from[idx]
        else:
            cache_custom_last_ds = ds
            cache_custom_last_ds_selections = selected

    #print("Custom selected: ", selected)

    hs = klo_hour
    hm = klo_min
    if hs > 23: hs = 0
    sel_klo = "%02d:%02d" % (hs, hm)

    selected2 = selected[ selected['klo'] == sel_klo ]
    
    if selected2.any().empty:
        selected2 = pd.DataFrame(make_custom_items(1))
            
    return selected2

def convert_to_proper_float(val):
    tmp = str(val).replace(',','.')
    tmp2 = float(tmp)
    return tmp2

def predict(model, X, samples = 1):
    printds("Starting predicting ................")
    pred = model.predict(X)
    printds("Predicted sample count {}..............".format(samples))
    printds(pred)
    return pred

def train(X, y, samples = 1):
    printds("Starting ................")
    print ("Nulls: ", check_inf(X))

# RMSE: 15
#    model = LinearRegression()

# RMSE: 
    model = LinearRegression()

# RMSE: 20
#    model = LogisticRegression(solver='liblinear',
#                               random_state=1)
#    model = GradientBoostingRegressor(random_state=1)
    printds("Fitting ................")

    my_X = X
    my_y = y
    
    if GENERATE_MAX_SAMPLE_COUNT > 0:
        my_X = X[:GENERATE_MAX_SAMPLE_COUNT]
        my_y = y[:GENERATE_MAX_SAMPLE_COUNT]

    model.fit(my_X, my_y)
    printds("Score:")
    printds(model.score(my_X,my_y))
   
    
    printds("Fitted .................")
    
    return model

if GENERATE_MAIN_SOURCES:
    printds("Starting generate data main sources for modelling.................")
    df_nordpool_full = pd.read_excel(DF_NAME_NORDPOOL_HISTORY_IN,verbose=True, skiprows = 2, usecols='A,B,H')
    df_nordpool_full = cleanup_names(df_nordpool_full)
    df_nordpool_full.columns = df_nordpool_full.columns.str.lower().str.replace('unnamed:_0', 'datestr')
    df_nordpool_full.columns = df_nordpool_full.columns.str.lower().str.replace('fi', 'price')
    
    df_saa = pd.read_csv(DF_NAME_WEATHER_HISTORY_IN)
    df_saa = cleanup_names(df_saa)
    del df_saa['aikavyohyke']

    df_custom_items = None
    #df_custom_items = pd.read_csv(DF_NAME_WEATHER_HISTORY_IN)

    df_custom_items = pd.DataFrame(make_custom_items(len(df_saa)))
    df_custom_items = cleanup_names(df_custom_items)

    put_cache_object(df_nordpool_full, DF_NAME_NORDPOOL_HISTORY_OUT)
    put_cache_object(df_saa, DF_NAME_WEATHER_HISTORY_OUT)
    put_cache_object(df_custom_items, DF_NAME_CUSTOM_HISTORY_OUT)

df_saa_historia_full =      load_from_cache(DF_NAME_WEATHER_HISTORY_OUT)
df_nordpool_historia_full = load_from_cache(DF_NAME_NORDPOOL_HISTORY_OUT)
df_custom_historia_full =   load_from_cache(DF_NAME_CUSTOM_HISTORY_OUT)

# Current data
df_nordpool_current_day_full = get_fresh_spot_electricity_country_data("fi")


# N samples
if GENERATE_MAX_SAMPLE_COUNT > 0:
    df_saa_historia_full =      df_saa_historia_full[:GENERATE_MAX_SAMPLE_COUNT]
    df_nordpool_historia_full = df_nordpool_historia_full[:GENERATE_MAX_SAMPLE_COUNT]
    df_custom_historia_full =   df_custom_historia_full[:GENERATE_MAX_SAMPLE_COUNT]
    printds("Data set capped")

# handle NAN's
df_saa_historia_full = df_saa_historia_full.fillna(0)
df_nordpool_historia_full = df_nordpool_historia_full.fillna(0)
df_custom_historia_full = df_custom_historia_full.fillna(0)

df_saa_historia_full_T = df_saa_historia_full.T
df_nordpool_historia_full_T = df_nordpool_historia_full.T
df_custom_historia_full_T = df_custom_historia_full.T

print(df_nordpool_historia_full_T.head())
print("---------------------------------------------")
print(df_custom_historia_full_T.head(20))

combined = []
df_combined = None


cmax = countdown = df_saa_historia_full_T.shape[1]
start_ds = getds()

if GENERATE_COMBINED_SOURCES:
    printds("Starting generate data for modelling.................")
    for saaindex in df_saa_historia_full_T:
        saaitem = df_saa_historia_full_T[saaindex]

        countdown -= 1
        left = cmax - countdown
        if (countdown % 100) == 0:
            ds = getds()
            print(start_ds, " -- ", ds, ": Count ", countdown, "/", cmax, "Done: ", int((left/cmax)*10000)/100, "%", end="\r");

        DS_d = saaitem['pv']
        DS_m = saaitem['kk']
        DS_y = saaitem['vuosi']
        DS_klo = saaitem['klo']
        DS_klo_hour = int(DS_klo[0:1])
        DS_klo_min = int(DS_klo[4:5])
        saaitem['klo'] = DS_klo_hour
        
        saaDS = "%02d-%02d-%04d" % (DS_d, DS_m, DS_y)

        porssiitems = find_electricity_stock_info_by_date(df_nordpool_historia_full, saaDS, DS_klo_hour)
        customitems = find_custom_info_by_date(df_custom_historia_full, saaDS, DS_klo_hour, DS_klo_min)
        
        #print("Custom generated", customitemsgen)
        #print("Custom got", customitems)
        
        save_item = saaitem

        if 'price' in porssiitems:
            if len(porssiitems['price'].values) > 0:
                save_item['price'] = convert_to_proper_float(porssiitems['price'].values[0])
        
        # Custom items
        
       
        if not customitems.empty:
            save_item["huone_lampotila"] =                 customitems["huone_lampotila"].values[0]
            save_item["kuumanveden_lampotila"] =           customitems["kuumanveden_lampotila"].values[0]
            save_item["porssisahkon_hinta_mwh"] =          customitems["porssisahkon_hinta_mwh"].values[0]
            save_item["saatoarvo_pumpulle"] =              customitems["saatoarvo_pumpulle"].values[0]
            save_item["saatoarvo_lammitysvastus"] =        customitems["saatoarvo_lammitysvastus"].values[0]
            save_item["patterille_lataamaan"] =            customitems["patterille_lataamaan"].values[0]
            save_item["porssisahko_paivan_ylin"] =         customitems["porssisahko_paivan_ylin"].values[0]
            save_item["porssisahko_paivan_alin"] =         customitems["porssisahko_paivan_alin"].values[0]
            save_item["porssisahkon_osto_hinta"] =         customitems["porssisahkon_osto_hinta"].values[0]
            save_item["porssisahkon_osto_kokonaishinta"] = customitems["porssisahkon_osto_kokonaishinta"].values[0]

            combined.append(save_item)
        #print(save_item)
        #exit()
        
    df_combined = pd.DataFrame(combined)
    put_cache_object(df_combined, DF_NAME_COMBINED_HISTORY)
    printds("End of generate data for modelling. Output file: %s" % DF_NAME_COMBINED_HISTORY)
else:
    printds("Starting load data for modelling.................")
    df_combined = get_cache_object(DF_NAME_COMBINED_HISTORY)
    printds("Loaded data for modelling.................")

print("Combined:", df_combined.T)

df_combined = df_combined.fillna(0)

df_train_full, df_test = train_test_split(df_combined, test_size=0.2 , random_state=1)
df_train, df_validate = train_test_split(df_train_full, test_size=0.33, random_state=11)

print("df_train_full shape  : ", df_train_full.shape);
print("df_test shape        : ", df_test.shape);
print("df_train shape       : ", df_train.shape);
print("df_validate shape    : ", df_validate.shape);



# Train/Prepare -------------------------------------

def prepare(key_label):

    global df_train
    global df_validate
    global df_test

    my_df_train = df_train.copy()
    my_df_validate = df_validate.copy()
    my_df_test = df_test.copy()

    my_y_train_values = my_df_train[key_label].values

#    print("Deleting key: ", key_label)
    del my_df_train[key_label]
    del my_df_validate[key_label]
    del my_df_test[key_label]

    my_df_train_dict = my_df_train.to_dict(orient='records')
    my_df_validate_dict = my_df_validate.to_dict(orient='records')
    my_df_test_dict = my_df_test.to_dict(orient='records')

    my_dv_train = DictVectorizer(sparse=False)
    my_dv_train.fit(my_df_train_dict)
    my_x_train = my_dv_train.transform(my_df_train_dict)

    my_dv_validate = DictVectorizer(sparse=False)
    my_dv_validate.fit(my_df_validate_dict)
    my_y_validate = my_dv_train.transform(my_df_validate_dict)

    my_dv_test = DictVectorizer(sparse=False)
    my_dv_test.fit(my_df_test_dict)
    my_y_test = my_dv_train.transform(my_df_test_dict)

#    print("Prepared: ")
#    print("     my_x_train shape        : ", my_x_train.shape);
#    print("     my_y_train_values shape : ", my_y_train_values.shape);
#    print("     my_y_validate shape     : ", my_y_validate.shape);
#    print("     my_y_test shape         : ", my_y_test.shape);

    return my_x_train, my_y_train_values, my_y_validate, my_y_test



models = {}

labels_to_generate = [
    'price',
    'kastepistelampotila_degc',
    'ilman_lampotila_degc',
    ]

print("Avail labels: ", df_combined.keys())

for key_label in df_combined.keys():

    modelfilename="model-for-%s-predict.bin" % (key_label)
    if GENERATE_MODELS:
        headeri = "Starting {} generate data for modelling.................".format(key_label)
        printds(headeri)

        x_train, y_train_values, y_validate, y_test = \
            prepare(key_label)

        print("x_train shape            : ", x_train.shape);
        print("y_train_values shape     : ", y_train_values.shape);
        print("y_validate shape         : ", y_validate.shape);
        print("y_test shape             : ", y_test.shape);

            
        model = train(x_train, y_train_values, 0)

        val_newshape = (y_validate.shape[0] , y_validate.shape[1])
        tst_newshape = (y_test.shape[0] , y_test.shape[1])
        
        pred_val = predict(model, y_validate, 0)
        pred_tst = predict(model, y_test, 0)
        
        #print("pred_val shape          : ", pred_val.shape);
        #print("pred_tst shape          : ", pred_tst.shape);
        #
        #print("y_validate shape        : ", y_validate.shape);
        #print("y_test shape            : ", y_test.shape);

        #rmse = mean_squared_error(y_validate, pred_val)
        #printds("Validation {} RMSE: {} ".format(key_label,rmse))
        #
        #rmse = mean_squared_error(y_test, pred_tst)
        #printds("Test       {} RMSE: {} ".format(key_label,rmse))
        
        put_cache_object(model, modelfilename)
        footteri = "End of {} generate data for modelling.................".format(key_label)
        printds(footteri)

    else:
        headeri = "Starting {} load data for modelling.................".format(key_label)
        printds(headeri)
        model = get_cache_object(modelfilename)
        footteri = "End of {} loading data for modelling.................".format(key_label)
        printds(footteri)

