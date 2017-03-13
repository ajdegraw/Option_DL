#import requests
import json
import re
import datetime
import os
from urllib.request import urlopen
import pandas as pd
import sys
import numpy as np

###################################################
#This function parses the google "json" to a correctly formatted json file
def parse_google_to_json(google_filename):
    file = open(google_filename,"r")
    contents = file.readlines()
    file.close()
    contents = contents[0]

    put_call = re.split("calls:", contents)
    put_call[1] = "calls:"+put_call[1]

    #Create the list of terms in the json incorrect formatting to replace to get correct formatting
    replace = ["expirations:", "calls:", "puts:", "cid:","name:", "strike:", "expiry:","oi:", "cs:", "cp:", "vol:", 
               "underlying_id:", "underlying_price:", "s:", "e:", "c:", "b:", "y:", "m:", "p:", "d:", "a:"]
    replace_with = []
    for term in replace:
        replace_with.append("\""+term[:-1]+"\":")
    
        #replace incorrect contents with correct contents
    for rep, repw in zip(replace, replace_with):
        put_call[0] = re.sub(rep, repw, put_call[0])
        put_call[1] = re.sub(rep, repw, put_call[1])
                   
    return put_call
    
###################################################    
    
def fix_google_json(json_str):
        #Create the list of terms in the json incorrect formatting to replace to get correct formatting
    replace = ["expirations:", "calls:", "puts:", "cid:","name:", "strike:", "expiry:","oi:", "cs:", "cp:", "vol:", 
               "underlying_id:", "underlying_price:", "s:", "e:", "c:", "b:", "y:", "m:", "p:", "d:", "a:"]
    replace_with = []
    for term in replace:
        replace_with.append("\""+term[:-1]+"\":")
        
    for rep, repw in zip(replace, replace_with):
        json_str = re.sub(rep, repw, json_str)
    
    return json_str
    
#################################################
    
def get_google_option_data(ticker_symb):
    url = "http://www.google.com/finance/option_chain?q="+tick+"&output=json"
    response = urlopen(url).read().decode('utf-8')
    return response

#################################################
    
def get_nasdaq_names_info(use = 'nol'):#new = False, usedate = False):
    #use == 'nol' uses local n.asdaq o.ption l.ist, use =='date' uses a file stored by date name\
    # use == 'new' retrieves and parses a file from nasdaq ftp.
    if use == 'new':
        if not os.path.exists("./Nasdaq_OptionLists/"+datestr):
            if not os.path.exists("./Nasdaq_OptionLists/"):
                os.makedirs("./Nasdaq_OptionLists/")
            print("Starting List Download from ftp.nasdaqtrader.com.")
            opt_list = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/options.txt"
            pull = urlopen(opt_list)
            response = pull.read().decode('utf-8')
            pull.close()
            f = open("./Nasdaq_OptionLists/"+datestr,"w")
            f.write(response)
            f.close()
            print("Finished List Download.")
            f = open("./Nasdaq_OptionLists/"+datestr,"r")
            nasdaq_list = f.readlines()
            f.close()
            
    if use == 'date':
#        f = open("./Nasdaq_OptionLists/NasdaqOptionList.csv")
        f = open("./Nasdaq_OptionLists/"+datestr,"r")
        nasdaq_list = f.readlines()
        f.close()
    if use == 'new' or use == 'date': 
        #Peeling off just names
        nasdaq_list = [line.rstrip() for line in nasdaq_list if len(line) > 1][1:]
        nasdaq_list = [line.split("|")[0] for line in nasdaq_list]
        nasdaq_list = sorted(list(set(nasdaq_list)))
    
        #Need to delete the "File created on " :
        for i in range(len(nasdaq_list)-1):
            if "File Creation Time" in nasdaq_list[i]:
                nasdaq_list.pop(i)
        nasdaq_df=pd.DataFrame(nasdaq_list)
        nasdaq_df.to_csv("./Nasdaq_OptionLists/NasdaqOptionList.csv", index=False, header=None)
    if use == 'nol':
        nasdaq_df = pd.read_csv("./Nasdaq_OptionLists/NasdaqOptionList.csv", header=None)
    
    return nasdaq_df
    
#################################################
    
    
now = datetime.datetime.now()
datestr = str(now.year)+"_"+str(now.month)+"_"+str(now.day)

tickers = np.array(get_nasdaq_names_info(use = 'nol'))[1:]
num_downloads = 0
for ticker in tickers:#tickers[:10]:
    #Output progress to terminal
    tick = ticker[0]
    sys.stdout.write("\r Currently working on {}    ".format(tick))
    #stockticker = "AAPL" #migrate to iterating over all stock ticker names in tickernamefile
    folders = "./OptionData/"+datestr#, "./OptionData/"+tick]
    if not os.path.exists(folders):
        os.makedirs(folders)
    #if not os.path.exists(folders[1]):
    #    os.makedirs(folders[1])
    datefirst_name = datestr+"_"+tick
    datelast_name = tick+"_"+datestr

    ########################################
    #Restructuring the json file from google and loading into df
    if (not os.path.exists("./OptionData/"+datestr+"/"+datefirst_name+".json") or \
        not os.path.exists("./OptionData/"+datestr+"/"+datefirst_name+"_puts.csv") or \
        not os.path.exists("./OptionData/"+datestr+"/"+datefirst_name+"_calls.csv")):
        
        try:
            response = get_google_option_data(tick)    
            fixed_json_str = fix_google_json(response)
            response_json = json.loads(fixed_json_str)
            dfputs = pd.DataFrame(response_json['puts'])
            dfcalls = pd.DataFrame(response_json['calls'])
            dfputs['asset_value'] = response_json['underlying_price']
            dfcalls['asset_value'] = response_json['underlying_price']
            dfputs['asset_id'] = response_json['underlying_id']
            dfcalls['asset_id'] = response_json['underlying_id']

        ###################################
        #Create filenames and write to appropriate json and csv files
        
            f = open("./OptionData/"+datestr+"/"+datefirst_name+".json", "w")
            f.write(json.dumps(response_json))
            f.close()
            dfputs.to_csv("./OptionData/"+datestr+"/"+datefirst_name+"_puts.csv")
            dfcalls.to_csv("./OptionData/"+datestr+"/"+datefirst_name+"_calls.csv")    
            num_downloads += 1
            
        ####################################
        except:
            continue


#Send a notification to log file that the files were downloaded        
LOG_PATH = "C:/Users/Admin/Dropbox/Option_Update/OptionDL.log"
with open(os.path.abspath(LOG_PATH), "a") as f:
    f.write("\n "+ str(datetime.datetime.now()) + ","+str(num_downloads) +" datasets, AIO Flagstaff")       
        
        
        
        