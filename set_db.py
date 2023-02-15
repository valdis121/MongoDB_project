import os
import glob
import pymongo
import xmltodict
import shutil
import datetime
import config_upa
from collections import defaultdict
def gen_train_id(data_dict, isCanceled=False):
    id=""
    if isCanceled:
        date = data_dict["CZCanceledPTTMessage"]["PlannedTransportIdentifiers"]
        for i in date:
            if i["ObjectType"] == "TR":
                id+=i["Company"] + "_" + i["Core"] + "_" + i["Variant"]
                return id
    else:
        date = data_dict["CZPTTCISMessage"]["Identifiers"]["PlannedTransportIdentifiers"]
        for i in date:
            if i["ObjectType"] == "TR":
                id+=i["Company"] + "_" + i["Core"] + "_" + i["Variant"]
                return id
def gen_connect_id(data_dict, isCanceled=False, isRelated=False):
    id=""
    if isCanceled:
        date = data_dict["CZCanceledPTTMessage"]["PlannedTransportIdentifiers"]
        for i in date:
            if i["ObjectType"] == "PA":
                id+=i["Company"] + "_" + i["Core"] + "_" + i["Variant"]
                return id
    elif isRelated:
        date = data_dict["CZPTTCISMessage"]["Identifiers"]["RelatedPlannedTransportIdentifiers"]
        id+=date["Company"] + "_" + date["Core"] + "_" + date["Variant"]
        return id
    else:
        date = data_dict["CZPTTCISMessage"]["Identifiers"]["PlannedTransportIdentifiers"]
        for i in date:
            if i["ObjectType"] == "PA":
                id+=i["Company"] + "_" + i["Core"] + "_" + i["Variant"]
                return id

def convert_date(date):
    date = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
    return date


print("Trying connection to MongoDB server (5000 ms timeout)...")
myclient = pymongo.MongoClient(config_upa.MONGOSERVER, serverSelectionTimeoutMS=5000)

# try block returns info about MongoDB connection, timeout erro elsewhere
try:
    myclient.server_info()
except:
    exit("Cannot establish connection to MongoDB server. Please check configuration file.")

print("Successfull connection to server " + config_upa.MONGOSERVER)
mongoDbName = myclient["UPA_cisjr"]
print(mongoDbName)
canceledCollName = mongoDbName["canceled"]
relatedCollName = mongoDbName["related"]
mainCollName = mongoDbName["main"]
locationsCollName = mongoDbName["locations"]

filter = '*.xml'

files = glob.glob(config_upa.FOLDER+"*")

n=len(files)
cnt=1
location_dict=defaultdict(list)
train_connection_dict=defaultdict(list)
for f in files:
    print("Reading file ({}/{}) : {}".format(cnt,n,f))
    isreleted=False
    with open(f, encoding="utf-8") as xml_file:
        data_dict = xmltodict.parse(xml_file.read(), encoding="utf-8")
        if "CZCanceledPTTMessage" in data_dict:
            data_dict["CZCanceledPTTMessage"]["PlannedCalendar"]["ValidityPeriod"]["StartDateTime"] = convert_date(data_dict["CZCanceledPTTMessage"]["PlannedCalendar"]["ValidityPeriod"]["StartDateTime"])
            data_dict["CZCanceledPTTMessage"]["PlannedCalendar"]["ValidityPeriod"]["EndDateTime"] = convert_date(data_dict["CZCanceledPTTMessage"]["PlannedCalendar"]["ValidityPeriod"]["EndDateTime"])
            train_id=gen_train_id(data_dict, True)
            connect_id=gen_connect_id(data_dict, True)
            if [x for x in canceledCollName.find({"TRAIN_ID": train_id, "PA_ID":connect_id, "ValidityPeriod":data_dict["CZCanceledPTTMessage"]["PlannedCalendar"]["ValidityPeriod"]})]==[]:
                cancel_dict = dict()
                cancel_dict["PA_ID"] = connect_id
                cancel_dict["TRAIN_ID"] = train_id
                cancel_dict["ValidityPeriod"] = data_dict["CZCanceledPTTMessage"]["PlannedCalendar"]["ValidityPeriod"]
                cancel_dict["BitmapDays"] = data_dict["CZCanceledPTTMessage"]["PlannedCalendar"]["BitmapDays"]
                x = canceledCollName.insert_one(cancel_dict)
        else:
            train_id=gen_train_id(data_dict)
            connect_id=gen_connect_id(data_dict)
            data_dict["CZPTTCISMessage"]["CZPTTInformation"]["PlannedCalendar"]["ValidityPeriod"]["StartDateTime"] = convert_date(data_dict["CZPTTCISMessage"]["CZPTTInformation"]["PlannedCalendar"]["ValidityPeriod"]["StartDateTime"])
            data_dict["CZPTTCISMessage"]["CZPTTInformation"]["PlannedCalendar"]["ValidityPeriod"]["EndDateTime"] = convert_date(data_dict["CZPTTCISMessage"]["CZPTTInformation"]["PlannedCalendar"]["ValidityPeriod"]["EndDateTime"])
                    
            if "RelatedPlannedTransportIdentifiers" in data_dict["CZPTTCISMessage"]["Identifiers"]:
                connect_id_n=gen_connect_id(data_dict,False,True)
                if [x for x in relatedCollName.find({"PA_ID_Related":connect_id_n, "ValidityPeriod":data_dict["CZPTTCISMessage"]["CZPTTInformation"]["PlannedCalendar"]["ValidityPeriod"]})]==[]:
                    isreleted=True
                    related_dict = dict()
                    related_dict["PA_ID"] = connect_id
                    related_dict["TRAIN_ID"] = train_id
                    related_dict["ValidityPeriod"] = data_dict["CZPTTCISMessage"]["CZPTTInformation"]["PlannedCalendar"]["ValidityPeriod"]
                    related_dict["PA_ID_Related"] = connect_id_n
                    related_dict["BitmapDays"] = data_dict["CZPTTCISMessage"]["CZPTTInformation"]["PlannedCalendar"]["BitmapDays"]
                    x = relatedCollName.insert_one(related_dict)
                else:
                    cnt+=1
                    continue
            if [x for x in mainCollName.find({"TRAIN_ID": train_id, "PA_ID":connect_id})]==[]:
                # prace s main daty
                data_dict["CZPTTCISMessage"]["CZPTTCreation"] = convert_date(data_dict["CZPTTCISMessage"]["CZPTTCreation"])
                del data_dict["CZPTTCISMessage"]["@xmlns:xsd"]
                del data_dict["CZPTTCISMessage"]["@xmlns:xsi"]
                data_dict["PA_ID"] = connect_id
                data_dict["TRAIN_ID"] = train_id
                x = mainCollName.insert_one(data_dict)
                for location in data_dict["CZPTTCISMessage"]["CZPTTInformation"]["CZPTTLocation"]:
                    if "TrainActivity" in location.keys():
                        activity=[x for x in location["TrainActivity"]]
                        if type(activity[0])==str:
                            if location["TrainActivity"]['TrainActivityType']=='0001':
                                x = locationsCollName.insert_one({"TRAIN_ID":train_id, "PA_ID":connect_id, "IsReleted":isreleted, "Location":location["Location"]["PrimaryLocationName"]})
                            continue
                        if '0001' in [x["TrainActivityType"] for x in location["TrainActivity"]]:
                            x = locationsCollName.insert_one({"TRAIN_ID":train_id, "PA_ID":connect_id, "IsReleted":isreleted, "Location":location["Location"]["PrimaryLocationName"]})
        cnt+=1
X = canceledCollName.create_index([('TRAIN_ID', pymongo.ASCENDING)])
X = canceledCollName.create_index([('PA_ID', pymongo.ASCENDING)])        
X = canceledCollName.create_index([('TRAIN_ID', pymongo.ASCENDING),('PA_ID', pymongo.ASCENDING)])
X = canceledCollName.create_index([('ValidityPeriod', pymongo.ASCENDING)])

X = mainCollName.create_index([('TRAIN_ID', pymongo.ASCENDING)])
X = mainCollName.create_index([('PA_ID', pymongo.ASCENDING)])        
X = mainCollName.create_index([('TRAIN_ID', pymongo.ASCENDING),('PA_ID', pymongo.ASCENDING)])

X = locationsCollName.create_index([('TRAIN_ID', pymongo.ASCENDING)])
X = locationsCollName.create_index([('PA_ID', pymongo.ASCENDING)])
X = locationsCollName.create_index([('Location', pymongo.ASCENDING)])                
X = locationsCollName.create_index([('TRAIN_ID', pymongo.ASCENDING),('PA_ID', pymongo.ASCENDING)])

X = relatedCollName.create_index([('PA_ID_Related', pymongo.ASCENDING)])  
X = relatedCollName.create_index([('ValidityPeriod', pymongo.ASCENDING)])


"""
result = mainCollName.find(
    {"CZPTTCISMessage.CZPTTInformation.CZPTTLocation.Location.PrimaryLocationName":"Brno hl. n."},
    {"CZPTTCISMessage.CZPTTInformation.CZPTTLocation.Location.PrimaryLocationName":1})
        
for doc in result:
    print(doc) 
"""
