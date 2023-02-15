import argparse
import datetime
import pymongo
import config_upa

parser = argparse.ArgumentParser(description='Skript, ktery podle zastavky odkud a kam a casu zjisti spojeni')
parser.add_argument("-o", "--odkud", dest="from_stop", help="zastavka odkud ma spojeni jet")
parser.add_argument("-d", "--do", dest="to_stop", help="zastavka kam ma spojeni jet")
parser.add_argument("-c", "--cas", dest="at_time", help="cas ve ktery ma zastavka jet. Ve formatu: 2022-04-21T09:40:19")

args = parser.parse_args()
TEST_DATE = datetime.datetime.strptime(args.at_time, "%Y-%m-%dT%H:%M:%S")

FROM = args.from_stop
TO = args.to_stop

mongoClient = pymongo.MongoClient(config_upa.MONGOSERVER)
db = mongoClient["UPA_cisjr"]

canceledCollection = db["canceled"]
relatedCollection = db['related']
mainCollection = db['main']
locationCollection = db['locations']

def is_day_in_mask(plannedCalendar):
    validityPeriod = plannedCalendar['ValidityPeriod']
    startDate = validityPeriod['StartDateTime']
    endDate = validityPeriod['EndDateTime']

    if type(startDate) == str:
        startDate = datetime.datetime.strptime(startDate, "%Y-%m-%dT%H:%M:%S")
    if type(endDate) == str:
        endDate = datetime.datetime.strptime(endDate, "%Y-%m-%dT%H:%M:%S")

    if startDate < TEST_DATE < endDate:
        daysDiff = TEST_DATE - startDate

        if plannedCalendar['BitmapDays'][daysDiff.days] == '1':
            return True
    return False
def get_location_transports():
    fromLocations = list(locationCollection.find({'Location': {'$eq': FROM}}))
    toLocations = list(locationCollection.find({'Location': {'$eq': TO}}))

    a = len(fromLocations)
    b = len(toLocations)
    trainIds = []

    for i in range(a):
        for j in range(b):
            if fromLocations[i]['TRAIN_ID'] == toLocations[j]['TRAIN_ID']:
                trainIds.append(fromLocations[i])
                break

    finalTrainIds = []

    for trainId in trainIds:
        relatedTrain = relatedCollection.find_one({'TRAIN_ID': {'$eq': trainId['PA_ID']}})

        if not trainId['IsReleted']:
            if relatedTrain is not None:
                if not is_day_in_mask(relatedTrain):
                    finalTrainIds.append(trainId)

        if trainId['IsReleted']:
            if relatedTrain is not None:
                if is_day_in_mask(relatedTrain):
                    finalTrainIds.append(mainCollection.find_one({'TRAIN_ID': {'$eq': relatedTrain['TRAIN_ID']}}))
                else:
                    finalTrainIds.append(trainId)
            else:
                finalTrainIds.append(trainId)
    return trainIds


locationTransports = get_location_transports()
trainsInPath = []

for locationTransport in locationTransports:
    foundTrains = mainCollection.find({'TRAIN_ID': {'$eq': locationTransport['TRAIN_ID']}})

    for train in foundTrains:
        trainsInPath.append(train)


trainsInDate = []

for trainInPath in trainsInPath:
    plannedCalendar = trainInPath['CZPTTCISMessage']['CZPTTInformation']['PlannedCalendar']
    if is_day_in_mask(plannedCalendar):
        trainsInDate.append(trainInPath)


def locations_are_in_order(train_stops):
    firstStop = False

    for location in train_stops:
        if location['Location']['PrimaryLocationName'] == FROM:
            firstStop = True

        if location['Location']['PrimaryLocationName'] == TO:
            break
    return firstStop


trainInDateAndTrajectory = []

for trainInDate in trainsInDate:
    locations = trainInDate['CZPTTCISMessage']['CZPTTInformation']['CZPTTLocation']
    if locations_are_in_order(locations):
        trainInDateAndTrajectory.append(trainInDate)


def is_in_canceled_trains(canceled_trains):
    for canceledTrain in canceled_trains:
        if is_day_in_mask(canceledTrain):
            return True
    return False


notCanceledTrains = []
for train in trainInDateAndTrajectory:
    canceledTrains = canceledCollection.find({'TRAIN_ID': {'$eq': train['TRAIN_ID']}})

    if not is_in_canceled_trains(list(canceledTrains)):
        notCanceledTrains.append(train)

print(f'Nalezeno {len(notCanceledTrains)} vlaku jdoucí od {FROM} do {TO} v čase {TEST_DATE}')

for notCanceledTrain in notCanceledTrains:
    print('VLAK: ', notCanceledTrain['TRAIN_ID'])

    for trainStop in notCanceledTrain['CZPTTCISMessage']['CZPTTInformation']['CZPTTLocation']:
        print(trainStop['Location']['PrimaryLocationName'], ': ', end='')
        try:
            timing = trainStop['TimingAtLocation']['Timing']

            if type(timing) == list:
                time = timing[0]['Time']
            else:
                time = timing['Time']
            print(str(time).split('.')[0])
        except:
            pass
print(f'Vlaku celkem: {len(notCanceledTrains)}')
