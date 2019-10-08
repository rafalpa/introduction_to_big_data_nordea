# Script to generate dummy signup data.
# The format is suitable for bulk insert into Elasticsearch.

# Here is a sample record generated by this script.
'''
{"index": {"_type": "signup", "_index": "signups"}}
{"metricCount": 1, "referrer": "HN", "client": "AndroidApp", "monthlySpend": 7632, "country": "U.K.", "signedUpOn": "2018-01-19T19:44:32", "metricName": "Signup"}
'''

import json
import random
import time
from datetime import datetime, timedelta
import calendar
import pytz

COUNTRIES = ['India', 'U.S.', 'U.K.', 'Germany', 'France', 'Japan', 'China']
REFERRERS = ['Google', 'Friend', 'Colleague', 'Reddit', 'HN']
CLIENTS = ['AndroidApp', 'iOSApp', 'Web']
END_TIME = datetime.now()
START_TIME =  END_TIME - timedelta(weeks=3)
END_TIME_IN_EPOCH = calendar.timegm(END_TIME.timetuple())
START_TIME_IN_EPOCH = calendar.timegm(START_TIME.timetuple())

index_name_dict = {"index": {"_index": "signups", "_type": "signup"}}
index_name_string = json.dumps(index_name_dict)

f = open('/home/rafalpa/signups.csv', 'w')

for i in range(1, 10000):
    record = {}
    record['metricName'] = 'Signup'
    record['metricCount'] = 1
    record['client'] = random.choice(CLIENTS)
    record['country'] = random.choice(COUNTRIES)
    record['referrer'] = random.choice(REFERRERS)
    random_timestamp = random.randrange(START_TIME_IN_EPOCH, END_TIME_IN_EPOCH)
    record['signedUpOn'] = datetime.fromtimestamp(random_timestamp).isoformat()
    record['monthlySpend'] = random.randrange(0, 10000)
    f.write(index_name_string)
    f.write("\n")
    f.write(json.dumps(record))
    f.write("\n")

f.close()