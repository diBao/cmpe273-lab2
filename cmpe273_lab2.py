#Author Di Bao

import logging
logging.basicConfig(level=logging.DEBUG)
import json
import httplib
from collections import OrderedDict
#import operator
#from flask import jsonify
from spyne import Application, rpc, ServiceBase, Integer, Unicode
from spyne import Iterable
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.server.wsgi import WsgiApplication

class HelloWorldService(ServiceBase):
    
    @rpc(float, float, float, _returns=Iterable(Unicode))
    def checkcrime(ctx, lat, lon, radius):
        url = "https://api.spotcrime.com/crimes.json?lat="+ \
            str(lat)+"&lon="+str(lon)+"&radius="+str(radius)+"&key=."
        conn = httplib.HTTPConnection("api.spotcrime.com")
        conn.request(method="GET",url=url) 

        response = conn.getresponse()
        #res = response.read()
        req = json.loads(response.read())
        key = req.keys()
        records = req[key[0]]   #save all the crime records
        
        totalCrime = len(records)
        typeList = {}
        streets = {}
        eventTimeCount = {
            "12:01am-3am" : 0, \
            "3:01am-6am" : 0, \
            "6:01am-9am" : 0, \
            "9:01am-12noon" : 0, \
            "12:01pm-3pm" : 0, \
            "3:01pm-6pm" : 0, \
            "6:01pm-9pm" : 0, \
            "9:01pm-12midnight" : 0
        }
        
        i = j = 0
        while i < totalCrime:
            record = (dict)(records[i])

            date = record["date"].split(' ')
            time = date[1].split(":")
            hour = (int)(time[0])
            minute = (int)(time[1])
            period = date[2]

            #time count
            if(period == "AM"):
                if(hour==12 and minute==0):
                    eventTimeCount["9:01pm-12midnight"] += 1
                elif(hour==12 and minute!=0) or (hour<=2) and (hour==3 and minute==0):
                    eventTimeCount["12:01am-3am"] += 1
                elif(hour==3 and minute!=0) or (hour<=5) and (hour==6 and minute==0):
                    eventTimeCount["3:01am-6am"] += 1
                elif(hour==6 and minute!=0) or (hour<=8) and (hour==9 and minute==0):
                    eventTimeCount["6:01am-9am"] += 1
                elif(hour==9 and minute!=0) or (hour<=11):
                    eventTimeCount["9:01am-12noon"] += 1
            elif(period == "PM"):
                if(hour==12 and minute==0):
                    eventTimeCount["9:01am-12noon"] += 1
                elif(hour==12 and minute!=0) or (hour<=2) and (hour==3 and minute==0):
                    eventTimeCount["12:01pm-3pm"] += 1
                elif(hour==3 and minute!=0) or (hour<=5) and (hour==6 and minute==0):
                    eventTimeCount["3:01pm-6pm"] += 1
                elif(hour==6 and minute!=0) or (hour<=8) and (hour==9 and minute==0):
                    eventTimeCount["6:01pm-9pm"] += 1
                elif(hour==9 and minute!=0) or (hour<=11):
                    eventTimeCount["9:01pm-12midnight"] += 1

            #type count
            criType = record["type"]
            if criType not in typeList.keys():
                typeList[criType] = 0
            typeList[criType] += 1

            #dangerous street count
            j = 0
            k = -1
            st = []
            address = record["address"].split(" ")
            street = []
            addLen = len(address)
            while (address[j] != "OF") and (address[j] != "&"):
                if(j < addLen-1):
                    j += 1
                else:
                    break
            j += 1
            
            if addLen <= 5 and ("BLOCK" not in address):
                st = ' '.join(address)
            elif j == addLen:
                k = -1
                while address[k] != "BLOCK":
                    k -= 1
                st = ' '.join(address[(addLen+k+1):addLen])
            else: 
                while j < addLen:
                    street.append(address[j])
                    j += 1
                st = ' '.join(street)

            if st not in streets.keys(): 
                streets[st] = 0
            streets[st] += 1
            
            i += 1

        #totalType = len(typeList)
        #totalStreet = len(streets)
        
        sortedType = OrderedDict(sorted(typeList.items(), key=lambda t: t[1], reverse=True))
        sortedStreet = OrderedDict(sorted(streets.items(), key=lambda t: t[1], reverse=True))

        res = { "total_crime" : totalCrime,
                "the_most_dangerous_streets" : sortedStreet.keys()[:3],
                "crime_type_count" : sortedType,
                "event_time_count" : eventTimeCount
        }

        yield res

application = Application([HelloWorldService],
    tns = 'spyne.examples.hello',
    in_protocol = HttpRpc(validator='soft'),
    out_protocol = JsonDocument()
)

if __name__ == '__main__':
    # You can use any Wsgi server. Here, we chose
    # Python's built-in wsgi server but you're not
    # supposed to use it in production.
    from wsgiref.simple_server import make_server

    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()