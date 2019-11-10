#!python3
###demo code provided by Steve Cope at www.steves-internet-guide.com
##email steve@steves-internet-guide.com
###Free to use for any purpose

"""
This will log messages to file.Los time,message and topic as JSON data
"""
import paho.mqtt.client as mqtt
import json
import os
import time
import sys, getopt, random
# import logging
# import mlogger as mlogger
import threading
from queue import Queue

from util.db_adapter import DbAdapter

q = Queue()

##### User configurable data section
username = ""
password = ""
verbose = False  # True to display all messages, False to display only changed messages
mqttclient_log = False  # MQTT client logs showing messages
# logging.basicConfig(level=logging.INFO)  # error logging
# use DEBUG,INFO,WARNING
####
options = dict()
##EDIT HERE ###############
brokers = ["13.232.85.94", "broker.hivemq.com"]
options["broker"] = brokers[0]
options["port"] = 1883
options["verbose"] = True
options["cname"] = ""
options["topics"] = [("JARS/+", 0)]
# options["topics"] = [("bbc/#", 0), ("homeautomation", 0), ("/HomeCtrl", 0), \
#                      ("/hometest", 0)]
# options["topics"] = [("steves-house/#", 0)]
# options["topics"] = [("sig/#", 0)]
# sql
db_file = "logs.db"
Table_name = "logs"
table_fields = {
    "id": "integer primary key autoincrement",
    "time": "int",
    "topic": "text",
    "sensor": "text",
    "message": "text", }
###
cname = ""
sub_flag = ""
timeout = 60
messages = dict()
last_message = dict()

DATABASE = {
    'host': '127.0.0.1',
    'port': 3306,
    'db': 'mqtt_db',
    'user': 'root',
    'password': 'password'
}


######
def command_input(options={}):
    topics_in = []
    qos_in = []

    valid_options = " -b <broker> -p <port>-t <topic> -q QOS -v <verbose> -h <help>\
-c <loop Time secs -d logging debug  -n Client ID or Name\
-i loop Interval -u Username -P Password\
"
    print_options_flag = False
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hb:i:dk:p:t:q:l:vn:u:P:")
    except getopt.GetoptError:
        print(sys.argv[0], valid_options)
        sys.exit(2)
    qos = 0

    for opt, arg in opts:
        if opt == '-h':
            print(sys.argv[0], valid_options)
            sys.exit()
        elif opt == "-b":
            options["broker"] = str(arg)
        elif opt == "-i":
            options["interval"] = int(arg)
        elif opt == "-k":
            options["keepalive"] = int(arg)
        elif opt == "-p":
            options["port"] = int(arg)
        elif opt == "-t":
            topics_in.append(arg)
        elif opt == "-q":
            qos_in.append(int(arg))
        elif opt == "-n":
            options["cname"] = arg
        elif opt == "-d":
            options["loglevel"] = "DEBUG"
        elif opt == "-P":
            options["password"] = str(arg)
        elif opt == "-u":
            options["username"] = str(arg)
        elif opt == "-v":
            options["verbose"] = True

    lqos = len(qos_in)
    for i in range(len(topics_in)):
        if lqos > i:
            topics_in[i] = (topics_in[i], int(qos_in[i]))
        else:
            topics_in[i] = (topics_in[i], 0)

    if topics_in:
        options["topics"] = topics_in  # array with qos


####

# callbacks -all others define in functions module
def on_connect(client, userdata, flags, rc):
    # logging.debug("Connected flags" + str(flags) + "result code " \
    #               + str(rc) + "client1_id")
    if rc == 0:
        client.connected_flag = True
    else:
        client.bad_connection_flag = True


def on_disconnect(client, userdata, rc):
    # logging.debug("disconnecting reason  " + str(rc))
    client.connected_flag = False
    client.disconnect_flag = True
    client.subscribe_flag = False


def on_subscribe(client, userdata, mid, granted_qos):
    m = "in on subscribe callback result " + str(mid)
    # logging.debug(m)
    client.subscribed_flag = True


def on_message(client, userdata, msg):
    topic = msg.topic
    m_decode = str(msg.payload.decode("utf-8", "ignore"))
    message_handler(client, m_decode, topic)
    # print("message received")


def message_handler(client, msg, topic):
    data = dict()
    tnow = time.localtime(time.time())
    m = time.asctime(tnow) + " " + topic + " " + msg
    data["time"] = int(time.time())
    data["topic"] = topic
    data["message"] = msg
    if has_changed(topic, msg):
        # print("storing changed data",topic, "   ",msg)
        q.put(data)  # put messages on queue


def has_changed(topic, msg):
    topic2 = topic.lower()
    if topic in last_message:
        if last_message[topic] == msg:
            return False
    last_message[topic] = msg
    return True


def log_worker():
    """runs in own thread to log data"""
    # create logger
    # logger = SQL_data_logger(db_file)
    # logger.drop_table("logs")
    # logger.create_table("logs", table_fields)
    while Log_worker_flag:
        while not q.empty():
            data = q.get()
            if data is None:
                continue
            try:
                time = data["time"]
                topic = data["topic"]
                message = data["message"]
                print(message)
                sensor = "Dummy-sensor"
                data_out = [time, topic, sensor, message]
                # data_query = "INSERT INTO " + \
                #              Table_name + "(time,topic,sensor,message)VALUES(?,?,?,?)"
                # # logger.Log_sensor(data_query, data_out)

                print(data_out)
                table_data = json.loads(data_out[3])
                if table_data["device"] == "PS1":
                    dump_ps1_data(table_data)
                elif table_data['device'] == 'PT1':
                    dump_pt1_data(table_data)
                    pass
            except Exception as e:
                print("problem with logging ", e)
    # logger.conn.close()

    # print("message saved ",results["message"])


########################

def dump_ps1_data(data):
    db_connection = DbAdapter(DATABASE)
    payload = {}
    payload['location'] = data['location']
    payload['device'] = data['device']
    cnt = 1
    for i in range(4):
        payload['ph{}'.format(cnt)] = data['data'][i]['ph{}'.format(cnt)]
        cnt += 1
    print(payload)

    db_connection.manipulate_record(
        'INSERT INTO ps1(location,device,ph1,ph2,ph3,ph4) '
        'VALUES (%s,%s,%s,%s,%s,%s)',
        [tuple(payload.values())])


def dump_pt1_data(data):
    db_connection = DbAdapter(DATABASE)
    payload = {}
    payload['location'] = data['location']
    payload['device'] = data['device']
    params = ['Temp', 'Hum']
    for i in range(2):
        payload[params[i]] = data['data'][i][params[i]]
    print(payload)
    db_connection.manipulate_record(
        'INSERT INTO pt1(location,device,temp, hum) '
        'VALUES (%s,%s,%s,%s)',
        [tuple(payload.values())])


####


def Initialise_clients(cname, cleansession=True):
    # flags set
    client = mqtt.Client(cname)
    if mqttclient_log:  # enable mqqt client logging
        client.on_log = on_log
    client.on_connect = on_connect  # attach function to callback
    client.on_message = on_message  # attach function to callback
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe
    return client


###


###########
def convert(t):
    d = ""
    for c in t:  # replace all chars outside BMP with a !
        d = d + (c if ord(c) < 0x10000 else '!')
    return (d)


def print_out(m):
    if display:
        print(m)


########################main program
if __name__ == "__main__" and len(sys.argv) >= 2:
    command_input(options)
    pass
verbose = options["verbose"]

if not options["cname"]:
    r = random.randrange(1, 10000)
    cname = "logger-" + str(r)
else:
    cname = "logger-" + str(options["cname"])

# Initialise_client_object() # add extra flags
# logging.info("creating client" + cname)
client = Initialise_clients(cname, False)  # create and initialise client object
topics = options["topics"]
broker = options["broker"]
port = 1883
keepalive = 60
# if options["username"] != "broker":
#     print("setting username")
#     client.username_pw_set(options["username"], options["password"])
# print("starting = ", options["username"])

##
t = threading.Thread(target=log_worker)  # start logger
Log_worker_flag = True
t.start()  # start logging thread
###
client.connected_flag = False  # flag for connection
client.bad_connection_flag = False
client.subscribed_flag = False
client.loop_start()
client.connect(broker, port)
while not client.connected_flag:  # wait for connection
    time.sleep(1)
client.subscribe(topics)
while not client.subscribed_flag:  # wait for connection
    time.sleep(1)
    print("waiting for subscribe")
print("subscribed ", topics)
# loop and wait until interrupted
try:
    while True:
        pass
except KeyboardInterrupt:
    print("interrrupted by keyboard")

client.loop_stop()  # final check for messages
time.sleep(5)
Log_worker_flag = False  # stop logging thread
print("ending ")
