#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2022 Tim Hawes
#
# SPDX-License-Identifier: MIT

import logging
import os
import queue
import threading
import time

import paho.mqtt.client as mqtt
import requests


logging.basicConfig(level=logging.INFO)

MQTT_HOST = os.environ.get("MQTT_HOST", "mqtt")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
INFLUX_URLS = os.environ["INFLUX_URLS"].split(" ")
INFLUX_MEASUREMENT_PREFIX = os.environ.get("INFLUX_MEASUREMENT_PREFIX", "mqtt.")
MAX_SEND_INTERVAL = int(os.environ.get("MAX_SEND_INTERVAL", "5"))
MAX_SEND_METRICS = int(os.environ.get("MAX_SEND_METRICS", "100"))

send_queue = queue.Queue()


class InfluxWriterThread(threading.Thread):
    def run(self):
        data = []
        last_send = time.time()
        while True:
            try:
                m = send_queue.get(timeout=1)
                data.append(m)
            except queue.Empty:
                pass
            if len(data) > MAX_SEND_METRICS or time.time() - last_send > MAX_SEND_INTERVAL:
                data_string = "\n".join(data)
                for url in INFLUX_URLS:
                    try:
                        r = requests.post(url, data=data_string)
                        r.raise_for_status()
                    except Exception as e:
                        logging.exception(
                            "Exception while sending to {} to {}".format(
                                data_string, url
                            )
                        )
                last_send = time.time()
                data = []


def on_connect(client, userdata, flags, rc):
    client.subscribe("#")


def on_message(client, userdata, msg):
    if msg.retain:
        return

    try:
        data = msg.payload.decode()
    except UnicodeDecodeError:
        logging.warning("parse error> {}".format(msg.payload))
        return

    logging.debug("mqtt> {} {}".format(msg.topic, msg.payload))

    if data.lower() in ["false", "f", "off", "low", "closed", "up"]:
        v = 0
    elif data.lower() in ["true", "t", "on", "high", "open", "down"]:
        v = 1
    elif data.startswith("{"):
        # ignore json
        return
    else:
        try:
            v = float(data)
        except ValueError:
            logging.debug("parse error> {}".format(data))
            return

    packet = "{}{} value={} {}".format(
        INFLUX_MEASUREMENT_PREFIX,
        msg.topic.replace("/", ".").replace(" ", "\\ "),
        v,
        int(time.time() * 1_000_000_000),
    )
    logging.debug("packet> {}".format(packet))
    send_queue.put(packet)


writer = InfluxWriterThread()
writer.daemon = True
writer.start()

m = mqtt.Client()
m.enable_logger()
m.on_connect = on_connect
m.on_message = on_message
m.connect(MQTT_HOST, MQTT_PORT)
m.loop_forever()
