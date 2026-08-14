"""
MQTT relay: subscribes to mosquitto (port 1884, receives from HiveMQ)
and republishes every message to zenoh-bridge-mqtt (port 1883).

This sidesteps mosquitto's bridge loop prevention, which blocks
forwarding messages received on one bridge out via another bridge.
"""
import time
import logging
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SOURCE_HOST = "localhost"
SOURCE_PORT = 1884   # mosquitto (receives from HiveMQ)

DEST_HOST = "localhost"
DEST_PORT = 1885     # zenoh-bridge-mqtt


def make_dest_client():
    c = mqtt.Client(client_id="relay-to-zenoh", clean_session=True)

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            log.info("Relay: connected to zenoh-bridge-mqtt (%s:%d)", DEST_HOST, DEST_PORT)
        else:
            log.warning("Relay: zenoh-bridge-mqtt connect failed rc=%d", rc)

    c.on_connect = on_connect
    return c


dest = make_dest_client()
dest.connect_async(DEST_HOST, DEST_PORT)
dest.loop_start()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Relay: connected to mosquitto (%s:%d), subscribing #", SOURCE_HOST, SOURCE_PORT)
        client.subscribe("#", qos=0)
    else:
        log.warning("Relay: mosquitto connect failed rc=%d", rc)


def on_message(client, userdata, msg):
    payload = b"routed " + msg.payload
    log.debug("Relay: %s (%d bytes) → zenoh", msg.topic, len(payload))
    dest.publish(msg.topic, payload, qos=0, retain=msg.retain)


def on_disconnect(client, userdata, rc):
    log.warning("Relay: disconnected from mosquitto (rc=%d), will reconnect", rc)


source = mqtt.Client(client_id="relay-from-mosquitto", clean_session=True)
source.on_connect = on_connect
source.on_message = on_message
source.on_disconnect = on_disconnect

while True:
    try:
        source.connect(SOURCE_HOST, SOURCE_PORT, keepalive=60)
        source.loop_forever()
    except Exception as e:
        log.error("Relay error: %s — retrying in 5s", e)
        time.sleep(5)
