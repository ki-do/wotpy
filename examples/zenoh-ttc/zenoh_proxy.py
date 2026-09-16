#!/usr/bin/env python

"""
Usage:
    python zenoh_proxy.py --source-td ./thing-td.json [--source-binding modbus] \
        [--router tcp/localhost:7447]
    python zenoh_proxy.py --source-td ./lorawan-td.json --lorawan-app-id <appID>
"""

import argparse
import asyncio
import base64
import copy
import json
import logging
import os
import re
import struct
import sys
from urllib.parse import urlparse

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

_EXAMPLE_DIR = os.path.dirname(os.path.abspath(__file__))
if _EXAMPLE_DIR not in sys.path:
    sys.path.insert(0, _EXAMPLE_DIR)

from tornado.ioloop import IOLoop

from wotpy.protocols.zenoh.server import ZenohServer
from wotpy.wot.servient import Servient
from wotpy.wot.td import ThingDescription
from wotpy.wot.thing import Thing

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

_RUNNING_SERVIENT = None
_LORAWAN_SUBSCRIBERS = []

TIMEOUT_PROP_READ = 30.0
TIMEOUT_PROP_WRITE = 30.0
TIMEOUT_HARD_FACTOR = 1.2
EVENT_RESUBSCRIBE_DELAY = 2.0
EVENT_RESUBSCRIBE_MAX_DELAY = 60.0

LORAWAN_WIRE_TYPES = {
    "u8": "B",
    "s8": "b",
    "u16": "H",
    "s16": "h",
    "u32": "I",
    "s32": "i",
    "u64": "Q",
    "s64": "q",
    "f32": "f",
    "f64": "d",
}


def _replace_forms(interactions):
    for interaction in interactions.values():
        interaction["forms"] = []


def _merge_generated_forms(proxy_td, generated_td):
    result = copy.deepcopy(proxy_td)
    for interaction_type in ("properties", "actions", "events"):
        generated_interactions = generated_td.get(interaction_type, {})
        for name, interaction in result.get(interaction_type, {}).items():
            if name in generated_interactions:
                interaction["forms"] = generated_interactions[name].get("forms", [])
    return result


def _decode_modbus_property_value(raw_value, source_property):
    forms = source_property.get("forms", []) if isinstance(source_property, dict) else []
    form = next((item for item in forms if "readproperty" in item.get("op", [])), None)
    if form is None:
        form = forms[0] if forms else {}

    wire_type = str(form.get("modbus:type", form.get("modv:dataType", ""))).lower()
    if wire_type not in ("float", "float32"):
        return raw_value

    if isinstance(raw_value, list) and len(raw_value) == 2 and all(isinstance(item, int) for item in raw_value):
        try:
            binary = raw_value[0].to_bytes(2, "big") + raw_value[1].to_bytes(2, "big")
            return round(struct.unpack(">f", binary)[0], 6)
        except (OverflowError, ValueError, struct.error):
            pass

    return raw_value


def build_property_read_proxy(consumed_thing, name, source_property=None):
    async def _proxy():
        awaitable = consumed_thing.properties[name].read(timeout=TIMEOUT_PROP_READ)
        raw_value = await asyncio.wait_for(awaitable, timeout=TIMEOUT_PROP_READ * TIMEOUT_HARD_FACTOR)
        return _decode_modbus_property_value(raw_value, source_property)

    return _proxy


def build_property_write_proxy(consumed_thing, name):
    async def _proxy(value):
        awaitable = consumed_thing.properties[name].write(value, timeout=TIMEOUT_PROP_WRITE)
        await asyncio.wait_for(awaitable, timeout=TIMEOUT_PROP_WRITE * TIMEOUT_HARD_FACTOR)

    return _proxy


def subscribe_event_proxy(consumed_thing, exposed_thing, name):
    state = {"subscription": None, "delay": EVENT_RESUBSCRIBE_DELAY}

    def subscribe():
        state["subscription"] = consumed_thing.events[name].subscribe(
            on_next=on_next,
            on_error=on_error)

    def on_next(item):
        state["delay"] = EVENT_RESUBSCRIBE_DELAY
        exposed_thing.events[name].emit(item.data)

    def on_error(error):
        delay = state["delay"]
        LOGGER.warning("Source event '%s' failed: %s (retrying in %.0fs)", name, error, delay)
        subscription = state["subscription"]
        if subscription is not None:
            subscription.dispose()
        asyncio.get_event_loop().call_later(delay, subscribe)
        state["delay"] = min(delay * 2, EVENT_RESUBSCRIBE_MAX_DELAY)

    subscribe()


def build_proxy_td(source_td, thing_id, thing_title, property_names):
    proxy_td = copy.deepcopy(source_td)
    proxy_td["properties"] = {
        name: proxy_td.get("properties", {})[name] for name in property_names
    }
    _replace_forms(proxy_td["properties"])
    _replace_forms(proxy_td.get("events", {}))

    if thing_id is not None:
        proxy_td["id"] = thing_id
    if thing_title is not None:
        proxy_td["title"] = thing_title

    return proxy_td


def preview_proxy_td(source_td, thing_id, thing_title, max_properties, router_url, event_topic_prefix=None):
    property_names = list(source_td.get("properties", {}).keys())[:max_properties]
    proxy_td = build_proxy_td(source_td, thing_id, thing_title, property_names)

    thing = Thing(thing_fragment=ThingDescription(proxy_td).to_thing_fragment())
    event_topic_builder = None
    if event_topic_prefix:
        event_topic_builder = lambda event: "{}/{}".format(event_topic_prefix, event.name)

    zenoh_server = ZenohServer(router_url=router_url, event_topic_builder=event_topic_builder)

    for interaction in thing.properties.values():
        for form in zenoh_server.build_forms(hostname=None, interaction=interaction):
            interaction.add_form(form)

    for interaction in thing.events.values():
        for form in zenoh_server.build_forms(hostname=None, interaction=interaction):
            interaction.add_form(form)

    generated_td = ThingDescription.from_thing(thing).to_dict()
    return _merge_generated_forms(proxy_td, generated_td)


async def expose_proxy(wot, consumed_thing, source_td, thing_id, thing_title, max_properties):
    property_names = list(consumed_thing.td.properties.keys())[:max_properties]
    proxy_td = build_proxy_td(source_td, thing_id, thing_title, property_names)

    exposed_thing = wot.produce(json.dumps(proxy_td))

    for name in property_names:
        exposed_thing.set_property_read_handler(
            name,
            build_property_read_proxy(consumed_thing, name, source_td.get("properties", {}).get(name)),
        )
        exposed_thing.set_property_write_handler(name, build_property_write_proxy(consumed_thing, name))

    for name in consumed_thing.td.events:
        subscribe_event_proxy(consumed_thing, exposed_thing, name)

    exposed_thing.expose()

    return exposed_thing


def infer_source_binding(source_td):
    schemes = [urlparse(str(source_td.get("base", ""))).scheme]

    for interaction_map in (source_td.get("properties", {}), source_td.get("events", {})):
        for interaction in interaction_map.values():
            for form in interaction.get("forms", []):
                schemes.append(urlparse(str(form.get("href", ""))).scheme)

    for scheme in schemes:
        if scheme.startswith("zenoh"):
            return "zenoh"
        if scheme.startswith("lorawan"):
            return "lorawan"
        if scheme.startswith("modbus"):
            return "modbus"
        if scheme.startswith("mqtt"):
            return "mqtt"

    raise ValueError("Could not infer a source binding; pass --source-binding explicitly")


def lorawan_event_topic_prefix(source_td, application_id):
    application_id = source_td.get("lorav:applicationID", application_id)
    dev_eui = source_td.get("lorav:devEUI")

    if not application_id:
        raise ValueError("LoRaWAN TD requires --lorawan-app-id or lorav:applicationID")
    if not dev_eui:
        raise ValueError("LoRaWAN TD is missing lorav:devEUI")

    return "application/{}/device/{}/event/up".format(application_id, dev_eui)


def decode_lorawan_events(source_td, uplink):
    """Decodes event values from a ChirpStack uplink using LoRaWAN form terms."""

    if isinstance(uplink, (bytes, bytearray)):
        payload = bytes(uplink)
    else:
        encoded = uplink.get("data") if isinstance(uplink, dict) else uplink
        if not isinstance(encoded, str):
            raise ValueError("LoRaWAN uplink must contain a base64 'data' value")
        payload = base64.b64decode(encoded, validate=True)

    values = {}
    for name, event in source_td.get("events", {}).items():
        form = next((item for item in event.get("forms", []) if "lorav:byteOffset" in item), None)
        if form is None:
            continue

        wire_type = form.get("lorav:wireType")
        if wire_type not in LORAWAN_WIRE_TYPES:
            raise ValueError("Unsupported LoRaWAN wire type '{}' for event '{}'".format(wire_type, name))

        byte_order = form.get("lorav:byteOrder", "big").lower()
        prefix = "<" if byte_order in ("little", "littleendian", "little-endian") else ">"
        value = struct.unpack_from(prefix + LORAWAN_WIRE_TYPES[wire_type], payload, int(form["lorav:byteOffset"]))[0]

        divisor = form.get("lorav:divisor")
        if divisor is not None:
            value /= divisor

        values[name] = value

    return values


def subscribe_lorawan_uplinks(zenoh_server, source_td, exposed_thing, application_id):
    """Subscribes to raw ChirpStack uplinks and emits decoded Thing events."""

    topic = lorawan_event_topic_prefix(source_td, application_id)
    loop = asyncio.get_running_loop()

    def on_uplink(sample):
        try:
            raw_payload = sample.payload.to_bytes()
            try:
                uplink = json.loads(raw_payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                uplink = raw_payload

            values = decode_lorawan_events(source_td, uplink)
            for name, value in values.items():
                loop.call_soon_threadsafe(exposed_thing.events[name].emit, value)
        except Exception:
            LOGGER.warning("Could not decode LoRaWAN uplink on %s", topic, exc_info=True)

    subscriber = zenoh_server._session.declare_subscriber(topic, on_uplink)
    _LORAWAN_SUBSCRIBERS.append(subscriber)
    LOGGER.info("Subscribed to LoRaWAN uplinks on %s", topic)

    return subscriber


def build_source_client(source_binding):
    if source_binding == "modbus":
        from wotpy.protocols.modbus.client import ModbusClient
        return ModbusClient()

    if source_binding == "mqtt":
        from wotpy.protocols.mqtt.client import MQTTClient
        return MQTTClient()

    raise ValueError("Unsupported source binding: {}".format(source_binding))


def _slugify_urn(value):
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _load_source(source_td_path, source_binding):
    with open(source_td_path, "r", encoding="utf-8") as fobj:
        source_td = json.load(fobj)

    source_binding = infer_source_binding(source_td) if source_binding == "auto" else source_binding

    return source_td, source_binding


def _proxy_identity(source_td, thing_id, thing_title, is_only_source):
    if thing_id is not None or thing_title is not None:
        return thing_id or source_td.get("id"), thing_title or source_td.get("title")

    if source_td.get("id") or source_td.get("title"):
        return source_td.get("id"), source_td.get("title")

    if is_only_source:
        return thing_id, thing_title

    slug = _slugify_urn(source_td.get("title", "thing"))
    return "urn:zenoh:proxy:{}".format(slug), source_td.get("title", "ZenohProxy")


async def main(source_td_paths, source_bindings, router_url, max_properties, thing_id, thing_title, catalogue_port, servient_id=None, lorawan_app_id=None, dry_run=False):
    source_bindings = list(source_bindings) + ["auto"] * (len(source_td_paths) - len(source_bindings))
    sources = [_load_source(path, binding) for path, binding in zip(source_td_paths, source_bindings)]
    is_only_source = len(sources) == 1

    if dry_run:
        previews = [
            preview_proxy_td(
                source_td,
                *_proxy_identity(source_td, thing_id, thing_title, is_only_source),
                max_properties,
                router_url,
                lorawan_event_topic_prefix(source_td, lorawan_app_id) if binding == "lorawan" else None)
            for source_td, binding in sources
        ]
        print(json.dumps(previews if len(previews) > 1 else previews[0], indent=2))
        IOLoop.current().stop()
        return

    bindings_needed = {binding for _, binding in sources if binding != "lorawan"}
    clients = {binding: build_source_client(binding) for binding in bindings_needed}

    lorawan_topics = {}
    for source_td, binding in sources:
        if binding == "lorawan":
            proxy_id, _ = _proxy_identity(source_td, thing_id, thing_title, is_only_source)
            lorawan_topics[proxy_id] = lorawan_event_topic_prefix(source_td, lorawan_app_id)

    def event_topic_builder(event):
        prefix = lorawan_topics.get(event.thing.id)
        if prefix:
            return "{}/{}".format(prefix, event.name)
        return "{}/event/{}/{}".format(zenoh_server.servient_id, event.thing.url_name, event.url_name)

    servient = Servient(catalogue_port=catalogue_port, clients=list(clients.values()))
    zenoh_server = ZenohServer(
        router_url=router_url,
        servient_id=servient_id,
        event_topic_builder=event_topic_builder if lorawan_topics else None)
    servient.add_server(zenoh_server)

    global _RUNNING_SERVIENT
    _RUNNING_SERVIENT = servient

    wot = await servient.start()

    for source_td, source_binding in sources:
        LOGGER.info("Consuming source Thing using the %s binding", source_binding)

        proxy_thing_id, proxy_thing_title = _proxy_identity(source_td, thing_id, thing_title, is_only_source)
        property_names = list(source_td.get("properties", {}).keys())[:max_properties]

        if source_binding == "lorawan":
            proxy_td = build_proxy_td(source_td, proxy_thing_id, proxy_thing_title, [])
            exposed_thing = wot.produce(json.dumps(proxy_td))
            exposed_thing.expose()
            subscribe_lorawan_uplinks(zenoh_server, source_td, exposed_thing, lorawan_app_id)
        else:
            consumed_thing = wot.consume(json.dumps(source_td))

            exposed_thing = await expose_proxy(
                wot=wot,
                consumed_thing=consumed_thing,
                source_td=source_td,
                thing_id=proxy_thing_id,
                thing_title=proxy_thing_title,
                max_properties=max_properties)

        exposed_td = _merge_generated_forms(
            build_proxy_td(source_td, proxy_thing_id, proxy_thing_title, property_names),
            ThingDescription.from_thing(exposed_thing.thing).to_dict(),
        )
        print(json.dumps(exposed_td, indent=2))

    LOGGER.info("Zenoh proxy for %d source(s) exposed on router %s", len(sources), router_url)

    if catalogue_port is not None:
        LOGGER.info("TD catalogue available at http://localhost:%s", catalogue_port)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Expose one or more source Things as Zenoh proxy Things")
    parser.add_argument("--source-td", action="append", required=True, help="Path to a valid source Thing Description (repeatable)")
    parser.add_argument("--source-binding", action="append", choices=["auto", "modbus", "mqtt", "lorawan"], default=[], help="Source protocol binding per --source-td, same order (default: infer from TD forms)")
    parser.add_argument("--router", default="tcp/localhost:7447", help="Zenoh router URL")
    parser.add_argument("--max-properties", type=int, default=15, help="Maximum number of properties to proxy")
    parser.add_argument("--thing-id", default=None, help="Override the ID of the exposed Zenoh Thing")
    parser.add_argument("--thing-title", default=None, help="Override the title of the exposed Zenoh Thing")
    parser.add_argument("--catalogue-port", type=int, default=9292, help="TD catalogue port (0 to disable)")
    parser.add_argument("--servient-id", default=None, help="Zenoh servient/topic namespace (default: 'wotpy'). Set this to avoid colliding with other proxy instances sharing the same router.")
    parser.add_argument("--lorawan-app-id", default=os.environ.get("LORAWAN_APP_ID"), help="ChirpStack application ID for LoRaWAN TDs (or set LORAWAN_APP_ID)")
    parser.add_argument("--dry-run", action="store_true", help="Only print the proxy TD(s); do not connect to any source or Zenoh")
    args = parser.parse_args()

    IOLoop.current().add_callback(
        main,
        source_td_paths=args.source_td,
        source_bindings=args.source_binding,
        router_url=args.router,
        max_properties=args.max_properties,
        thing_id=args.thing_id,
        thing_title=args.thing_title,
        catalogue_port=args.catalogue_port or None,
        servient_id=args.servient_id,
        lorawan_app_id=args.lorawan_app_id,
        dry_run=args.dry_run)

    try:
        IOLoop.current().start()
    except KeyboardInterrupt:
        LOGGER.info("Interrupted, closing the Zenoh session...")
        for subscriber in _LORAWAN_SUBSCRIBERS:
            subscriber.undeclare()
        if _RUNNING_SERVIENT is not None:
            IOLoop.current().run_sync(_RUNNING_SERVIENT.shutdown)
