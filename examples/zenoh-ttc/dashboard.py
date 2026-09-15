#!/usr/bin/env python

"""
Zenoh IoT Onboarding Dashboard.

Usage:
    python dashboard.py [--router tcp/localhost:7447] [--port 8899]
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import uuid
from collections import deque

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import tornado.web
from tornado.ioloop import IOLoop

import zenoh_proxy

from wotpy.protocols.modbus.client import ModbusClient
from wotpy.protocols.mqtt.client import MQTTClient
from wotpy.protocols.zenoh.client import ZenohClient
from wotpy.protocols.zenoh.server import ZenohServer
from wotpy.utils.utils import to_json_obj
from wotpy.wot.servient import Servient
from wotpy.wot.td import ThingDescription

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

EVENT_BUFFER_SIZE = 50
TIMEOUT_ACTION = 30.0
TIMEOUT_ACTION_HARD_FACTOR = 1.2

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))

APP = None


def build_action_invoke_proxy(consumed_thing, name):
    """Factory for proxy Action invocation handlers."""

    async def _proxy(params):
        input_value = params.get("input") if isinstance(params, dict) else None
        awaitable = consumed_thing.actions[name].invoke(input_value, timeout=TIMEOUT_ACTION)
        return await asyncio.wait_for(awaitable, timeout=TIMEOUT_ACTION * TIMEOUT_ACTION_HARD_FACTOR)

    return _proxy


def build_dashboard_proxy_td(source_td, thing_id, thing_title, property_names, action_names):
    """Builds the fresh Thing Description for the onboarded device's Zenoh proxy Thing."""

    source_properties = source_td.get("properties", {})
    source_actions = source_td.get("actions", {})
    source_events = source_td.get("events", {})

    return {
        "@context": [
            "https://www.w3.org/2019/wot/td/v1",
            "https://www.w3.org/2022/wot/td/v1.1",
        ],
        "id": thing_id,
        "title": thing_title,
        "description": "Zenoh proxy for the onboarded source Thing '{}'".format(source_td.get("title", "")),
        "securityDefinitions": {"nosec_sc": {"scheme": "nosec"}},
        "security": "nosec_sc",
        "properties": {
            name: zenoh_proxy._strip_binding_terms(source_properties[name]) for name in property_names
        },
        "actions": {
            name: {key: value for key, value in source_actions[name].items() if key != "forms"}
            for name in action_names
        },
        "events": {
            name: zenoh_proxy._strip_binding_terms(source_events[name]) for name in source_events
        },
    }


class DashboardApp:
    """Owns the two servients (source-side proxy, Zenoh-side consumer) and the
    in-memory registry of onboarded devices."""

    def __init__(self, router_url, servient_id):
        self._router_url = router_url
        self._servient_id = servient_id
        self.proxy_wot = None
        self.consumer_wot = None
        self.devices = {}

    async def start(self):
        proxy_servient = Servient(catalogue_port=None, clients=[ModbusClient(), MQTTClient()])
        proxy_servient.add_server(ZenohServer(router_url=self._router_url, servient_id=self._servient_id))
        self.proxy_wot = await proxy_servient.start()

        consumer_servient = Servient(catalogue_port=None, clients=[ZenohClient()])
        self.consumer_wot = await consumer_servient.start()

        self._proxy_servient = proxy_servient
        self._consumer_servient = consumer_servient

    async def shutdown(self):
        for device_id in list(self.devices.keys()):
            await self.offboard(device_id)

        await self._consumer_servient.shutdown()
        await self._proxy_servient.shutdown()

    async def onboard(self, td_bytes, binding_hint="auto"):
        source_tds = json.loads(td_bytes)
        if not isinstance(source_tds, list):
            source_tds = [source_tds]
        if not source_tds:
            raise ValueError("TD file contains no Thing Descriptions")

        return [await self._onboard_td(source_td, binding_hint) for source_td in source_tds]

    async def _onboard_td(self, source_td, binding_hint="auto"):
        if not isinstance(source_td, dict):
            raise ValueError("Each Thing Description must be a JSON object")

        binding = zenoh_proxy.infer_source_binding(source_td) if binding_hint in (None, "auto") else binding_hint

        suffix = uuid.uuid4().hex[:8]
        base_slug = zenoh_proxy._slugify_urn(source_td.get("title", "device")) or "device"
        device_id = "urn:dashboard:device:{}-{}".format(base_slug, suffix)
        device_title = "{} ({})".format(source_td.get("title", "Device"), suffix)

        if binding == "zenoh":
            exposed_thing = None
            exposed_td_dict = source_td
            device_consumed_thing = self.consumer_wot.consume(json.dumps(source_td))
            property_names = list(device_consumed_thing.td.properties.keys())
            action_names = list(device_consumed_thing.td.actions.keys())
            event_names = list(device_consumed_thing.td.events.keys())
        else:
            consumed_thing = self.proxy_wot.consume(json.dumps(source_td))

            property_names = list(consumed_thing.td.properties.keys())
            action_names = list(consumed_thing.td.actions.keys())
            event_names = list(consumed_thing.td.events.keys())

            proxy_td = build_dashboard_proxy_td(source_td, device_id, device_title, property_names, action_names)
            exposed_thing = self.proxy_wot.produce(json.dumps(proxy_td))

            for name in property_names:
                exposed_thing.set_property_read_handler(
                    name,
                    zenoh_proxy.build_property_read_proxy(
                        consumed_thing,
                        name,
                        source_td.get("properties", {}).get(name),
                    ),
                )
                exposed_thing.set_property_write_handler(name, zenoh_proxy.build_property_write_proxy(consumed_thing, name))

            for name in action_names:
                exposed_thing.set_action_handler(name, build_action_invoke_proxy(consumed_thing, name))

            for name in event_names:
                zenoh_proxy.subscribe_event_proxy(consumed_thing, exposed_thing, name)

            exposed_thing.expose()

            exposed_td_dict = ThingDescription.from_thing(exposed_thing.thing).to_dict()

            device_consumed_thing = self.consumer_wot.consume(json.dumps(exposed_td_dict))

        event_buffers = {name: deque(maxlen=EVENT_BUFFER_SIZE) for name in event_names}
        subscriptions = []

        for name in event_names:
            def on_event(item, event_name=name):
                event_buffers[event_name].append({"id": uuid.uuid4().hex, "value": to_json_obj(item.data)})

            def on_error(error, event_name=name):
                LOGGER.warning("Event subscription '%s' failed: %s", event_name, error)

            subscriptions.append(device_consumed_thing.on_event(name).subscribe(
                on_next=on_event, on_error=on_error))

        device = {
            "id": device_id,
            "title": device_title,
            "binding": binding,
            "exposed_thing": exposed_thing,
            "consumed_thing": device_consumed_thing,
            "td": exposed_td_dict,
            "event_buffers": event_buffers,
            "subscriptions": subscriptions,
        }

        self.devices[device_id] = device

        LOGGER.info("Onboarded device '%s' (%s binding) as %s", device_title, binding, device_id)

        return device

    async def offboard(self, device_id):
        device = self.devices.pop(device_id, None)

        if device is None:
            raise KeyError(device_id)

        for subscription in device["subscriptions"]:
            subscription.dispose()

        if device["exposed_thing"] is not None:
            device["exposed_thing"].destroy()

        LOGGER.info("Offboarded device '%s'", device["title"])

    def get_device(self, device_id):
        device = self.devices.get(device_id)

        if device is None:
            raise KeyError(device_id)

        return device


def device_summary(device):
    """Returns the JSON-safe subset of a device record used by the frontend."""

    return {
        "id": device["id"],
        "title": device["title"],
        "binding": device["binding"],
        "properties": device["td"].get("properties", {}),
        "actions": device["td"].get("actions", {}),
        "events": device["td"].get("events", {}),
    }


class BaseHandler(tornado.web.RequestHandler):
    """Base handler that serializes responses as JSON and normalizes errors."""

    def write_json(self, obj):
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(obj))

    def write_error(self, status_code, **kwargs):
        message = str(kwargs["exc_info"][1]) if "exc_info" in kwargs else self._reason
        self.set_header("Content-Type", "application/json")
        self.finish(json.dumps({"error": message}))

    def json_body(self):
        try:
            return json.loads(self.request.body or b"{}")
        except json.JSONDecodeError:
            raise tornado.web.HTTPError(400, reason="Malformed JSON body")


class DevicesHandler(BaseHandler):
    def get(self):
        self.write_json({"devices": [device_summary(d) for d in APP.devices.values()]})


class OnboardHandler(BaseHandler):
    async def post(self):
        td_files = self.request.files.get("td")

        if not td_files:
            raise tornado.web.HTTPError(400, reason="Missing 'td' file")

        binding_hint = self.get_body_argument("binding", "auto")

        try:
            devices = await APP.onboard(td_files[0]["body"], binding_hint)
        except Exception as error:
            LOGGER.warning("Onboarding failed: %s", error, exc_info=True)
            raise tornado.web.HTTPError(400, reason=str(error))

        summaries = [device_summary(device) for device in devices]
        self.write_json(summaries[0] if len(summaries) == 1 else {"devices": summaries})


class OffboardHandler(BaseHandler):
    async def post(self, device_id):
        try:
            await APP.offboard(device_id)
        except KeyError:
            raise tornado.web.HTTPError(404, reason="Unknown device")

        self.write_json({"ok": True})


class ReadPropertyHandler(BaseHandler):
    async def post(self, device_id, prop_name):
        try:
            device = APP.get_device(device_id)
        except KeyError:
            raise tornado.web.HTTPError(404, reason="Unknown device")

        try:
            value = await device["consumed_thing"].read_property(prop_name)
        except Exception as error:
            raise tornado.web.HTTPError(502, reason=str(error))

        self.write_json({"value": to_json_obj(value)})


class WritePropertyHandler(BaseHandler):
    async def post(self, device_id, prop_name):
        try:
            device = APP.get_device(device_id)
        except KeyError:
            raise tornado.web.HTTPError(404, reason="Unknown device")

        body = self.json_body()

        try:
            await device["consumed_thing"].write_property(prop_name, body.get("value"))
        except Exception as error:
            raise tornado.web.HTTPError(502, reason=str(error))

        self.write_json({"ok": True})


class InvokeActionHandler(BaseHandler):
    async def post(self, device_id, action_name):
        try:
            device = APP.get_device(device_id)
        except KeyError:
            raise tornado.web.HTTPError(404, reason="Unknown device")

        body = self.json_body()

        try:
            result = await device["consumed_thing"].invoke_action(action_name, body.get("input"))
        except Exception as error:
            raise tornado.web.HTTPError(502, reason=str(error))

        self.write_json({"result": to_json_obj(result)})


class EventsLatestHandler(BaseHandler):
    def get(self, device_id, event_name):
        try:
            device = APP.get_device(device_id)
        except KeyError:
            raise tornado.web.HTTPError(404, reason="Unknown device")

        buffer = device["event_buffers"].get(event_name)

        if buffer is None:
            raise tornado.web.HTTPError(404, reason="Unknown event")

        self.write_json({"items": list(buffer)})


def make_application():
    return tornado.web.Application([
        (r"/api/devices", DevicesHandler),
        (r"/api/devices/onboard", OnboardHandler),
        (r"/api/devices/([^/]+)/offboard", OffboardHandler),
        (r"/api/devices/([^/]+)/properties/([^/]+)/read", ReadPropertyHandler),
        (r"/api/devices/([^/]+)/properties/([^/]+)/write", WritePropertyHandler),
        (r"/api/devices/([^/]+)/actions/([^/]+)/invoke", InvokeActionHandler),
        (r"/api/devices/([^/]+)/events/([^/]+)/latest", EventsLatestHandler),
        (r"/(.*)", tornado.web.StaticFileHandler, {
            "path": os.path.join(_THIS_DIR, "dashboard_static"),
            "default_filename": "index.html",
        }),
    ])


async def main(router_url, port, servient_id):
    global APP

    APP = DashboardApp(router_url=router_url, servient_id=servient_id)
    await APP.start()

    make_application().listen(port)

    LOGGER.info("Dashboard running at http://localhost:%s", port)
    LOGGER.info("Onboarded devices are exposed over Zenoh router %s (servient_id=%s)", router_url, servient_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zenoh IoT onboarding dashboard")
    parser.add_argument("--router", default="tcp/localhost:7447", help="Zenoh router URL")
    parser.add_argument("--port", type=int, default=8899, help="Dashboard HTTP port")
    parser.add_argument("--servient-id", default="dashboard-proxy", help="Zenoh servient/topic namespace for onboarded devices")
    args = parser.parse_args()

    IOLoop.current().add_callback(main, args.router, args.port, args.servient_id)

    try:
        IOLoop.current().start()
    except KeyboardInterrupt:
        LOGGER.info("Interrupted, shutting down...")
        if APP is not None:
            IOLoop.current().run_sync(APP.shutdown)
