#!/usr/bin/env python

"""Discover and consume all Things published by one or more WoT TD catalogues over Zenoh.

Usage:
    python app.py [--catalogue http://localhost:9292] [--subscription-seconds 15]
    python app.py --catalogue http://localhost:9292 --catalogue http://localhost:9293
    python app.py --write-value '210'
    python app.py --action-input '"input value"'
"""

import argparse
import asyncio
import json
import logging
import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import requests
from tornado.ioloop import IOLoop

from wotpy.protocols.zenoh.client import ZenohClient
from wotpy.wot.servient import Servient

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

_RUNNING_SERVIENT = None


def _catalogue_td_urls(catalogue_url):
    response = requests.get(catalogue_url, timeout=5)
    response.raise_for_status()

    catalogue = response.json()
    return {
        title: url if url.startswith("http") else catalogue_url.rstrip("/") + url
        for title, url in catalogue.items()
    }


def _input_required(action):
    return bool(action.input)


async def _consume_thing(wot, title, td_url, write_value, action_input, subscriptions):
    response = requests.get(td_url, timeout=5)
    response.raise_for_status()
    consumed_thing = wot.consume(response.text)

    LOGGER.info("Thing '%s': %s", title, td_url)

    for name, prop in consumed_thing.td.properties.items():
        if not prop.write_only:
            try:
                value = await consumed_thing.read_property(name)
                LOGGER.info("  read %s = %r", name, value)
            except Exception as error:
                LOGGER.warning("  read %s failed: %s", name, error)

        if prop.observable:
            def on_change(item, property_name=name, thing_title=title):
                LOGGER.info("  update %s.%s = %r", thing_title, property_name, item.data.value)

            subscriptions.append(consumed_thing.on_property_change(name).subscribe(
                on_change,
                on_error=lambda error, property_name=name: LOGGER.warning(
                    "  observe %s failed: %s", property_name, error)))

        if write_value is not None and not prop.read_only:
            try:
                await consumed_thing.write_property(name, write_value)
                LOGGER.info("  wrote %s = %r", name, write_value)
            except Exception as error:
                LOGGER.warning("  write %s failed: %s", name, error)

    for name, action in consumed_thing.td.actions.items():
        if _input_required(action) and action_input is None:
            LOGGER.info("  skipped action %s: provide --action-input to invoke it", name)
            continue

        try:
            result = await consumed_thing.invoke_action(name, action_input)
            LOGGER.info("  action %s = %r", name, result)
        except Exception as error:
            LOGGER.warning("  action %s failed: %s", name, error)

    for name in consumed_thing.td.events:
        def on_event(item, event_name=name, thing_title=title):
            LOGGER.info("  event %s.%s = %r", thing_title, event_name, item.data)

        subscriptions.append(consumed_thing.on_event(name).subscribe(
            on_event,
            on_error=lambda error, event_name=name: LOGGER.warning(
                "  subscribe %s failed: %s", event_name, error)))


async def main(catalogue_urls, subscription_seconds, write_value, action_input):
    global _RUNNING_SERVIENT

    td_urls = {}
    for catalogue_url in catalogue_urls:
        found = _catalogue_td_urls(catalogue_url)
        if not found:
            LOGGER.warning("No Things found at %s", catalogue_url)
        td_urls.update(found)

    if not td_urls:
        return

    servient = Servient(catalogue_port=None, clients=[ZenohClient()])
    _RUNNING_SERVIENT = servient
    wot = await servient.start()
    subscriptions = []

    try:
        for title, td_url in td_urls.items():
            await _consume_thing(wot, title, td_url, write_value, action_input, subscriptions)

        if subscriptions and subscription_seconds > 0:
            LOGGER.info("Listening for Zenoh updates/events for %s seconds", subscription_seconds)
            await asyncio.sleep(subscription_seconds)
    finally:
        for subscription in subscriptions:
            subscription.dispose()
        await servient.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume every Zenoh Thing in one or more WoT TD catalogues")
    parser.add_argument("--catalogue", action="append", default=None, metavar="URL", help="WoT TD catalogue URL (repeatable, default: http://localhost:9292)")
    parser.add_argument("--subscription-seconds", type=float, default=15, help="Seconds to listen for property updates and events")
    parser.add_argument("--write-value", default=None, metavar="JSON", help="JSON value written to every writable property")
    parser.add_argument("--action-input", default=None, metavar="JSON", help="JSON input for actions that require input")
    args = parser.parse_args()

    try:
        write_value = json.loads(args.write_value) if args.write_value is not None else None
        action_input = json.loads(args.action_input) if args.action_input is not None else None
    except json.JSONDecodeError as error:
        parser.error("--write-value and --action-input must contain valid JSON: {}".format(error))

    try:
        IOLoop.current().run_sync(lambda: main(
            catalogue_urls=args.catalogue or ["http://localhost:9292"],
            subscription_seconds=args.subscription_seconds,
            write_value=write_value,
            action_input=action_input))
    except KeyboardInterrupt:
        LOGGER.info("Interrupted, closing the Zenoh session...")
        if _RUNNING_SERVIENT is not None:
            IOLoop.current().run_sync(_RUNNING_SERVIENT.shutdown)
