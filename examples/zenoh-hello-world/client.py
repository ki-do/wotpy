#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: MIT

"""
Zenoh Hello World - Client (Consumer)

Connects to the WoT Thing exposed by server.py and demonstrates:
  1. Reading the 'message' property
  2. Writing the 'message' property (publish)
  3. Observing the 'message' property (subscribe to updates)
  4. Invoking the 'greeting' action
  5. Subscribing to the 'newMessage' event

Prerequisites:
  - A running Zenoh router
  - server.py running and connected to the same router

Usage:
  python client.py [--router tcp/localhost:7447] [--catalogue http://localhost:9090]
"""

import argparse
import asyncio
import json
import logging
import os
import sys

# Allow running this script directly from the examples/ directory even when
# wotpy is not installed as a package: add the repo root to sys.path.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import requests
from tornado.ioloop import IOLoop

from wotpy.protocols.zenoh.client import ZenohClient
from wotpy.wot.servient import Servient
from wotpy.wot.wot import WoT

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

THING_TITLE = "HelloWorldZenoh"


async def main(router_url: str, catalogue_url: str):
    LOGGER.info("Fetching TD from catalogue: %s", catalogue_url)

    # ── Fetch the TD from the server's catalogue ──────────────────────────────
    catalogue_resp = requests.get(catalogue_url, timeout=5)
    catalogue_resp.raise_for_status()
    things = catalogue_resp.json()

    # The catalogue returns a dict of {thing_title: td_path} where td_path may be
    # a relative path (e.g. "/helloworldzenoh") — make it absolute.
    td_path = next(
        url for title, url in things.items() if THING_TITLE.lower() in title.lower()
    )
    td_url = td_path if td_path.startswith("http") else catalogue_url.rstrip("/") + td_path
    LOGGER.info("Found TD URL: %s", td_url)

    td_resp = requests.get(td_url, timeout=5)
    td_resp.raise_for_status()
    td_str = td_resp.text
    LOGGER.info("TD fetched successfully")

    # ── Build a WoT Consumer ──────────────────────────────────────────────────
    zenoh_client = ZenohClient()
    servient = Servient(catalogue_port=None)
    servient.add_client(zenoh_client)

    wot = await servient.start()

    consumed_thing = wot.consume(td_str)

    # ── 1. Read property ──────────────────────────────────────────────────────
    LOGGER.info("--- [1] Reading 'message' property ---")
    value = await consumed_thing.read_property("message")
    LOGGER.info("  Current value: %r", value)

    # ── 2. Write property (publish) ───────────────────────────────────────────
    new_value = "Hello from the Zenoh client!"
    LOGGER.info("--- [2] Writing 'message' property: %r ---", new_value)
    await consumed_thing.write_property("message", new_value)

    # Read it back to verify
    value = await consumed_thing.read_property("message")
    LOGGER.info("  Value after write: %r", value)

    # ── 3. Observe property (subscribe to updates) ────────────────────────────
    LOGGER.info("--- [3] Observing 'message' property for 5 seconds ---")
    received_updates = []

    def on_property_change(event):
        val = event.data.value
        LOGGER.info("  [observe] Property changed → %r", val)
        received_updates.append(val)

    subscription = consumed_thing.on_property_change("message").subscribe(
        on_property_change,
        on_error=lambda e: LOGGER.error("  [observe] Error: %s", e),
    )

    # Trigger a few changes while we are observing
    for i in range(3):
        await asyncio.sleep(1)
        msg = f"Update #{i + 1} from client"
        LOGGER.info("  Writing update: %r", msg)
        await consumed_thing.write_property("message", msg)

    await asyncio.sleep(1)
    subscription.dispose()
    LOGGER.info("  Received %d update(s) while observing", len(received_updates))

    # ── 4. Invoke action ──────────────────────────────────────────────────────
    LOGGER.info("--- [4] Invoking 'greeting' action ---")
    greeting = await consumed_thing.invoke_action("greeting", "WoTPy User")
    LOGGER.info("  Action result: %r", greeting)

    # ── 5. Subscribe to event ─────────────────────────────────────────────────
    LOGGER.info("--- [5] Subscribing to 'newMessage' event for 3 seconds ---")
    received_events = []

    def on_new_message(event):
        LOGGER.info("  [event] newMessage → %r", event.data)
        received_events.append(event.data)

    event_subscription = consumed_thing.on_event("newMessage").subscribe(
        on_new_message,
        on_error=lambda e: LOGGER.error("  [event] Error: %s", e),
    )

    await asyncio.sleep(3)
    event_subscription.dispose()
    LOGGER.info("  Received %d event(s) while subscribed", len(received_events))

    # ── Done ──────────────────────────────────────────────────────────────────
    LOGGER.info("Done. Shutting down client.")
    await servient.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zenoh Hello-World WoT client")
    parser.add_argument(
        "--router",
        default="tcp/localhost:7447",
        help="Zenoh router URL (default: tcp/localhost:7447)",
    )
    parser.add_argument(
        "--catalogue",
        default="http://localhost:9191",
        help="URL of the WoT TD catalogue served by the Thing (default: http://localhost:9191)",
    )
    args = parser.parse_args()

    IOLoop.current().run_sync(lambda: main(args.router, args.catalogue))
