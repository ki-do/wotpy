#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: MIT

"""
Zenoh Hello World - Server (Thing)

Exposes a WoT Thing over Zenoh with:
  - A 'message' property (read/write/observable)
  - A 'greeting' action that returns a personalised greeting string
  - A 'newMessage' event emitted whenever the message property changes

Prerequisites:
  - A running Zenoh router, e.g.:
      docker run --rm -p 7447:7447/tcp eclipse/zenoh

Usage:
  python server.py [--router tcp/localhost:7447]
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

from tornado.ioloop import IOLoop

from wotpy.protocols.zenoh.server import ZenohServer
from wotpy.wot.servient import Servient

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# ── Thing Description ────────────────────────────────────────────────────────

THING_ID = "urn:hello-world-zenoh"
THING_TITLE = "HelloWorldZenoh"

TD = {
    "@context": [
        "https://www.w3.org/2019/wot/td/v1",
        "https://www.w3.org/2022/wot/td/v1.1",
    ],
    "id": THING_ID,
    "title": THING_TITLE,
    "description": "A simple Hello-World Thing exposed over Zenoh",
    "securityDefinitions": {
        "nosec_sc": {"scheme": "nosec"}
    },
    "security": "nosec_sc",
    "properties": {
        "message": {
            "title": "Message",
            "description": "A greeting message that can be read, written and observed",
            "type": "string",
            "observable": True,
        }
    },
    "actions": {
        "greeting": {
            "title": "Greeting",
            "description": "Returns a personalised greeting for the given name",
            "input": {
                "type": "string",
                "description": "Name of the person to greet"
            },
            "output": {
                "type": "string",
                "description": "Personalised greeting string"
            }
        }
    },
    "events": {
        "newMessage": {
            "title": "New Message",
            "description": "Fired whenever the 'message' property is updated",
            "data": {
                "type": "string"
            }
        }
    }
}

# ── State ─────────────────────────────────────────────────────────────────────

_current_message = "Hello, World!"

# ── Handlers ─────────────────────────────────────────────────────────────────


async def message_read_handler():
    """Returns the current message value."""
    LOGGER.info("Reading 'message' property: %s", _current_message)
    return _current_message


async def message_write_handler(value):
    """Stores the new message value and emits a 'newMessage' event."""
    global _current_message
    _current_message = value
    LOGGER.info("Writing 'message' property: %s", _current_message)


async def greeting_action_handler(parameters):
    """Returns a personalised greeting for the name passed as input."""
    name = parameters.get("input", "stranger")
    result = f"Hello, {name}! (from Zenoh)"
    LOGGER.info("Action 'greeting' called with input=%r → %s", name, result)
    return result


# ── Main ─────────────────────────────────────────────────────────────────────


async def main(router_url: str):
    LOGGER.info("Starting Zenoh Hello-World server → router: %s", router_url)

    zenoh_server = ZenohServer(router_url=router_url)

    servient = Servient(catalogue_port=9191)
    servient.add_server(zenoh_server)

    wot = await servient.start()

    LOGGER.info("Producing and exposing Thing: %s", THING_ID)
    exposed_thing = wot.produce(json.dumps(TD))

    # Wire up handlers
    exposed_thing.set_property_read_handler("message", message_read_handler)
    exposed_thing.set_property_write_handler("message", message_write_handler)
    exposed_thing.set_action_handler("greeting", greeting_action_handler)

    exposed_thing.expose()

    LOGGER.info("Thing is live. TD catalogue at http://localhost:9191")
    LOGGER.info(
        "Zenoh topics (servient_id=%r):\n"
        "  property read/write : zenoh://%s/%s/property/requests/%s/message\n"
        "  property observe    : zenoh://%s/%s/property/updates/%s/message\n"
        "  action invoke       : zenoh://%s/%s/action/requests/%s/greeting\n"
        "  event subscribe     : zenoh://%s/%s/event/updates/%s/newMessage",
        zenoh_server.servient_id,
        router_url, zenoh_server.servient_id, THING_TITLE.lower(),
        router_url, zenoh_server.servient_id, THING_TITLE.lower(),
        router_url, zenoh_server.servient_id, THING_TITLE.lower(),
        router_url, zenoh_server.servient_id, THING_TITLE.lower(),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zenoh Hello-World WoT server")
    parser.add_argument(
        "--router",
        default="tcp/localhost:7447",
        help="Zenoh router URL (default: tcp/localhost:7447)",
    )
    args = parser.parse_args()

    IOLoop.current().add_callback(main, args.router)
    IOLoop.current().start()
