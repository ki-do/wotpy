#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: MIT

"""
Consume a Modbus Thing from a TD file.

This example accepts TDs that use "modbus:*" terms and normalizes them to the
"modv:*" terms currently used by this repository's Modbus client.

Usage:
    python client.py [--host 192.168.20.76] [--port 502] [--td ./sentron4220.tm.jsonld]
"""

import argparse
import asyncio
import copy
import json
import logging
import os
import re
import struct
import sys
from urllib.parse import parse_qs, urlsplit

# Allow running this script directly from the examples/ directory even when
# wotpy is not installed as a package: add the repo root to sys.path.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from wotpy.support import is_modbus_supported
from wotpy.protocols.modbus.client import ModbusClient
from wotpy.wot.servient import Servient

try:
    import zenoh
except ImportError:
    zenoh = None

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

DEFAULT_TD_PATH = os.path.join(os.path.dirname(__file__), "sentron4220.tm.jsonld")

# Map common lower-case entity names to the current ModbusEntity enum values.
ENTITY_MAP = {
    "coil": "Coil",
    "discreteinput": "DiscreteInput",
    "holdingregister": "HoldingRegister",
    "inputregister": "InputRegister",
}


def _format_host_for_uri(host):
    host_str = str(host)
    if ":" in host_str and not host_str.startswith("["):
        return f"[{host_str}]"
    return host_str


def _safe_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _extract_connection_from_base(td_doc):
    parsed = urlsplit(str(td_doc.get("base") or ""))
    host = parsed.hostname or "localhost"
    port = parsed.port or 1502

    unit_id = 1
    base_path = (parsed.path or "").strip("/")
    if base_path:
        unit_id = _safe_int(base_path.split("/")[0], 1)

    return host, port, unit_id


def _extract_address_quantity(form):
    href = str(form.get("href") or "")
    parsed = urlsplit(href)
    path = (parsed.path or "").strip("/")
    query = parse_qs(parsed.query or "")

    quantity = None
    if "quantity" in query and query["quantity"]:
        quantity = _safe_int(query["quantity"][0], None)

    unit_id = None
    address = None

    segments = [seg for seg in path.split("/") if seg]
    if segments:
        if parsed.scheme.startswith("modbus") and len(segments) >= 2:
            unit_id = _safe_int(segments[-2], None)
            address = _safe_int(segments[-1], None)
        else:
            address = _safe_int(segments[-1], None)

    return unit_id, address, quantity


def _normalize_thing_model_fields(td_doc):
    # wot.consume() expects a Thing Description, while sentron4220.tm.jsonld is a Thing Model.
    # Remove ThingModel marker and ensure version.instance exists.
    type_val = td_doc.get("@type")
    if isinstance(type_val, str):
        if type_val == "tm:ThingModel":
            td_doc.pop("@type", None)
    elif isinstance(type_val, list):
        filtered_types = [item for item in type_val if item != "tm:ThingModel"]
        if filtered_types:
            td_doc["@type"] = filtered_types
        else:
            td_doc.pop("@type", None)

    version = td_doc.get("version")
    if isinstance(version, dict) and "instance" not in version:
        model_version = version.get("model")
        if model_version is not None:
            version["instance"] = str(model_version)
        else:
            version["instance"] = "1.0.0"


def _normalize_modbus_form(form, host, port, default_unit_id):
    unit_id = form.get("modbus:unitID", form.get("modv:unitID"))
    address = form.get("modbus:address", form.get("modv:address"))
    quantity = form.get("modbus:quantity", form.get("modv:quantity"))

    href_unit, href_address, href_quantity = _extract_address_quantity(form)
    unit_id = _safe_int(unit_id, href_unit if href_unit is not None else default_unit_id)
    address = _safe_int(address, href_address if href_address is not None else 0)
    quantity = _safe_int(quantity, href_quantity if href_quantity is not None else 1)

    if form.get("modv:zeroBasedAddressing") is False and address > 0:
        address -= 1

    entity_raw = form.get("modbus:entity", form.get("modv:entity", "holdingregister"))
    entity_key = str(entity_raw).lower()
    entity = ENTITY_MAP.get(entity_key, "HoldingRegister")

    host_uri = _format_host_for_uri(host)
    form["href"] = f"modbus+tcp://{host_uri}:{port}/{int(unit_id)}/{int(address)}?quantity={int(quantity)}"

    # Keep both key styles so this TD remains interoperable across consumers.
    form["modv:unitID"] = int(unit_id)
    form["modv:address"] = int(address)
    form["modv:quantity"] = int(quantity)
    form["modv:entity"] = entity


def build_runtime_td(td_doc, host, port):
    runtime_td = copy.deepcopy(td_doc)
    _normalize_thing_model_fields(runtime_td)
    host_uri = _format_host_for_uri(host)
    _, _, base_unit_id = _extract_connection_from_base(runtime_td)
    runtime_td["base"] = f"modbus+tcp://{host_uri}:{port}"

    for prop in runtime_td.get("properties", {}).values():
        for form in prop.get("forms", []):
            _normalize_modbus_form(form, host=host, port=port, default_unit_id=base_unit_id)

    return runtime_td


def _decode_modbus_value(raw_value):
    # For PAC4220 values exposed as xsd:hexBinary, many measurements are float32
    # encoded as two 16-bit registers in big-endian word order.
    if isinstance(raw_value, list) and len(raw_value) == 2 and all(isinstance(item, int) for item in raw_value):
        try:
            binary = raw_value[0].to_bytes(2, "big") + raw_value[1].to_bytes(2, "big")
            decoded = struct.unpack(">f", binary)[0]
            return round(decoded, 6)
        except (OverflowError, ValueError, struct.error):
            return None

    return None


def _build_voltage_key(prop_name, key_prefix):
    match = re.match(r"^Measure_0_V_(L[123])_\d+$", prop_name)
    if not match:
        return None

    return "{}/{}".format(key_prefix.rstrip("/"), match.group(1))


def _open_zenoh_session(config_path=None, mode=None, connect_endpoints=None, listen_endpoints=None):
    if zenoh is None:
        raise RuntimeError("Zenoh support not available. Install dependency: pip install eclipse-zenoh")

    conf = zenoh.Config.from_file(config_path) if config_path else zenoh.Config()

    if mode is not None:
        conf.insert_json5("mode", json.dumps(mode))

    if connect_endpoints:
        conf.insert_json5("connect/endpoints", json.dumps(connect_endpoints))

    if listen_endpoints:
        conf.insert_json5("listen/endpoints", json.dumps(listen_endpoints))

    return zenoh.open(conf)


async def _run_interactions(
    td_runtime,
    servient,
    max_properties=15,
    zenoh_session=None,
    zenoh_key_prefix="demo/voltage",
    repeat=False,
    poll_interval=1.0
):
    wot = await servient.start()
    consumed_thing = wot.consume(json.dumps(td_runtime))

    # Keep compatibility with the old demo TD if it contains a writable test property.
    if "write_property" in consumed_thing.td.properties:
        LOGGER.info("Writing property 'write_property' = 200")
        await consumed_thing.write_property("write_property", [200, 0, 0, 0])

        written_val = await consumed_thing.read_property("write_property")
        LOGGER.info("Read back 'write_property': %r", written_val)

    property_names = list(consumed_thing.td.properties.keys())[:max_properties]

    while True:
        for prop_name in property_names:
            try:
                raw_value = await consumed_thing.read_property(prop_name)
                decoded_value = _decode_modbus_value(raw_value)

                if decoded_value is not None:
                    LOGGER.info("%s = %.6f", prop_name, decoded_value)

                    if zenoh_session is not None:
                        voltage_key = _build_voltage_key(prop_name, zenoh_key_prefix)
                        if voltage_key:
                            zenoh_session.put(voltage_key, str(decoded_value))
                            LOGGER.info("Published %s -> %s", voltage_key, decoded_value)
                else:
                    LOGGER.info("%s = %r", prop_name, raw_value)
            except Exception as ex:
                LOGGER.warning("Could not read property '%s': %s", prop_name, ex)

        if not repeat:
            break

        await asyncio.sleep(poll_interval)


async def main(
    td_path,
    host,
    port,
    max_properties,
    zenoh_publish=False,
    zenoh_key_prefix="demo/voltage",
    zenoh_mode=None,
    zenoh_connect=None,
    zenoh_listen=None,
    zenoh_config=None,
    repeat=False,
    poll_interval=1.0
):
    if not is_modbus_supported():
        raise RuntimeError(
            "Modbus support is not enabled. Install optional dependency: pip install pymodbus"
        )

    with open(td_path, "r", encoding="utf-8") as fobj:
        td_doc = json.load(fobj)

    td_host, td_port, _ = _extract_connection_from_base(td_doc)
    target_host = host if host is not None else td_host
    target_port = port if port is not None else td_port

    host_candidates = [target_host]
    if target_host == "localhost":
        host_candidates.extend(["::1", "127.0.0.1"])

    # Keep order while removing duplicates.
    host_candidates = list(dict.fromkeys(host_candidates))

    last_error = None
    zenoh_session = None

    if zenoh_publish:
        zenoh_session = _open_zenoh_session(
            config_path=zenoh_config,
            mode=zenoh_mode,
            connect_endpoints=zenoh_connect,
            listen_endpoints=zenoh_listen
        )
        LOGGER.info("Opened Zenoh session for publishing on '%s/*'", zenoh_key_prefix.rstrip("/"))

    try:
        for candidate in host_candidates:
            td_runtime = build_runtime_td(td_doc, host=candidate, port=target_port)
            LOGGER.info("Connecting to Modbus Thing at %s:%s", candidate, target_port)

            # Keep only the Modbus binding client to avoid protocol auto-selection
            # falling back to non-Modbus clients.
            servient = Servient(catalogue_port=None, clients=[ModbusClient()])

            try:
                await _run_interactions(
                    td_runtime=td_runtime,
                    servient=servient,
                    max_properties=max_properties,
                    zenoh_session=zenoh_session,
                    zenoh_key_prefix=zenoh_key_prefix,
                    repeat=repeat,
                    poll_interval=poll_interval
                )
                await servient.shutdown()
                return
            except ConnectionError as ex:
                last_error = ex
                LOGGER.warning("Connection attempt failed for %s:%s (%s)", candidate, target_port, ex)
                await servient.shutdown()
    finally:
        if zenoh_session is not None:
            zenoh_session.close()

    if last_error is not None:
        raise last_error


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume a Modbus Thing/ThingModel TD")
    parser.add_argument("--host", default=None, help="Modbus host (default: host from TD base)")
    parser.add_argument("--port", type=int, default=None, help="Modbus port (default: port from TD base)")
    parser.add_argument("--max-properties", type=int, default=15, help="Maximum number of properties to read (default: 15)")
    parser.add_argument("--zenoh-publish", action="store_true", help="Publish decoded L1/L2/L3 voltages to Zenoh")
    parser.add_argument("--zenoh-key-prefix", default="demo/voltage", help="Zenoh key prefix for published voltage values")
    parser.add_argument("--zenoh-mode", choices=["peer", "client"], default=None, help="Zenoh session mode")
    parser.add_argument("--zenoh-connect", action="append", default=None, metavar="ENDPOINT", help="Zenoh endpoints to connect to")
    parser.add_argument("--zenoh-listen", action="append", default=None, metavar="ENDPOINT", help="Zenoh endpoints to listen on")
    parser.add_argument("--zenoh-config", default=None, metavar="FILE", help="Zenoh configuration file")
    parser.add_argument("--repeat", action="store_true", help="Continuously poll and publish values")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Seconds between polling cycles when --repeat is used")
    parser.add_argument("--td", default=DEFAULT_TD_PATH, help="Path to Thing Description JSON")
    args = parser.parse_args()

    asyncio.run(
        main(
            td_path=args.td,
            host=args.host,
            port=args.port,
            max_properties=args.max_properties,
            zenoh_publish=args.zenoh_publish,
            zenoh_key_prefix=args.zenoh_key_prefix,
            zenoh_mode=args.zenoh_mode,
            zenoh_connect=args.zenoh_connect,
            zenoh_listen=args.zenoh_listen,
            zenoh_config=args.zenoh_config,
            repeat=args.repeat,
            poll_interval=args.poll_interval
        )
    )
