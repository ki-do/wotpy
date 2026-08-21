#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: MIT

"""
Client implementation for the Modbus TCP protocol binding.
"""

import asyncio
import logging
import urllib.parse

import reactivex

from wotpy.protocols.client import BaseProtocolClient
from wotpy.protocols.enums import InteractionVerbs, Protocols
from wotpy.protocols.exceptions import FormNotFoundException
from wotpy.protocols.modbus.connection import ModbusConnection
from wotpy.protocols.modbus.enums import (
    ModbusEntity,
    ModbusFunction,
    ModbusSchemes,
    ModbusVocabularyKeys,
)
from wotpy.protocols.modbus.utils import modbus_function_to_entity, normalize_modbus_function
from wotpy.protocols.utils import is_scheme_form
from wotpy.wot.events import EmittedEvent, PropertyChangeEmittedEvent, PropertyChangeEventInit


class ModbusClient(BaseProtocolClient):
    """Implementation of the protocol client interface for Modbus TCP."""

    DEFAULT_PORT = 502
    DEFAULT_TIMEOUT_MS = 1000
    DEFAULT_POLLING_MS = 2000

    def __init__(self, timeout_default_ms=DEFAULT_TIMEOUT_MS, polling_default_ms=DEFAULT_POLLING_MS):
        self._timeout_default_ms = timeout_default_ms
        self._polling_default_ms = polling_default_ms
        self._connections = {}
        self._connections_lock = asyncio.Lock()
        self._logr = logging.getLogger(__name__)

    @classmethod
    def _pick_modbus_href(cls, td, forms, op=None):
        """Picks the most appropriate Modbus form href from the given forms."""

        def is_op_form(form):
            try:
                return op is None or op == form.op or op in form.op
            except TypeError:
                return False

        try:
            return next(
                form.href for form in forms
                if is_scheme_form(form, td.base, ModbusSchemes.MODBUS_TCP) and is_op_form(form)
            )
        except StopIteration:
            return None

    @staticmethod
    def _parse_href(href):
        """Parses modbus+tcp://host:port/unit/address?quantity=N style hrefs."""

        parsed = urllib.parse.urlparse(href)

        if parsed.scheme != ModbusSchemes.MODBUS_TCP:
            raise ValueError("Unsupported scheme for Modbus href: {}".format(parsed.scheme))

        path_parts = [part for part in parsed.path.split("/") if part]

        if len(path_parts) < 2:
            raise ValueError("Malformed Modbus href. Expected /<unit>/<address>: {}".format(href))

        host = parsed.hostname
        port = parsed.port or ModbusClient.DEFAULT_PORT

        if not host:
            raise ValueError("Malformed Modbus href. Missing host: {}".format(href))

        query = urllib.parse.parse_qs(parsed.query)
        quantity = query.get("quantity", [None])[0]

        return {
            "host": host,
            "port": port,
            "unit_id": int(path_parts[0]),
            "address": int(path_parts[1]),
            "quantity": int(quantity) if quantity is not None else None,
        }

    @staticmethod
    def _form_raw_value(form, key, default=None):
        """Gets custom vocabulary values from the underlying raw form dictionary."""

        raw = {}

        form_dict_obj = getattr(form, "form_dict", None)
        if form_dict_obj is not None:
            raw = getattr(form_dict_obj, "_init", {}) or {}
        elif hasattr(form, "to_dict"):
            raw = form.to_dict() or {}

        return raw.get(key, default)

    def _build_op_config(self, form, input_value=None, is_write=False):
        href_data = self._parse_href(form.href)

        timeout_ms = self._form_raw_value(form, ModbusVocabularyKeys.TIMEOUT, self._timeout_default_ms)
        polling_ms = self._form_raw_value(form, ModbusVocabularyKeys.POLLING_TIME, self._polling_default_ms)

        unit_id = self._form_raw_value(form, ModbusVocabularyKeys.UNIT_ID, href_data["unit_id"])
        address = self._form_raw_value(form, ModbusVocabularyKeys.ADDRESS, href_data["address"])

        func_raw = self._form_raw_value(form, ModbusVocabularyKeys.FUNCTION)
        entity = self._form_raw_value(form, ModbusVocabularyKeys.ENTITY)
        quantity = self._form_raw_value(form, ModbusVocabularyKeys.QUANTITY, href_data["quantity"])

        function_code = normalize_modbus_function(func_raw) if func_raw is not None else None

        if entity is None and function_code is not None:
            entity = modbus_function_to_entity(function_code)

        if entity is None:
            entity = ModbusEntity.HOLDING_REGISTER

        if quantity is None:
            if isinstance(input_value, (list, tuple)):
                quantity = len(input_value)
            else:
                quantity = 1

        if function_code is None:
            # is_write reflects which operation the caller is performing, not whether a
            # value happens to be present: writing None/null must still resolve a write function.
            if entity == ModbusEntity.COIL:
                function_code = (ModbusFunction.READ_COIL if not is_write else
                                 ModbusFunction.WRITE_SINGLE_COIL if int(quantity) == 1 else
                                 ModbusFunction.WRITE_MULTIPLE_COILS)
            elif entity == ModbusEntity.HOLDING_REGISTER:
                function_code = (ModbusFunction.READ_HOLDING_REGISTERS if not is_write else
                                 ModbusFunction.WRITE_SINGLE_HOLDING_REGISTER if int(quantity) == 1 else
                                 ModbusFunction.WRITE_MULTIPLE_HOLDING_REGISTERS)
            elif entity == ModbusEntity.INPUT_REGISTER:
                function_code = ModbusFunction.READ_INPUT_REGISTER
            elif entity == ModbusEntity.DISCRETE_INPUT:
                function_code = ModbusFunction.READ_DISCRETE_INPUT
            else:
                raise ValueError("Unsupported Modbus entity: {}".format(entity))

        return {
            "host": href_data["host"],
            "port": href_data["port"],
            "unit_id": int(unit_id),
            "address": int(address),
            "quantity": int(quantity),
            "function": function_code,
            "entity": entity,
            "timeout_ms": int(timeout_ms),
            "polling_ms": int(polling_ms),
        }

    async def _get_connection(self, host, port, timeout_ms):
        key = "{}:{}".format(host, port)

        async with self._connections_lock:
            if key not in self._connections:
                self._connections[key] = ModbusConnection(host=host, port=port, timeout_ms=timeout_ms)

            return self._connections[key]

    @staticmethod
    def _coerce_write_payload(config, value):
        function_code = config["function"]

        if function_code == ModbusFunction.WRITE_SINGLE_COIL:
            return bool(value)

        if function_code == ModbusFunction.WRITE_MULTIPLE_COILS:
            if isinstance(value, (list, tuple)):
                return [bool(item) for item in value]

            return [bool(value)]

        if function_code == ModbusFunction.WRITE_SINGLE_HOLDING_REGISTER:
            return int(value)

        if function_code == ModbusFunction.WRITE_MULTIPLE_HOLDING_REGISTERS:
            if isinstance(value, (list, tuple)):
                return [int(item) for item in value]

            return [int(value)]

        raise ValueError("Function code is not writable: {}".format(function_code))

    @staticmethod
    def _coerce_read_result(config, values):
        if config["quantity"] <= 1 and len(values):
            return values[0]

        return values

    @property
    def protocol(self):
        """Protocol of this client instance.
        A member of the Protocols enum."""

        return Protocols.MODBUS

    def is_supported_interaction(self, td, name):
        """Returns True if any of the forms for the interaction are Modbus forms."""

        forms = td.get_forms(name)

        forms_modbus = [
            form for form in forms
            if is_scheme_form(form, td.base, ModbusSchemes.list())
        ]

        return len(forms_modbus) > 0

    def set_security(self, security_scheme_dict, credentials):
        """Modbus binding currently ignores WoT security metadata."""

        return False

    async def invoke_action(self, td, name, input_value, timeout=None):
        """Invokes an action by writing to the configured Modbus resource."""

        form_href = self._pick_modbus_href(td, td.get_action_forms(name), op=InteractionVerbs.INVOKE_ACTION)

        if form_href is None:
            raise FormNotFoundException()

        form = next(form for form in td.get_action_forms(name) if form.href == form_href)

        config = self._build_op_config(form, input_value=input_value)
        connection = await self._get_connection(config["host"], config["port"], config["timeout_ms"])

        payload = self._coerce_write_payload(config, input_value)

        await connection.write(
            function_code=config["function"],
            address=config["address"],
            quantity=config["quantity"],
            unit_id=config["unit_id"],
            payload=payload,
        )

        return None

    async def write_property(self, td, name, value, timeout=None):
        """Writes a property value to the configured Modbus resource."""

        form_href = self._pick_modbus_href(td, td.get_property_forms(name), op=InteractionVerbs.WRITE_PROPERTY)

        if form_href is None:
            form_href = self._pick_modbus_href(td, td.get_property_forms(name))

        if form_href is None:
            raise FormNotFoundException()

        form = next(form for form in td.get_property_forms(name) if form.href == form_href)

        config = self._build_op_config(form, input_value=value, is_write=True)
        connection = await self._get_connection(config["host"], config["port"], config["timeout_ms"])

        payload = self._coerce_write_payload(config, value)

        await connection.write(
            function_code=config["function"],
            address=config["address"],
            quantity=config["quantity"],
            unit_id=config["unit_id"],
            payload=payload,
        )

    async def read_property(self, td, name, timeout=None):
        """Reads a property value from the configured Modbus resource."""

        form_href = self._pick_modbus_href(td, td.get_property_forms(name), op=InteractionVerbs.READ_PROPERTY)

        if form_href is None:
            form_href = self._pick_modbus_href(td, td.get_property_forms(name))

        if form_href is None:
            raise FormNotFoundException()

        form = next(form for form in td.get_property_forms(name) if form.href == form_href)

        config = self._build_op_config(form, input_value=None)
        connection = await self._get_connection(config["host"], config["port"], config["timeout_ms"])

        values = await connection.read(
            entity=config["entity"],
            address=config["address"],
            quantity=config["quantity"],
            unit_id=config["unit_id"],
        )

        return self._coerce_read_result(config, values)

    def on_property_change(self, td, name):
        """Subscribes to property changes by polling the Modbus resource."""

        forms = td.get_property_forms(name)
        form_href = self._pick_modbus_href(td, forms, op=InteractionVerbs.OBSERVE_PROPERTY)

        if form_href is None:
            form_href = self._pick_modbus_href(td, forms, op=InteractionVerbs.READ_PROPERTY)

        if form_href is None:
            form_href = self._pick_modbus_href(td, forms)

        if form_href is None:
            raise FormNotFoundException()

        form = next(form for form in forms if form.href == form_href)
        config = self._build_op_config(form, input_value=None)

        def subscribe(observer, scheduler):
            state = {
                "active": True,
                "task": None,
                "last_value": object(),
                "seen_once": False,
            }

            async def callback():
                while state["active"]:
                    try:
                        value = await self.read_property(td, name)

                        if (not state["seen_once"]) or (value != state["last_value"]):
                            state["seen_once"] = True
                            state["last_value"] = value
                            init = PropertyChangeEventInit(name=name, value=value)
                            observer.on_next(PropertyChangeEmittedEvent(init=init))
                    except Exception as ex:
                        observer.on_error(ex)
                        return

                    await asyncio.sleep(max(config["polling_ms"] / 1000.0, 0.05))

            state["task"] = asyncio.create_task(callback())

            def unsubscribe():
                state["active"] = False
                if state["task"]:
                    state["task"].cancel()

            return unsubscribe

        return reactivex.create(subscribe)

    def on_event(self, td, name):
        """Subscribes to a Modbus event form by polling the referenced resource."""

        forms = td.get_event_forms(name)
        form_href = self._pick_modbus_href(td, forms, op=InteractionVerbs.SUBSCRIBE_EVENT)

        if form_href is None:
            form_href = self._pick_modbus_href(td, forms)

        if form_href is None:
            raise FormNotFoundException()

        form = next(form for form in forms if form.href == form_href)
        config = self._build_op_config(form, input_value=None)

        def subscribe(observer, scheduler):
            state = {
                "active": True,
                "task": None,
            }

            async def callback():
                while state["active"]:
                    try:
                        value = await self._read_form_value(form)
                        observer.on_next(EmittedEvent(init=value, name=name))
                    except Exception as ex:
                        observer.on_error(ex)
                        return

                    await asyncio.sleep(max(config["polling_ms"] / 1000.0, 0.05))

            state["task"] = asyncio.create_task(callback())

            def unsubscribe():
                state["active"] = False
                if state["task"]:
                    state["task"].cancel()

            return unsubscribe

        return reactivex.create(subscribe)

    async def _read_form_value(self, form):
        config = self._build_op_config(form, input_value=None)
        connection = await self._get_connection(config["host"], config["port"], config["timeout_ms"])
        values = await connection.read(
            entity=config["entity"],
            address=config["address"],
            quantity=config["quantity"],
            unit_id=config["unit_id"],
        )
        return self._coerce_read_result(config, values)

    def on_td_change(self, url):
        """Subscribes to Thing Description changes on a remote Thing."""

        raise NotImplementedError
