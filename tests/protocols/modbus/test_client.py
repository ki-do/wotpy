#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: MIT

from wotpy.protocols.modbus.client import ModbusClient
from wotpy.protocols.modbus.enums import ModbusEntity, ModbusFunction


class _DummyFormDict:
    def __init__(self, init_data):
        self._init = init_data


class _DummyForm:
    def __init__(self, href, init_data=None):
        self.href = href
        self.form_dict = _DummyFormDict(init_data or {})


def test_parse_href_with_quantity():
    parsed = ModbusClient._parse_href("modbus+tcp://127.0.0.1:1502/1/10?quantity=4")

    assert parsed["host"] == "127.0.0.1"
    assert parsed["port"] == 1502
    assert parsed["unit_id"] == 1
    assert parsed["address"] == 10
    assert parsed["quantity"] == 4


def test_build_op_config_infers_read_function():
    client = ModbusClient()
    form = _DummyForm(
        href="modbus+tcp://127.0.0.1:1502/1/10?quantity=2",
        init_data={"modv:entity": ModbusEntity.HOLDING_REGISTER},
    )

    config = client._build_op_config(form)

    assert config["function"] == ModbusFunction.READ_HOLDING_REGISTERS
    assert config["quantity"] == 2


def test_build_op_config_infers_write_function():
    client = ModbusClient()
    form = _DummyForm(
        href="modbus+tcp://127.0.0.1:1502/1/20",
        init_data={"modv:entity": ModbusEntity.COIL},
    )

    config = client._build_op_config(form, input_value=[True, False])

    assert config["function"] == ModbusFunction.WRITE_MULTIPLE_COILS
    assert config["quantity"] == 2
