#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: MIT

import pytest

from wotpy.protocols.modbus.enums import ModbusEntity, ModbusFunction
from wotpy.protocols.modbus.utils import modbus_function_to_entity, normalize_modbus_function


@pytest.mark.parametrize(
    "val,expected",
    [
        ("readCoil", ModbusFunction.READ_COIL),
        ("writeSingleCoil", ModbusFunction.WRITE_SINGLE_COIL),
        (ModbusFunction.READ_HOLDING_REGISTERS, ModbusFunction.READ_HOLDING_REGISTERS),
    ],
)
def test_normalize_modbus_function(val, expected):
    assert normalize_modbus_function(val) == expected


@pytest.mark.parametrize(
    "func,expected",
    [
        (ModbusFunction.READ_COIL, ModbusEntity.COIL),
        (ModbusFunction.READ_DISCRETE_INPUT, ModbusEntity.DISCRETE_INPUT),
        (ModbusFunction.READ_INPUT_REGISTER, ModbusEntity.INPUT_REGISTER),
        (ModbusFunction.READ_HOLDING_REGISTERS, ModbusEntity.HOLDING_REGISTER),
        (ModbusFunction.WRITE_MULTIPLE_COILS, ModbusEntity.COIL),
        (ModbusFunction.WRITE_MULTIPLE_HOLDING_REGISTERS, ModbusEntity.HOLDING_REGISTER),
    ],
)
def test_modbus_function_to_entity(func, expected):
    assert modbus_function_to_entity(func) == expected
