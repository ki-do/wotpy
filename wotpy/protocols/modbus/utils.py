#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SPDX-License-Identifier: MIT

"""
Helper functions for Modbus entities and function code mapping.
"""

from wotpy.protocols.modbus.enums import ModbusEntity, ModbusFunction, ModbusFunctionName


_FUNCTION_NAME_MAP = {
    ModbusFunctionName.READ_COIL: ModbusFunction.READ_COIL,
    ModbusFunctionName.READ_DISCRETE_INPUT: ModbusFunction.READ_DISCRETE_INPUT,
    ModbusFunctionName.READ_HOLDING_REGISTERS: ModbusFunction.READ_HOLDING_REGISTERS,
    ModbusFunctionName.READ_INPUT_REGISTER: ModbusFunction.READ_INPUT_REGISTER,
    ModbusFunctionName.WRITE_SINGLE_COIL: ModbusFunction.WRITE_SINGLE_COIL,
    ModbusFunctionName.WRITE_SINGLE_HOLDING_REGISTER: ModbusFunction.WRITE_SINGLE_HOLDING_REGISTER,
    ModbusFunctionName.WRITE_MULTIPLE_COILS: ModbusFunction.WRITE_MULTIPLE_COILS,
    ModbusFunctionName.WRITE_MULTIPLE_HOLDING_REGISTERS: ModbusFunction.WRITE_MULTIPLE_HOLDING_REGISTERS,
    ModbusFunctionName.READ_DEVICE_IDENTIFICATION: ModbusFunction.READ_DEVICE_IDENTIFICATION,
}


def normalize_modbus_function(value):
    """Returns the integer function code for a numeric or named value."""

    if value is None:
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, str):
        if value in _FUNCTION_NAME_MAP:
            return _FUNCTION_NAME_MAP[value]

        for enum_name in ModbusFunctionName.list():
            if enum_name.lower() == value.lower():
                return _FUNCTION_NAME_MAP[enum_name]

    raise ValueError("Unknown Modbus function: {}".format(value))


def modbus_function_to_entity(modbus_fun):
    """Maps a Modbus function code to its corresponding entity."""

    modbus_fun = normalize_modbus_function(modbus_fun)

    if modbus_fun in (ModbusFunction.READ_COIL,
                      ModbusFunction.WRITE_SINGLE_COIL,
                      ModbusFunction.WRITE_MULTIPLE_COILS):
        return ModbusEntity.COIL

    if modbus_fun == ModbusFunction.READ_DISCRETE_INPUT:
        return ModbusEntity.DISCRETE_INPUT

    if modbus_fun == ModbusFunction.READ_INPUT_REGISTER:
        return ModbusEntity.INPUT_REGISTER

    if modbus_fun in (ModbusFunction.READ_HOLDING_REGISTERS,
                      ModbusFunction.WRITE_SINGLE_HOLDING_REGISTER,
                      ModbusFunction.WRITE_MULTIPLE_HOLDING_REGISTERS):
        return ModbusEntity.HOLDING_REGISTER

    raise ValueError("Cannot convert {} to Modbus entity".format(modbus_fun))
