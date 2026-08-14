# Modbus localhost consumer example

This example consumes a Thing Description and connects to a Modbus endpoint at localhost:1502.

Files:
- `thing-td.json`: TD provided for the PowerMeter Thing.
- `client.py`: consumer script.

## Why normalization is needed

The current `wotpy` Modbus binding expects:
- `modv:*` vocabulary keys (for example `modv:unitID`, `modv:address`), and
- `modbus+tcp://host:port/unit/address?quantity=N` form URLs.

The provided TD uses `modbus:*` keys and `href: "/"` forms under `base`.
`client.py` normalizes this at runtime so you can use the TD as-is.

## Run

From repository root:

```bash
python examples/modbus-localhost/client.py --host localhost --port 1502
```

Optional:

```bash
python examples/modbus-localhost/client.py --td examples/modbus-localhost/thing-td.json
```

If Modbus support is missing, install:

```bash
pip install pymodbus
```
