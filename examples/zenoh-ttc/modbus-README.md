# Modbus localhost consumer example

This example consumes a Thing Description and connects to a Modbus endpoint at localhost:1502.

Files:
- `thing-td.json`: TD for the PowerMeter Thing, using explicit `modbus+tcp://` forms
  and `modv:*` vocabulary keys.
- `client.py`: consumer script.

## Run

From repository root:

```bash
python examples/zenoh-ttc/client.py --host localhost --port 1502
```

Optional:

```bash
python examples/zenoh-ttc/client.py --td examples/zenoh-ttc/thing-td.json
```

If Modbus support is missing, install:

```bash
pip install pymodbus
```
