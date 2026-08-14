FROM eclipse/zenoh-bridge-mqtt:latest

RUN mkdir -p /etc/zenoh

COPY config.json5 /etc/zenoh/config.json5

EXPOSE 1883

CMD ["-c", "/etc/zenoh/config.json5"]