### Objective

This example shows how we can run telegraf and grafana to show CPU, MEM and DISK usage directly on a host (or send from multiple hosts using telegraf to an observability host)

### Things to note

The docker compose file contains all components necessary to run this example in a 'All-in-One' Scenario, where we want to monitor the usage metrics via Telegraf Agent and want to inspect it on that host directly.

Data is fed from Telegraf Agent directly into Gigapi and can be consumed either via Grafana or via Gigapi UI.

A token can be specified, but is not necessary. You may see a warning about a token being unspecified, this is okay and acceptable.

### How to run

On a new host place this folders contents,
then run:
```bash
docker compose pull
```
To get the latest images.
And then run:
```bash
docker compose up -d
```
To start the services.

Once up, you can either navigate to the localhost port 3000 for Grafana (default user:password is admin:admin) or to port 7971 to use Gigapi UI.

Telegraf will send usage metrics based on the telegraf.conf. Using telegraf's documentation you can add additional metrics to your hearts content.

You may also use other agents, utilizing the influxdb line format exporter.