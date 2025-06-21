# GigaGen
> GigAPI Timeseries Generator

Gigagen generates random timeseries data in line protocol format in real-time streaming or fast bulk backfilling modes.

## Features

- **Real-time mode**: Continuously generates and sends data points
- **Backfill mode**: Generate historical data with timestamps for fast bulk loading
- **Batch sending**: Send thousands of points per request for maximum performance
- **Flexible time formats**: Support for durations like `1h`, `30m`, `2h30m`, `1d`
- **Multiple tag sets**: Randomly cycles through different tag combinations
- **Configurable everything**: Database, series name, value ranges, frequency, and more

## Quick Start

```bash
# Make executable
chmod +x gigagen.sh

# Real-time mode (default)
./gigagen.sh -u https://gigapi:7971/write

# Backfill last hour with 1-minute intervals
./gigagen.sh -b -D 1h -i 1m -u https://gigapi:7971/write
```

## Usage

```
./gigagen.sh [OPTIONS]
```

### Core Options

| Option | Description | Default |
|--------|-------------|---------|
| `-d, --db NAME` | Database name | `mydb` |
| `-s, --series NAME` | Series name | `weather` |
| `-v, --value-name NAME` | Value field name | `temperature` |
| `-m, --min VALUE` | Minimum random value | `70` |
| `-M, --max VALUE` | Maximum random value | `100` |
| `-u, --url URL` | API endpoint URL | `https://gigapi:7971/write` |

### Real-time Mode Options

| Option | Description | Default |
|--------|-------------|---------|
| `-f, --frequency SEC` | Frequency in seconds | `5` |

### Backfill Mode Options

| Option | Description | Default |
|--------|-------------|---------|
| `-b, --backfill` | Enable backfill mode | `false` |
| `-D, --duration DURATION` | How far back to generate data | `1h` |
| `-i, --interval INTERVAL` | Time between data points | `1m` |
| `-B, --batch-size SIZE` | Points per HTTP request | `100` |

### Tag Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `-t, --tags TAG1,TAG2` | Space-separated tag sets | `location=us-midwest,season=summer location=us-east,season=summer location=us-west,season=summer` |

## Examples

### Real-time Mode

```bash
# Basic real-time generation
./gigagen.sh -u https://gigapi:7971/write

# Custom database and series
./gigagen.sh -d production -s cpu_usage -v percent -m 0 -M 100 -f 10 -u https://gigapi:7971/write

# Custom tags for server monitoring
./gigagen.sh -t 'host=server1,env=prod host=server2,env=dev host=server3,env=staging' -u https://gigapi:7971/write
```

### Backfill Mode

```bash
# Backfill last hour with 1-minute intervals
./gigagen.sh -b -D 1h -i 1m -u https://gigapi:7971/write

# High-frequency backfill with large batches for speed
./gigagen.sh -b -D 1d -i 30s -B 1000 -u https://gigapi:7971/write

# Custom scenario: Temperature data for last week
./gigagen.sh -b -D 7d -i 5m -d weather -s outdoor_temp -v celsius -m -10 -M 35 -u https://gigapi:7971/write

# Memory usage backfill
./gigagen.sh -b -D 6h -i 15s -s memory -v usage_mb -m 1000 -M 8000 -B 500 -u https://gigapi:7971/write
```

### Advanced Examples

```bash
# IoT sensor simulation - last 24 hours
./gigagen.sh -b -D 24h -i 2m \
  -d iot_sensors \
  -s sensor_data \
  -t 'device=sensor001,location=warehouse device=sensor002,location=office device=sensor003,location=factory' \
  -v humidity -m 30 -M 80 \
  -B 2000 \
  -u https://gigapi:7971/write

# Stock price simulation
./gigagen.sh -b -D 30d -i 1h \
  -d trading \
  -s stock_prices \
  -t 'symbol=AAPL symbol=GOOGL symbol=MSFT symbol=TSLA' \
  -v price -m 100 -M 300 \
  -B 1000 \
  -u https://gigapi:7971/write
```

## Time Format Guide

### Duration Examples
- `30s` - 30 seconds
- `5m` - 5 minutes  
- `2h` - 2 hours
- `1d` - 1 day
- `2h30m` - 2 hours and 30 minutes
- `1d12h` - 1 day and 12 hours

### Interval Examples
- `1s` - Every second
- `30s` - Every 30 seconds
- `1m` - Every minute
- `5m` - Every 5 minutes
- `1h` - Every hour

## Performance Tips

### Batch Size Recommendations
- **Small datasets** (< 1K points): `100-500`
- **Medium datasets** (1K-100K points): `500-2000` 
- **Large datasets** (> 100K points): `1000-5000`
- **API rate limits**: Start with `100` and increase until you hit limits

### Speed Optimization
```bash
# Maximum speed backfill
./gigagen.sh -b -D 1d -i 10s -B 5000 -u https://gigapi:7971/write

# Conservative for slower APIs  
./gigagen.sh -b -D 1d -i 1m -B 100 -u https://gigapi:7971/write
```

## Output Format

The script generates InfluxDB line protocol:

**Real-time mode** (no timestamp):
```
weather,location=us-midwest,season=summer temperature=87
```

**Backfill mode** (with nanosecond timestamp):
```
weather,location=us-midwest,season=summer temperature=87 1640995200000000000
```

## Troubleshooting

**Connection errors**: Check your API URL and ensure the endpoint is accessible
```bash
curl -v https://gigapi:7971/write?db=test
```

**Invalid time format**: Use formats like `1h`, `30m`, `2h30m`
```bash
# ❌ Wrong
./gigagen.sh -b -D "1 hour" -u https://gigapi:7971/write

# ✅ Correct  
./gigagen.sh -b -D 1h -u https://gigapi:7971/write
```

**Batch too large**: Reduce batch size if you get timeout errors
```bash
./gigagen.sh -b -D 1h -i 1m -B 50 -u https://gigapi:7971/write
```

## License

MIT License - feel free to modify and distribute!
