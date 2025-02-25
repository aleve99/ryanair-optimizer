# Ryanair Optimizer

A powerful tool that optimizes the cheapest return flights from an origin airport across all available destinations.

## Overview

Ryanair Optimizer helps you find the best flight deals from your chosen airport. It can search for:
- One-way flights to any or specific destinations
- Round-trip flights with customizable stay duration
- Multi-trip itineraries with multiple stops

## Installation

```bash
# Clone the repository
git clone https://github.com/aleve99/ryanair-optimizer.git
cd ryanair-optimizer

# Install dependencies
pip install -r requirements.txt
```

## Usage

```bash
python -m main [arguments]
```

## Command-Line Arguments

### Required Arguments

| Argument | Description |
|----------|-------------|
| `--origin` | The origin airport (IATA code), e.g., `DUB` for Dublin |

### Optional Arguments

#### Flight Search Parameters

| Argument | Description | Default |
|----------|-------------|---------|
| `--dests` | Destination airports (IATA codes). If missing, search across all destinations. For multiple destinations, separate with pipe symbol (e.g., `LHR\|CDG\|FCO`) | All destinations |
| `--from-date` | The first date (YYYY-MM-DD) to search for flights | Today |
| `--to-date` | The last date (YYYY-MM-DD) to search for flights | Ryanair's maximum booking range |
| `--min-nights` | The minimum number of nights for round-trip stays (must be ≥ 0) | None (one-way search) |
| `--max-nights` | The maximum number of nights for round-trip stays (must be ≥ 0 and ≥ min-nights) | None (one-way search) |
| `--max-price` | The maximum price for a flight | 30 |

#### Multi-Trip Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--multi-trip` | Use multi-trip optimizer to find itineraries with multiple stops | False |
| `--cutoff` | The maximum number of flights in a multi-trip | 4 |

#### Configuration Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--config-path` | Path to TOML file for proxy and other configurations | `config/config.toml` |
| `--proxy-path` | Path to .txt file with a proxy domain for each line | `config/proxy_list.txt` |
| `--no-proxy` | Disable the use of proxies | False |
| `--use-usd` | Display prices in USD (otherwise uses the origin airport currency) | False |
| `--serve-html` | Serve the fares in a web server | False |

## Use Cases

### 1. One-Way Flight Search

Search for one-way flights from Dublin to any destination:

```bash
python -m main --origin DUB
```

Search for one-way flights from Dublin to specific destinations:

```bash
python -m main --origin DUB --dests LHR|CDG|FCO
```

### 2. Round-Trip Flight Search

Search for round-trip flights from Dublin with a stay of 3-7 nights:

```bash
python -m main --origin DUB --min-nights 3 --max-nights 7
```

Search for round-trip flights from Dublin to Rome with a stay of 2-5 nights:

```bash
python -m main --origin DUB --dests FCO --min-nights 2 --max-nights 5
```

### 3. Multi-Trip Itineraries

Search for multi-trip itineraries from Dublin with up to 3 flights:

```bash
python -m main --origin DUB --multi-trip --cutoff 3
```

### 4. Date Range Specification

Search for flights within a specific date range:

```bash
python -m main --origin DUB --from-date 2023-06-01 --to-date 2023-08-31
```

### 5. Price Filtering

Search for flights with a maximum price of 50:

```bash
python -m main --origin DUB --max-price 50
```

### 6. Currency Options

Display prices in USD:

```bash
python -m main --origin DUB --use-usd
```

### 7. Proxy Configuration

Use custom proxy configuration:

```bash
python -m main --origin DUB --config-path my_config.toml --proxy-path my_proxies.txt
```

Disable proxy usage:

```bash
python -m main --origin DUB --no-proxy
```

### 8. Web Interface

Serve results in a web interface:

```bash
python -m main --origin DUB --serve-html
```

## Output

The search results are saved in the `fares/` directory in CSV format and can be viewed in a web interface if the `--serve-html` option is used.

## Notes

- When using `--min-nights` and `--max-nights`, both must be provided together
- The `--min-nights` value must be less than the `--max-nights` value
- For multi-trip searches, the `--cutoff` parameter limits the maximum number of flights in an itinerary