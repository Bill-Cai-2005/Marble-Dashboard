# Marble Watchlist Tool

A comprehensive Streamlit application for managing stock watchlists with real-time data from NYSE and NASDAQ.

## Features

### Universal Watchlist
- Fetch all tickers from NYSE and NASDAQ exchanges
- Refresh data for all stocks at once
- View comprehensive stock metrics including:
  - Market Cap
  - Daily Stock Change % (automatically adjusts based on market session)
  - Volume
  - Industry
- Custom time range selection for stock change calculations
- Sortable columns
- Export to CSV

### Custom Watchlists
- Create and name multiple custom watchlists
- Add any ticker to any watchlist
- Refresh data for specific watchlists
- Same comprehensive metrics as Universal Watchlist
- Custom time range selection
- Sortable columns
- Export to CSV

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your Polygon API key (recommended: environment variable):
```bash
# PowerShell
$env:POLYGON_API_KEY="your_polygon_api_key"
```

Alternative: create `.streamlit/secrets.toml`:
```toml
POLYGON_API_KEY = "your_polygon_api_key"
```

## Usage

1. Run the Streamlit application:
```bash
streamlit run app.py
```

2. Navigate between Universal Watchlist and Custom Watchlists using the sidebar

3. For Universal Watchlist:
   - Click "Fetch NYSE/NASDAQ Tickers" to load all available tickers
   - Optionally enable custom time range
   - Click "Refresh All Stocks" to fetch current data

4. For Custom Watchlists:
   - Create a new watchlist by entering a name
   - Add tickers to your watchlist
   - Click "Refresh Watchlist" to fetch data
   - Use sorting options to organize your data

## Data Storage

- Custom watchlists are stored in `data/watchlists.json`
- Ticker lists are cached in `data/nyse_nasdaq_tickers.json`

## Notes

- **Polygon API Integration**: The application uses Polygon API for ticker universe and stock/fundamental data
- **Parallel Refresh**: Multi-threaded refresh is used to speed up watchlist updates
- **Optional Market Cap Filter**: You can restrict refresh to stocks with market cap >= $500M
- Data fetching is optimized for speed and supports cache loading to avoid unnecessary refreshes
- API key is loaded securely from `POLYGON_API_KEY` (env var) or Streamlit secrets
