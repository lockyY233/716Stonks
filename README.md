# 716Stonks

A Discord stock simulator bot.

## Setup

1. Create a file named `TOKEN` in the project root.
2. Put your Discord bot token on a single line in that file.
3. Install dependencies:

```powershell
pip install -r requirements.txt
```

## Run

```powershell
python -m stockbot
```

## Deploy (VPS)

### Configure

Create a local `.env.deploy` file (ignored by git):

```
VPS_USER=ubuntu
VPS_HOST=1.2.3.4
VPS_PORT=22
VPS_DIR=/home/ubuntu/apps/716Stonks
APP_NAME=716Stonks
PYTHON=python3
SETUP_SYSTEMD=0
```

### Deploy from Mac

```bash
./scripts/deploy.sh
```

Normal deploy syncs files, installs dependencies, and restarts the existing
`systemd` service `${APP_NAME}.service`.

For first deploy (or whenever you want to recreate/update the unit file), run:

```bash
SETUP_SYSTEMD=1 ./scripts/deploy.sh
```

### VS Code task

Run the “Deploy to VPS” task. It uses `.env.deploy` for the VPS settings.

## Dashboard (Live Stocks)

Run a lightweight live dashboard web app:

```bash
python scripts/dashboard_web.py
```

Defaults:
- Host: `127.0.0.1`
- Port: `8082`
- DB path: `data/stockbot.db`

Optional env overrides:
- `DASHBOARD_HOST`
- `DASHBOARD_PORT`
- `DASHBOARD_DB_PATH`

Features:
- Main page shows live stock table + mini trends.
- Left sidebar has quick controls for `base_price` and `slope`.
- Click `Open` on a company to view `/company/<SYMBOL>`:
  - live-updating chart
  - editable parameters (`name`, `current_price`, `base_price`, `slope`, `drift`, `liquidity`, `impact_power`)
