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
```

### Deploy from Mac

```bash
./scripts/deploy.sh
```

### VS Code task

Run the “Deploy to VPS” task. It uses `.env.deploy` for the VPS settings.
