#!/usr/bin/env bash
set -euo pipefail

API_ID=""
API_HASH=""
API_BEARER=""
REPO=""
BRANCH="main"
PORT="8080"
HOST="0.0.0.0"
SERVICE_NAME="tg-mirror"
SERVICE_USER="$(whoami)"
APP_DIR="/opt/tg-mirror"
RESET="0"

usage() {
  echo "Usage:"
  echo "  install.sh --repo <git_url> --api-id <id> --api-hash <hash> [--branch main] [--port 8080] [--bearer token] [--reset]"
  exit 1
}

while [ $# -gt 0 ]; do
  case "$1" in
    --api-id) API_ID="$2"; shift 2;;
    --api-hash) API_HASH="$2"; shift 2;;
    --bearer) API_BEARER="$2"; shift 2;;
    --repo) REPO="$2"; shift 2;;
    --branch) BRANCH="$2"; shift 2;;
    --port) PORT="$2"; shift 2;;
    --host) HOST="$2"; shift 2;;
    --user) SERVICE_USER="$2"; shift 2;;
    --reset) RESET="1"; shift 1;;
    -h|--help) usage;;
    *) echo "Unknown arg: $1"; usage;;
  esac
done

[ -n "$REPO" ] || { echo "Missing --repo"; usage; }
[ -n "$API_ID" ] || { echo "Missing --api-id"; usage; }
[ -n "$API_HASH" ] || { echo "Missing --api-hash"; usage; }

echo "[1/9] Sync time (fix telethon MTProto old message)..."
sudo timedatectl set-ntp true >/dev/null 2>&1 || true
sudo apt-get update -y
sudo apt-get install -y git curl ca-certificates python3 python3-venv python3-pip chrony
sudo systemctl enable --now chrony >/dev/null 2>&1 || true

echo "[2/9] Prepare $APP_DIR..."
sudo mkdir -p "$APP_DIR"
sudo chown -R "$SERVICE_USER":"$SERVICE_USER" "$APP_DIR"

cd "$APP_DIR"

echo "[3/9] Clone/Pull repo..."
if [ ! -d ".git" ]; then
  git clone --branch "$BRANCH" "$REPO" .
else
  git fetch --all
  git reset --hard "origin/$BRANCH"
fi

echo "[4/9] Create venv + install deps..."
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip wheel setuptools

if [ -f requirements.txt ]; then
  pip install -r requirements.txt
else
  pip install -U fastapi "uvicorn[standard]" telethon aiohttp pydantic-settings python-multipart
fi

echo "[5/9] Create .env ..."
cat > .env <<EOF
API_ID=$API_ID
API_HASH=$API_HASH
API_BEARER=$API_BEARER

BIND_HOST=$HOST
BIND_PORT=$PORT

# realtime tune
SCAN_INTERVAL_SEC=2
BATCH_MAX=50
SESS_RESCAN_SEC=20
HEALTHCHECK_INTERVAL_SEC=45

INCLUDE_MEDIA=true
MEDIA_MAX_MB=50
EOF

echo "[6/9] Ensure folders + permissions..."
mkdir -p sessions sessions_pending
if [ "$RESET" = "1" ]; then
  rm -f state.json
fi
sudo chown -R "$SERVICE_USER":"$SERVICE_USER" "$APP_DIR"
sudo chmod -R 755 "$APP_DIR/sessions" "$APP_DIR/sessions_pending" || true

echo "[7/9] Create systemd service: $SERVICE_NAME"
sudo tee "/etc/systemd/system/${SERVICE_NAME}.service" >/dev/null <<EOF
[Unit]
Description=TG Mirror (FastAPI + Telethon)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$APP_DIR
EnvironmentFile=$APP_DIR/.env
ExecStart=$APP_DIR/.venv/bin/uvicorn app:app --host $HOST --port $PORT --log-level info
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
EOF

echo "[8/9] Start service..."
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME" >/dev/null 2>&1 || true
sudo systemctl restart "$SERVICE_NAME"

echo "[9/9] Quick check..."
sleep 1
curl -fsS "http://127.0.0.1:$PORT/status" >/dev/null && echo "OK: /status reachable" || echo "WARN: /status not reachable yet"
echo "Logs: sudo journalctl -u $SERVICE_NAME -f --no-pager"
