"""
Alerta Rompimento Semanal — versão Web Service (Render.com).

Expõe endpoints HTTP:
  GET  /         → health check
  POST /scan     → roda a varredura e manda alertas via Telegram (chamado pelo cron-job.org)
  GET  /scan     → mesmo que POST, pra facilitar testes manuais
  GET  /state    → mostra o estado anti-spam atual (JSON)
  POST /reset    → reseta manualmente o estado anti-spam

Configuração via variáveis de ambiente (Render):
  TELEGRAM_BOT_TOKEN  → obrigatório
  TELEGRAM_CHAT_ID    → obrigatório (pode ter múltiplos separados por vírgula)
  N_WEEKS             → 1, 2, 3 ou 4 (padrão: 2)
  WEBHOOK_SECRET      → opcional, se setado exige header X-Secret no /scan

Estado anti-spam: persistido em /tmp/alert_state.json (efêmero no Render free tier,
mas como reseta toda semana, isso não importa - se o serviço reiniciar no meio
da semana, no máximo você recebe alertas duplicados de tickers já alertados).

Lista de tickers: weekly_tickers.txt no diretório do app.
"""
import os
import sys
import json
import datetime as dt
from pathlib import Path

import pandas as pd
import yfinance as yf
import requests
from flask import Flask, jsonify, request, abort


SCRIPT_DIR = Path(__file__).parent
TICKERS_FILE = SCRIPT_DIR / "weekly_tickers.txt"
# /tmp é writable no Render. Estado é efêmero mas reseta semanalmente mesmo, então OK.
STATE_FILE = Path("/tmp/alert_state.json")

# ============ CONFIGURAÇÃO ============
N_WEEKS = int(os.environ.get("N_WEEKS", "2"))
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
# ======================================


app = Flask(__name__)


# =========================================
# LÓGICA DE DETECÇÃO (igual à versão CLI)
# =========================================

def load_tickers():
    if not TICKERS_FILE.exists():
        return []
    tickers = []
    for line in TICKERS_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        tickers.append(line.upper())
    return tickers


def current_week_id():
    today = dt.date.today()
    iso = today.isocalendar()
    return f"{iso[0]}-{iso[1]:02d}"


def load_state():
    if not STATE_FILE.exists():
        return {"week_id": current_week_id(), "alerts": {}}
    try:
        state = json.loads(STATE_FILE.read_text())
        if state.get("week_id") != current_week_id():
            return {"week_id": current_week_id(), "alerts": {}}
        return state
    except Exception:
        return {"week_id": current_week_id(), "alerts": {}}


def save_state(state):
    state["week_id"] = current_week_id()
    STATE_FILE.write_text(json.dumps(state, indent=2))


def fetch_intraday_prices(ticker, lookback_days=60):
    try:
        daily = yf.Ticker(ticker).history(period=f"{lookback_days}d", auto_adjust=True)
        if daily.empty or len(daily) < 25:
            return None, None

        try:
            intraday = yf.Ticker(ticker).history(period="1d", interval="1m", auto_adjust=True)
            if not intraday.empty:
                current_price = float(intraday["Close"].iloc[-1])
                current_high_today = float(intraday["High"].max())
                current_low_today = float(intraday["Low"].min())
            else:
                current_price = float(daily["Close"].iloc[-1])
                current_high_today = float(daily["High"].iloc[-1])
                current_low_today = float(daily["Low"].iloc[-1])
        except Exception:
            current_price = float(daily["Close"].iloc[-1])
            current_high_today = float(daily["High"].iloc[-1])
            current_low_today = float(daily["Low"].iloc[-1])

        return daily, {
            "price": current_price,
            "high_today": current_high_today,
            "low_today": current_low_today,
        }
    except Exception as e:
        print(f"  ⚠ Erro {ticker}: {e}", flush=True)
        return None, None


def detect_breakout(daily_df, current, n_weeks):
    df = daily_df.copy()
    iso = pd.to_datetime(df.index).isocalendar()
    df["_week_id"] = iso.year.astype(str) + "-" + iso.week.astype(str).str.zfill(2)

    today_week = current_week_id()
    weeks_in_data = list(dict.fromkeys(df["_week_id"].tolist()))
    if today_week in weeks_in_data:
        weeks_in_data.remove(today_week)

    if len(weeks_in_data) < n_weeks:
        return None

    target_weeks = weeks_in_data[-n_weeks:]
    mask = df["_week_id"].isin(target_weeks)
    ref_high = float(df.loc[mask, "High"].max())
    ref_low = float(df.loc[mask, "Low"].min())

    sma20 = df["Close"].rolling(window=20, min_periods=20).mean()
    if pd.isna(sma20.iloc[-1]) or pd.isna(sma20.iloc[-2]):
        return None
    sma_today = float(sma20.iloc[-1])
    sma_yest = float(sma20.iloc[-2])
    sma_rising = sma_today > sma_yest
    sma_falling = sma_today < sma_yest

    breakout_high = current["high_today"] > ref_high
    breakout_low = current["low_today"] < ref_low

    long_signal = breakout_high and sma_rising
    short_signal = breakout_low and sma_falling

    if not (long_signal or short_signal):
        return None

    return {
        "long": long_signal,
        "short": short_signal,
        "current_price": current["price"],
        "current_high": current["high_today"],
        "current_low": current["low_today"],
        "ref_high": ref_high,
        "ref_low": ref_low,
        "sma20": sma_today,
        "n_weeks": n_weeks,
    }


def is_market_open_us():
    now_utc = dt.datetime.utcnow()
    ny_hour = (now_utc.hour - 5) % 24
    weekday = now_utc.weekday()
    if weekday >= 5:
        return False
    return 9 <= ny_hour < 16 or (ny_hour == 9 and now_utc.minute >= 30)


def send_telegram_single(ticker, breakout, direction, n_weeks, bot_token, chat_ids_raw):
    """Envia 1 mensagem por ativo+direção pro Telegram (formato compacto, real-time)."""
    now = dt.datetime.now().strftime("%H:%M")
    icon = "🟢" if direction == "LONG" else "🔴"
    if direction == "LONG":
        body = (
            f"  {icon} *LONG* — High {breakout['current_high']:.2f} rompeu máx {n_weeks}S "
            f"de U$ {breakout['ref_high']:.2f}\n"
            f"     SMA20: {breakout['sma20']:.2f} ↗ subindo"
        )
    else:
        body = (
            f"  {icon} *SHORT* — Low {breakout['current_low']:.2f} rompeu mín {n_weeks}S "
            f"de U$ {breakout['ref_low']:.2f}\n"
            f"     SMA20: {breakout['sma20']:.2f} ↘ descendo"
        )

    message = (
        f"🔔 *Rompimento Semanal ({n_weeks}S)* — {now}\n\n"
        f"*{ticker}* · preço atual U$ {breakout['current_price']:.2f}\n"
        f"{body}"
    )

    chat_ids = [c.strip() for c in chat_ids_raw.split(",") if c.strip()]
    success = 0
    for cid in chat_ids:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        try:
            r = requests.post(url, json={
                "chat_id": cid, "text": message,
                "parse_mode": "Markdown", "disable_web_page_preview": True,
            }, timeout=15)
            if r.status_code == 200:
                success += 1
            else:
                print(f"  ⚠ Erro {r.status_code} chat {cid}: {r.text}", flush=True)
        except Exception as e:
            print(f"  ⚠ Falha chat {cid}: {e}", flush=True)
    return success > 0


def run_scan(force=False):
    """Roda uma varredura completa. Retorna dict com resultado."""
    if not force and not is_market_open_us():
        return {"status": "market_closed", "alerts_sent": 0}

    tickers = load_tickers()
    if not tickers:
        return {"status": "no_tickers", "alerts_sent": 0}

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_ids = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not bot_token or not chat_ids:
        return {"status": "missing_secrets", "alerts_sent": 0}

    state = load_state()
    sent = 0
    new_alerts = []

    for ticker in tickers:
        daily, current = fetch_intraday_prices(ticker)
        if daily is None or current is None:
            continue

        breakout = detect_breakout(daily, current, N_WEEKS)
        if not breakout:
            continue

        already_long = state["alerts"].get(f"{ticker}_LONG", False)
        already_short = state["alerts"].get(f"{ticker}_SHORT", False)

        # Alerta IMEDIATO por direção, com persistência logo após cada envio
        if breakout["long"] and not already_long:
            if send_telegram_single(ticker, breakout, "LONG", N_WEEKS, bot_token, chat_ids):
                state["alerts"][f"{ticker}_LONG"] = True
                save_state(state)  # persiste a cada alerta enviado
                sent += 1
                new_alerts.append(f"{ticker}_LONG")

        if breakout["short"] and not already_short:
            if send_telegram_single(ticker, breakout, "SHORT", N_WEEKS, bot_token, chat_ids):
                state["alerts"][f"{ticker}_SHORT"] = True
                save_state(state)
                sent += 1
                new_alerts.append(f"{ticker}_SHORT")

    return {
        "status": "ok",
        "alerts_sent": sent,
        "new_alerts": new_alerts,
        "tickers_scanned": len(tickers),
        "week_id": state["week_id"],
        "total_alerts_this_week": len(state["alerts"]),
    }


# =========================================
# ENDPOINTS
# =========================================

def check_secret():
    """Se WEBHOOK_SECRET estiver setado, exige header X-Secret na request."""
    if not WEBHOOK_SECRET:
        return True
    return request.headers.get("X-Secret") == WEBHOOK_SECRET


@app.route("/")
def home():
    return jsonify({
        "service": "Weekly Breakout Alert",
        "status": "alive",
        "n_weeks": N_WEEKS,
        "market_open": is_market_open_us(),
        "tickers": len(load_tickers()),
        "endpoints": ["/scan", "/state", "/reset"],
    })


@app.route("/scan", methods=["GET", "POST"])
def scan_endpoint():
    if not check_secret():
        abort(401, "invalid secret")
    force = request.args.get("force", "").lower() == "true"
    result = run_scan(force=force)
    return jsonify(result)


@app.route("/state")
def state_endpoint():
    return jsonify(load_state())


@app.route("/reset", methods=["POST"])
def reset_endpoint():
    if not check_secret():
        abort(401, "invalid secret")
    save_state({"week_id": current_week_id(), "alerts": {}})
    return jsonify({"status": "reset"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
