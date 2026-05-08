"""
live_trading_engine.py
──────────────────────
Single-file Nifty options live trading engine (WebSocket), adapted for weekly workflow with manual exits.

Merges: live_engine_ws.py · zerodha_ws.py · survivor.py · wave.py
        nifty.py · time_utils.py

Key fixes vs original code
──────────────────────────
1. KiteTicker.connect() — removed unsupported `disable_reconnect` kwarg;
   reconnect is now controlled via KiteTicker(reconnect=...) constructor arg.
2. Auth hardening — token expiry is caught at every broker call, not just
   every 10 minutes.  Engine pauses+logs rather than sys.exit(1).
3. WebSocket error/close callbacks are wired so drops are visible in logs.
4. Order placement wraps every kiteconnect exception and distinguishes
   auth errors from transient errors.
5. Heartbeat now also logs queue depth so you can see if ticks are backing up.
6. SurvivorArm reset logic bug fixed (was resetting ref to wrong side).
7. Wave/Survivor trigger state now advances only after a successful order.
8. Open-position ceilings removed; one-lot entries can continue indefinitely
   as long as trigger and delta logic allow.

v2 changes
──────────
9.  ATR primed from 14 trading days of daily candles (was 5 days of 15min).

    ATR period config set to 14 candles.  This gives a stable 2-week vol picture.
8.  Distance formula: distance = max(min_points, ATR*atr_mult, VIX_move).
    atr_mult now defaults to 1.0 (was 1.5) — the ATR itself already captures
    a full-day expected move; multiplying by 1.5 was double-counting.
9.  Wave gap changed from 80pt → 20pt base for earlier Wave participation.
10. Delta band tightened from ±5.0 → ±0.50 per Raahi's "delta close to zero"
    philosophy.  rm_trigger_delta changed from 4.0 → 0.30 to match.
    delta_tilt_soft changed from 2.0 → 0.20.
11. All datetime.now() calls replaced with ist_now() — engine is hard-locked
    to Asia/Kolkata (IST) regardless of machine timezone or IP location.
12. 9:45 AM gate removed. Anchor drops on first tick after token confirm — no time gate.
    Delta management is active all day. Emergency mode (|Δ| ≥ 2.0) fires even outside
    normal hours and stops only when |Δ| < 1.0.
13. Overnight hedge module REMOVED. Replaced with Aggressive Delta Neutralization via selling.
    Full day: sell OTM to neutralize delta when |Δ| ≥ 2.0 (normal RM runs continuously).
    Power Hour (15:15–15:25): threshold tightens to 1.0; engine targets Δ=0 aggressively.
    No buying of protective options at any time. Pure credit-based delta management.

Usage
─────
    python live_trading_engine.py
    # Live-only build. Paper trading is removed.
"""

from __future__ import annotations

# ── stdlib ────────────────────────────────────────────────────────────────────
import argparse
import csv
import importlib.util
import re
import json
import logging
import math
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
_BOT_DIR = Path("/opt/niftybot")
from queue import Empty, Queue
from collections import deque
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    from importlib import metadata as importlib_metadata
except Exception:  # pragma: no cover
    import importlib_metadata  # type: ignore

# ── IST timezone (hard-locked — no DST, always +05:30) ───────────────────────
try:
    from zoneinfo import ZoneInfo as _ZoneInfo   # Python 3.9+
    _IST = _ZoneInfo("Asia/Kolkata")
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo as _ZoneInfo
        _IST = _ZoneInfo("Asia/Kolkata")
    except ImportError:
        # Pure-stdlib fallback: fixed UTC+05:30 offset (IST has no DST ever)
        _IST = None  # type: ignore

def ist_now() -> datetime:
    """
    Return current wall-clock time in IST (Asia/Kolkata = UTC+05:30).
    This is timezone-naive for compatibility with the rest of the codebase,
    but is derived from UTC so it is always correct regardless of the machine's
    local timezone or IP-inferred location.
    """
    import time as _time
    utc_ts = _time.time()
    ist_offset_secs = 5 * 3600 + 30 * 60   # +05:30, no DST adjustment ever
    return datetime.utcfromtimestamp(utc_ts + ist_offset_secs)

# ── dependency bootstrap + third-party imports ─────────────────────────────────
DEPENDENCY_SPECS = [
    {"module": "pandas", "package": "pandas", "min_version": None, "required": True},
    {"module": "kiteconnect", "package": "kiteconnect>=5.1.0", "version_package": "kiteconnect", "min_version": "5.1.0", "required": True},
    {"module": "dotenv", "package": "python-dotenv", "version_package": "python-dotenv", "min_version": None, "required": True},
    {"module": "rich", "package": "rich", "min_version": None, "required": True},
    {"module": "mibian", "package": "mibian", "min_version": None, "required": True},
    {"module": "scipy", "package": "scipy", "min_version": None, "required": True},
]


def _version_tuple(version_str: str) -> Tuple[int, ...]:
    parts = re.findall(r"\d+", str(version_str))
    if not parts:
        return (0,)
    return tuple(int(p) for p in parts[:4])


def _bootstrap_dependencies(auto_install: bool = True) -> None:
    needed_specs: List[str] = []
    reasons: List[str] = []

    for dep in DEPENDENCY_SPECS:
        module_name = dep["module"]
        package_spec = dep["package"]
        version_package = dep.get("version_package") or module_name
        min_version = dep.get("min_version")

        if importlib.util.find_spec(module_name) is None:
            needed_specs.append(package_spec)
            reasons.append(f"{package_spec} (missing)")
            continue

        if min_version:
            try:
                installed_version = importlib_metadata.version(version_package)
            except Exception:
                installed_version = "0"
            if _version_tuple(installed_version) < _version_tuple(str(min_version)):
                needed_specs.append(package_spec)
                reasons.append(
                    f"{package_spec} (installed {installed_version}, need >= {min_version})"
                )

    if not needed_specs:
        return

    print("Checking Python dependencies...")
    print("Installing/upgrading packages: " + ", ".join(reasons))
    if not auto_install:
        sys.exit(
            "Missing or outdated Python packages detected. Install/upgrade them manually and rerun the script."
        )

    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--upgrade", *sorted(set(needed_specs))]
        )
        print("Dependency installation completed successfully.")
    except Exception as exc:
        sys.exit(
            "Failed to install required Python packages automatically. "
            f"Install them manually and rerun the script. Error: {exc}"
        )


_bootstrap_dependencies(auto_install=True)

import pandas as pd
from kiteconnect import KiteConnect, KiteTicker

try:
    from dotenv import load_dotenv, set_key
    load_dotenv()
    HAS_DOTENV = True
except Exception:
    HAS_DOTENV = False

try:
    from rich.console import Group
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    HAS_RICH = True
except Exception:
    HAS_RICH = False

try:
    import mibian
    HAS_MIBIAN = True
except Exception:
    HAS_MIBIAN = False

try:
    from scipy.stats import norm as _norm
    import math as _math
    HAS_SCIPY = True
except Exception:
    HAS_SCIPY = False


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — LOGGER
# ══════════════════════════════════════════════════════════════════════════════

def setup_logger(
    level: str = "INFO",
    log_dir: str = "logs",
    enable_console: bool = True,
) -> logging.Logger:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s | %(levelname)s | nifty_engine | %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"
    logger = logging.getLogger("nifty_engine")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if logger.handlers:
        return logger
    if enable_console:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter(fmt, date_fmt))
        logger.addHandler(sh)
    fh = logging.FileHandler(
        Path(log_dir) / f"{ist_now().strftime('%Y%m%d')}.log",
        encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter(fmt, date_fmt))
    logger.addHandler(fh)
    return logger


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — TIME UTILS
# ══════════════════════════════════════════════════════════════════════════════

def parse_hhmm(value: str) -> dtime:
    hh, mm = value.split(":")
    return dtime(hour=int(hh), minute=int(mm))


def in_time_window(now: datetime, start_hhmm: str, end_hhmm: str) -> bool:
    return parse_hhmm(start_hhmm) <= now.time() <= parse_hhmm(end_hhmm)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — MODELS
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class TradeIntent:
    source: str          # "wave" | "survivor"
    requested_side: str  # "CE" | "PE"
    reason: str


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — NIFTY HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def instruments_to_df(instruments: List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(instruments)
    if "expiry" in df.columns:
        df["expiry"] = pd.to_datetime(df["expiry"]).dt.date
    return df


def resolve_nifty_option_chain(df: pd.DataFrame, today: date) -> pd.DataFrame:
    opts = df[
        (df["name"] == "NIFTY") &
        (df["instrument_type"].isin(["CE", "PE"])) &
        (df["expiry"] >= today)
    ].copy()
    if opts.empty:
        raise RuntimeError("No active NIFTY options found.")
    return opts


def resolve_nifty_future_contract(df: pd.DataFrame, today: date) -> Optional[dict]:
    futs = df[
        (df["name"] == "NIFTY") &
        (df["instrument_type"] == "FUT") &
        (df["expiry"] >= today)
    ].sort_values("expiry")
    if futs.empty:
        return None
    return futs.iloc[0].to_dict()


def available_expiries(chain: pd.DataFrame, today: date) -> List[date]:
    expiries = sorted(x for x in chain["expiry"].dropna().unique() if x >= today)
    if not expiries:
        raise RuntimeError("No valid option expiry found.")
    return expiries


def nearest_expiry(chain: pd.DataFrame, today: date) -> date:
    return available_expiries(chain, today)[0]


def resolve_selected_expiry(
    chain: pd.DataFrame,
    today: date,
    selected_expiry: Optional[Any] = None,
    expiry_offset: int = 0,
) -> date:
    expiries = available_expiries(chain, today)
    if selected_expiry not in (None, ""):
        try:
            chosen = pd.to_datetime(selected_expiry).date()
        except Exception as e:
            raise RuntimeError(f"Invalid selected expiry: {selected_expiry}") from e
        if chosen not in expiries:
            raise RuntimeError(
                f"Selected expiry {chosen} not found in live NIFTY option chain. "
                f"Available expiries: {', '.join(str(x) for x in expiries[:8])}"
            )
        return chosen
    idx = max(0, int(expiry_offset or 0))
    if idx >= len(expiries):
        raise RuntimeError(
            f"Requested expiry offset {idx} but only {len(expiries)} expiry dates are available."
        )
    return expiries[idx]


def lot_size_from_chain(chain: pd.DataFrame, expiry: date) -> int:
    rows = chain[chain["expiry"] == expiry]
    if rows.empty:
        raise RuntimeError("No rows for chosen expiry.")
    return int(rows.iloc[0]["lot_size"])


def strike_step_from_chain(chain: pd.DataFrame, expiry: date) -> int:
    rows = chain[chain["expiry"] == expiry].sort_values("strike")
    strikes = sorted(rows["strike"].dropna().astype(float).unique())
    if len(strikes) < 2:
        return 50
    diffs = [
        int(round(strikes[i + 1] - strikes[i]))
        for i in range(len(strikes) - 1)
        if strikes[i + 1] > strikes[i]
    ]
    return min(d for d in diffs if d > 0) if diffs else 50


def round_to_step(value: float, step: int) -> int:
    return int(round(value / step) * step)


def pick_option_by_strike(
    chain: pd.DataFrame, expiry: date, strike: int, opt_type: str
) -> Optional[dict]:
    rows = chain[
        (chain["expiry"] == expiry) &
        (chain["strike"].astype(float) == float(strike)) &
        (chain["instrument_type"] == opt_type)
    ]
    return rows.iloc[0].to_dict() if not rows.empty else None


def get_distance_points(
    spot: float,
    atr: Optional[float],
    vix_value: float,
    dte_days: int,
    atr_mult: float,
    vix_mult: float,
    min_points: int,
    step: int,
) -> Tuple[int, float, float]:
    atr_move = 0.0 if atr is None else atr * atr_mult
    vix_move = (
        spot * (vix_value / 100.0) * ((max(dte_days, 1) / 365.0) ** 0.5) * vix_mult
    )
    points = max(float(min_points), float(atr_move), float(vix_move))
    return round_to_step(points, step), float(atr_move), float(vix_move)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — GREEKS
# ══════════════════════════════════════════════════════════════════════════════

def _norm_pdf(x: float) -> float:
    if HAS_SCIPY:
        return float(_norm.pdf(x))
    return 0.3989422804014327 * math.exp(-0.5 * x * x)


def _safe_time_to_expiry(days_to_expiry: float) -> float:
    return max(float(days_to_expiry), 1e-6) / 365.0


def _bs_price(spot: float, strike: float, T: float, r: float, sigma: float, opt_type: str) -> float:
    if T <= 0 or sigma <= 0 or spot <= 0 or strike <= 0:
        intrinsic = max(0.0, spot - strike) if opt_type == "CE" else max(0.0, strike - spot)
        return intrinsic
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if opt_type == "CE":
        return float(spot * _norm.cdf(d1) - strike * math.exp(-r * T) * _norm.cdf(d2))
    return float(strike * math.exp(-r * T) * _norm.cdf(-d2) - spot * _norm.cdf(-d1))


def _b76_price(forward: float, strike: float, T: float, r: float, sigma: float, opt_type: str) -> float:
    if T <= 0 or sigma <= 0 or forward <= 0 or strike <= 0:
        intrinsic = max(0.0, forward - strike) if opt_type == "CE" else max(0.0, strike - forward)
        return intrinsic
    d1 = (math.log(forward / strike) + 0.5 * sigma * sigma * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    disc = math.exp(-r * T)
    if opt_type == "CE":
        return float(disc * (forward * _norm.cdf(d1) - strike * _norm.cdf(d2)))
    return float(disc * (strike * _norm.cdf(-d2) - forward * _norm.cdf(-d1)))


def _solve_implied_volatility(model: str, underlying_price: float, strike: float, days_to_expiry: float, interest_rate_pct: float, premium: float, opt_type: str, fallback_vol_pct: float) -> float:
    if premium <= 0 or underlying_price <= 0 or strike <= 0:
        return float(fallback_vol_pct)
    T = _safe_time_to_expiry(days_to_expiry)
    r = interest_rate_pct / 100.0
    lo, hi = 0.01, 5.0
    pricing_fn = _b76_price if model == "sensibull" else _bs_price
    try:
        price_lo = pricing_fn(underlying_price, strike, T, r, lo, opt_type)
        price_hi = pricing_fn(underlying_price, strike, T, r, hi, opt_type)
        target = float(premium)
        if not (price_lo <= target <= price_hi):
            return float(fallback_vol_pct)
        for _ in range(80):
            mid = (lo + hi) / 2.0
            price_mid = pricing_fn(underlying_price, strike, T, r, mid, opt_type)
            if abs(price_mid - target) < 1e-4:
                return mid * 100.0
            if price_mid > target:
                hi = mid
            else:
                lo = mid
        return ((lo + hi) / 2.0) * 100.0
    except Exception:
        return float(fallback_vol_pct)


def calculate_option_greeks(model: str, underlying_price: float, strike: float, days_to_expiry: float, interest_rate_pct: float, premium: float, opt_type: str, fallback_vol_pct: float) -> Dict[str, float]:
    model = str(model or "zerodha").lower()
    if model not in {"zerodha", "sensibull"}:
        model = "zerodha"
    T = _safe_time_to_expiry(days_to_expiry)
    r = interest_rate_pct / 100.0
    sigma_pct = _solve_implied_volatility(model, underlying_price, strike, days_to_expiry, interest_rate_pct, premium, opt_type, fallback_vol_pct)
    sigma = max(sigma_pct / 100.0, 1e-6)
    if underlying_price <= 0 or strike <= 0:
        return {"iv": sigma_pct, "delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}

    if model == "sensibull":
        d1 = (math.log(underlying_price / strike) + 0.5 * sigma * sigma * T) / (sigma * math.sqrt(T))
        disc = math.exp(-r * T)
        delta = disc * _norm.cdf(d1) if opt_type == "CE" else disc * (_norm.cdf(d1) - 1.0)
        gamma = disc * _norm_pdf(d1) / (underlying_price * sigma * math.sqrt(T))
        vega = disc * underlying_price * _norm_pdf(d1) * math.sqrt(T) / 100.0
        price_now = _b76_price(underlying_price, strike, T, r, sigma, opt_type)
        price_next = _b76_price(underlying_price, strike, max(T - (1.0 / 365.0), 1e-8), r, sigma, opt_type)
    else:
        d1 = (math.log(underlying_price / strike) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        delta = _norm.cdf(d1) if opt_type == "CE" else (_norm.cdf(d1) - 1.0)
        gamma = _norm_pdf(d1) / (underlying_price * sigma * math.sqrt(T))
        vega = underlying_price * _norm_pdf(d1) * math.sqrt(T) / 100.0
        price_now = _bs_price(underlying_price, strike, T, r, sigma, opt_type)
        price_next = _bs_price(underlying_price, strike, max(T - (1.0 / 365.0), 1e-8), r, sigma, opt_type)
    theta = price_next - price_now
    return {"iv": float(sigma_pct), "delta": float(delta), "gamma": float(gamma), "theta": float(theta), "vega": float(vega)}


def option_delta(
    spot: float,
    strike: float,
    days_to_expiry: int,
    interest_rate_pct: float,
    volatility_pct: float,
    opt_type: str,
    engine: str = "mibian",
) -> float:
    premium = _bs_price(spot, strike, _safe_time_to_expiry(days_to_expiry), interest_rate_pct / 100.0, max(volatility_pct / 100.0, 1e-6), opt_type)
    return float(calculate_option_greeks("zerodha", spot, strike, days_to_expiry, interest_rate_pct, premium, opt_type, volatility_pct)["delta"])


def select_volatility(
    greeks_mode: str,
    stable_vol: float,
    vix_value: float,
    todays_vol_fallback: float,
) -> float:
    if greeks_mode == "stable":
        return float(stable_vol)
    if greeks_mode == "vix":
        return float(vix_value) if vix_value and vix_value > 0 else float(todays_vol_fallback)
    if greeks_mode == "implied":
        return float(vix_value) if vix_value and vix_value > 0 else float(stable_vol)
    return float(todays_vol_fallback)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — ATR BUFFER
# ══════════════════════════════════════════════════════════════════════════════

class ATRBuffer:
    def __init__(self, period: int = 14):
        self.period = period
        self._prev_close: Optional[float] = None
        self._trs: List[float] = []
        self._atr: Optional[float] = None

    def update(self, high: float, low: float, close: float) -> None:
        if self._prev_close is not None:
            tr = max(high - low, abs(high - self._prev_close), abs(low - self._prev_close))
        else:
            tr = high - low
        self._trs.append(tr)
        if len(self._trs) >= self.period:
            self._atr = sum(self._trs[-self.period:]) / self.period
        self._prev_close = close

    def value(self) -> Optional[float]:
        return self._atr


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — PORTFOLIO
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Position:
    source: str
    opt_type: str
    tradingsymbol: str
    strike: float
    expiry: date
    entry_price: float
    qty: int
    lots: int
    distance: float
    entry_time: datetime
    current_price: float = 0.0
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    iv: float = 0.0


class Portfolio:
    def __init__(self):
        self.positions: List[Position] = []
        self.realized_by_strategy: Dict[str, float] = {}

    def add(self, pos: Position) -> None:
        self.positions.append(pos)

    def remove(self, pos: Position) -> None:
        self.positions = [p for p in self.positions if p is not pos]

    def settle_expired(self, spot: float, now: datetime) -> List[str]:
        events = []
        alive = []
        for p in self.positions:
            if p.expiry < now.date():
                pnl = (p.entry_price - spot) * p.qty  # short option settled at spot
                self.realized_by_strategy[p.source] = (
                    self.realized_by_strategy.get(p.source, 0.0) + pnl
                )
                events.append(
                    f"EXPIRED {p.tradingsymbol} pnl={pnl:.2f} source={p.source}"
                )
            else:
                alive.append(p)
        self.positions = alive
        return events

    def mark(
        self,
        spot: float,
        vix_value: float,
        now: datetime,
        greeks_engine: str,
        greeks_mode: str,
        stable_volatility: float,
        interest_rate_pct: float,
        greek_style: str = "zerodha",
        greek_underlying_price: Optional[float] = None,
    ) -> Tuple[float, float, float, float, float, Dict[str, float]]:
        total_delta = 0.0
        total_gamma = 0.0
        total_theta = 0.0
        total_vega = 0.0
        mtm_total = 0.0
        mtm_by_strategy: Dict[str, float] = {}

        vol = select_volatility(greeks_mode, stable_volatility, vix_value, stable_volatility)
        underlying = float(greek_underlying_price or spot)

        for p in self.positions:
            dte = max((p.expiry - now.date()).days, 1)
            greeks = calculate_option_greeks(greek_style, underlying, p.strike, dte, interest_rate_pct, max(p.current_price, 0.01), p.opt_type, vol)
            p.iv = greeks["iv"]
            p.delta = greeks["delta"]
            p.gamma = greeks["gamma"]
            p.theta = greeks["theta"]
            p.vega = greeks["vega"]
            total_delta += (-p.delta) * p.lots
            total_gamma += (-p.gamma) * p.lots
            total_theta += (-p.theta) * p.qty
            total_vega += (-p.vega) * p.qty
            mtm = (p.entry_price - p.current_price) * p.qty
            mtm_total += mtm
            mtm_by_strategy[p.source] = mtm_by_strategy.get(p.source, 0.0) + mtm

        return total_delta, total_gamma, total_theta, total_vega, mtm_total, mtm_by_strategy


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — RISK MANAGER


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — RISK MANAGER
# ══════════════════════════════════════════════════════════════════════════════

class PassiveRiskManager:
    def __init__(self, delta_band: float, delta_tilt_soft: float, rm_trigger_delta: float):
        self.base_delta_band = float(delta_band)
        self.base_delta_tilt_soft = float(delta_tilt_soft)
        self.base_rm_trigger_delta = float(rm_trigger_delta)

    def params_for_dte(self, dte_days: int) -> Tuple[float, float, float]:
        """
        Expiry-aware delta policy for NIFTY weekly options.
        Far from expiry we allow the book to breathe more. As expiry approaches
        we tighten the soft tilt threshold, the RM force threshold, and the
        post-trade delta band so the book lands more controlled into expiry.
        """
        dte = max(int(dte_days), 0)
        if dte >= 6:
            return 0.70, 0.45, 0.70
        if dte >= 4:
            return 0.55, 0.35, 0.55
        if dte >= 2:
            return 0.35, 0.22, 0.35
        return 0.20, 0.10, 0.20

    def decide_side(self, portfolio_delta: float, requested_side: str, dte_days: int) -> Tuple[str, str]:
        delta_band, delta_tilt_soft, rm_trigger_delta = self.params_for_dte(dte_days)
        if portfolio_delta > rm_trigger_delta:
            return "CE", f"rm_forced_ce_dte_{max(int(dte_days), 0)}"
        if portfolio_delta < -rm_trigger_delta:
            return "PE", f"rm_forced_pe_dte_{max(int(dte_days), 0)}"
        if portfolio_delta > delta_tilt_soft and requested_side == "PE":
            return "CE", f"rm_tilt_ce_dte_{max(int(dte_days), 0)}"
        if portfolio_delta < -delta_tilt_soft and requested_side == "CE":
            return "PE", f"rm_tilt_pe_dte_{max(int(dte_days), 0)}"
        return requested_side, "strategy"

    def allows_delta(self, old_delta: float, new_delta: float, dte_days: int) -> bool:
        """
        Portfolio-level delta admission gate.

        The portfolio delta already includes ALL live broker positions across
        expiries, so this check evaluates the whole book as one combined
        position. A trade is allowed if it lands the book inside the active
        band, or if it clearly repairs total portfolio delta toward zero.

        To keep adjustments smooth, a trade that crosses through zero is only
        accepted when the resulting overshoot on the other side is modest.
        This avoids flip-flops where one repair leg immediately creates a new
        imbalance in the opposite direction.
        """
        delta_band, delta_tilt_soft, _ = self.params_for_dte(dte_days)
        old_abs = abs(float(old_delta))
        new_abs = abs(float(new_delta))
        improve_by = old_abs - new_abs

        # Normal case: if the post-trade portfolio lands inside the active
        # DTE-aware band, the trade is fine.
        if new_abs <= delta_band:
            return True

        # Repair case: if the proposed trade improves total portfolio delta,
        # allow it even if it does not fully restore the book in one step.
        min_improve = 0.02
        if improve_by >= min_improve:
            crossed_zero = (old_delta < 0 < new_delta) or (old_delta > 0 > new_delta)
            if crossed_zero:
                max_smooth_overshoot = max(delta_tilt_soft, 0.15)
                if new_abs > max_smooth_overshoot:
                    return False
            return True

        return False

    def tilt_soft_for_dte(self, dte_days: int) -> float:
        _, delta_tilt_soft, _ = self.params_for_dte(dte_days)
        return delta_tilt_soft


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — STRATEGIES
# ══════════════════════════════════════════════════════════════════════════════

class SurvivorArm:
    """
    Trend-following Survivor strategy.
    Activates when spot is `activation_offset` (100) points away from anchor.
    Fires every `gap` (20) points thereafter.
    """
    def __init__(self, gap: float, activation_offset: float = 100.0):
        self.gap = float(gap)
        self.activation_offset = float(activation_offset)
        self.anchor = None
        self.up_index = 0
        self.down_index = 0

    def preview_intents(self, spot: float, wave_up_exhausted: bool = False, wave_down_exhausted: bool = False) -> list:
        intents = []
        if self.anchor is None:
            return intents

        # Upward Trend Logic (Sell PE) — only after wave up ladder exhausted
        if wave_up_exhausted:
            target_up = self.anchor + self.activation_offset + (self.up_index * self.gap)
            if spot >= target_up:
                intents.append(TradeIntent("survivor", "PE", f"survivor_up_move_{self.up_index + 1}"))

        # Downward Trend Logic (Sell CE) — only after wave down ladder exhausted
        if wave_down_exhausted:
            target_down = self.anchor - self.activation_offset - (self.down_index * self.gap)
            if spot <= target_down:
                intents.append(TradeIntent("survivor", "CE", f"survivor_down_move_{self.down_index + 1}"))

        return intents

    def commit_intent(self, intent) -> None:
        if intent.source != "survivor":
            return
        if intent.requested_side == "PE":
            self.up_index += 1
        elif intent.requested_side == "CE":
            self.down_index += 1

    def ensure_seed(self, spot: float) -> None:
        """Set anchor on first call if not already set. Safe to call every tick."""
        if self.anchor is None:
            self.anchor = spot

@dataclass
class WaveState:
    anchor: Optional[float] = None


class WaveArm:
    """
    FIXED-ANCHOR Wave with config-driven ladder + cooldown.

    - Anchor is set once on first live spot after script start
    - Anchor never moves during the session
    - Upside and downside ladders are tracked independently
    - Each side has a cooldown
    - The ladder is derived from `wave_gap`, so config changes actually matter

    IMPORTANT
    ─────────
    Trigger detection is side-effect free.  Ladder indexes advance only after
    the corresponding order is actually accepted, so blocked orders cannot
    silently consume a Wave level.
    """

    def __init__(self, gap: float, ladder_count: int = 6, ladder_step: float = 15.0, cooldown: float = 120.0):
        self.gap = float(gap)
        self.anchor: Optional[float] = None
        ladder_count = max(1, int(ladder_count))
        ladder_step = float(ladder_step)
        self.ladder = [self.gap + (i * ladder_step) for i in range(ladder_count)]
        self.up_index = 0
        self.down_index = 0
        self.last_order_time_up = 0.0
        self.last_order_time_down = 0.0
        self.cooldown = float(cooldown)

    def ensure_anchor(self, spot: float) -> None:
        # Anchor is set explicitly by engine in evaluate() on first tick after token confirm.
        # It is NOT set automatically here — prevents premature anchor setting.
        pass

    def preview_intent(
        self, spot: float, portfolio_delta: float, delta_tilt_soft: float
    ) -> Optional[TradeIntent]:
        self.ensure_anchor(spot)
        now_ts = time.time()

        # Anchor not set yet — cannot compute wave levels
        if self.anchor is None:
            return None

        if self.up_index < len(self.ladder):
            level_up = self.anchor + self.ladder[self.up_index]
            if spot >= level_up and (now_ts - self.last_order_time_up) >= self.cooldown:
                return TradeIntent("wave", "CE", f"wave_ladder_up_{self.up_index + 1}")

        if self.down_index < len(self.ladder):
            level_down = self.anchor - self.ladder[self.down_index]
            if spot <= level_down and (now_ts - self.last_order_time_down) >= self.cooldown:
                return TradeIntent("wave", "PE", f"wave_ladder_down_{self.down_index + 1}")

        return None

    def commit_intent(self, intent: TradeIntent, now_ts: Optional[float] = None) -> None:
        if intent.source != "wave":
            return
        now_ts = time.time() if now_ts is None else float(now_ts)
        if intent.requested_side == "CE" and self.up_index < len(self.ladder):
            self.last_order_time_up = now_ts
            self.up_index += 1
        elif intent.requested_side == "PE" and self.down_index < len(self.ladder):
            self.last_order_time_down = now_ts
            self.down_index += 1


class AuthError(Exception):

    """Raised when Kite session is expired or invalid."""


class ZerodhaBroker:
    """
    Thin wrapper around KiteConnect with:
    - Automatic detection of auth errors on every call
    - WebSocket managed via KiteTicker
    - Reconnect controlled via KiteTicker constructor (not connect() kwarg)
    """

    _AUTH_PHRASES = (
        "Invalid `api_key` or `access_token`",
        "Incorrect `api_key` or `access_token`",
        "Token is invalid",
        "session expired",
    )

    def __init__(self, api_key: str, access_token: str):
        self.api_key = api_key
        self.access_token = access_token
        self._kite = KiteConnect(api_key=api_key)
        self._kite.set_access_token(access_token)
        self._ticker: Optional[KiteTicker] = None

    # ── internal helper ───────────────────────────────────────────────────────

    def _is_auth_error(self, exc: Exception) -> bool:
        msg = str(exc)
        return any(p.lower() in msg.lower() for p in self._AUTH_PHRASES)

    def _call(self, fn: Callable, *args, **kwargs):
        """Wrap every KiteConnect call; convert auth errors to AuthError."""
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if self._is_auth_error(e):
                raise AuthError(str(e)) from e
            raise

    # ── market data ───────────────────────────────────────────────────────────

    def instruments_nfo(self) -> List[dict]:
        return self._call(self._kite.instruments, "NFO")

    def historical(
        self, token: int, from_dt: datetime, to_dt: datetime, interval: str
    ) -> List[dict]:
        return self._call(
            self._kite.historical_data, token, from_dt, to_dt, interval
        )

    def ltp(self, symbols: List[str]) -> Dict[str, Any]:
        return self._call(self._kite.ltp, symbols)

    def positions(self) -> Dict[str, Any]:
        return self._call(self._kite.positions)

    def margins(self, segment: str = "equity") -> Dict[str, Any]:
        return self._call(self._kite.margins, segment)

    def is_session_valid(self) -> bool:
        try:
            self._kite.profile()
            return True
        except Exception as e:
            if self._is_auth_error(e):
                return False
            return True  # network hiccup ≠ auth failure

    # ── order placement ───────────────────────────────────────────────────────

    def place_market_order(
        self,
        tradingsymbol: str,
        exchange: str,
        transaction_type: str,
        quantity: int,
        product: str,
        tag: str,
        variety: str,
        market_protection: Any = -1,
    ) -> str:
        return self._call(
            self._kite.place_order,
            variety=variety,
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            transaction_type=transaction_type,
            quantity=quantity,
            product=product,
            order_type=self._kite.ORDER_TYPE_MARKET,
            tag=tag,
            market_protection=market_protection,
        )

    # ── websocket ─────────────────────────────────────────────────────────────

    def connect_ticker(
        self,
        tokens: List[int],
        mode: str,
        on_ticks: Callable,
        on_order_update: Optional[Callable] = None,
        on_connect: Optional[Callable] = None,
        on_close: Optional[Callable] = None,
        on_error: Optional[Callable] = None,
        reconnect: bool = True,
    ) -> None:
        """
        FIX: KiteTicker reconnect is set in the *constructor*, not in connect().
        Older code passed `disable_reconnect` to connect() which broke in newer
        kiteconnect versions.
        """
        self._ticker = KiteTicker(
            self.api_key,
            self.access_token,
            reconnect=reconnect,          # ← correct place for reconnect flag
        )

        def _on_connect(ws, response):
            ws.subscribe(tokens)
            mode_map = {
                "ltp": ws.MODE_LTP,
                "quote": ws.MODE_QUOTE,
                "full": ws.MODE_FULL,
            }
            ws.set_mode(mode_map.get(mode, ws.MODE_LTP), tokens)
            if on_connect:
                on_connect(response)

        def _on_ticks(ws, ticks):
            if on_ticks:
                on_ticks(ticks)

        def _on_order_update(ws, data):
            if on_order_update:
                on_order_update(data)

        def _on_close(ws, code, reason):
            if on_close:
                on_close(code, reason)

        def _on_error(ws, code, reason):
            if on_error:
                on_error(code, reason)

        self._ticker.on_connect = _on_connect
        self._ticker.on_ticks = _on_ticks
        self._ticker.on_order_update = _on_order_update
        self._ticker.on_close = _on_close
        self._ticker.on_error = _on_error

        self._ticker.connect(threaded=True)   # ← no unsupported kwargs

    def is_connected(self) -> bool:
        return bool(self._ticker and self._ticker.is_connected())

    def stop_ticker(self) -> None:
        if self._ticker:
            try:
                self._ticker.close()
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 11 — LIVE ENGINE  (auth-hardened, no sys.exit on token expiry)
# ══════════════════════════════════════════════════════════════════════════════

class LiveEngineWS:
    def __init__(self, cfg: dict, broker: ZerodhaBroker):
        self.cfg = cfg
        self.broker = broker

        self.audit_dir = Path("outputs/live")
        self.log_dir = self.audit_dir / "logs"
        self.orders_dir = self.audit_dir / "orders"
        self.order_updates_dir = self.audit_dir / "order_updates"
        self.runs_dir = self.audit_dir / "runs"
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.orders_dir.mkdir(parents=True, exist_ok=True)
        self.order_updates_dir.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)

        self.logger = setup_logger(
            level=cfg["log_level"],
            log_dir=str(self.log_dir),
            enable_console=not bool(cfg.get("dashboard_enabled", True)),
        )
        self.portfolio = Portfolio()
        self.wave = WaveArm(
            cfg["wave_gap"],
            ladder_count=int(cfg.get("wave_ladder_count", 6)),
            ladder_step=float(cfg.get("wave_ladder_step", 15.0)),
            cooldown=float(cfg.get("wave_cooldown_seconds", 120.0)),
        )
        self.survivor = SurvivorArm(cfg["surv_gap"], cfg["surv_reset"])
        self.brain = PassiveRiskManager(
            cfg["delta_band"], cfg["delta_tilt_soft"], cfg["rm_trigger_delta"]
        )
        self.tick_queue: Queue = Queue()
        self._auth_paused = False   # True when session is known bad

        self.instruments_df = instruments_to_df(self.broker.instruments_nfo())
        self.chain = resolve_nifty_option_chain(
            self.instruments_df, ist_now().date()
        )
        self.future_contract = resolve_nifty_future_contract(self.instruments_df, ist_now().date())
        self.future_symbol = self.future_contract["tradingsymbol"] if self.future_contract else None
        self.future_token = int(self.future_contract["instrument_token"]) if self.future_contract else None
        self.future_price: Optional[float] = None
        self.available_expiries = available_expiries(self.chain, ist_now().date())
        self.expiry = resolve_selected_expiry(
            self.chain,
            ist_now().date(),
            selected_expiry=cfg.get("selected_expiry"),
            expiry_offset=int(cfg.get("expiry_offset", 0) or 0),
        )
        self.lot_size = lot_size_from_chain(self.chain, self.expiry)
        self.strike_step = strike_step_from_chain(self.chain, self.expiry)
        self.atr = ATRBuffer(period=int(cfg["atr_period"]))
        self.last_daily_atr_refresh_date: Optional[date] = None
        self.today_ist: date = ist_now().date()

        self.spot: Optional[float] = None
        self.vix: Optional[float] = None
        self.last_snapshot_ts = 0.0
        self.last_heartbeat = 0.0
        self.last_session_check = time.time()

        # Flat-market decay rule state
        self.spot_history = deque(maxlen=4000)  # (timestamp, spot)
        self.last_entry_time_by_side = {"CE": None, "PE": None}
        self.last_trigger_time = None
        self.last_flat_decay_time_by_side = {"CE": 0.0, "PE": 0.0}
        self.last_session_check_dt = ist_now()
        self.last_tick_ts: Optional[datetime] = None
        self.last_order_event: str = "-"
        self.last_error: str = "-"
        self.decision_lines: List[str] = ["Waiting for evaluation"]
        self.last_tick_token: Optional[int] = None
        self.last_tick_price: Optional[float] = None
        self.recent_events: List[str] = []
        self.dashboard_enabled = bool(cfg.get("dashboard_enabled", True)) and HAS_RICH
        self.stop_requested = False
        self.status_file = _BOT_DIR / "outputs/live/engine_status.json"

        # ── Anchor system ─────────────────────────────────────────────────────
        # Anchor is set on the very first live tick the engine receives.
        # No time gate, no token gate. Wherever the market is when the script
        # starts, that is the anchor. At midnight the anchor is cleared so a
        # fresh anchor is set on the first tick of the new trading day.
        self.last_midnight_reset_date: Optional[date] = None  # track midnight reset

        self.last_position_price_refresh = 0.0
        self.daily_loss_stop_date: Optional[date] = None
        self.daily_loss_stop_triggered = False
        self.cached_portfolio_delta = 0.0
        self.cached_portfolio_gamma = 0.0
        self.cached_portfolio_theta = 0.0
        self.cached_portfolio_vega = 0.0
        self.last_risk_calc_at: Optional[datetime] = None
        self.last_snapshot_at: Optional[datetime] = None
        self.cached_mtm_total = 0.0
        self.cached_mtm_by_strategy: Dict[str, float] = {}
        self.cached_active_volatility = float(cfg.get("stable_volatility", 20.0))
        self.last_positions_sync_ts = 0.0
        self.workflow_mode = str(cfg.get("workflow_mode", "intraday")).lower()
        self.state_path = Path(cfg.get("weekly_state_path", str(self.audit_dir / "weekly_state.json")))
        self.run_started_at = ist_now()
        self.global_order_cooldown_seconds = float(cfg.get("post_order_cooldown_seconds", 90.0))
        self.next_order_allowed_ts = 0.0
        self.rejected_signal_cooldowns: Dict[str, float] = {}
        self.consecutive_margin_rejections = 0
        self.margin_watch_mode = False
        self.margin_required_floor = 0.0
        self.last_margin_check_ts = 0.0
        self.cached_margin_available = 0.0
        self.order_csv_path = self.orders_dir / f"{self.run_started_at.strftime('%Y%m%d')}_orders.csv"
        self.order_updates_jsonl_path = self.order_updates_dir / f"{self.run_started_at.strftime('%Y%m%d')}_order_updates.jsonl"
        self.run_meta_path = self.runs_dir / f"{self.run_started_at.strftime('%Y%m%d')}_runs.csv"
        self._init_daily_audit_files()
        self._record_run_event("START")

        self.logger.info(
            f"Engine init expiry={self.expiry} lot_size={self.lot_size} "
            f"strike_step={self.strike_step} live_only=True "
            f"greeks_engine={cfg['greeks_engine']} greeks_mode={cfg['greeks_mode']} greek_style={cfg.get('greek_style', 'sensibull')} "
            f"stable_vol={cfg['stable_volatility']} expiry_preference={cfg.get('expiry_preference_label', 'current')} "
            f"market_protection={cfg.get('market_protection', -1)}"
        )
        self._info("MANUAL EXIT MODE active: stop-loss, MTM loss guard, and forced expiry-day closes are disabled; greek-based balancing remains enabled")
        self._prime_atr()
        self.bootstrap_existing_positions()
        self._load_or_initialize_weekly_state()
        self._save_weekly_state()

    def _push_event(self, message: str) -> None:
        stamp = ist_now().strftime("%H:%M:%S")
        entry = f"{stamp} | {message}"
        self.recent_events.append(entry)
        max_events = int(self.cfg.get("dashboard_max_events", 12))
        self.recent_events = self.recent_events[-max_events:]

    def _info(self, message: str) -> None:
        self.logger.info(message)
        self._push_event(message)

    def _warning(self, message: str) -> None:
        self.logger.warning(message)
        self.last_error = message
        self._push_event(f"WARN {message}")

    def _error(self, message: str) -> None:
        self.logger.error(message)
        self.last_error = message
        self._push_event(f"ERROR {message}")

    def _exception(self, message: str) -> None:
        self.logger.exception(message)
        self.last_error = message
        self._push_event(f"EXC {message}")

    def _append_csv_row(self, path: Path, headers: List[str], row: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        exists = path.exists()
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not exists or path.stat().st_size == 0:
                writer.writeheader()
            if row:
                safe_row = {k: (json.dumps(v, default=str) if isinstance(v, (dict, list, tuple)) else v) for k, v in row.items()}
                writer.writerow(safe_row)

    def _init_daily_audit_files(self) -> None:
        self._append_csv_row(
            self.order_csv_path,
            [
                "timestamp_ist", "event_type", "mode", "transaction_type", "tradingsymbol", "qty",
                "source", "side", "reason", "order_id", "status", "exchange", "product", "variety",
                "market_protection", "expiry", "expiry_preference", "workflow_mode", "spot", "vix", "portfolio_delta",
                "mtm_total", "notes",
            ],
            {},
        )
        if self.order_updates_jsonl_path.exists() is False:
            self.order_updates_jsonl_path.touch()
        self._append_csv_row(
            self.run_meta_path,
            [
                "timestamp_ist", "event", "mode", "workflow_mode", "selected_expiry",
                "expiry_preference", "config_path", "notes",
            ],
            {},
        )

    def _record_run_event(self, event: str, notes: str = "") -> None:
        self._append_csv_row(
            self.run_meta_path,
            [
                "timestamp_ist", "event", "mode", "workflow_mode", "selected_expiry",
                "expiry_preference", "config_path", "notes",
            ],
            {
                "timestamp_ist": ist_now().isoformat(),
                "event": event,
                "mode": "LIVE",
                "workflow_mode": self.workflow_mode,
                "selected_expiry": str(self.expiry),
                "expiry_preference": self.cfg.get("expiry_preference_label", "current"),
                "config_path": self.cfg.get("config_path", ""),
                "notes": notes,
            },
        )

    def _record_order_event(
        self,
        event_type: str,
        transaction_type: str,
        tradingsymbol: str,
        qty: int,
        source: str,
        side: str,
        reason: str,
        order_id: str = "",
        status: str = "",
        notes: str = "",
        market_protection: Any = "",
    ) -> None:
        self._append_csv_row(
            self.order_csv_path,
            [
                "timestamp_ist", "event_type", "mode", "transaction_type", "tradingsymbol", "qty",
                "source", "side", "reason", "order_id", "status", "exchange", "product", "variety",
                "market_protection", "expiry", "expiry_preference", "workflow_mode", "spot", "vix", "portfolio_delta",
                "mtm_total", "notes",
            ],
            {
                "timestamp_ist": ist_now().isoformat(),
                "event_type": event_type,
                "mode": "LIVE",
                "transaction_type": transaction_type,
                "tradingsymbol": tradingsymbol,
                "qty": qty,
                "source": source,
                "side": side,
                "reason": reason,
                "order_id": order_id,
                "status": status,
                "exchange": "NFO",
                "product": self.cfg.get("product", ""),
                "variety": self.cfg.get("variety", ""),
                "market_protection": market_protection,
                "expiry": str(self.expiry),
                "expiry_preference": self.cfg.get("expiry_preference_label", "current"),
                "workflow_mode": self.workflow_mode,
                "spot": self.spot,
                "vix": self.vix,
                "portfolio_delta": self.cached_portfolio_delta,
                "mtm_total": self.cached_mtm_total,
                "notes": notes,
            },
        )

    def _record_order_update(self, data: dict) -> None:
        payload = {"timestamp_ist": ist_now().isoformat(), "data": data}
        with open(self.order_updates_jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")

    # ── startup helpers ───────────────────────────────────────────────────────

    def _prime_atr(self) -> None:
        """
        Seed ATR strictly from DAILY candles.

        Important:
        - This ATR is intended to represent a stable 14-trading-day daily range.
        - It must NOT be updated from live ticks, otherwise repeated tick updates
          with high=low=close collapse the rolling ATR toward 0.
        - We therefore only refresh it from daily candles, typically once per day.
        """
        try:
            token = int(self.cfg["nifty_index_instrument_token"])
            now = ist_now()
            # Ask for enough calendar days to reliably get >= 14 completed sessions.
            candles = self.broker.historical(
                token, now - timedelta(days=40), now, "day"
            ) or []

            if not candles:
                self._warning("ATR prime skipped: no daily candles returned")
                return

            period = int(self.cfg["atr_period"])

            # Historical day candles can include today's still-forming candle.
            # Keep only completed sessions strictly before 'today' in IST.
            completed = []
            today_ist = now.date()
            for row in candles:
                raw_dt = row.get("date")
                row_date = None
                if isinstance(raw_dt, datetime):
                    row_date = raw_dt.date()
                elif isinstance(raw_dt, str):
                    try:
                        row_date = pd.to_datetime(raw_dt).date()
                    except Exception:
                        row_date = None
                if row_date is None:
                    continue
                if row_date < today_ist:
                    completed.append(row)

            if len(completed) < period:
                self._warning(
                    f"ATR prime partial: only {len(completed)} completed daily candles "
                    f"available, need {period} for full ATR"
                )

            self.atr = ATRBuffer(period=period)
            for row in completed[-period:]:
                self.atr.update(
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                )

            atr_val = self.atr.value()
            if atr_val is None:
                self._warning(
                    f"ATR prime incomplete: ATR unavailable after loading "
                    f"{min(len(completed), period)} daily sessions"
                )
            else:
                self._info(
                    f"ATR primed value={atr_val:.4f} "
                    f"using {min(len(completed), period)} completed daily sessions"
                )

            self.last_daily_atr_refresh_date = today_ist

        except AuthError:
            self._handle_auth_error("ATR prime")
        except Exception as e:
            self._exception(f"ATR prime failed: {e}")

    def _refresh_daily_atr_if_needed(self) -> None:
        """
        Refresh the daily ATR once per IST day.
        This keeps ATR on a 14-trading-day basis without contaminating it with ticks.
        """
        current_date = ist_now().date()
        if current_date != self.today_ist:
            self.today_ist = current_date
        if self.last_daily_atr_refresh_date == self.today_ist:
            return
        self._prime_atr()

    def _roll_daily_guards_if_needed(self, now: datetime) -> None:
        if self.daily_loss_stop_date != now.date():
            self.daily_loss_stop_date = now.date()
            self.daily_loss_stop_triggered = False

    def _effective_dte_days(self, now: datetime) -> int:
        return max((self.expiry - now.date()).days, 0)

    def _is_expiry_day_mode(self, now: datetime) -> bool:
        return self._effective_dte_days(now) <= int(self.cfg.get("expiry_day_dte_threshold", 1))

    def _expiry_day_allows_new_entries(self, now: datetime) -> bool:
        if not self._is_expiry_day_mode(now):
            return True
        cutoff = self.cfg.get("expiry_day_new_entries_cutoff", "13:00")
        return now.time() <= parse_hhmm(cutoff)

    def _portfolio_nearest_expiry_dte(self, now: datetime) -> int:
        dtes = [max((p.expiry - now.date()).days, 0) for p in self.portfolio.positions if getattr(p, "qty", 0) > 0]
        if dtes:
            return min(dtes)
        return self._effective_dte_days(now)

    def _entry_expiry_for_new_orders(self, now: datetime) -> date:
        """
        New entries are allowed only when the candidate expiry has more than the
        configured minimum DTE. If the selected expiry is at 1 DTE or 0 DTE,
        automatically jump to the next available expiry for fresh positions.
        Existing positions across expiries remain tracked for Greeks.
        """
        min_new_entry_dte = int(self.cfg.get("min_new_entry_dte_same_expiry", 1))
        start_idx = max(0, int(self.cfg.get("expiry_offset", 0) or 0))
        expiries = [x for x in self.available_expiries if x >= now.date()]
        if not expiries:
            return self.expiry
        for exp in expiries[start_idx:]:
            if (exp - now.date()).days > min_new_entry_dte:
                return exp
        return expiries[-1]

    def _current_rm_dte(self, now: datetime) -> int:
        return min(self._portfolio_nearest_expiry_dte(now), max((self._entry_expiry_for_new_orders(now) - now.date()).days, 0))

    def bootstrap_existing_positions(self) -> None:
        try:
            payload = self.broker.positions()
            net_positions = payload.get("net", []) if isinstance(payload, dict) else []
            imported = skipped = 0
            for row in net_positions:
                exchange = row.get("exchange")
                ts = row.get("tradingsymbol", "")
                qty = int(row.get("quantity", 0) or 0)
                avg_price = float(row.get("average_price", 0) or 0)

                if (
                    exchange != "NFO"
                    or "NIFTY" not in ts
                    or not (ts.endswith("CE") or ts.endswith("PE"))
                    or qty >= 0
                ):
                    skipped += 1
                    continue

                found = self.instruments_df[self.instruments_df["tradingsymbol"] == ts]
                if found.empty:
                    skipped += 1
                    continue

                r = found.iloc[0]
                lots = max(1, abs(qty) // max(1, self.lot_size))
                self.portfolio.add(
                    Position(
                        source="bootstrapped",
                        opt_type=str(r["instrument_type"]),
                        tradingsymbol=ts,
                        strike=float(r["strike"]),
                        expiry=r["expiry"],
                        entry_price=avg_price,
                        qty=abs(qty),
                        lots=lots,
                        distance=0.0,
                        entry_time=ist_now(),
                        current_price=avg_price,
                    )
                )
                imported += 1
                self._info(
                    f"BOOTSTRAP imported {ts} qty={abs(qty)} lots={lots} avg_price={avg_price}"
                )
            self._info(
                f"Bootstrap complete imported={imported} skipped={skipped}"
            )

            # ── Bootstrap delta imbalance warning ────────────────────────────
            # If the opening portfolio is already severely off-side, warn before
            # the session starts so the user can rebalance manually.
            if imported > 0 and self.portfolio.positions:
                try:
                    now_tmp = ist_now()
                    bootstrap_spot = float(self.spot or 0)
                    if bootstrap_spot > 0:
                        stable_vol = float(self.cfg.get("stable_volatility", 20.0))
                        est_delta = 0.0
                        for p in self.portfolio.positions:
                            dte_tmp = max((p.expiry - now_tmp.date()).days, 1)
                            g = calculate_option_greeks(
                                "sensibull", bootstrap_spot, p.strike, dte_tmp,
                                float(self.cfg.get("interest_rate", 6.5)),
                                max(p.entry_price, 0.01), p.opt_type, stable_vol
                            )
                            est_delta += (-g["delta"]) * p.lots
                        now_dte = max((self.expiry - now_tmp.date()).days, 0)
                        _, _, rm_trigger = self.brain.params_for_dte(now_dte)
                        if abs(est_delta) > rm_trigger:
                            self._warning(
                                f"⚠ BOOTSTRAP DELTA WARNING: estimated opening delta={est_delta:.3f} "
                                f"exceeds rm_trigger={rm_trigger:.2f} (DTE={now_dte}). "
                                f"RM will override wave intents until corrected. "
                                f"Consider rebalancing before session start."
                            )
                        else:
                            self._info(
                                f"Bootstrap delta check: est_delta={est_delta:.3f} "
                                f"within rm_trigger={rm_trigger:.2f} — portfolio balanced at open."
                            )
                except Exception as _de:
                    self.logger.debug(f"Bootstrap delta estimate skipped: {_de}")
        except AuthError:
            self._handle_auth_error("bootstrap")
        except Exception as e:
            self._exception(f"Bootstrap positions failed: {e}")

    # ── weekly workflow state ───────────────────────────────────────────────────

    def _current_cycle_id(self) -> str:
        return str(self.expiry)

    def _state_payload(self) -> Dict[str, Any]:
        return {
            "cycle_id": self._current_cycle_id(),
            "workflow_mode": self.workflow_mode,
            "wave": {
                "anchor": self.wave.anchor,
                "up_index": self.wave.up_index,
                "down_index": self.wave.down_index,
            },
            "survivor": {
                "anchor": self.survivor.anchor,
                "up_index": self.survivor.up_index,
                "down_index": self.survivor.down_index,
            },
            "last_entry_time_by_side": self.last_entry_time_by_side,
            "last_flat_decay_time_by_side": self.last_flat_decay_time_by_side,
            "tracked_symbols": sorted(p.tradingsymbol for p in self.portfolio.positions),
            "saved_at": ist_now().isoformat(),
        }

    def _save_weekly_state(self) -> None:
        if self.workflow_mode != "weekly":
            return
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps(self._state_payload(), default=str, indent=2), encoding="utf-8")
        except Exception as e:
            self._warning(f"Weekly state save failed: {e}")

    def _clear_weekly_state(self) -> None:
        self.wave.anchor = None
        self.wave.up_index = 0
        self.wave.down_index = 0
        self.survivor.anchor = None
        self.survivor.up_index = 0
        self.survivor.down_index = 0
        self.last_entry_time_by_side = {"CE": None, "PE": None}
        self.last_flat_decay_time_by_side = {"CE": 0.0, "PE": 0.0}
        self.last_trigger_time = None

    def _load_or_initialize_weekly_state(self) -> None:
        if self.workflow_mode != "weekly":
            return
        had_existing_state = self.state_path.exists()
        self._clear_weekly_state()
        if had_existing_state:
            self._info("Fresh-start mode: ignored saved weekly state and reset anchor/strategy state for this launch")
        else:
            self._info("Fresh-start mode: initialized empty weekly state for this launch")

    def _signal_key(self, intent: TradeIntent, final_side: str, tradingsymbol: str) -> str:
        return f"{intent.source}|{intent.reason}|{final_side}|{tradingsymbol}"

    def _cleanup_rejection_cooldowns(self, now_ts: float) -> None:
        expired = [k for k, v in self.rejected_signal_cooldowns.items() if v <= now_ts]
        for k in expired:
            self.rejected_signal_cooldowns.pop(k, None)

    def _extract_margin_numbers(self, message: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Parse required and available margin from Kite error messages.
        Handles both formats:
          - Old colon style:  "required: 1234.56 ... available: 789.00"
          - Kite live style:  "Required margin is 1234.56 but available margin is 789.00"
        """
        required = None
        available = None
        try:
            m = re.search(
                r"required(?:\s+margin)?\s*(?:is|:)\s*([0-9][0-9,]*\.?[0-9]*)",
                message, flags=re.IGNORECASE
            )
            if m:
                required = float(m.group(1).replace(',', ''))
        except Exception:
            required = None
        try:
            m = re.search(
                r"available(?:\s+margin)?\s*(?:is|:)\s*([0-9][0-9,]*\.?[0-9]*)",
                message, flags=re.IGNORECASE
            )
            if m:
                available = float(m.group(1).replace(',', ''))
        except Exception:
            available = None
        return required, available

    def _classify_order_error(self, message: str) -> str:
        msg = str(message or "").lower()
        if any(x in msg for x in ["insufficient funds", "insufficient balance", "margin", "required:", "available:", "required margin"]):
            return "margin"
        if any(x in msg for x in ["maximum allowed order requests exceeded", "too many requests", "rate limit", "429"]):
            return "rate_limit"
        if any(x in msg for x in ["network", "timeout", "tempor", "connection reset", "gateway"]):
            return "transient"
        return "other"

    def _current_margin_available(self) -> float:
        try:
            payload = self.broker.margins("equity")
        except AuthError:
            self._handle_auth_error("margins")
            return 0.0
        except Exception as e:
            self._warning(f"Margin check failed: {e}")
            return 0.0

        candidates: List[float] = []
        def walk(obj: Any) -> None:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    lk = str(k).lower()
                    if lk in {"available", "net", "live_balance", "cash", "opening_balance", "payin", "collateral", "adhoc_margin"}:
                        if isinstance(v, (int, float)):
                            candidates.append(float(v))
                    walk(v)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item)

        walk(payload)
        return max(candidates) if candidates else 0.0

    def _margin_watch_allows_entries(self, now_ts: float) -> bool:
        if not self.margin_watch_mode:
            return True
        check_every = float(self.cfg.get("margin_watch_check_seconds", 15.0))
        if (now_ts - self.last_margin_check_ts) < check_every:
            return self.cached_margin_available >= self.margin_required_floor > 0

        self.last_margin_check_ts = now_ts
        self.cached_margin_available = self._current_margin_available()
        if self.margin_required_floor > 0 and self.cached_margin_available >= self.margin_required_floor:
            self.margin_watch_mode = False
            self.consecutive_margin_rejections = 0
            self._info(
                f"Margin watch cleared: available={self.cached_margin_available:.2f} required_floor={self.margin_required_floor:.2f}"
            )
            return True

        self._info(
            f"Margin watch active: available={self.cached_margin_available:.2f} required_floor={self.margin_required_floor:.2f}; waiting for next eligible signal"
        )
        return False

    def _handle_order_rejection(self, intent: TradeIntent, final_side: str, tradingsymbol: str, reason: str, error_text: str) -> None:
        category = self._classify_order_error(error_text)
        now_ts = time.time()
        signal_key = self._signal_key(intent, final_side, tradingsymbol)

        rejection_cooldown = float(self.cfg.get("rejection_cooldown_seconds", 180.0))
        if category == "margin":
            rejection_cooldown = float(self.cfg.get("margin_rejection_cooldown_seconds", 600.0))
        elif category == "rate_limit":
            rejection_cooldown = float(self.cfg.get("rate_limit_cooldown_seconds", 300.0))
        elif category == "transient":
            rejection_cooldown = float(self.cfg.get("transient_rejection_cooldown_seconds", 120.0))

        self.rejected_signal_cooldowns[signal_key] = now_ts + rejection_cooldown
        self.next_order_allowed_ts = max(self.next_order_allowed_ts, now_ts + min(30.0, rejection_cooldown))

        if category == "margin":
            self.consecutive_margin_rejections += 1
            required, available = self._extract_margin_numbers(error_text)
            if required is not None:
                self.margin_required_floor = max(self.margin_required_floor, required)
            if available is not None:
                self.cached_margin_available = available
            if self.consecutive_margin_rejections >= 3:
                self.margin_watch_mode = True
            self._warning(
                f"Order rejected on margin. signal_blocked={rejection_cooldown:.0f}s consecutive_margin_rejections={self.consecutive_margin_rejections} reason={reason} symbol={tradingsymbol}"
            )
            return

        self.consecutive_margin_rejections = 0
        if category == "rate_limit":
            self._warning(
                f"Order rate-limited. Backing off for {rejection_cooldown:.0f}s and skipping this signal. symbol={tradingsymbol}"
            )
            return

        self._warning(
            f"Order rejected category={category}. Skipping this signal for {rejection_cooldown:.0f}s and continuing engine flow. symbol={tradingsymbol}"
        )

    def _seed_strategy_state_if_needed(self, spot: float) -> None:
        self.wave.ensure_anchor(float(spot))
        self.survivor.ensure_seed(float(spot))

    def _weekly_allows_new_entries(self, now: datetime, portfolio_delta: float) -> bool:
        """
        Weekly entry gate based on DAYS-TO-EXPIRY, not weekday names.

        We first resolve the actual expiry that fresh orders would use from the
        live option chain. That means if the currently selected expiry is at
        1 DTE or 0 DTE and fresh entries are configured to roll forward, this
        gate evaluates the rolled expiry instead of the calendar weekday.

        Defaults are intentionally broad so the engine can trade the next weekly
        contract regardless of whether the exchange's weekly expiry is Tuesday,
        Thursday, or changes again in the future.
        """
        if self.workflow_mode != "weekly":
            return True

        entry_expiry = self._entry_expiry_for_new_orders(now)
        entry_dte = max((entry_expiry - now.date()).days, 0)

        min_dte = int(self.cfg.get("weekly_entry_min_dte", 0))
        max_dte = int(self.cfg.get("weekly_entry_max_dte", 30))
        if entry_dte < min_dte or entry_dte > max_dte:
            return False

        rebalance_only_dte = int(self.cfg.get("weekly_rebalance_only_dte", -1))
        if entry_dte <= rebalance_only_dte:
            return abs(portfolio_delta) >= float(self.cfg.get("weekly_expiry_rebalance_delta", 0.20))

        return True

    def _sync_positions_from_broker(self, force: bool = False) -> None:
        now_ts = time.time()
        every = float(self.cfg.get("positions_sync_seconds", 30.0))
        if not force and (now_ts - self.last_positions_sync_ts) < every:
            return
        try:
            payload = self.broker.positions()
        except AuthError:
            self._handle_auth_error("positions_sync")
            return
        except Exception as e:
            self._warning(f"Positions sync failed: {e}")
            return
        net = payload.get("net", []) if isinstance(payload, dict) else []
        live_shorts = {}
        for row in net:
            ts = str(row.get("tradingsymbol", ""))
            qty = int(row.get("quantity", 0) or 0)
            if row.get("exchange") != "NFO" or "NIFTY" not in ts or qty >= 0 or not (ts.endswith("CE") or ts.endswith("PE")):
                continue
            live_shorts[ts] = row

        tracked = {p.tradingsymbol: p for p in self.portfolio.positions}

        for ts, pos in list(tracked.items()):
            row = live_shorts.get(ts)
            if row is None:
                self.portfolio.remove(pos)
                exit_price = float(pos.current_price) if pos.current_price > 0 else float(pos.entry_price)
                manual_pnl = (float(pos.entry_price) - exit_price) * int(pos.qty)
                self.portfolio.realized_by_strategy["manual"] = (
                    self.portfolio.realized_by_strategy.get("manual", 0.0) + manual_pnl
                )
                self._info(
                    f"POSITION REMOVED {ts} reason=manual_or_external_close "
                    f"entry={pos.entry_price:.2f} exit_est={exit_price:.2f} "
                    f"realized_pnl={manual_pnl:.2f} "
                    f"manual_total={self.portfolio.realized_by_strategy.get('manual', 0.0):.2f}"
                )
                continue
            qty = abs(int(row.get("quantity", 0) or 0))
            avg_price = float(row.get("average_price", pos.entry_price) or pos.entry_price)
            pos.qty = qty
            pos.lots = max(1, qty // max(1, self.lot_size))
            if pos.entry_price <= 0:
                pos.entry_price = avg_price

        for ts, row in live_shorts.items():
            if ts in tracked:
                continue
            found = self.instruments_df[self.instruments_df["tradingsymbol"] == ts]
            if found.empty:
                continue
            r = found.iloc[0]
            qty = abs(int(row.get("quantity", 0) or 0))
            avg_price = float(row.get("average_price", 0) or 0)
            lots = max(1, qty // max(1, self.lot_size))
            self.portfolio.add(Position(
                source="external",
                opt_type=str(r["instrument_type"]),
                tradingsymbol=ts,
                strike=float(r["strike"]),
                expiry=r["expiry"],
                entry_price=avg_price,
                qty=qty,
                lots=lots,
                distance=0.0,
                entry_time=ist_now(),
                current_price=avg_price,
            ))
            self._info(f"POSITION IMPORTED {ts} qty={qty} lots={lots} reason=external_or_manual_add")

        if self.workflow_mode == "weekly":
            if not self.portfolio.positions and bool(self.cfg.get("reset_state_when_flat", True)):
                # Only clear weekly state if the anchor has not been set yet today.
                if self.wave.anchor is None:
                    self._clear_weekly_state()
            self._save_weekly_state()
        self.last_positions_sync_ts = now_ts

    # ── auth handling ─────────────────────────────────────────────────────────

    def _handle_auth_error(self, context: str) -> None:
        """
        Log the auth error and pause the engine.
        Does NOT call sys.exit — the run() loop will keep retrying the session
        check every 60 s so you can refresh the token without restarting.
        """
        self.logger.critical(
            f"AUTH ERROR in [{context}] — Kite session invalid. "
            f"Please refresh your access_token in .env and restart the engine. "
            f"Engine is PAUSED (no new orders will be placed)."
        )
        self.last_error = f"AUTH ERROR in [{context}]"
        self._push_event(f"AUTH ERROR in [{context}] — engine paused")
        self._auth_paused = True

    def _check_session(self) -> None:
        self.last_session_check_dt = ist_now()
        valid = self.broker.is_session_valid()
        if valid:
            if self._auth_paused:
                self._info("Session re-validated — engine RESUMED.")
            self._auth_paused = False
        else:
            self._handle_auth_error("session_check")

    def _env_token_hash(self) -> str:
        """Return a short fingerprint of the current .env access token."""
        import hashlib
        try:
            from dotenv import dotenv_values
            vals = dotenv_values("/opt/niftybot/.env")
            token = vals.get("KITE_ACCESS_TOKEN", "")
            return hashlib.md5(token.encode()).hexdigest()[:12]
        except Exception:
            return ""

    def _check_for_fresh_token(self) -> None:
        """
        Poll .env every 30 seconds for a new access token written by tokenbot.
        When a new token is detected:
          1. Re-validate the session with the new token
          2. Resume the engine (clear auth_paused)

        NOTE: Anchor is NOT set here. Anchor drops on the very next tick after
        this method detects a new token — no time gate, no waiting for 9:45.
        """
        current_hash = self._env_token_hash()
        if current_hash == self.last_env_token_hash:
            return  # token unchanged — nothing to do

        self._info("New token detected in .env — attempting session refresh...")
        try:
            from dotenv import dotenv_values
            vals = dotenv_values("/opt/niftybot/.env")
            new_token = vals.get("KITE_ACCESS_TOKEN", "").strip()
            if not new_token:
                return

            # Update broker session with new token
            self.broker._kite.set_access_token(new_token)
            if not self.broker.is_session_valid():
                self._error("New token validation failed — still paused")
                return

            self.last_env_token_hash = current_hash
            self._auth_paused = False
            self._info(
                "Session refreshed from new token — "
                "ANCHOR WILL DROP ON THE VERY NEXT TICK (no time gate)"
            )

        except Exception as e:
            self._exception(f"Fresh token check failed: {e}")

    def _midnight_reset_if_needed(self, now: datetime) -> None:
        """
        At midnight IST, clear anchor and wave/survivor state so a fresh anchor
        is set on the first tick of the new trading day. No token or login gate —
        the anchor drops automatically when the next tick arrives.
        Greeks and position tracking continue uninterrupted.
        """
        today = now.date()
        if self.last_midnight_reset_date == today:
            return  # already reset today

        if now.hour == 0 and now.minute < 5:
            self.last_midnight_reset_date = today
            self.wave.anchor = None
            self.wave.up_index = 0
            self.wave.down_index = 0
            self.survivor.anchor = None
            self.survivor.up_index = 0
            self.survivor.down_index = 0
            self.last_entry_time_by_side = {"CE": None, "PE": None}
            self.last_flat_decay_time_by_side = {"CE": 0.0, "PE": 0.0}
            self.last_trigger_time = None
            self._info(
                "MIDNIGHT RESET — anchor cleared, wave/survivor reset. "
                "Fresh anchor will drop on first tick of the new day."
            )
            self._save_weekly_state()

    # ── websocket callbacks ───────────────────────────────────────────────────

    def on_ticks(self, ticks: list) -> None:
        for tick in ticks:
            self.tick_queue.put(tick)

    def on_order_update(self, data: dict) -> None:
        self._record_order_update(data)
        self._info(f"ORDER_UPDATE {data}")
        # Detect COMPLETE-before-OPEN sequencing inversion.
        # Zerodha occasionally delivers COMPLETE before OPEN for very fast fills.
        # This is benign — log it at DEBUG so it's visible in the audit trail.
        status = str(data.get("status", ""))
        order_id = str(data.get("order_id", ""))
        if status == "COMPLETE":
            if not getattr(self, f"_seen_open_{order_id}", False):
                self.logger.debug(
                    f"ORDER_SEQ_NOTE: {order_id} received COMPLETE before OPEN — "
                    f"fast fill / exchange sequencing inversion (benign)."
                )
        elif status == "OPEN":
            setattr(self, f"_seen_open_{order_id}", True)

    def on_connect(self, response) -> None:
        self._info(f"WebSocket connected: {response}")

    def on_close(self, code, reason) -> None:
        self._warning(f"WebSocket closed code={code} reason={reason}")

    def on_error(self, code, reason) -> None:
        self._error(f"WebSocket error code={code} reason={reason}")

    # ── internal tick / snapshot ──────────────────────────────────────────────

    def _handle_tick(self, tick: dict) -> None:
        token = tick.get("instrument_token")
        lp = float(tick.get("last_price") or tick.get("ltp") or 0)
        now = ist_now()
        self.last_tick_ts = now
        self.last_tick_token = int(token) if token is not None else None
        self.last_tick_price = lp
        if token == int(self.cfg["nifty_index_instrument_token"]):
            self.spot = lp
            self.spot_history.append((time.time(), lp))
        elif token == int(self.cfg["india_vix_instrument_token"]):
            self.vix = lp
        elif self.future_token is not None and token == int(self.future_token):
            self.future_price = lp

    def _persist_snapshot(self, payload: dict) -> None:
        fn = self.audit_dir / f"{ist_now().strftime('%Y%m%d')}.jsonl"
        with open(fn, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")

    # ── option selection ──────────────────────────────────────────────────────


    def _record_entry(self, side: str) -> None:
        now_ts = time.time()
        self.last_entry_time_by_side[side] = now_ts
        self.last_trigger_time = now_ts
        # Emergency mode: reduce cooldown when delta is extreme
        _abs_delta = abs(self.cached_portfolio_delta) if hasattr(self, 'cached_portfolio_delta') else 0
        _emergency_threshold = float(self.cfg.get("delta_emergency_threshold", 2.0))
        _emergency_stop_threshold = float(self.cfg.get("delta_emergency_stop_threshold", 1.0))
        _emergency_cooldown = 30.0 if _abs_delta >= _emergency_threshold else self.global_order_cooldown_seconds
        self.next_order_allowed_ts = max(self.next_order_allowed_ts, now_ts + _emergency_cooldown)
        if _abs_delta >= _emergency_threshold:
            msg = (
                f"⚠ EMERGENCY DELTA MODE ACTIVE: |Δ|={_abs_delta:.3f} ≥ {_emergency_threshold:.1f} — "
                f"cooldown reduced to {_emergency_cooldown:.0f}s — "
                f"emergency stops when |Δ| < {_emergency_stop_threshold:.1f}"
            )
            self._warning(msg)
            self._set_decision(
                f"⚠ EMERGENCY DELTA MODE ACTIVE",
                f"|Δ|={_abs_delta:.3f} ≥ threshold={_emergency_threshold:.1f}",
                f"Emergency cooldown={_emergency_cooldown:.0f}s",
                f"Stops when |Δ| < {_emergency_stop_threshold:.1f}",
            )
        self._save_weekly_state()

    def _commit_strategy_intent(self, intent: TradeIntent, now_ts: Optional[float] = None) -> None:
        if intent.source == "wave":
            self.wave.commit_intent(intent, now_ts=now_ts)
        elif intent.source == "survivor":
            self.survivor.commit_intent(intent)

    def _open_positions_count_by_side(self, side: str) -> int:
        return sum(1 for p in self.portfolio.positions if p.opt_type == side)

    def _open_lots_total(self) -> int:
        return sum(int(p.lots) for p in self.portfolio.positions)

    def _estimate_todays_realized_vol(self) -> float:
        lookback_minutes = int(self.cfg.get("todays_vol_lookback_minutes", 30))
        fallback = float(self.cfg.get("todays_volatility_fallback", 20.0))
        if len(self.spot_history) < 5:
            return fallback

        cutoff = time.time() - (lookback_minutes * 60)
        recent = [(ts, px) for ts, px in self.spot_history if ts >= cutoff and px > 0]
        if len(recent) < 5:
            return fallback

        df = pd.DataFrame(recent, columns=["ts", "price"])
        df["dt"] = pd.to_datetime(df["ts"], unit="s")
        minute = df.set_index("dt")["price"].resample("1min").last().dropna()
        if len(minute) < 3:
            return fallback

        log_returns = minute.apply(math.log).diff().dropna()
        if log_returns.empty:
            return fallback

        annualizer = math.sqrt(252 * 375)
        realized_vol = float(log_returns.std(ddof=0) * annualizer * 100.0)
        if not math.isfinite(realized_vol) or realized_vol <= 0:
            return fallback
        return realized_vol

    def _active_volatility(self, vix_value: float) -> float:
        mode = str(self.cfg.get("greeks_mode", "stable"))
        if mode == "todays":
            return self._estimate_todays_realized_vol()
        return select_volatility(
            mode,
            float(self.cfg.get("stable_volatility", 20.0)),
            vix_value,
            float(self.cfg.get("todays_volatility_fallback", 20.0)),
        )

    def _refresh_open_position_prices(self, force: bool = False) -> None:
        if not self.portfolio.positions:
            return
        now_ts = time.time()
        refresh_every = float(self.cfg.get("position_price_refresh_seconds", 5.0))
        if not force and (now_ts - self.last_position_price_refresh) < refresh_every:
            return

        symbols = [f"NFO:{p.tradingsymbol}" for p in self.portfolio.positions]
        try:
            quotes = self.broker.ltp(symbols)
        except AuthError:
            self._handle_auth_error("position_ltp_refresh")
            return
        except Exception as e:
            self._exception(f"Position LTP refresh failed: {e}")
            return

        for p in self.portfolio.positions:
            key = f"NFO:{p.tradingsymbol}"
            quote = quotes.get(key)
            if quote and quote.get("last_price") is not None:
                p.current_price = float(quote["last_price"])
        self.last_position_price_refresh = now_ts

    def _record_close(self, pos: Position, exit_price: float, reason: str) -> None:
        pnl = (float(pos.entry_price) - float(exit_price)) * int(pos.qty)
        self.portfolio.realized_by_strategy[pos.source] = (
            self.portfolio.realized_by_strategy.get(pos.source, 0.0) + pnl
        )
        self.portfolio.remove(pos)
        self._info(
            f"CLOSED source={pos.source} side={pos.opt_type} symbol={pos.tradingsymbol} "
            f"exit_price={exit_price:.2f} pnl={pnl:.2f} reason={reason}"
        )
        self._save_weekly_state()

    def _close_position(self, pos: Position, reason: str) -> bool:
        exit_price = float(pos.current_price) if pos.current_price > 0 else float(pos.entry_price)
        order_id, order_error = self._place(
            tradingsymbol=pos.tradingsymbol,
            qty=pos.qty,
            source=pos.source,
            side=pos.opt_type,
            reason=reason,
            transaction_type="BUY",
        )
        if order_id is None:
            return False
        self._record_close(pos, exit_price, reason)
        return True

    def _enforce_position_stop_losses(self) -> None:
        """Manual-exit mode: stop-loss driven auto-closes are disabled."""
        return

    def _enforce_expiry_day_itm_closes(self, now: datetime, spot: float) -> None:
        """Manual-exit mode: expiry-day forced ITM closes are disabled."""
        return

    def _enforce_preexpiry_closes(self, now: datetime) -> None:
        """
        Close positions that expire on the next trading day near the end of the
        current session. Example: if weekly expiry is Tuesday, all Tuesday-expiry
        positions are exited late on Monday.
        """
        cutoff = self.cfg.get("preexpiry_force_close_time", "15:20")
        if now.time() < parse_hhmm(cutoff):
            return
        targets = [
            p for p in list(self.portfolio.positions)
            if getattr(p, "qty", 0) > 0 and max((p.expiry - now.date()).days, 0) == 1
        ]
        if not targets:
            return
        for pos in targets:
            ok = self._close_position(pos, reason="preexpiry_dte1_forced_exit")
            if ok:
                self._info(
                    f"PRE-EXPIRY EXIT {pos.tradingsymbol} expiry={pos.expiry} qty={pos.qty} reason=preexpiry_dte1_forced_exit"
                )

    def _update_risk_cache(self, portfolio_delta: float, portfolio_gamma: float, portfolio_theta: float, portfolio_vega: float, mtm_total: float, mtm_by_strategy: Dict[str, float], active_volatility: float, calc_time: Optional[datetime] = None) -> None:
        self.cached_portfolio_delta = float(portfolio_delta)
        self.cached_portfolio_gamma = float(portfolio_gamma)
        self.cached_portfolio_theta = float(portfolio_theta)
        self.cached_portfolio_vega = float(portfolio_vega)
        self.last_risk_calc_at = calc_time or ist_now()
        self.cached_mtm_total = float(mtm_total)
        self.cached_mtm_by_strategy = dict(mtm_by_strategy)
        self.cached_active_volatility = float(active_volatility)

    def _check_daily_loss_guard(self, now: datetime, mtm_total: float) -> None:
        """Manual-exit mode: daily loss guard is disabled so greek-based adjustments can continue."""
        self.daily_loss_stop_triggered = False
        self.daily_loss_stop_date = now.date()
        return

    def _should_add_flat_decay(self, side: str, now_ts: float) -> bool:
        if not self.cfg.get("enable_flat_decay", True):
            return False

        last_entry = self.last_entry_time_by_side.get(side)
        if last_entry is None:
            return False

        wait_secs = int(self.cfg.get("flat_decay_wait_minutes", 15)) * 60
        if now_ts - last_entry < wait_secs:
            return False

        if self.last_trigger_time is not None and self.last_trigger_time > last_entry:
            return False

        if now_ts - self.last_flat_decay_time_by_side.get(side, 0.0) < wait_secs:
            return False

        if not self.spot_history:
            return False

        cutoff = now_ts - wait_secs
        recent = [px for ts, px in self.spot_history if ts >= cutoff]
        if len(recent) < 2:
            return False

        band = float(self.cfg.get("flat_decay_range_points", 25.0))
        if max(recent) - min(recent) > (band * 2.0):
            return False

        return True

    def _choose_option(
        self, spot: float, atr: Optional[float], vix_value: float, opt_type: str,
        entry_expiry: Optional[date] = None, wave_step_index: int = 0
    ):
        """
        Keep ATR/VIX distance as the base selector, then refine strike choice by:
        1) minimum premium floor
        2) preferred absolute delta band (nearest possible strike around ATR strike)
        3) fallback wider delta band
        4) fallback premium-only nearest strike

        wave_step_index: 0-based index of the wave ladder step currently firing.
        Each step adds one extra Nifty strike interval (strike_step) of OTM distance
        to guarantee each wave step selects a progressively further OTM strike.

        Uses a single batch LTP request for all candidate symbols.
        """
        now = ist_now()
        trade_expiry = entry_expiry or self.expiry
        dte = max((trade_expiry - now.date()).days, 1)
        distance, atr_move, vix_move = get_distance_points(
            spot, atr, vix_value, dte,
            self.cfg["atr_multiplier"], self.cfg["vix_multiplier"],
            min_points=50, step=self.strike_step,
        )

        # Add progressive OTM offset per wave step so each step
        # lands on a different, further-OTM strike.
        step_offset = wave_step_index * self.strike_step
        distance = distance + step_offset

        base_strike = round_to_step(
            spot + (distance if opt_type == "CE" else -distance),
            self.strike_step,
        )

        vol_for_delta = self._active_volatility(vix_value)

        min_premium = float(self.cfg.get("min_premium_to_sell", 30.0))
        preferred_min = float(self.cfg.get("preferred_abs_delta_min", 0.12))
        preferred_max = float(self.cfg.get("preferred_abs_delta_max", 0.15))
        fallback_min = float(self.cfg.get("fallback_abs_delta_min", 0.10))
        fallback_max = float(self.cfg.get("fallback_abs_delta_max", 0.18))
        max_steps = int(self.cfg.get("max_strike_adjust_steps", 6))

        offsets = [0]
        for step_idx in range(1, max_steps + 1):
            if opt_type == "CE":
                offsets.extend([-step_idx, step_idx])
            else:
                offsets.extend([step_idx, -step_idx])

        candidates = []
        symbols = []
        for off in offsets:
            strike = base_strike + off * self.strike_step
            row = pick_option_by_strike(self.chain, trade_expiry, strike, opt_type)
            if row is None:
                continue
            symbol = f"NFO:{row['tradingsymbol']}"
            candidates.append({
                "row": row,
                "symbol": symbol,
                "offset_rank": abs(off),
            })
            symbols.append(symbol)

        if not candidates:
            return None, None, None, None, None

        try:
            quotes = self.broker.ltp(symbols)
        except AuthError:
            self._handle_auth_error("ltp_lookup")
            return None, None, None, None, None
        except Exception as e:
            self._exception(f"Option selection LTP lookup failed: {e}")
            return None, None, None, None, None

        enriched = []
        for c in candidates:
            quote = quotes.get(c["symbol"], {})
            premium = float(quote.get("last_price") or 0.0)
            if premium <= 0:
                continue
            row = c["row"]
            greek_style = str(self.cfg.get("greek_style", "sensibull")).lower()
            greek_underlying = self.future_price if (greek_style == "sensibull" and self.future_price) else spot
            delta_val = calculate_option_greeks(greek_style, greek_underlying, float(row["strike"]), dte, float(self.cfg["interest_rate"]), premium, opt_type, vol_for_delta)["delta"]
            enriched.append({
                "row": row,
                "premium": premium,
                "abs_delta": abs(float(delta_val)),
                "distance": abs(float(row["strike"]) - spot),
                "offset_rank": c["offset_rank"],
            })

        if not enriched:
            return None, None, None, None, None

        preferred = [
            c for c in enriched
            if c["premium"] >= min_premium and preferred_min <= c["abs_delta"] <= preferred_max
        ]
        if preferred:
            best = sorted(preferred, key=lambda c: (c["offset_rank"], c["distance"]))[0]
            return best["row"], best["premium"], float(best["distance"]), float(atr_move), float(vix_move)

        fallback = [
            c for c in enriched
            if c["premium"] >= min_premium and fallback_min <= c["abs_delta"] <= fallback_max
        ]
        if fallback:
            best = sorted(fallback, key=lambda c: (c["offset_rank"], c["distance"]))[0]
            return best["row"], best["premium"], float(best["distance"]), float(atr_move), float(vix_move)

        premium_only = [c for c in enriched if c["premium"] >= min_premium]
        if premium_only:
            best = sorted(premium_only, key=lambda c: (c["offset_rank"], c["distance"]))[0]
            return best["row"], best["premium"], float(best["distance"]), float(atr_move), float(vix_move)

        return None, None, None, None, None

    def _next_lots(self, capital: float) -> int:
        # HARD LOCK: always trade exactly 1 lot per order
        return 1

    # ── order placement ───────────────────────────────────────────────────────

    def _place(
        self,
        tradingsymbol: str,
        qty: int,
        source: str,
        side: str,
        reason: str,
        transaction_type: str = "SELL",
    ) -> Tuple[Optional[str], Optional[str]]:
        tx = transaction_type.upper()
        market_protection = self.cfg.get("market_protection", -1)

        try:
            order_id = self.broker.place_market_order(
                tradingsymbol=tradingsymbol,
                exchange="NFO",
                transaction_type=tx,
                quantity=qty,
                product=self.cfg["product"],
                tag=f"{source}_{side}",
                variety=self.cfg["variety"],
                market_protection=market_protection,
            )
            self.last_order_event = f"LIVE {tx} {tradingsymbol} qty={qty} mp={market_protection} order_id={order_id}"
            self._record_order_event(
                event_type="broker_order",
                transaction_type=tx,
                tradingsymbol=tradingsymbol,
                qty=qty,
                source=source,
                side=side,
                reason=reason,
                order_id=order_id,
                status="submitted",
                market_protection=market_protection,
            )
            self._info(f"[LIVE] order_id={order_id} {tx} {tradingsymbol} qty={qty}")
            return order_id, None
        except AuthError:
            self._record_order_event(
                event_type="broker_order_error",
                transaction_type=tx,
                tradingsymbol=tradingsymbol,
                qty=qty,
                source=source,
                side=side,
                reason=reason,
                status="auth_error",
                notes="Kite session invalid during order placement",
                market_protection=market_protection,
            )
            self._handle_auth_error(f"place_order:{tradingsymbol}")
            return None, "Kite session invalid during order placement"
        except Exception as e:
            error_text = str(e)
            self._record_order_event(
                event_type="broker_order_error",
                transaction_type=tx,
                tradingsymbol=tradingsymbol,
                qty=qty,
                source=source,
                side=side,
                reason=reason,
                status="error",
                notes=error_text,
                market_protection=market_protection,
            )
            self._exception(f"Order placement failed {tradingsymbol}: {e}")
            return None, error_text

    # ── main evaluation loop ──────────────────────────────────────────────────
    # ── main evaluation loop ──────────────────────────────────────────────────

    def evaluate(self) -> None:
        if self._auth_paused:
            self._set_decision("Auth paused", "Waiting for valid Kite session")
            return
        if self.spot is None or self.vix is None:
            self._set_decision("Waiting for market data", "Spot/VIX not available yet")
            return

        self._refresh_daily_atr_if_needed()
        now = ist_now()
        self._roll_daily_guards_if_needed(now)

        # Midnight reset — clears anchor at start of each new day
        self._midnight_reset_if_needed(now)



        # Poll .env for a fresh token written by tokenbot
        self._check_for_fresh_token()

        self._sync_positions_from_broker()

        spot = float(self.spot)
        vix_value = float(self.vix)
        atr = self.atr.value()

        wave_up_next, wave_down_next = self._wave_next_levels()
        wave_up_hit = wave_up_next is not None and spot >= wave_up_next
        wave_down_hit = wave_down_next is not None and spot <= wave_down_next
        surv_up = self.survivor.anchor is not None and spot >= (self.survivor.anchor + self.survivor.activation_offset + self.survivor.up_index * self.survivor.gap)
        surv_down = self.survivor.anchor is not None and spot <= (self.survivor.anchor - self.survivor.activation_offset - self.survivor.down_index * self.survivor.gap)

        self._refresh_open_position_prices()
        self._enforce_preexpiry_closes(now)

        for ev in self.portfolio.settle_expired(spot, now):
            self.logger.info(f"SETTLED {ev}")

        active_vol = self._active_volatility(vix_value)
        greek_style = str(self.cfg.get("greek_style", "sensibull")).lower()
        greek_underlying = self.future_price if (greek_style == "sensibull" and self.future_price) else spot
        portfolio_delta, portfolio_gamma, portfolio_theta, portfolio_vega, mtm_total, mtm_by_strategy = self.portfolio.mark(
            spot, vix_value, now,
            greeks_engine=self.cfg["greeks_engine"],
            greeks_mode=self.cfg["greeks_mode"],
            stable_volatility=active_vol,
            interest_rate_pct=float(self.cfg["interest_rate"]),
            greek_style=greek_style,
            greek_underlying_price=greek_underlying,
        )
        self._update_risk_cache(portfolio_delta, portfolio_gamma, portfolio_theta, portfolio_vega, mtm_total, mtm_by_strategy, active_vol, calc_time=now)
        self._check_daily_loss_guard(now, mtm_total)

        payload = {
            "time": now.isoformat(),
            "risk_calc_time": self.last_risk_calc_at.isoformat() if self.last_risk_calc_at else None,
            "spot": spot,
            "vix": vix_value,
            "atr": atr,
            "portfolio_delta": self.cached_portfolio_delta,
            "portfolio_gamma": self.cached_portfolio_gamma,
            "portfolio_theta": self.cached_portfolio_theta,
            "portfolio_vega": self.cached_portfolio_vega,
            "mtm_total": self.cached_mtm_total,
            "mtm_by_strategy": mtm_by_strategy,
            "open_positions": len(self.portfolio.positions),
            "active_volatility": active_vol,
            "daily_loss_stop": self.daily_loss_stop_triggered,
        }

        if time.time() - self.last_snapshot_ts >= int(self.cfg.get("snapshot_flush_seconds", 30)):
            # Detect stale data after market close (last tick > 60s ago)
            _last_tick_age = (now - self.last_tick_ts).total_seconds() if self.last_tick_ts else 0
            _is_stale = _last_tick_age > 60
            _stale_flush_every = int(self.cfg.get("stale_snapshot_flush_seconds", 300))
            if _is_stale and (time.time() - self.last_snapshot_ts) < _stale_flush_every:
                pass  # skip — reduce frequency on stale data
            else:
                _stale_tag = " [STALE]" if _is_stale else ""
                self.last_snapshot_at = now
                self._info(
                    f"SNAPSHOT{_stale_tag} calc_at={self._format_dt(self.last_risk_calc_at)} spot={spot:.2f} vix={vix_value:.2f} "
                    f"atr={atr if atr is not None else 'NA':.4g} "
                    f"delta={self.cached_portfolio_delta:.3f} gamma={self.cached_portfolio_gamma:.5f} theta={self.cached_portfolio_theta:.2f} vega={self.cached_portfolio_vega:.2f} mtm={self.cached_mtm_total:.2f} "
                    f"positions={len(self.portfolio.positions)} vol={active_vol:.2f}"
                )
                self._persist_snapshot(payload)
                self.last_snapshot_ts = time.time()
            # Status file always written regardless of stale state
            self._write_status_file(
                spot=spot,
                delta=self.cached_portfolio_delta,
                mtm=self.cached_mtm_total,
                theta=self.cached_portfolio_theta,
                positions=len(self.portfolio.positions),
            )

        # ── Power Hour Aggressive Rebalance (15:15–15:25) ────────────────────
        # Runs independently of the main trading gate. If we are in the Power
        # Hour window and delta exceeds the tighter power_hour_delta_threshold,
        # this fires a sell-based neutralization and returns immediately.
        # This allows the engine to correct delta even while wave/survivor are
        # blocked — it is the final credit-based delta sweep before hard close.
        _ph_start = now.replace(hour=15, minute=15, second=0, microsecond=0)
        _ph_end   = now.replace(hour=15, minute=25, second=0, microsecond=0)
        if _ph_start <= now <= _ph_end:
            self._aggressive_rebalance_if_needed(now, spot, atr or 300, vix_value)
            # After Power Hour fires, re-check cooldown and return if we just placed an order
            if time.time() < self.next_order_allowed_ts:
                return

        # Trading windows:
        # 09:15 – 15:14 → Normal trading (wave + survivor + RM). Emergency Δ threshold = 2.0
        # 15:15 – 15:25 → Power Hour: aggressive sell-based rebalance (handled above).
        #                  Wave + survivor blocked. RM threshold tightens to 1.0.
        # After 15:25   → Hard close. No new orders. Emergency RM still watches.
        market_open        = now.replace(hour=9,  minute=15, second=0, microsecond=0)
        delta_mgmt_start   = now.replace(hour=15, minute=15, second=0, microsecond=0)
        hard_close         = now.replace(hour=15, minute=25, second=0, microsecond=0)

        before_open   = now < market_open
        normal_window = market_open <= now < delta_mgmt_start
        delta_window  = delta_mgmt_start <= now < hard_close
        after_close   = now >= hard_close

        if before_open or after_close:
            # Outside trading hours — no wave/survivor, but delta emergency RM still runs
            abs_delta_now = abs(portfolio_delta)
            emergency_threshold = float(self.cfg.get("delta_emergency_threshold", 2.0))
            if abs_delta_now >= emergency_threshold:
                # Emergency delta mode: fall through to RM execution below,
                # but block wave/survivor. The intents loop handles the rest.
                self._set_decision(
                    f"⚠ EMERGENCY DELTA MODE: |Δ|={abs_delta_now:.3f} ≥ {emergency_threshold:.1f}",
                    f"Outside normal hours but RM is active — correcting delta",
                    f"Time={now.strftime('%H:%M:%S')}",
                )
            else:
                self._set_decision(
                    f"Gate: market window closed {now.strftime('%H:%M:%S')}",
                    f"Wave↑ hit={wave_up_hit} next={wave_up_next:.2f}" if wave_up_next is not None else "Wave↑ unavailable",
                    f"Wave↓ hit={wave_down_hit} next={wave_down_next:.2f}" if wave_down_next is not None else "Wave↓ unavailable",
                )
                return

        # 15:00-15:25: delta management only — block wave and survivor, but RM runs all day
        # When before_open or after_close AND emergency delta, we also block wave/survivor
        delta_mgmt_only = delta_window or before_open or after_close

        # Anchor: set once on first tick the engine sees a live spot price.
        # No time gate. No token gate. Wherever the market is when the script
        # starts, that becomes the anchor immediately.
        if self.wave.anchor is None:
            self.wave.anchor = spot
            self.wave.up_index = 0
            self.wave.down_index = 0
            self.survivor.anchor = spot
            self.survivor.up_index = 0
            self.survivor.down_index = 0
            self.last_entry_time_by_side = {"CE": None, "PE": None}
            self.last_flat_decay_time_by_side = {"CE": 0.0, "PE": 0.0}
            self.last_trigger_time = None
            self._save_weekly_state()
            self._info(
                f"ANCHOR SET at spot={spot:.2f} — engine live, trading starts now. "
                f"Survivor activates at ±{self.survivor.activation_offset:.0f}pts, "
                f"fires every {self.cfg.get('surv_gap', 20):.0f}pts"
            )

        if not self._expiry_day_allows_new_entries(now):
            self._set_decision("Gate: expiry-day entry cutoff active", f"Entry expiry={self._entry_expiry_for_new_orders(now)}")
            return
        if not self._weekly_allows_new_entries(now, portfolio_delta):
            entry_expiry = self._entry_expiry_for_new_orders(now)
            entry_dte = max((entry_expiry - now.date()).days, 0)
            self._set_decision(
                "Gate: DTE entry rule blocked",
                f"Entry expiry={entry_expiry} DTE={entry_dte}",
                f"Wave↑ hit={wave_up_hit} next={wave_up_next:.2f}" if wave_up_next is not None else "Wave↑ unavailable",
                f"Wave↓ hit={wave_down_hit} next={wave_down_next:.2f}" if wave_down_next is not None else "Wave↓ unavailable",
            )
            return
        # Manual-exit mode: do not halt new greek-based adjustments due to MTM drawdown.

        intents: List[TradeIntent] = []
        survivor_intents: List[TradeIntent] = []

        # Emergency mode indicator — visible in thinking box at all times
        _abs_delta_check = abs(portfolio_delta)
        _emerg_thresh = float(self.cfg.get("delta_emergency_threshold", 2.0))
        _emerg_stop = float(self.cfg.get("delta_emergency_stop_threshold", 1.0))
        if _abs_delta_check >= _emerg_thresh:
            self._set_decision(
                f"⚠ EMERGENCY DELTA MODE ACTIVE",
                f"|Δ|={_abs_delta_check:.3f} ≥ threshold={_emerg_thresh:.1f}",
                f"RM forcing correction — cooldown=30s",
                f"Emergency stops when |Δ| < {_emerg_stop:.1f}",
            )
        elif getattr(self, "_emergency_was_active", False) and _abs_delta_check < _emerg_stop:
            # Emergency has just cleared
            self._emergency_was_active = False
            msg = f"✓ EMERGENCY DELTA MODE CLEARED: |Δ|={_abs_delta_check:.3f} < {_emerg_stop:.1f} — normal RM resumed"
            self._info(msg)
            self._set_decision(msg)
        if _abs_delta_check >= _emerg_thresh:
            self._emergency_was_active = True

        # In delta management window (15:00-15:25) block wave and survivor
        if delta_mgmt_only:
            wave_intent      = None
            survivor_intents = []
        if not delta_mgmt_only and self.cfg.get("enable_survivor", True):
            # Wave is exhausted when all ladder steps have fired
            wave_up_exhausted   = self.wave.up_index   >= len(self.wave.ladder)
            wave_down_exhausted = self.wave.down_index >= len(self.wave.ladder)
            survivor_intents = self.survivor.preview_intents(
                spot,
                wave_up_exhausted=wave_up_exhausted,
                wave_down_exhausted=wave_down_exhausted,
            )
            # Log every survivor evaluation so silent blocks are visible
            if self.survivor.anchor is not None:
                _surv_up_thresh = self.survivor.anchor + self.survivor.activation_offset + self.survivor.up_index * self.survivor.gap
                _surv_dn_thresh = self.survivor.anchor - self.survivor.activation_offset - self.survivor.down_index * self.survivor.gap
                if survivor_intents:
                    for _si in survivor_intents:
                        self.logger.debug(
                            f"SURVIVOR_INTENT: {_si.reason} spot={spot:.2f} "
                            f"threshold_up={_surv_up_thresh:.2f} threshold_dn={_surv_dn_thresh:.2f} "
                            f"wave_up_exhausted={wave_up_exhausted} wave_dn_exhausted={wave_down_exhausted} "
                            f"→ proceeding to RM check"
                        )
                else:
                    self.logger.debug(
                        f"SURVIVOR_INACTIVE: spot={spot:.2f} "
                        f"need_up>={_surv_up_thresh:.2f} need_dn<={_surv_dn_thresh:.2f} "
                        f"wave_up_exhausted={wave_up_exhausted} wave_dn_exhausted={wave_down_exhausted} "
                        f"up_idx={self.survivor.up_index} dn_idx={self.survivor.down_index}"
                    )
        if survivor_intents:
            # Smooth coordination rule: if Survivor has a valid trigger,
            # let Survivor take precedence for this evaluation cycle so
            # Wave and Survivor do not compete on the same tick.
            intents.extend(survivor_intents)
        elif self.cfg.get("enable_wave", True):
            if not delta_mgmt_only:
                wave_intent = self.wave.preview_intent(spot, portfolio_delta, self.brain.tilt_soft_for_dte(self._current_rm_dte(now)))
            else:
                wave_intent = None
            if wave_intent:
                intents.append(wave_intent)
                self.logger.debug(
                    f"WAVE_TRIGGER: {wave_intent.reason} tick_spot={spot:.2f} "
                    f"requested={wave_intent.requested_side} → proceeding to RM check"
                )

        if intents:
            self.last_trigger_time = time.time()

        now_ts = time.time()
        if not intents:
            if self._should_add_flat_decay("CE", now_ts):
                intents.append(TradeIntent("flat_decay", "CE", "flat_decay_ce"))
                self.last_flat_decay_time_by_side["CE"] = now_ts
            elif self._should_add_flat_decay("PE", now_ts):
                intents.append(TradeIntent("flat_decay", "PE", "flat_decay_pe"))
                self.last_flat_decay_time_by_side["PE"] = now_ts

        if not intents:
            self._set_decision(
                "No trigger -> no order",
                f"Wave↑ hit={wave_up_hit} next={wave_up_next:.2f}" if wave_up_next is not None else "Wave↑ unavailable",
                f"Wave↓ hit={wave_down_hit} next={wave_down_next:.2f}" if wave_down_next is not None else "Wave↓ unavailable",
                f"Survivor↑ hit={surv_up} Survivor↓ hit={surv_down}",
                f"Portfolio Δ={portfolio_delta:.3f}",
            )
            return

        now_ts = time.time()
        self._cleanup_rejection_cooldowns(now_ts)
        if now_ts < self.next_order_allowed_ts:
            self._set_decision("Gate: global cooldown active", f"Seconds left={max(0, int(self.next_order_allowed_ts-now_ts))}", f"Portfolio Δ={portfolio_delta:.3f}")
            return
        if not self._margin_watch_allows_entries(now_ts):
            self._set_decision("Gate: margin watch active", f"Available={self.cached_margin_available:.2f}", f"Required floor={self.margin_required_floor:.2f}")
            return

        capital = float(self.cfg["base_capital"]) + sum(self.portfolio.realized_by_strategy.values())
        lots = self._next_lots(capital)

        for intent in intents:
            entry_expiry = self._entry_expiry_for_new_orders(now)
            entry_dte_days = max((entry_expiry - now.date()).days, 0)
            rm_dte_days = self._current_rm_dte(now)
            final_side, rm_reason = self.brain.decide_side(portfolio_delta, intent.requested_side, rm_dte_days)

            # Track consecutive RM overrides of Wave — signals persistent delta imbalance
            _rm_overrode = (final_side != intent.requested_side and intent.source == "wave")
            if _rm_overrode:
                self._rm_override_streak = getattr(self, "_rm_override_streak", 0) + 1
                _streak_warn = int(self.cfg.get("rm_override_streak_warn", 3))
                if self._rm_override_streak >= _streak_warn:
                    self._warning(
                        f"RM_OVERRIDE_STREAK {self._rm_override_streak}: wave requested "
                        f"{intent.requested_side} but RM keeps forcing {final_side} "
                        f"(rm_reason={rm_reason} delta={portfolio_delta:.3f}). "
                        f"Persistent delta imbalance — consider rebalancing."
                    )
            else:
                self._rm_override_streak = 0

            # Extract wave step index for progressive strike selection
            wave_step_idx = 0
            if intent.source == "wave":
                import re as _re
                m = _re.search(r"wave_ladder_(?:up|down)_(\d+)", intent.reason)
                if m:
                    wave_step_idx = int(m.group(1)) - 1  # convert 1-based to 0-based

            option_row, premium, distance, atr_move, vix_move = self._choose_option(
                spot, atr, vix_value, final_side,
                entry_expiry=entry_expiry,
                wave_step_index=wave_step_idx
            )
            if option_row is None:
                self._set_decision(
                    f"Intent={intent.source}:{intent.reason}",
                    *self._trade_transparency_lines(intent.requested_side, final_side, rm_reason, portfolio_delta),
                    f"Blocked: no eligible option for {entry_expiry}",
                )
                self._info(
                    f"Skip source={intent.source} requested={intent.requested_side} final={final_side} expiry={entry_expiry} reason=no_eligible_option"
                )
                continue

            dte_days = max((entry_expiry - now.date()).days, 1)
            vol_for_delta = active_vol
            greek_style = str(self.cfg.get("greek_style", "sensibull")).lower()
            greek_underlying = self.future_price if (greek_style == "sensibull" and self.future_price) else spot
            delta = calculate_option_greeks(greek_style, greek_underlying, float(option_row["strike"]), dte_days, float(self.cfg["interest_rate"]), max(float(premium), 0.01), final_side, vol_for_delta)["delta"]
            new_delta = portfolio_delta + (-delta) * lots
            if not self.brain.allows_delta(portfolio_delta, new_delta, rm_dte_days):
                improve = abs(portfolio_delta)-abs(new_delta)
                self._set_decision(
                    f"Intent={intent.source}:{intent.reason}",
                    *self._trade_transparency_lines(intent.requested_side, final_side, rm_reason, portfolio_delta, new_delta),
                    f"Blocked by delta gate improve={improve:.3f}",
                    f"rm_dte={rm_dte_days}",
                )
                self._info(
                    f"BLOCKED source={intent.source} requested={intent.requested_side} "
                    f"final={final_side} rm_reason={rm_reason} "
                    f"symbol={option_row['tradingsymbol'] if option_row else 'N/A'} "
                    f"expiry={entry_expiry} old_delta={portfolio_delta:.3f} new_delta={new_delta:.3f} "
                    f"improve={abs(portfolio_delta)-abs(new_delta):.3f} rm_dte={rm_dte_days}"
                )
                continue

            signal_key = self._signal_key(intent, final_side, option_row["tradingsymbol"])
            if self.rejected_signal_cooldowns.get(signal_key, 0.0) > now_ts:
                secs = max(0, int(self.rejected_signal_cooldowns.get(signal_key, 0.0)-now_ts))
                self._set_decision(
                    f"Intent={intent.source}:{intent.reason}",
                    *self._trade_transparency_lines(intent.requested_side, final_side, rm_reason, portfolio_delta, new_delta),
                    f"Gate: rejected-signal cooldown {secs}s",
                    f"Symbol={option_row['tradingsymbol']}",
                )
                continue

            qty = lots * self.lot_size

            # Pre-flight margin check — skip if we know it will be rejected
            min_buffer = float(self.cfg.get("min_free_margin_buffer", 150000.0))
            if min_buffer > 0:
                live_margin = self._current_margin_available()
                if live_margin < min_buffer:
                    self._set_decision(
                        f"Intent={intent.source}:{intent.reason}",
                        *self._trade_transparency_lines(intent.requested_side, final_side, rm_reason, portfolio_delta),
                        f"Gate: pre-flight margin below buffer ({live_margin:.0f} < {min_buffer:.0f})",
                    )
                    self._info(
                        f"SKIPPED pre-flight margin: available={live_margin:.2f} buffer={min_buffer:.2f} symbol={option_row['tradingsymbol']}"
                    )
                    continue

            order_id, order_error = self._place(option_row["tradingsymbol"], qty, intent.source, final_side, rm_reason)
            if order_id is None:
                self._set_decision(
                    f"Intent={intent.source}:{intent.reason}",
                    *self._trade_transparency_lines(intent.requested_side, final_side, rm_reason, portfolio_delta, new_delta),
                    f"Order rejected for {option_row['tradingsymbol']}",
                    (order_error or 'order rejected')[:96],
                )
                self._handle_order_rejection(intent, final_side, option_row["tradingsymbol"], rm_reason, order_error or "order rejected")
                continue

            self.consecutive_margin_rejections = 0
            self.next_order_allowed_ts = max(self.next_order_allowed_ts, time.time() + self.global_order_cooldown_seconds)
            self._commit_strategy_intent(intent, now_ts=time.time())

            self.portfolio.add(
                Position(
                    source=intent.source,
                    opt_type=final_side,
                    tradingsymbol=option_row["tradingsymbol"],
                    strike=float(option_row["strike"]),
                    expiry=entry_expiry,
                    entry_price=float(premium),
                    qty=qty,
                    lots=lots,
                    distance=float(distance),
                    entry_time=now,
                    current_price=float(premium),
                )
            )
            self._record_entry(final_side)
            old_delta_for_display = portfolio_delta
            portfolio_delta = new_delta
            self.cached_portfolio_delta = portfolio_delta

            self.last_order_event = (
                f"{intent.source.upper()} {final_side} {option_row['tradingsymbol']} "
                f"qty={qty} premium={premium:.2f} order_id={order_id}"
            )
            self._set_decision(
                f"Intent={intent.source}:{intent.reason}",
                *self._trade_transparency_lines(intent.requested_side, final_side, rm_reason, old_delta_for_display, new_delta),
                f"Executed {option_row['tradingsymbol']} qty={qty}",
            )
            self._info(
                f"EXECUTED source={intent.source} requested={intent.requested_side} final={final_side} "
                f"strategy_reason={intent.reason} rm_reason={rm_reason} symbol={option_row['tradingsymbol']} "
                f"premium={premium:.2f} distance={distance} atr_move={atr_move:.2f} vix_move={vix_move:.2f} "
                f"delta_mode={self.cfg['greeks_mode']} delta_vol_used={vol_for_delta:.2f} lots={lots} qty={qty} order_id={order_id}"
            )
            self._save_weekly_state()
            break


    def _format_dt(self, value: Optional[datetime]) -> str:
        return value.strftime("%H:%M:%S") if value else "-"

    def _write_status_file(self, spot, delta, mtm, theta, positions) -> None:
        """Write live engine state including all open positions to engine_status.json."""
        try:
            import json as _json
            # Build live positions list from portfolio
            live_positions = []
            for p in self.portfolio.positions:
                live_positions.append({
                    "symbol": p.tradingsymbol,
                    "side": p.opt_type,
                    "qty": int(p.qty),
                    "entry": float(p.entry_price),
                    "lots": int(p.lots),
                })
            payload = {
                "thinking": self.decision_lines,
                "spot": spot,
                "delta": delta,
                "mtm": mtm,
                "theta": theta,
                "positions": positions,
                "live_positions": live_positions,
                "anchor": self.wave.anchor,
                "auth_paused": self._auth_paused,
                "updated_at": ist_now().strftime("%H:%M:%S"),
            }
            status_file = Path("/opt/niftybot/outputs/live/engine_status.json")
            status_file.parent.mkdir(parents=True, exist_ok=True)
            status_file.write_text(_json.dumps(payload, default=str))
        except Exception:
            pass

    def _aggressive_rebalance_if_needed(self, now: datetime, spot: float, atr: float, vix_value: float) -> None:
        """
        AGGRESSIVE DELTA NEUTRALIZATION (Power Hour: 15:15–15:25).

        This method is the ONLY delta-correction mechanism after 15:15.
        It SELLS OTM options to neutralize delta — no buying at any time.

        Phases:
          09:15–15:14 : Normal trading — PassiveRiskManager handles delta via selling.
                        Emergency threshold = delta_emergency_threshold (2.0).
          15:15–15:25 : Power Hour — threshold drops to power_hour_delta_threshold (1.0).
                        Engine calculates exact lots needed to bring Δ → 0 and fires
                        market sell orders aggressively. Cooldown reduced to 20s.
          After 15:25 : Engine hard stops. No orders.

        Directional logic (SELL only):
          Positive Δ (long delta) → SELL CE (adds negative delta)
          Negative Δ (short delta) → SELL PE (adds positive delta)

        Strike selection: targets premium in power_hour_min_premium–power_hour_max_premium
        band for liquidity. Falls back to first available strike above min_premium_to_sell.

        Margin: pre-flight check uses power_hour_margin_buffer before each order.
        """
        power_hour_start  = now.replace(hour=15, minute=15, second=0, microsecond=0)
        power_hour_cutoff = now.replace(hour=15, minute=25, second=0, microsecond=0)

        if not (power_hour_start <= now <= power_hour_cutoff):
            return  # only runs in Power Hour window

        ph_threshold = float(self.cfg.get("power_hour_delta_threshold", 1.0))
        delta = self.cached_portfolio_delta

        if abs(delta) < ph_threshold:
            return  # delta already acceptable — nothing to do

        # Determine which side to sell to correct delta
        # Positive delta → sell CE (CE short adds negative delta to book)
        # Negative delta → sell PE (PE short adds positive delta to book)
        sell_side = "CE" if delta > 0 else "PE"
        entry_expiry = self._entry_expiry_for_new_orders(now)
        dte_days = max((entry_expiry - now.date()).days, 1)

        # ── Strike selection for Power Hour sells ────────────────────────────
        # Target strikes ~200pts from spot for meaningful delta impact and
        # higher premium collection. Scan from ~150pts to ~300pts OTM so the
        # engine lands near the 200pt zone but can step out if that exact
        # strike has no liquidity.
        #
        # power_hour_otm_points (default 200): centre of the target zone.
        # Candidates are built from (centre - 2 steps) to (centre + 4 steps)
        # so the engine always has options to pick from around the target.
        ph_otm_points = float(self.cfg.get("power_hour_otm_points", 200.0))
        ph_min_premium = float(self.cfg.get("power_hour_min_premium", 30.0))

        active_vol = self._active_volatility(vix_value)
        greek_style = str(self.cfg.get("greek_style", "sensibull")).lower()
        greek_underlying = self.future_price if (greek_style == "sensibull" and self.future_price) else spot

        # Build candidate strikes centred on ~200pts OTM, scanning ±4 steps
        centre_strike = round_to_step(
            (spot + ph_otm_points) if sell_side == "CE" else (spot - ph_otm_points),
            self.strike_step
        )
        offsets = [0, -1, 1, -2, 2, -3, 3, -4, 4]  # steps relative to centre
        candidates = []
        for off in offsets:
            strike = centre_strike + off * self.strike_step
            if strike <= 0:
                continue
            row = pick_option_by_strike(self.chain, entry_expiry, int(strike), sell_side)
            if row is None:
                continue
            sym = f"NFO:{row['tradingsymbol']}"
            candidates.append({"row": row, "sym": sym, "strike": strike, "offset_rank": abs(off)})

        if not candidates:
            self._warning(f"POWER_HOUR: no candidate strikes found for {sell_side} near {centre_strike} — skipping")
            return

        # Fetch LTPs
        try:
            quotes = self.broker.ltp([c["sym"] for c in candidates])
        except Exception as e:
            self._warning(f"POWER_HOUR: LTP fetch failed: {e}")
            return

        # Pick the closest-to-centre strike that has premium ≥ min_premium.
        # If nothing meets min_premium, take highest-premium candidate available.
        for c in candidates:
            ltp = float((quotes.get(c["sym"]) or {}).get("last_price") or 0)
            c["ltp"] = ltp

        eligible = sorted(
            [c for c in candidates if c["ltp"] >= ph_min_premium],
            key=lambda c: c["offset_rank"]
        )
        if not eligible:
            # Fallback: best available premium regardless of floor
            eligible = sorted(
                [c for c in candidates if c["ltp"] > 0],
                key=lambda c: -c["ltp"]
            )

        chosen = eligible
        if not chosen:
            self._warning(
                f"POWER_HOUR: no sellable {sell_side} option found "
                f"near {centre_strike} (min premium ₹{ph_min_premium:.0f}) — skipping"
            )
            return

        best = chosen[0]  # closest OTM that meets premium criteria
        ltp = best["ltp"]

        # ── Lot calculation: how many lots to drive Δ → 0 ────────────────────
        option_delta = calculate_option_greeks(
            greek_style, greek_underlying, float(best["row"]["strike"]),
            dte_days, float(self.cfg["interest_rate"]),
            max(ltp, 0.01), sell_side, active_vol
        )["delta"]

        # Each lot SHORT contributes (-option_delta) to portfolio delta
        per_lot_delta_contrib = -float(option_delta)  # selling flips sign
        if abs(per_lot_delta_contrib) < 1e-6:
            self._warning("POWER_HOUR: option delta too small to compute lot count — skipping")
            return

        # How many lots needed to zero the portfolio delta?
        lots_needed = max(1, int(abs(delta) / abs(per_lot_delta_contrib)))
        max_lots = int(self.cfg.get("power_hour_max_lots", 5))
        lots_to_fire = min(lots_needed, max_lots)
        qty = lots_to_fire * self.lot_size

        # ── Margin pre-flight ─────────────────────────────────────────────────
        ph_margin_buffer = float(self.cfg.get("power_hour_margin_buffer", 100000.0))
        live_margin = self._current_margin_available()
        if live_margin < ph_margin_buffer:
            self._warning(
                f"POWER_HOUR: margin insufficient for sell — "
                f"available={live_margin:.0f} < buffer={ph_margin_buffer:.0f}"
            )
            self._set_decision(
                f"🚫 POWER HOUR: margin insufficient",
                f"Available=₹{live_margin:.0f} < Buffer=₹{ph_margin_buffer:.0f}",
                f"|Δ|={abs(delta):.3f} threshold={ph_threshold:.1f}",
            )
            return

        tradingsymbol = best["row"]["tradingsymbol"]
        new_delta_est = delta + per_lot_delta_contrib * lots_to_fire

        msg = (
            f"⚡ POWER HOUR REBALANCE: SELL {sell_side} {tradingsymbol} "
            f"lots={lots_to_fire} qty={qty} ltp={ltp:.2f} "
            f"Δ {delta:.3f} → est {new_delta_est:.3f} "
            f"(threshold={ph_threshold:.1f})"
        )
        self._info(msg)
        self._set_decision(
            f"⚡ POWER HOUR REBALANCE",
            f"SELL {sell_side} {tradingsymbol}",
            f"lots={lots_to_fire}  ltp=₹{ltp:.2f}",
            f"Δ {delta:.3f} → est {new_delta_est:.3f}",
            f"threshold={ph_threshold:.1f}  margin=₹{live_margin:.0f}",
        )

        order_id, order_error = self._place(
            tradingsymbol, qty,
            "power_hour_rebalance", sell_side,
            f"power_hour_sell_{sell_side.lower()}_delta={delta:.3f}",
            transaction_type="SELL",
        )

        if order_id:
            # Track the position and apply a short cooldown so we don't spam
            entry_expiry2 = self._entry_expiry_for_new_orders(now)
            self.portfolio.add(Position(
                source="power_hour_rebalance",
                opt_type=sell_side,
                tradingsymbol=tradingsymbol,
                strike=float(best["row"]["strike"]),
                expiry=entry_expiry2,
                entry_price=float(ltp),
                qty=qty,
                lots=lots_to_fire,
                distance=abs(float(best["row"]["strike"]) - spot),
                entry_time=now,
                current_price=float(ltp),
            ))
            self.cached_portfolio_delta = new_delta_est
            ph_cooldown = float(self.cfg.get("power_hour_cooldown_seconds", 20.0))
            self.next_order_allowed_ts = max(self.next_order_allowed_ts, time.time() + ph_cooldown)
            self._save_weekly_state()
        else:
            self._warning(f"POWER_HOUR: order failed — {order_error}")

    def _set_decision(self, *lines: str) -> None:
        cleaned = [str(x)[:96] for x in lines if x]
        self.decision_lines = cleaned[:8] if cleaned else ["-"]

    def _trade_transparency_lines(self, requested_side: str, final_side: str, rm_reason: str, old_delta: float, new_delta: Optional[float] = None) -> List[str]:
        lines = [
            f"Requested={requested_side}",
            f"Final={final_side}",
            f"RM={rm_reason}",
            f"oldΔ={old_delta:.3f}",
        ]
        if new_delta is not None:
            lines.append(f"newΔ={new_delta:.3f}")
        trade_type = "Normal strategy-side trade" if requested_side == final_side and rm_reason == "strategy" else "Repair override / RM-altered trade"
        lines.append(trade_type)
        return lines

    def _wave_next_levels(self) -> Tuple[Optional[float], Optional[float]]:
        wave_anchor = getattr(self.wave, "anchor", None)
        wave_up_next = None
        wave_down_next = None
        if wave_anchor is not None:
            if getattr(self.wave, "up_index", 0) < len(getattr(self.wave, "ladder", [])):
                wave_up_next = wave_anchor + self.wave.ladder[self.wave.up_index]
            if getattr(self.wave, "down_index", 0) < len(getattr(self.wave, "ladder", [])):
                wave_down_next = wave_anchor - self.wave.ladder[self.wave.down_index]
        return wave_up_next, wave_down_next

    def _thinking_box(self) -> Panel:
        body = Text("\n".join(self.decision_lines) if self.decision_lines else "-")
        return Panel(body, title="Thinking", border_style="bright_magenta")

    def _build_market_panel(self) -> Panel:
        tbl = Table.grid(padding=(0, 1))
        tbl.add_column(justify="left")
        tbl.add_column(justify="right")
        tbl.add_row("Spot", f"{self.spot:.2f}" if self.spot is not None else "-")
        tbl.add_row("VIX", f"{self.vix:.2f}" if self.vix is not None else "-")
        tbl.add_row("Nifty Fut", f"{self.future_price:.2f}" if self.future_price is not None else "-")
        tbl.add_row("Greek style", str(self.cfg.get("greek_style", "sensibull")).upper())
        tbl.add_row("Workflow", self.workflow_mode.upper())
        tbl.add_row("Expiry", str(self.expiry))
        tbl.add_row("Expiry mode", str(self.cfg.get("expiry_preference_label", "current")))
        tbl.add_row("Strike step", str(self.strike_step))
        wave_anchor = getattr(self.wave, "anchor", None)
        wave_up_next = None
        wave_down_next = None
        if wave_anchor is not None:
            if getattr(self.wave, "up_index", 0) < len(getattr(self.wave, "ladder", [])):
                wave_up_next = wave_anchor + self.wave.ladder[self.wave.up_index]
            if getattr(self.wave, "down_index", 0) < len(getattr(self.wave, "ladder", [])):
                wave_down_next = wave_anchor - self.wave.ladder[self.wave.down_index]
        # Display the actual next-fire level based on current up/down index
        surv_anchor = self.survivor.anchor
        if surv_anchor is not None:
            surv_pe_next = surv_anchor + self.survivor.activation_offset + self.survivor.up_index * self.survivor.gap
            surv_ce_next = surv_anchor - self.survivor.activation_offset - self.survivor.down_index * self.survivor.gap
        else:
            surv_pe_next = None
            surv_ce_next = None
        tbl.add_row("Wave anchor", f"{wave_anchor:.2f}" if wave_anchor is not None else "-")
        tbl.add_row("Wave ↑ next", f"{wave_up_next:.2f}" if wave_up_next is not None else "-")
        tbl.add_row("Wave ↓ next", f"{wave_down_next:.2f}" if wave_down_next is not None else "-")
        tbl.add_row("Survivor anchor", f"{surv_anchor:.2f}" if surv_anchor is not None else "-")
        tbl.add_row("Survivor PE ↑", f"{surv_pe_next:.2f}" if surv_pe_next is not None else "-")
        tbl.add_row("Survivor CE ↓", f"{surv_ce_next:.2f}" if surv_ce_next is not None else "-")
        tbl.add_row("Last tick", self._format_dt(self.last_tick_ts))
        return Panel(tbl, title="Market", border_style="cyan")

    def _build_health_panel(self) -> Panel:
        tbl = Table.grid(padding=(0, 1))
        tbl.add_column(justify="left")
        tbl.add_column(justify="right")
        tbl.add_row("WebSocket", "CONNECTED" if self.broker.is_connected() else "DISCONNECTED")
        tbl.add_row("Auth paused", str(self._auth_paused))
        tbl.add_row("Queue depth", str(self.tick_queue.qsize()))
        tbl.add_row("Mode", "LIVE")
        tbl.add_row("Last session check", self._format_dt(self.last_session_check_dt))
        tbl.add_row("Last order", self.last_order_event[:48] if self.last_order_event else "-")
        tbl.add_row("Last error", self.last_error[:48] if self.last_error else "-")
        return Panel(Group(tbl, self._thinking_box()), title="Health", border_style="magenta")

    def _build_positions_panel(self) -> Panel:
        tbl = Table(expand=True)
        tbl.add_column("Symbol", overflow="fold")
        tbl.add_column("Side", width=4)
        tbl.add_column("Qty", justify="right")
        tbl.add_column("Entry", justify="right")
        tbl.add_column("LTP", justify="right")
        tbl.add_column("P&L", justify="right")
        if not self.portfolio.positions:
            tbl.add_row("-", "-", "-", "-", "-", "-")
        else:
            # Merge same-symbol positions into one row
            merged = {}
            for p in self.portfolio.positions:
                sym = p.tradingsymbol
                pnl = (p.entry_price - p.current_price) * p.qty
                if sym not in merged:
                    merged[sym] = {
                        "opt_type": p.opt_type,
                        "qty": p.qty,
                        "total_cost": p.entry_price * p.qty,
                        "pnl": pnl,
                        "ltp": p.current_price,
                    }
                else:
                    merged[sym]["qty"] += p.qty
                    merged[sym]["total_cost"] += p.entry_price * p.qty
                    merged[sym]["pnl"] += pnl

            for sym, d in list(merged.items())[:12]:
                avg = d["total_cost"] / d["qty"] if d["qty"] else 0
                tbl.add_row(
                    sym,
                    d["opt_type"],
                    str(d["qty"]),
                    f"{avg:.2f}",
                    f"{d['ltp']:.2f}",
                    f"{d['pnl']:.2f}",
                )
        return Panel(tbl, title="Positions", border_style="green")

    def _build_risk_panel(self) -> Panel:
        atr = self.atr.value()
        portfolio_delta = self.cached_portfolio_delta
        portfolio_gamma = self.cached_portfolio_gamma
        portfolio_theta = self.cached_portfolio_theta
        portfolio_vega = self.cached_portfolio_vega
        mtm_total = self.cached_mtm_total
        realized = sum(self.portfolio.realized_by_strategy.values()) if self.portfolio.realized_by_strategy else 0.0

        tbl = Table.grid(padding=(0, 1))
        tbl.add_column(justify="left")
        tbl.add_column(justify="right")
        tbl.add_row("ATR 14D", f"{atr:.2f}" if atr is not None else "-")
        tbl.add_row("Portfolio Δ", f"{portfolio_delta:.3f}")
        tbl.add_row("Portfolio Γ", f"{portfolio_gamma:.5f}")
        tbl.add_row("Portfolio Θ", f"{portfolio_theta:.2f}")
        tbl.add_row("Portfolio Vega", f"{portfolio_vega:.2f}")
        tbl.add_row("Greek calc at", self._format_dt(self.last_risk_calc_at))
        tbl.add_row("Last snapshot", self._format_dt(self.last_snapshot_at))
        tbl.add_row("MTM", f"{mtm_total:.2f}")
        tbl.add_row("Realized", f"{realized:.2f}")
        tbl.add_row("Fallback vol", f"{self.cached_active_volatility:.2f}")
        tbl.add_row("Wave gap", f"{self.cfg['wave_gap']}")
        tbl.add_row("Survivor gap", f"{self.cfg['surv_gap']}")
        tbl.add_row("Survivor reset", f"{self.cfg['surv_reset']}")
        tbl.add_row("Manual exits", "ON")
        tbl.add_row("State file", self.state_path.name if self.workflow_mode == "weekly" else "-")
        tbl.add_row("Open positions", str(len(self.portfolio.positions)))
        return Panel(tbl, title="Risk / ATR / Delta", border_style="yellow")

    def _build_events_panel(self) -> Panel:
        lines = self.recent_events[-int(self.cfg.get("dashboard_max_events", 12)):]
        body = Text("\n".join(lines) if lines else "No events yet")
        return Panel(body, title="Recent Events", border_style="white")

    def _build_dashboard(self):
        if not HAS_RICH:
            return None
        layout = Layout()
        layout.split_column(
            Layout(name="top", size=17),
            Layout(name="positions", ratio=1),
            Layout(name="events", size=max(8, int(self.cfg.get("dashboard_max_events", 12)) + 2)),
        )
        layout["top"].split_row(
            Layout(self._build_market_panel(), name="market"),
            Layout(self._build_risk_panel(), name="risk"),
            Layout(self._build_health_panel(), name="health"),
        )
        layout["positions"].update(self._build_positions_panel())
        layout["events"].update(self._build_events_panel())
        return layout

    # ── run loop ──────────────────────────────────────────────────────────────

    def _handle_stop_signal(self, signum, frame) -> None:
        self.stop_requested = True
        self._warning(f"Received signal {signum}; shutting down gracefully")

    def run(self) -> None:
        tokens = [
            int(self.cfg["nifty_index_instrument_token"]),
            int(self.cfg["india_vix_instrument_token"]),
        ]
        if self.future_token is not None:
            tokens.append(int(self.future_token))
        self.broker.connect_ticker(
            tokens=tokens,
            mode=self.cfg.get("websocket_mode", "ltp"),
            on_ticks=self.on_ticks,
            on_order_update=self.on_order_update,
            on_connect=self.on_connect,
            on_close=self.on_close,
            on_error=self.on_error,
            reconnect=bool(self.cfg.get("websocket_reconnect", True)),
        )
        self._info(f"Waiting for WebSocket ticks on tokens={tokens}")

        live_ctx = (
            Live(
                self._build_dashboard(),
                refresh_per_second=int(self.cfg.get("dashboard_refresh_per_second", 4)),
                screen=True,
            )
            if self.dashboard_enabled
            else None
        )

        previous_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, self._handle_stop_signal)

        def _refresh_dashboard():
            if live_ctx is not None:
                live_ctx.update(self._build_dashboard())

        try:
            if live_ctx is not None:
                live_ctx.__enter__()

            while not self.stop_requested:
                if time.time() - self.last_session_check > 300:
                    self._check_session()
                    self.last_session_check = time.time()

                try:
                    tick = self.tick_queue.get(timeout=1.0)
                    self._handle_tick(tick)
                    self.evaluate()
                    _refresh_dashboard()
                except Empty:
                    if time.time() - self.last_heartbeat > 30:
                        self._info(
                            f"Heartbeat ws_connected={self.broker.is_connected()} "
                            f"queue_depth={self.tick_queue.qsize()} "
                            f"auth_paused={self._auth_paused} "
                            f"anchor={self.wave.anchor}"
                        )
                        self.last_heartbeat = time.time()
                        self._check_for_fresh_token()
                        # Write status file on heartbeat so Telegram /think
                        # shows fresh data even when no ticks arrive (overnight)
                        if self.spot is not None:
                            self._write_status_file(
                                spot=float(self.spot),
                                delta=self.cached_portfolio_delta,
                                mtm=self.cached_mtm_total,
                                theta=self.cached_portfolio_theta,
                                positions=len(self.portfolio.positions),
                            )
                    _refresh_dashboard()
                except KeyboardInterrupt:
                    self._info("Stopped by user (KeyboardInterrupt)")
                    self.stop_requested = True
                    _refresh_dashboard()
                except Exception as e:
                    self._exception(f"Engine loop error: {e}")
                    _refresh_dashboard()
        finally:
            self._record_run_event("STOP", notes=f"stop_requested={self.stop_requested}")
            self.broker.stop_ticker()
            signal.signal(signal.SIGTERM, previous_sigterm)
            if live_ctx is not None:
                live_ctx.__exit__(None, None, None)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 12 — DEFAULT CONFIG


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 12 — DEFAULT CONFIG
# ══════════════════════════════════════════════════════════════════════════════

DEFAULT_CFG: Dict[str, Any] = {
    # --- broker / session ---
    "product": "NRML",
    "variety": "regular",
    "log_level": "INFO",

    # --- instruments ---
    "nifty_index_instrument_token": 256265,
    "india_vix_instrument_token": 264969,

    # --- greeks ---
    "greeks_engine": "scipy",           # retained for compatibility
    "greek_style": "sensibull",         # locked: Sensibull-style (Black-76 + futures)
    "greeks_mode": "implied",           # "implied" | "stable" | "vix" | "todays"
    "stable_volatility": 20.0,
    "todays_volatility_fallback": 20.0,
    "interest_rate": 6.5,

    # --- ATR ---
    # atr_period: number of daily sessions used for ATR calculation.
    # We fetch 22 calendar days to guarantee ≥14 trading sessions.
    # atr_multiplier: kept at 1.0 — a 14-day ATR already captures a full
    # expected daily range; multiplying further over-widens strikes.
    "atr_period": 14,
    "atr_multiplier": 1.0,

    # --- VIX distance ---
    # vix_multiplier: scales the VIX-annualised expected move.
    # Formula: spot * (VIX/100) * sqrt(DTE/365) * vix_multiplier
    # Keep at 1.0 — this is already a statistically meaningful 1-sigma move.
    "vix_multiplier": 1.0,

    # --- strategies ---
    "enable_survivor": True,
    "delta_emergency_threshold": 3.0,   # |delta| above this → emergency mode (30s cooldown)
    "delta_emergency_stop_threshold": 2.0,  # emergency clears when |delta| brought below this
    "rm_override_streak_warn": 3,       # warn after this many consecutive RM wave overrides

    # --- Aggressive Delta Neutralization (Power Hour: 15:15–15:25) ---
    # Overnight hedge is DISABLED. Delta is corrected by SELLING options only.
    # power_hour_delta_threshold: tighter threshold during Power Hour (15:15–15:25).
    #   Engine targets Δ=0 aggressively during this window.
    # power_hour_min_premium: minimum LTP for a Power Hour sell to be considered sellable.
    # power_hour_max_premium: preferred upper bound on LTP (avoid deep ITM strikes).
    # power_hour_max_lots: cap on lots fired in a single Power Hour correction.
    # power_hour_cooldown_seconds: time between Power Hour orders (allows fills to settle).
    # power_hour_margin_buffer: minimum free margin required before firing a Power Hour sell.
    "overnight_hedge_enabled": False,           # DISABLED — we sell to neutralize, not buy
    "power_hour_delta_threshold": 1.0,          # |delta| threshold during 15:15–15:25
    "power_hour_otm_points": 200.0,             # target strike distance from spot (pts)
    "power_hour_min_premium": 30.0,             # min ₹ LTP for Power Hour sell candidate
    "power_hour_max_lots": 5,                   # max lots to sell per Power Hour trigger
    "power_hour_cooldown_seconds": 20.0,        # seconds between Power Hour orders
    "power_hour_margin_buffer": 100000.0,       # min free margin before Power Hour sell
    "surv_gap": 20.0,       # fires every 20pts after survivor activates
    "surv_reset": 100.0,    # reset after 100pt reversal

    "enable_wave": True,
    # wave_gap: 20pt base — tighter Wave trigger for earlier participation.
    # On expiry day you may want to reduce this further manually if desired.
    "wave_gap": 20.0,
    "wave_ladder_count": 6,
    "wave_ladder_step": 15.0,
    "wave_cooldown_seconds": 90.0,
    "post_order_cooldown_seconds": 90.0,
    "rejection_cooldown_seconds": 180.0,
    "margin_rejection_cooldown_seconds": 600.0,
    "rate_limit_cooldown_seconds": 300.0,
    "transient_rejection_cooldown_seconds": 120.0,
    "margin_watch_check_seconds": 15.0,

    # --- risk (Raahi Bhushan delta-neutral approach) ---
    # Raahi explicitly targets delta "close to zero" (his words: "Delta-neutral,
    # similar to complex calendar option positions where Delta is kept close to zero").
    # delta_band: maximum absolute portfolio delta allowed after a new trade.
    #   Set to 0.50 — tighter than the original 5.0.  With typical OTM options
    #   at 0.10–0.20 delta per lot, this allows 2–5 open lots before the band blocks.
    # rm_trigger_delta: threshold at which the risk manager overrides the
    #   strategy's side choice to force rebalancing.  Set to 0.30.
    # delta_tilt_soft: threshold at which Wave widens its gap multiplier.
    #   Set to 0.20 so any meaningful tilt triggers the 1.7× caution factor.
    "delta_band": 0.70,
    "delta_tilt_soft": 0.45,
    "rm_trigger_delta": 0.70,
    "min_premium_to_sell": 40.0,   # raised from 30 — ensures meaningful premium collected

    # --- strike selection refinement ---
    # Keep ATR/VIX distance as the base reference, then choose the nearest
    # strike around that ATR-based zone that fits premium + preferred delta.
    "enable_delta_preference": True,
    "preferred_abs_delta_min": 0.12,
    "preferred_abs_delta_max": 0.18,   # extended from 0.15 for better premium collection
    "fallback_abs_delta_min": 0.12,
    "fallback_abs_delta_max": 0.22,   # extended fallback for low VIX periods
    "max_strike_adjust_steps": 6,

    # --- sizing ---
    "base_capital": 500000.0,
    "base_lots_per_order": 1,
    # Open-position ceilings are intentionally disabled. The bot may keep
    # adding one-lot entries as long as trigger + delta logic allows.
    "enable_compounding": False,

    # --- time windows (all in IST — engine is hard-locked to IST) ---
    # market_start: 09:15 IST — anchor drops on first tick after token confirm.
    # 9:45 gate removed: the engine can trade from market open once token is confirmed.
    # market_end_new_entries: 15:00 IST — stop fresh wave/survivor entries; RM/delta active all day.
    "market_start": "09:15",
    "market_end_new_entries": "15:00",
    "expiry_day_dte_threshold": 1,
    "min_new_entry_dte_same_expiry": 1,
    "expiry_day_new_entries_cutoff": "13:00",
    "expiry_day_force_close_cutoff": "13:00",
    "preexpiry_force_close_time": "15:20",

    # --- websocket ---
    "websocket_mode": "ltp",
    "websocket_reconnect": True,

    # --- risk / decay / prices (manual exits enabled: no auto stop-loss or MTM halt) ---
    "max_daily_loss": 0.0,
    "stop_loss_multiplier": 0.0,
    "position_price_refresh_seconds": 5.0,
    "todays_vol_lookback_minutes": 30,
    "enable_flat_decay": False,
    "flat_decay_wait_minutes": 15,
    "flat_decay_range_points": 25.0,
    "auto_buyback_enabled": False,
    "positions_sync_seconds": 30.0,

    # --- weekly workflow ---
    "workflow_mode": "weekly",
    "weekly_state_path": "outputs/live/weekly_state.json",
    "selected_expiry": None,
    "expiry_offset": 0,
    "expiry_preference_label": "current",
    "weekly_entry_min_dte": 0,
    "weekly_entry_max_dte": 30,
    "weekly_rebalance_only_dte": -1,
    "weekly_expiry_rebalance_delta": 0.20,
    "reset_state_when_flat": True,

    # --- audit ---
    "snapshot_flush_seconds": 30,
    "stale_snapshot_flush_seconds": 300,  # after market close, snapshot every 5 min not 30s
    "dashboard_enabled": True,
    "dashboard_refresh_per_second": 4,
    "dashboard_max_events": 12,
}



# ══════════════════════════════════════════════════════════════════════════════
# SECTION 13 — ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def _deep_merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_config_override(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _prompt_terminal_value(prompt: str, allow_empty: bool = False, secret: bool = False) -> str:
    while True:
        try:
            if secret:
                import getpass
                value = getpass.getpass(prompt)
            else:
                value = input(prompt)
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit("Input cancelled by user. Exiting.")

        value = value.strip()
        if value or allow_empty:
            return value
        print("This value cannot be empty. Please try again.")


def _save_credentials_to_env(
    api_key: str,
    api_secret: str,
    access_token: str = "",
    token_generated_at: str = "",
) -> None:
    if not HAS_DOTENV:
        return
    env_path = Path(".env")
    env_path.touch(exist_ok=True)
    set_key(str(env_path), "KITE_API_KEY", api_key)
    if api_secret:
        set_key(str(env_path), "KITE_API_SECRET", api_secret)
    if access_token:
        set_key(str(env_path), "KITE_ACCESS_TOKEN", access_token)
    if token_generated_at:
        set_key(str(env_path), "KITE_ACCESS_TOKEN_GENERATED_AT", token_generated_at)


def _load_saved_api_credentials() -> Tuple[str, str]:
    return (os.getenv("KITE_API_KEY") or "").strip(), (os.getenv("KITE_API_SECRET") or "").strip()


def _load_saved_access_token() -> str:
    return (os.getenv("KITE_ACCESS_TOKEN") or "").strip()


def _get_or_store_api_credentials(reset: bool = False) -> Tuple[str, str]:
    if not reset:
        api_key, api_secret = _load_saved_api_credentials()
        if api_key and api_secret:
            print("Using saved Kite API key and secret from .env")
            return api_key, api_secret

    print()
    print("First-time Kite setup")
    print("─────────────────────")
    api_key = _prompt_terminal_value("Enter Kite API key: ")
    api_secret = _prompt_terminal_value("Enter Kite API secret: ", secret=True)
    _save_credentials_to_env(api_key, api_secret)
    print("API key and secret saved to .env. Future runs will reuse them automatically.")
    return api_key, api_secret


def _print_mac_open_helper(login_url: str) -> None:
    print("If you are SSHed into the VPS from your Mac, open the login page on your Mac with:")
    print(f"  open '{login_url}'")
    print()


def _generate_daily_access_token(api_key: str, api_secret: str, save: bool = True) -> str:
    kite = KiteConnect(api_key=api_key)
    login_url = kite.login_url()

    print()
    print("Daily Kite login")
    print("────────────────")
    print("Using the saved API key and API secret from .env.")
    print("Complete the Zerodha login in your browser, then paste the request_token below.")
    print()
    print("Login URL:")
    print(login_url)
    print()
    _print_mac_open_helper(login_url)
    print("Browser auto-open is disabled for VPS/SSH use. Open the URL on your local Mac manually.")
    print()

    request_token = _prompt_terminal_value("Paste request_token here: ")
    session = kite.generate_session(request_token, api_secret=api_secret)
    access_token = session["access_token"]
    generated_at = ist_now().isoformat()
    print("Access token generated successfully.")

    if save:
        _save_credentials_to_env(api_key, api_secret, access_token, generated_at)
        print("Saved the fresh access token to .env for same-day restarts.")

    return access_token


def _validate_access_token(api_key: str, access_token: str) -> bool:
    if not access_token:
        return False
    try:
        broker = ZerodhaBroker(api_key=api_key, access_token=access_token)
        return broker.is_session_valid()
    except Exception:
        return False


def _resolve_runtime_credentials(force_new_token: bool = False, reset_creds: bool = False) -> Tuple[str, str]:
    print()
    print("Kite authentication")
    print("──────────────────")
    api_key, api_secret = _get_or_store_api_credentials(reset=reset_creds)

    if not force_new_token:
        saved_access_token = _load_saved_access_token()
        if saved_access_token:
            print("Found saved access token in .env. Validating it...")
            if _validate_access_token(api_key, saved_access_token):
                print("Saved access token is still valid. Reusing it for this run.")
                return api_key, saved_access_token
            print("Saved access token is missing, expired, or invalid. Generating a fresh token.")
        else:
            print("No saved access token found. Generating today's token.")
    else:
        print("Forced fresh token requested. Generating today's token.")

    access_token = _generate_daily_access_token(api_key, api_secret, save=True)
    return api_key, access_token


def _validate_runtime_config(cfg: Dict[str, Any]) -> None:
    # Lock the runtime to Sensibull-style Greeks so the dashboard and
    # adjustment logic always use one consistent convention.
    cfg["greek_style"] = "sensibull"

    mp = cfg.get("market_protection", -1)
    try:
        mp_float = float(mp)
    except Exception:
        sys.exit("Invalid market_protection in config. Use -1 for automatic protection or a number between 1 and 100.")

    if mp_float == 0:
        # Kite API rejects market_protection=0 for MARKET orders — 0 means "no protection"
        # which the exchange does not allow. Use -1 for Zerodha's automatic circuit-limit
        # protection, or a value 1–100 for explicit percentage protection.
        sys.exit("market_protection=0 is not allowed for API MARKET orders. Use -1 for automatic protection or a value between 1 and 100.")
    if mp_float < -1 or mp_float > 100:
        sys.exit("market_protection must be -1 or between 1 and 100.")


def _prompt_expiry_choice(broker: ZerodhaBroker) -> Tuple[date, int, str]:
    """
    Ask whether new SELL orders should target the current weekly expiry, the
    next weekly expiry, or the week after that. Existing open positions
    continue to be tracked regardless of their own expiry.
    """
    print("\nFetching live NIFTY expiries from the exchange contract master...")
    instruments_df = instruments_to_df(broker.instruments_nfo())
    chain = resolve_nifty_option_chain(instruments_df, ist_now().date())
    expiries = available_expiries(chain, ist_now().date())
    if len(expiries) == 1:
        only_expiry = expiries[0]
        print(f"Only one active NIFTY expiry is currently available: {only_expiry}")
        return only_expiry, 0, "current"

    labels = [("current", "Current week"), ("next", "Next week"), ("week_after", "Week after")]
    print("Available weekly expiries for new orders:")
    for idx, expiry in enumerate(expiries[:3], start=1):
        _label_key, label_text = labels[idx - 1]
        print(f"  {idx}) {label_text:<12}: {expiry}")
    print("Existing open positions will still be imported from broker and used for delta management.")

    prompt = "Trade which expiry for NEW orders? [1=current"
    if len(expiries) >= 2:
        prompt += " / 2=next"
    if len(expiries) >= 3:
        prompt += " / 3=week after"
    prompt += "] (default 1): "

    while True:
        raw = input(prompt).strip().lower()
        if raw in ("", "1", "c", "cur", "current", "current week", "current_week"):
            return expiries[0], 0, "current"
        if len(expiries) >= 2 and raw in ("2", "n", "next", "next week", "next_week"):
            return expiries[1], 1, "next"
        if len(expiries) >= 3 and raw in ("3", "w", "wa", "week after", "week_after", "after", "third"):
            return expiries[2], 2, "week_after"
        allowed = ["1"]
        if len(expiries) >= 2:
            allowed.append("2")
        if len(expiries) >= 3:
            allowed.append("3")
        print(f"Please enter one of: {', '.join(allowed)}.")


def main():
    parser = argparse.ArgumentParser(description="Nifty Options Live Engine")
    parser.add_argument("--live", action="store_true", help="Ignored; script is always live")
    parser.add_argument(
        "--use-saved-token", action="store_true",
        help="Only use the saved KITE_ACCESS_TOKEN from .env; exit if it is missing or invalid"
    )
    parser.add_argument(
        "--force-new-token", action="store_true",
        help="Ignore any saved token and force today's login flow"
    )
    parser.add_argument(
        "--reset-creds", action="store_true",
        help="Re-enter and overwrite the saved API key and API secret in .env"
    )
    parser.add_argument(
        "--check-deps-only", action="store_true",
        help="Check/install dependencies at startup and exit"
    )
    parser.add_argument("--config", type=str, help="Path to JSON config override file")
    parser.add_argument(
        "--expiry-choice",
        choices=["current", "next", "week_after"],
        help="Optional non-interactive choice for new-order expiry selection"
    )
    args = parser.parse_args()

    if args.check_deps_only:
        print("Dependency check complete.")
        return

    cfg = dict(DEFAULT_CFG)
    cfg = _deep_merge_dict(cfg, _load_config_override(args.config))
    cfg["config_path"] = args.config or ""
    _validate_runtime_config(cfg)

    if args.use_saved_token:
        api_key, _api_secret = _get_or_store_api_credentials(reset=args.reset_creds)
        access_token = _load_saved_access_token()
        if not access_token:
            sys.exit(
                "KITE_ACCESS_TOKEN missing from .env. "
                "Run without --use-saved-token to generate today's token."
            )
        print("Using saved access token from .env. Validating it...")
        if not _validate_access_token(api_key, access_token):
            sys.exit(
                "Saved KITE_ACCESS_TOKEN is invalid or expired. "
                "Run without --use-saved-token to generate today's token."
            )
    else:
        api_key, access_token = _resolve_runtime_credentials(
            force_new_token=bool(args.force_new_token),
            reset_creds=bool(args.reset_creds),
        )

    broker = ZerodhaBroker(api_key=api_key, access_token=access_token)

    print("Validating session...")
    if not broker.is_session_valid():
        sys.exit(
            "Session validation failed immediately after login. "
            "This usually means the access_token was already used or has expired. "
            "Please log in again."
        )
    print("Session valid ✓")
    print("Session valid ✓  —  LIVE trading active. All orders go directly to Zerodha.\n")

    if args.expiry_choice:
        instruments_df = instruments_to_df(broker.instruments_nfo())
        chain = resolve_nifty_option_chain(instruments_df, ist_now().date())
        offset_map = {"current": 0, "next": 1, "week_after": 2}
        offset = offset_map[args.expiry_choice]
        selected_expiry = resolve_selected_expiry(chain, ist_now().date(), expiry_offset=offset)
        expiry_label = args.expiry_choice
    else:
        selected_expiry, offset, expiry_label = _prompt_expiry_choice(broker)

    cfg["selected_expiry"] = str(selected_expiry)
    cfg["expiry_offset"] = int(offset)
    cfg["expiry_preference_label"] = expiry_label

    print(
        f"Selected expiry for NEW orders: {selected_expiry} ({expiry_label} week). "
        "Existing open positions of any expiry will still be tracked for delta and manual management. "
        "Fresh entries auto-roll to the next available expiry once the chosen expiry reaches 1 DTE or 0 DTE. "
        "Positions expiring tomorrow are force-exited near the close today."
    )
    print("All stop-loss and forced auto-exit features are disabled. Only your manual exits or exchange expiry will remove positions.\n")

    engine = LiveEngineWS(cfg=cfg, broker=broker)

    mode_label = "LIVE"
    print(f"\nStarting engine | mode={mode_label} | websocket=True\n")
    engine.run()


if __name__ == "__main__":
    main()
