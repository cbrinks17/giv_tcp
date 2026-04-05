"""
Tests for the dataSmoother2 spike-rejection logic in GivTCP/read.py.

Imports the real dataSmoother2 from read.py by injecting lightweight fake
modules into sys.modules before import, satisfying all of read.py's heavy
dependencies (givenergy_modbus_async, MQTT, Redis, etc.) without a live
environment.

Run with:  pytest tests/test_datasmoother.py -v
"""

import sys
import os
import types
import logging
import datetime

import pytest

# ---------------------------------------------------------------------------
# 1. Stubs — defined first so they can be referenced in fake modules below
# ---------------------------------------------------------------------------

class GEType:
    def __init__(self, smooth, onlyIncrease, allowZero=False, min=0, max="maxTotalEnergy"):
        self.smooth = smooth
        self.onlyIncrease = onlyIncrease
        self.allowZero = allowZero
        self.min = min
        self.max = max


class maxvalues:
    single_phase = {
        "maxTotalEnergy": 10_000_000,
        "maxTodayEnergy": 100,
        "maxInvPower": 20000,
        "maxPower": 20000,
        "maxBatPower": 13000,
        "-maxInvPower": -20000,
        "-maxPower": -20000,
        "-maxBatPower": -13000,
        "maxExport": 20000,
        "maxTemp": 100,
        "-maxTemp": -100,
        "maxCellVoltage": 350,
        "maxCost": 100,
        "maxRate": 2,
    }
    three_phase = {
        "maxTotalEnergy": 100_000_000,
        "maxTodayEnergy": 100_000,
        "maxInvPower": 11000,
        "maxPower": 30000,
        "maxBatPower": 13000,
        "-maxInvPower": -11000,
        "-maxPower": -30000,
        "-maxBatPower": -13000,
        "maxExport": 20000,
        "maxTemp": 100,
        "-maxTemp": -100,
        "maxCellVoltage": 500,
        "maxCost": 100,
        "maxRate": 2,
    }


ENTITY_LUT = {
    # smooth=True, onlyIncrease=True
    "Import_Energy_Total_kWh":       GEType(smooth=True,  onlyIncrease=True),
    "Export_Energy_Total_kWh":       GEType(smooth=True,  onlyIncrease=True),
    "Load_Energy_Today_kWh":         GEType(smooth=True,  onlyIncrease=True, max="maxTodayEnergy"),
    "PV_Energy_Total_kWh":           GEType(smooth=True,  onlyIncrease=True),
    # smooth=False, onlyIncrease=True — Fix 4 is their only upward-spike protection
    "Generation_Energy_Total_kWh":   GEType(smooth=False, onlyIncrease=True),
    "Inverter_Out_Energy_Total_kWh": GEType(smooth=False, onlyIncrease=True),
    "AC_Discharge_Energy_Total_kWh": GEType(smooth=False, onlyIncrease=True),
}


class _FakeGivLUTClass:
    logger = logging.getLogger("test_givtcp")


class _FakeEntityType:
    entity_type = ENTITY_LUT


class _FakeSettings:
    data_smoother = "high"
    default_path = "/tmp"
    cache_location = "/tmp"
    serial_number = "TEST123"
    givtcp_instance = "1"


# ---------------------------------------------------------------------------
# 2. Inject fake modules into sys.modules before importing read
# ---------------------------------------------------------------------------

def _fake_mod(name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


_fake_mod("givenergy_modbus_async")
_fake_mod("givenergy_modbus_async.model",          TimeSlot=object)
_fake_mod("givenergy_modbus_async.model.register", Model=object, Enable=object, HR=object)
_fake_mod("givenergy_modbus_async.model.plant",    Plant=object, Inverter=object)
_fake_mod("givenergy_modbus_async.client")
_fake_mod("givenergy_modbus_async.client.client",  commands=object)
_fake_mod("givenergy_modbus_async.exceptions",     CommunicationError=Exception)
_fake_mod("write")
_fake_mod("mqtt", GivMQTT=object)
_fake_mod("GivLUT",
    GivLUT=_FakeGivLUTClass,
    maxvalues=maxvalues,
    InvType=object,
    GivClientAsync=object,
)
_fake_mod("entity_lut", Entity_Type=_FakeEntityType)
_fake_settings_instance = _FakeSettings()
_fake_mod("settings", GiV_Settings=_fake_settings_instance)

# ---------------------------------------------------------------------------
# 3. Import the real read.py and point its globals at our stubs
# ---------------------------------------------------------------------------

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "GivTCP"))
import read  # noqa: E402  — must come after sys.modules injection above

read.givLUT = ENTITY_LUT
read.GiV_Settings = _fake_settings_instance
read.maxvalues = maxvalues

# ---------------------------------------------------------------------------
# 4. Test helpers
# ---------------------------------------------------------------------------

_NOW    = datetime.datetime(2024, 6, 15, 14, 30, 0)
_LAST   = _NOW.isoformat()
_INVTYPE = "SA"  # single-phase


def _run(name, old, new, smoother="high", invtype=_INVTYPE, inv_time=_NOW, last=_LAST):
    read.GiV_Settings.data_smoother = smoother
    return read.dataSmoother2([name, new], [name, old], last, invtype, inv_time)


# ---------------------------------------------------------------------------
# Tests: massive spikes must be rejected (Fix 4)
# ---------------------------------------------------------------------------

def test_import_total_massive_spike_rejected():
    """20,000 → 1,700,000 kWh (8500% jump) must be rejected."""
    assert _run("Import_Energy_Total_kWh", old=20_000, new=1_700_000) == 20_000


def test_export_total_70pct_spike_rejected():
    """7,841 → 13,300 kWh (~70% jump) must be rejected at smoothRate=0.25."""
    assert _run("Export_Energy_Total_kWh", old=7_841, new=13_300) == 7_841


def test_load_today_doubling_spike_rejected():
    """30 → 60 kWh today (100% jump) must be rejected."""
    assert _run("Load_Energy_Today_kWh", old=30, new=60) == 30


def test_pv_total_large_spike_rejected():
    """5,000 → 10,000 kWh (100% jump) must be rejected."""
    assert _run("PV_Energy_Total_kWh", old=5_000, new=10_000) == 5_000


# ---------------------------------------------------------------------------
# Tests: smooth=False, onlyIncrease=True — Fix 4 is their only protection
# ---------------------------------------------------------------------------

def test_generation_total_spike_rejected_smooth_false():
    """Generation_Energy_Total_kWh has smooth=False; Fix 4 must still block spikes."""
    assert _run("Generation_Energy_Total_kWh", old=5_000, new=50_000) == 5_000


def test_inverter_out_total_spike_rejected_smooth_false():
    assert _run("Inverter_Out_Energy_Total_kWh", old=8_000, new=80_000) == 8_000


def test_ac_discharge_total_spike_rejected_smooth_false():
    assert _run("AC_Discharge_Energy_Total_kWh", old=3_000, new=30_000) == 3_000


# ---------------------------------------------------------------------------
# Tests: decreases must be rejected
# ---------------------------------------------------------------------------

def test_import_total_decrease_rejected():
    assert _run("Import_Energy_Total_kWh", old=20_000, new=19_000) == 20_000


def test_generation_total_decrease_rejected():
    assert _run("Generation_Energy_Total_kWh", old=5_000, new=4_900) == 5_000


# ---------------------------------------------------------------------------
# Tests: normal small increments must pass through
# ---------------------------------------------------------------------------

def test_import_total_small_increment_passes():
    assert _run("Import_Energy_Total_kWh", old=20_000, new=20_000.1) == 20_000.1


def test_export_total_small_increment_passes():
    assert _run("Export_Energy_Total_kWh", old=7_841, new=7_841.5) == 7_841.5


def test_generation_total_small_increment_passes():
    """smooth=False entity: small increment must pass Fix 4."""
    assert _run("Generation_Energy_Total_kWh", old=5_000, new=5_001) == 5_001


def test_import_total_just_below_threshold_passes():
    """24% jump is just under smoothRate=0.25 — must pass."""
    old = 10_000
    new = old * 1.24
    assert _run("Import_Energy_Total_kWh", old=old, new=new) == new


def test_import_total_just_above_threshold_rejected():
    """26% jump is just over smoothRate=0.25 — must be rejected."""
    old = 10_000
    new = old * 1.26
    assert _run("Import_Energy_Total_kWh", old=old, new=new) == old


# ---------------------------------------------------------------------------
# Tests: midnight window (00:00–00:04) — Today resets must pass through
# ---------------------------------------------------------------------------

def test_today_stat_at_00_00_passes():
    """Exact midnight reset must pass through."""
    t = datetime.datetime(2024, 6, 16, 0, 0, 0)
    assert _run("Load_Energy_Today_kWh", old=45, new=0.1, inv_time=t, last=t.isoformat()) == 0.1


def test_today_stat_at_00_04_passes():
    """Reset at 00:04 (inverter clock drift) must still pass through."""
    t = datetime.datetime(2024, 6, 16, 0, 4, 0)
    assert _run("Load_Energy_Today_kWh", old=45, new=0.1, inv_time=t, last=t.isoformat()) == 0.1


def test_today_stat_at_00_05_blocked():
    """At 00:05 the grace window has closed — a large drop should be rejected."""
    t = datetime.datetime(2024, 6, 16, 0, 5, 0)
    assert _run("Load_Energy_Today_kWh", old=45, new=0.1, inv_time=t, last=t.isoformat()) == 45


def test_today_stat_mid_morning_blocked():
    """A drop at 10:00 is not a midnight reset and must be rejected."""
    t = datetime.datetime(2024, 6, 16, 10, 0, 0)
    assert _run("Load_Energy_Today_kWh", old=45, new=0.1, inv_time=t, last=t.isoformat()) == 45


# ---------------------------------------------------------------------------
# Tests: smoother setting affects threshold
# ---------------------------------------------------------------------------

def test_medium_smoother_rejects_40pct_jump():
    """smoothRate=0.35 for 'medium' — 40% jump must be rejected."""
    assert _run("Import_Energy_Total_kWh", old=10_000, new=14_000, smoother="medium") == 10_000


def test_low_smoother_allows_40pct_jump():
    """smoothRate=0.50 for 'low' — 40% jump is under threshold and must pass."""
    assert _run("Import_Energy_Total_kWh", old=10_000, new=14_000, smoother="low") == 14_000
