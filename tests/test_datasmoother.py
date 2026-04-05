"""
Tests for the dataSmoother2 spike-rejection logic in GivTCP/read.py.

Covers all 4 entity categories and the key spike scenarios from the v3 spike-fix work:
  - Import/Export/Load/PV _Total (smooth=True, onlyIncrease=True)
  - Generation/Inverter_Out/AC_Discharge _Total (smooth=False, onlyIncrease=True)
  - Decrease rejection
  - Normal small increments that must pass through
  - Today stats at midnight (special-case pass-through)

Run with:  pytest tests/test_datasmoother.py -v
"""

import datetime
import sys
import types
import logging

import pytest

# ---------------------------------------------------------------------------
# Minimal stubs so we can import just dataSmoother2 without pulling in the
# entire GivTCP runtime (Redis, MQTT, Modbus libs, HA supervisor, etc.)
# ---------------------------------------------------------------------------

# Stub: GEType (mirrors entity_lut.GEType)
class GEType:
    def __init__(self, smooth, onlyIncrease, allowZero=False, min=0, max="maxTotalEnergy"):
        self.smooth = smooth
        self.onlyIncrease = onlyIncrease
        self.allowZero = allowZero
        self.min = min
        self.max = max

# Stub: maxvalues (copied from GivLUT.py)
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

# Stub: entity lookup table — only the entities we test
ENTITY_LUT = {
    # smooth=True, onlyIncrease=True
    "Import_Energy_Total_kWh":      GEType(smooth=True,  onlyIncrease=True),
    "Export_Energy_Total_kWh":      GEType(smooth=True,  onlyIncrease=True),
    "Load_Energy_Today_kWh":        GEType(smooth=True,  onlyIncrease=True,  max="maxTodayEnergy"),
    "PV_Energy_Total_kWh":          GEType(smooth=True,  onlyIncrease=True),
    # smooth=False, onlyIncrease=True  (Fix 4 is their ONLY upward-spike protection)
    "Generation_Energy_Total_kWh":  GEType(smooth=False, onlyIncrease=True),
    "Inverter_Out_Energy_Total_kWh":GEType(smooth=False, onlyIncrease=True),
    "AC_Discharge_Energy_Total_kWh":GEType(smooth=False, onlyIncrease=True),
}

# Stub: GiV_Settings (data_smoother controls smoothRate threshold)
class _FakeSettings:
    data_smoother = "high"   # smoothRate = 0.25  (strictest — worst-case test)

# Install stubs as fake modules so read.py-style code can reference them
_fake_settings_mod = types.ModuleType("settings")
_fake_settings_mod.GiV_Settings = _FakeSettings()

# ---------------------------------------------------------------------------
# Inline copy of dataSmoother2 with stubs wired in.
# Keep this in sync with GivTCP/read.py whenever that function changes.
# ---------------------------------------------------------------------------
logger = logging.getLogger("test_datasmoother")

def dataSmoother2(dataNew, dataOld, lastUpdate, invtype, inv_time,
                  *, _settings=_FakeSettings, _lut=ENTITY_LUT,
                  _maxvalues=maxvalues):
    """Inlined copy of read.py::dataSmoother2 with injected stubs."""
    try:
        newData = dataNew[1]
        oldData = dataOld[1]
        name    = dataNew[0]
        lookup  = _lut[name]

        if newData is None or oldData is None:
            logger.debug("Nonetype in old or new data for %s so using new value", name)
            return newData

        if _settings.data_smoother.lower() == "high":
            smoothRate = 0.25
            abssmooth  = 1000
        elif _settings.data_smoother.lower() == "medium":
            smoothRate = 0.35
            abssmooth  = 5000
        else:
            smoothRate = 0.50
            abssmooth  = 7000

        if not isinstance(newData, (int, float)):
            return newData

        if "3ph" not in invtype:
            min_ = _maxvalues.single_phase[lookup.min] if isinstance(lookup.min, str) else lookup.min
            max_ = _maxvalues.single_phase[lookup.max] if isinstance(lookup.max, str) else lookup.max
        else:
            min_ = _maxvalues.three_phase[lookup.min] if isinstance(lookup.min, str) else lookup.min
            max_ = _maxvalues.three_phase[lookup.max] if isinstance(lookup.max, str) else lookup.max

        now  = inv_time
        then = datetime.datetime.fromisoformat(lastUpdate)

        # Special-case: midnight Today stats (5-min window for inverter clock drift)
        if now.hour == 0 and now.minute < 5 and "Today" in name:
            logger.debug("Midnight and %s so accepting value as is: %s", name, newData)
            return newData

        # Discard non-allowed zeros
        if newData == 0 and not lookup.allowZero:
            logger.debug("%s is Zero so using old value", name)
            return oldData

        # Min/Max bounds
        if newData < float(min_) or newData > float(max_):
            logger.debug("%s is outside allowable bounds: %s", name, newData)
            return oldData

        # onlyIncrease: reject decreases, and (Fix 4) reject rapid increases
        if lookup.onlyIncrease:
            if (oldData - newData) > 0.11:
                logger.debug("%s has decreased so using old value", name)
                return oldData
            if oldData > 1 and newData > oldData:
                dataDelta = (newData - oldData) / oldData
                if dataDelta > smoothRate:
                    logger.debug("%s increased too rapidly: %s->%s so using previous value",
                                 name, oldData, newData)
                    return oldData

        # Smooth data (only for smooth=True entities)
        if lookup.smooth and _settings.data_smoother.lower() != "none":
            if newData != oldData:
                if any(word in name.lower() for word in ["power", "_to_"]):
                    if abs(newData - oldData) > abssmooth:
                        # checkRawcache not available in tests; treat as "not persistent" → reject
                        logger.debug("%s jumped too far: %s->%s so using previous value",
                                     name, oldData, newData)
                        return oldData
                else:
                    if oldData != 0:
                        if abs(newData - oldData) < 1:
                            return newData
                        timeDelta = (now - then).total_seconds()
                        dataDelta = abs(newData - oldData) / oldData
                        if dataDelta > smoothRate and timeDelta < 60:
                            logger.debug("%s jumped too far in a single read: %s->%s so using previous value",
                                         name, oldData, newData)
                            return oldData

    except Exception as e:
        logger.error("dataSmoother2 Error: %s", e)
        return newData

    return newData


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_NOW = datetime.datetime(2024, 6, 15, 14, 30, 0)
_LAST = _NOW.isoformat()   # same timestamp → timeDelta ≈ 0 (mirrors v3 behaviour)
_INVTYPE = "SA"            # single-phase


def _run(name, old, new, smoother="high", invtype=_INVTYPE, inv_time=_NOW, last=_LAST):
    """Convenience wrapper."""
    class S:
        data_smoother = smoother
    return dataSmoother2([name, new], [name, old], last, invtype, inv_time, _settings=S)


# ---------------------------------------------------------------------------
# Tests: massive spikes must be rejected (Fix 4)
# ---------------------------------------------------------------------------

def test_import_total_massive_spike_rejected():
    """20,000 → 1,700,000 kWh (8500% jump) must be rejected."""
    result = _run("Import_Energy_Total_kWh", old=20_000, new=1_700_000)
    assert result == 20_000


def test_export_total_70pct_spike_rejected():
    """7,841 → 13,300 kWh (~70% jump) must be rejected at smoothRate=0.25."""
    result = _run("Export_Energy_Total_kWh", old=7_841, new=13_300)
    assert result == 7_841


def test_load_today_doubling_spike_rejected():
    """30 → 60 kWh today (100% jump) must be rejected."""
    result = _run("Load_Energy_Today_kWh", old=30, new=60)
    assert result == 30


def test_pv_total_large_spike_rejected():
    """5,000 → 10,000 kWh (100% jump) must be rejected."""
    result = _run("PV_Energy_Total_kWh", old=5_000, new=10_000)
    assert result == 5_000


# ---------------------------------------------------------------------------
# Tests: smooth=False, onlyIncrease=True — Fix 4 is their only protection
# ---------------------------------------------------------------------------

def test_generation_total_spike_rejected_smooth_false():
    """Generation_Energy_Total_kWh has smooth=False; Fix 4 must still block spikes."""
    result = _run("Generation_Energy_Total_kWh", old=5_000, new=50_000)
    assert result == 5_000


def test_inverter_out_total_spike_rejected_smooth_false():
    result = _run("Inverter_Out_Energy_Total_kWh", old=8_000, new=80_000)
    assert result == 8_000


def test_ac_discharge_total_spike_rejected_smooth_false():
    result = _run("AC_Discharge_Energy_Total_kWh", old=3_000, new=30_000)
    assert result == 3_000


# ---------------------------------------------------------------------------
# Tests: decreases must be rejected
# ---------------------------------------------------------------------------

def test_import_total_decrease_rejected():
    """Import total decreasing is not physical — must be rejected."""
    result = _run("Import_Energy_Total_kWh", old=20_000, new=19_000)
    assert result == 20_000


def test_generation_total_decrease_rejected():
    result = _run("Generation_Energy_Total_kWh", old=5_000, new=4_900)
    assert result == 5_000


# ---------------------------------------------------------------------------
# Tests: normal small increments must pass through
# ---------------------------------------------------------------------------

def test_import_total_small_increment_passes():
    """A realistic small increment (0.1 kWh) must not be rejected."""
    result = _run("Import_Energy_Total_kWh", old=20_000, new=20_000.1)
    assert result == 20_000.1


def test_export_total_small_increment_passes():
    result = _run("Export_Energy_Total_kWh", old=7_841, new=7_841.5)
    assert result == 7_841.5


def test_generation_total_small_increment_passes():
    """smooth=False entity: small increment must pass Fix 4."""
    result = _run("Generation_Energy_Total_kWh", old=5_000, new=5_001)
    assert result == 5_001


def test_import_total_just_below_threshold_passes():
    """24% jump is just under smoothRate=0.25 — must pass."""
    old = 10_000
    new = old * 1.24   # 24% increase
    result = _run("Import_Energy_Total_kWh", old=old, new=new)
    assert result == new


def test_import_total_just_above_threshold_rejected():
    """26% jump is just over smoothRate=0.25 — must be rejected."""
    old = 10_000
    new = old * 1.26   # 26% increase
    result = _run("Import_Energy_Total_kWh", old=old, new=new)
    assert result == old


# ---------------------------------------------------------------------------
# Tests: midnight window (00:00 to 00:04) — Today resets must pass through
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
    result = _run("Import_Energy_Total_kWh", old=10_000, new=14_000, smoother="medium")
    assert result == 10_000


def test_low_smoother_allows_40pct_jump():
    """smoothRate=0.50 for 'low' — 40% jump is under threshold and must pass."""
    result = _run("Import_Energy_Total_kWh", old=10_000, new=14_000, smoother="low")
    assert result == 14_000
