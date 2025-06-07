# core/entry_logic.py
"""
Orchestrator für die Einstiegslogik: Wrapper um die Konfigurations-Logik aus config.entry_logic.
Verwendet den Spread, um den Entry-Preis-Offset an den Broker-Spread anzupassen.
"""
from typing import List, Optional, Dict
from config.entry_logic import ConfigEntryLogic
from config.phase import Candle
from core.phase_manager import PhaseStateMachine



class EntryLogicManager:
    """
    Wrapper um die Logik aus config.entry_logic.
    - Spread ist Pflichtparameter.
    """
    def __init__(self, phase_machine: PhaseStateMachine, spread: float):
        self.spread = spread
        self.logic = ConfigEntryLogic(phase_machine, spread)

    def check_buy_stop(
        self,
        candles: List[Candle],
        current_ask: float,
        stop_level: float,
        tick_size: float
    ) -> Optional[Dict[str, float]]:
        return self.logic.check_buy_stop(candles, current_ask, stop_level, tick_size)

    def check_sell_stop(
        self,
        candles: List[Candle],
        current_bid: float,
        stop_level: float,
        tick_size: float
    ) -> Optional[Dict[str, float]]:
        return self.logic.check_sell_stop(candles, current_bid, stop_level, tick_size)
