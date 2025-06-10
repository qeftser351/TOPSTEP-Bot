import requests
import functools
import time
from dotenv import load_dotenv, find_dotenv
import os
from typing import Optional, List, Dict
import json
from datetime import datetime, timedelta
import pandas as pd


load_dotenv()  # Umgebungsvariablen (API_USER/API_KEY) laden

class ProjectXAPI:
    def __init__(self, username, api_key):
        self.base_url = os.getenv("API_BASE_URL", "https://api.topstepx.com")
        self.username = username
        self.api_key = api_key
        self.token = None
        self.token_timestamp = 0
        self.token_lifetime = 60 * 60 * 23  # 23 Stunden, konservativ
        self.session = requests.Session()

        self.authenticate()  # Initial holen

    def _auth_header(self) -> dict:
        return {"Authorization": f"Bearer {self.token}"}


    def authenticate(self):
        url = f"{self.base_url}/api/Auth/loginKey"
        # korrekt: Feldnamen userName und apiKey, nicht dein Username als Key
        payload = {
            "userName": self.username,
            "apiKey":   self.api_key
        }
        # Debug-Ausgaben wie im Testskript
        print("→ Versuche, mich einzuloggen mit URL:", url)
        print("→ Payload:", payload)
        r = requests.post(url, json=payload, timeout=10)
        print("→ Request-URL:   ", r.request.url)
        print("→ Request-Body:  ", r.request.body)
        print("→ Request-Headers:", r.request.headers)
        print("→ Response-Status:", r.status_code)
        print("→ Response-Body:", r.text)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            ec = data.get("errorCode", "n/a")
            raise Exception(f"Authentication failed (errorCode={ec})")
        self.token = data["token"]
        self.token_timestamp = time.time()

    # Simple Zeitprüfung, Option: Validierung über API
    def token_is_valid(self):
        return self.token and (time.time() - self.token_timestamp < self.token_lifetime)

    #Überprüfung des Tokens
    def ensure_token(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            max_retries = 3
            delay = 1

            min_interval = 0.3  # z.B. 300 ms minimaler Abstand, experimentell anpassen
            now = time.time()
            if hasattr(self, '_last_api_call_time'):
                elapsed = now - self._last_api_call_time
                if elapsed < min_interval:
                    wait = min_interval - elapsed
                    print(f"[RATE LIMIT] Warte {wait:.2f}s vor nächstem API-Call")
                    time.sleep(wait)

            for attempt in range(max_retries):
                try:
                    if not self.token_is_valid() or not self.validate_session():
                        self.authenticate()
                    result = func(self, *args, **kwargs)
                    self._last_api_call_time = time.time()  # Zeitstempel aktualisieren
                    return result
                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code == 503:
                        print(f"[WARN] 503 Service Unavailable, Retry {attempt+1}/{max_retries} in {delay}s...")
                        time.sleep(delay)
                        delay *= 2
                        continue
                    raise
            raise RuntimeError(f"API 503 Service Unavailable nach {max_retries} Versuchen")
        return wrapper


    
    #Session-Validierung
    def validate_session(self):
        url = f"{self.base_url}/api/Auth/validate"
        headers = self._auth_header()
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("success", False)

        
    #Welche Konten gibt es
    @ensure_token
    def get_active_accounts(self):
        url = f"{self.base_url}/api/Account/search"
        headers = self._auth_header()
        payload = {"onlyActiveAccounts": True}
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data.get("accounts")
        else:
            raise Exception(f"Failed to retrieve accounts: {data.get('errorMessage')}")
        
        
    #Wie ist der Stand (Balance, Margin, etc.) vom Konto
    @ensure_token
    def get_account_details(self, account_id):
        url = f"{self.base_url}/api/Account/details"  # ohne /{account_id}
        headers = self._auth_header()
        payload = {"accountId": account_id}
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data.get("account")
        else:
            raise Exception(f"Failed to retrieve account details: {data.get('errorMessage')}")

        


        


    @ensure_token
    def get_contract_by_name(self, symbol_name: str):
        # symbol_name ist z.B. "MNQM5"
        contracts = self.search_contracts(search_text=symbol_name)
        for contract in contracts:
            if contract["name"] == symbol_name:
                return contract
        raise Exception(f"Contract {symbol_name} nicht gefunden in Suchergebnis")





    #Paare durchsuchen
    @ensure_token
    def search_contracts(self, search_text="", live=False):
        url = f"{self.base_url}/api/Contract/search"
        headers = self._auth_header()
        payload = {
            "searchText": search_text,
            "live": live
        }
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data.get("contracts", [])
        else:
            raise Exception(f"Contract search failed: {data.get('errorMessage')}")


    
    #historische Kerzen abrufen
    @ensure_token
    def get_candles(
        self,
        contract_id: str,
        unit: int,
        unit_number: int,
        limit: int = 100,
        live: bool = False,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        include_partial_bar: bool = False
    ):
        url = f"{self.base_url}/api/History/retrieveBars"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

        payload = {
            "contractId": contract_id,
            "unit": unit,
            "unitNumber": unit_number,
            "limit": limit,
            "live": live,
            "includePartialBar": include_partial_bar
        }

        # Nur bei historischen Abfragen ein Zeitfenster setzen
        if not live:
            if end_time is None:
                end_time = datetime.utcnow()
            if start_time is None:
                start_time = end_time - timedelta(days=5)

            payload["startTime"] = start_time.isoformat() + "Z"
            payload["endTime"] = end_time.isoformat() + "Z"

        response = self.session.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json().get("bars", [])








    #"Live"-Kerzen abholen
    @ensure_token
    def get_latest_candle(
        self,
        contract_id: str,
        unit: int,
        unit_number: int,
    ) -> Optional[dict]:
        now = datetime.utcnow()
        start_time = now - timedelta(seconds=unit * unit_number * 2)

        bars = self.get_candles(
            contract_id=contract_id,
            unit=unit,
            unit_number=unit_number,
            limit=10,  # Mehr als 1, damit mehrere Bars für Sortierung vorliegen
            live=False,
            start_time=start_time,
            end_time=now,
            include_partial_bar=True
        )
    
        print(f"[LIVE DEBUG] Raw Bars von API ({contract_id}): {bars}")

        if not bars:
            return None

        # Sortiere Bars nach Zeitstempel aufsteigend
        bars_sorted = sorted(bars, key=lambda x: x["t"])

        # Nehme die Bar mit dem neuesten Zeitstempel (kann Partial Bar sein)
        bar = bars_sorted[-1]

        return {
            "timestamp": pd.to_datetime(bar["t"], utc=True).to_pydatetime().replace(tzinfo=None),
            "open": bar["o"],
            "high": bar["h"],
            "low":  bar["l"],
            "close": bar["c"],
            "volume": bar["v"]
        }









    #Offene Orders abrufen
    @ensure_token
    def get_open_orders(self, account_id: int):
        url = f"{self.base_url}/api/Order/{account_id}/open"  # ← korrekt
        headers = self._auth_header()
        response = requests.get(url, headers=headers)
        if response.status_code == 404:
            print(f"⚠️  Keine offenen Orders für Konto {account_id} gefunden.")
            return []  # Leere Liste zurückgeben
        response.raise_for_status()
        return response.json()


    #Order-Details
    @ensure_token
    def get_order_details(self, order_id):
        url = f"{self.base_url}/api/Order/details"
        headers = self._auth_header()
        payload = {"orderId": order_id}
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data.get("order")
        else:
            raise Exception(f"Failed to retrieve order details: {data.get('errorMessage')}")
        

    @ensure_token
    def get_current_price(self, contract_id: str) -> dict:
        """
        Liefert den aktuellen Marktpreis (Bid, Ask) über Contract Details oder Latest Candle.
        """
        contract = self.get_contract_details_by_id(contract_id)
        bid = contract.get("bid")
        ask = contract.get("ask")
    
        # Falls nicht vorhanden, versuche den letzten Candle-Preis als Fallback
        if bid is None or ask is None:
            candle = self.get_latest_candle(contract_id, unit=1, unit_number=1)
            if candle is not None:
             # Nutze Close als Ersatz
                bid = candle["close"]
                ask = candle["close"]
            else:
                raise RuntimeError(f"Keine Preisinformationen für Contract {contract_id} verfügbar")

        return {"bid": bid, "ask": ask}




    #Übersicht aller Positionen vom Konto
    @ensure_token
    def get_positions(self, account_id: int):
        url = f"{self.base_url}/api/Position/search"
        headers = self._auth_header()
        payload = {"accountId": account_id}
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 404:
            print(f"⚠️  Keine Positionen für Konto {account_id} gefunden.")
            return []
        response.raise_for_status()
        return response.json().get("positions", [])

        
    #Welche Trades sind alle gelaufen
    @ensure_token
    def get_position_history(self, account_id, from_time=None, to_time=None, limit=100):
        url = f"{self.base_url}/api/Position/history"
        headers = self._auth_header()
        payload = {
            "accountId": account_id,
            "from": from_time,
            "to": to_time,
            "limit": limit
        }
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data.get("positions", [])
        else:
            raise Exception(f"Failed to retrieve position history: {data.get('errorMessage')}")

        
    #spezifische Abfrage einer einzelnen Position
    @ensure_token
    def get_position_details(self, position_id):
        url = f"{self.base_url}/api/Position/details"
        headers = self._auth_header()
        payload = {"positionId": position_id}
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data.get("position")
        else:
            raise Exception(f"Failed to retrieve position details: {data.get('errorMessage')}")


    #Was für Orders wurden alles gesetzt, aber nicht ausgelöst
    @ensure_token
    def get_order_history(self, account_id, from_time=None, to_time=None, limit=100):
        url = f"{self.base_url}/api/Order/history"
        headers = self._auth_header()
        payload = {
            "accountId": account_id,
            "from": from_time,
            "to": to_time,
            "limit": limit
        }
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data.get("orders", [])
        else:
            raise Exception(f"Failed to retrieve order history: {data.get('errorMessage')}")


    #Volumen/Margin-Handling
    @ensure_token
    def get_contract_details(self, contract_id: str) -> dict:
        url = f"{self.base_url}/api/Contract/searchById"
        headers = self._auth_header()
        payload = {"contractId": contract_id}

        print(f"→ GET CONTRACT DETAILS via searchById mit contractId: {contract_id}")
        print("→ Payload:", payload)

        response = self.session.post(url, json=payload, headers=headers)
        print("→ Response:", response.status_code, response.text)

        response.raise_for_status()
        data = response.json()
        contracts = data.get("contracts") or data.get("contract")
        if contracts:
            return contracts[0] if isinstance(contracts, list) else contracts
        raise RuntimeError("No contract found in response.")



    #entscheidet welche get_contract-Variante gezogen werden muss
    @ensure_token
    def get_contract(self, contract_id: str) -> dict:
        return self.get_contract_by_name(contract_id)


    @ensure_token
    def get_quote_by_symbol(self, symbol_name: str) -> dict:
        url = f"{self.base_url}/api/Quote/{symbol_name}"
        headers = self._auth_header()
        response = self.session.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    

    @ensure_token
    def get_contract_details_by_id(self, contract_id: str) -> dict:
        url = f"{self.base_url}/api/Contract/searchById"
        headers = self._auth_header()
        payload = {"contractId": contract_id}

        print(f"→ GET CONTRACT DETAILS via /searchById mit contractId: {contract_id}")
        print("→ Payload:", payload)

        response = self.session.post(url, json=payload, headers=headers)

        # Debug-Ausgabe Roh-Antwort
        print(f"→ Response Status: {response.status_code}")
        print(f"→ Response Body: {response.text}")

        response.raise_for_status()
        data = response.json()
        contract = data.get("contract")
        if contract:
            return contract
        raise RuntimeError("No contract found in response.")



    @ensure_token
    def get_quote(self, contract_id: str) -> dict:
        url = f"{self.base_url}/api/Quote"
        headers = self._auth_header()
        headers["Content-Type"] = "application/json"
        payload = {"contractId": contract_id}
    
        print(f"[DEBUG] API-Aufruf get_quote mit contract_id: {contract_id}")
        print(f"[DEBUG] URL: {url}")
        print(f"[DEBUG] Payload: {payload}")
    
        response = self.session.post(url, json=payload, headers=headers)
        print(f"[DEBUG] Response Status: {response.status_code}")
        print(f"[DEBUG] Response Text: {response.text}")
    
        if response.status_code == 404:
            print(f"[WARN] Quote für Contract-ID {contract_id} nicht gefunden, versuche Fallback mit get_latest_candle()")
            candle = self.get_latest_candle(contract_id, unit=1, unit_number=1)
            if candle:
                print(f"[DEBUG] Fallback Candle gefunden: close={candle['close']}")
                return {"bid": candle["close"], "ask": candle["close"]}
            else:
                print(f"[ERROR] Kein Fallback Candle für Contract-ID {contract_id} verfügbar")
                response.raise_for_status()
    
        response.raise_for_status()
        data = response.json()
        print(f"[DEBUG] Quote-Daten: {data}")
    
        bid = data.get("bid")
        ask = data.get("ask")
        if bid is None or ask is None:
            print(f"[WARN] Bid oder Ask fehlt in Quote-Daten für Contract-ID {contract_id}")
    
        return {
            "bid": bid,
            "ask": ask
        }


        
        
    #Order platzieren
    @ensure_token
    def place_order(self, account_id, contract_id, order_type, side, size, limit_price=None, stop_price=None, trail_price=None, custom_tag=None, linked_order_id=None):
        url = f"{self.base_url}/api/Order/place"
        headers = self._auth_header()
        payload = {
            "accountId": account_id,
            "contractId": contract_id,
            "type": order_type,
            "side": side,
            "size": size,
            "limitPrice": limit_price,
            "stopPrice": stop_price,
            "trailPrice": trail_price,
            "customTag": custom_tag,
            "linkedOrderId": linked_order_id
        }
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data.get("orderId")
        else:
            raise Exception(f"Order placement failed: {data.get('errorMessage')}")
        
    #Order stornieren
    @ensure_token
    def cancel_order(self, order_id):
        url = f"{self.base_url}/api/Order/cancel"
        headers = self._auth_header()
        payload = {"orderId": order_id}
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return True
        else:
            raise Exception(f"Failed to cancel order: {data.get('errorMessage')}")
    
    
    #Methode zum Verändern des StopLoss (BE und Trailing)
    @ensure_token
    def update_position_stop(self, position_id, stop_loss=None, take_profit=None):
        url = f"{self.base_url}/api/Position/updateStop"
        headers = self._auth_header()
        payload = {
            "positionId": position_id,
            "stopLoss": stop_loss,
            "takeProfit": take_profit
        }
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if not data.get("success"):
            raise Exception(f"Failed to update position stop: {data.get('errorMessage')}")
        return True
    
    #offenen Trade schließen (bin ich mir nicht sicher ob wir den brauchen)
    @ensure_token
    def close_position(self, position_id):
        url = f"{self.base_url}/api/Position/close"
        headers = self._auth_header()
        payload = {"positionId": position_id}
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if not data.get("success"):
            raise Exception(f"Failed to close position: {data.get('errorMessage')}")
        return True

    #Token ausloggen
    @ensure_token
    def logout(self):
        url = f"{self.base_url}/api/Auth/logout"
        headers = self._auth_header()
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("success", False)




