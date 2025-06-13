import requests
import functools
import time
from dotenv import load_dotenv, find_dotenv
import os
from typing import Optional, List, Dict
import json
from datetime import datetime, timedelta
import pandas as pd
from ws_client_signalr import MarketWSClient 
from typing import Optional

# Typisierung vermeiden harte Abhängigkeiten
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ws_client_signalr import MarketWSClient


load_dotenv()  # Umgebungsvariablen (API_USER/API_KEY) laden

class ProjectXAPI:
    def __init__(self, username, api_key):
        print("[DEBUG] ProjectXAPI geladen aus:", __file__)
        self.base_url = os.getenv("API_BASE_URL", "https://api.topstepx.com")
        self.username = username
        self.api_key = api_key
        self.token = None
        self.token_timestamp = 0
        self.token_lifetime = 60 * 60 * 23
        self.token_file = "session_token.json"
        self.session = requests.Session()
        self._last_api_call_time = 0
        self.ws_client = None

        # Nur dann authentifizieren, wenn kein gültiger Token geladen wurde
        if not self._load_token_from_file():
            self._retry_authenticate_with_backoff()
        else:
            self.session.headers.update({
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            })



    def attach_ws_client(self, ws_client):
        self.ws_client = ws_client

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
        
        # Persistenz sichern
        self._save_token_to_file()
        
        # Header zentral für alle folgenden Requests setzen
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        })

    # Simple Zeitprüfung, Option: Validierung über API
    def token_is_valid(self):
        return self.token and (time.time() - self.token_timestamp < self.token_lifetime)

    #Überprüfung des Tokens
    def ensure_token(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            max_retries = 3
            base_delay = 1.0
            min_interval = 1.0  # Minimum Abstand zwischen API-Requests (sek.)

            now = time.time()
            elapsed = now - self._last_api_call_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)

            for attempt in range(max_retries):
                try:
                    if not self.token_is_valid() or not self.validate_session():
                        self.authenticate()

                    response = func(self, *args, **kwargs)
                    self._last_api_call_time = time.time()
                    return response

                except requests.exceptions.RequestException as e:
                    is_503 = isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 503
                    if attempt < max_retries - 1 and (is_503 or isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError))):
                        delay = base_delay * (2 ** attempt)  # Exponentielles Backoff
                        print(f"[WARN] API-Fehler ({e}), Retry {attempt + 1}/{max_retries} in {delay:.1f}s")
                        time.sleep(delay)
                        continue
                    raise
            raise RuntimeError(f"API-Request fehlgeschlagen nach {max_retries} Versuchen")
        return wrapper
    
    
    def _load_token_from_file(self, path="session_token.json"):
        if not os.path.exists(path):
            return False
        try:
            with open(path, "r") as f:
                token_data = json.load(f)
            if token_data["username"] != self.username:
                print("[WARN] Token-Datei gehört zu anderem Benutzer – ignoriere sie.")
                return False
            self.token = token_data["token"]
            self.token_timestamp = token_data["timestamp"]
            if self.token_is_valid():
                print(f"[INFO] Gültiger Token aus Datei geladen.")
                return True
            else:
                print(f"[INFO] Token aus Datei ist abgelaufen.")
                return False
        except Exception as e:
            print(f"[WARN] Fehler beim Laden des Tokens: {e}")
            return False
        
        
    def clear_token_file(self):
        try:
            os.remove(self.token_file)
            print("[INFO] Token-Datei gelöscht.")
        except FileNotFoundError:
            pass



    
    #Session-Validierung
    def validate_session(self):
        url = f"{self.base_url}/api/Auth/validate"
        headers = self._auth_header()
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("success", False)
    
    def _retry_authenticate_with_backoff(self, retries=3, base_delay=1.0):
        for attempt in range(retries):
            try:
                self.authenticate()
                return  # Erfolg → raus
            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    delay = base_delay * (2 ** attempt)
                    print(f"[WARN] Authentifizierung fehlgeschlagen (Versuch {attempt+1}/{retries}): {e}. Warte {delay:.1f}s...")
                    time.sleep(delay)
                else:
                    raise RuntimeError(f"Login fehlgeschlagen nach {retries} Versuchen: {e}")

    def _save_token_to_file(self, path="session_token.json"):
        token_data = {
            "token": self.token,
            "timestamp": self.token_timestamp,
            "username": self.username
        }
        with open(path, "w") as f:
            json.dump(token_data, f)

        
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

        if not end_time:
            end_time = datetime.utcnow()

        if not start_time:
            # Dynamisch sinnvolle Zeitspanne setzen, abhängig von unit_number
            if unit == 1 and unit_number == 15:
                start_time = end_time - timedelta(minutes=5)  # max. 20 Bars ≈ stabil
            elif unit == 1 and unit_number == 180:
                start_time = end_time - timedelta(hours=1.5)
            elif unit == 1 and unit_number == 900:
                start_time = end_time - timedelta(hours=12)
            else:
                start_time = end_time - timedelta(days=5)  # Fallback für Minutenkerzen oder Tests

        payload = {
            "contractId": contract_id,
         "unit": unit,
            "unitNumber": unit_number,
            "startTime": start_time.isoformat() + "Z",
            "endTime": end_time.isoformat() + "Z",
            "limit": limit,
            "live": live,
            "includePartialBar": include_partial_bar
        }

        print("[DEBUG] Sende Candle-Request an ProjectX:")
        print("URL:", url)
        print("Payload:", json.dumps(payload, indent=2))

        response = self.session.post(url, json=payload, headers=headers)
        response.raise_for_status()

        return response.json().get("bars", [])







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
        if self.ws_client and contract_id in self.ws_client.latest_quotes:
            return self.ws_client.latest_quotes[contract_id]
        return {}





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
    def get_contract_details_by_id(self, contract_id: str) -> dict:
        url = f"{self.base_url}/api/Contract/searchById"
        headers = self._auth_header()
        payload = {"contractId": contract_id}
        response = self.session.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        contract = data.get("contract") or (data.get("contracts")[0] if data.get("contracts") else None)
        if contract:
            return contract
        raise RuntimeError("No contract found in response.")



        
        
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

    def __getattr__(self, name):
        print(f"[CRITICAL] Jemand ruft {name} auf, das existiert nicht in ProjectXAPI!")
        raise AttributeError(f"'ProjectXAPI' object has no attribute '{name}'")



