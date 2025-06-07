import requests
import functools
import time
from dotenv import load_dotenv, find_dotenv
import os
from typing import Optional, List, Dict
import json
from datetime import datetime, timedelta

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
            if not self.token_is_valid() or not self.validate_session():
                self.authenticate()
            return func(self, *args, **kwargs)
        return wrapper
    
    #Session-Validierung
    def validate_session(self):
        url = f"{self.base_url}/api/Auth/validate"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("success", False)

        
    #Welche Konten gibt es
    @ensure_token
    def get_active_accounts(self):
        url = f"{self.base_url}/api/Account/search"
        headers = {"Authorization": f"Bearer {self.token}"}
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
        url = f"{self.base_url}/api/Account/details/{account_id}"
        headers = {"Authorization": f"Bearer {self.token}"}
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
        url = f"{self.base_url}/api/Contract/searchById"
        headers = {"Authorization": f"Bearer {self.token}"}
        payload = {"contractId": symbol_name}  # ← ja, "contractId" ist der Feldname
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data.get("contract")
        else:
            raise Exception(f"Contract lookup failed: {data.get('errorMessage')}")


    #Paare durchsuchen
    @ensure_token
    def search_contracts(self, search_text="", live=False):
        url = f"{self.base_url}/api/Contract/search"
        headers = {"Authorization": f"Bearer {self.token}"}
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

        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(days=5)

        payload = {
            "contractId": contract_id,
            "unit": unit,
            "unitNumber": unit_number,
            "limit": limit,
            "live": live,
            "includePartialBar": include_partial_bar,
            "startTime": start_time.isoformat() + "Z",
            "endTime": end_time.isoformat() + "Z"
        }

        print("→ Request an API:", json.dumps(payload, indent=2))
        response = self.session.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json().get("bars", [])

    #Live-Kerzen abholen
    @ensure_token
    def get_latest_candle(
        self,
        contract_id: str,
        unit: int,
        unit_number: int,
    ) -> Optional[dict]:
        """
        Liefert die jeweils letzte abgeschlossene Bar 
        im Live-Modus (z.B. für 15-Sekunden-Bars).
        """
        bars = self.get_candles(
            contract_id=contract_id,
            unit=unit,
            unit_number=unit_number,
            limit=1,                   # nur die letzte Kerze
            live=True,                 # Live-Modus
            include_partial_bar=False  # keine unvollständige Bar
        )
        return bars[0] if bars else None



    #Offene Orders abrufen
    @ensure_token
    def get_open_orders(self, account_id: int):
        url = f"{self.base_url}/api/Order/{account_id}/open"  # ← korrekt
        headers = {"Authorization": f"Bearer {self.token}"}
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
        headers = {"Authorization": f"Bearer {self.token}"}
        payload = {"orderId": order_id}
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data.get("order")
        else:
            raise Exception(f"Failed to retrieve order details: {data.get('errorMessage')}")
        
        
    #Realtime-Kurse oder Quotes (weis ich nicht?)    
    @ensure_token
    def get_quote(self, contract_id: str):
        url = f"{self.base_url}/api/Quote/{contract_id}"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()




    #Übersicht aller Positionen vom Konto
    @ensure_token
    def get_positions(self, account_id: int):
        url = f"{self.base_url}/api/Position/search"
        headers = {"Authorization": f"Bearer {self.token}"}
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
        headers = {"Authorization": f"Bearer {self.token}"}
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
        headers = {"Authorization": f"Bearer {self.token}"}
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
        headers = {"Authorization": f"Bearer {self.token}"}
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
    def get_contract_details(self, contract_id):
        url = f"{self.base_url}/api/Contract/details"
        headers = {"Authorization": f"Bearer {self.token}"}
        payload = {"contractId": contract_id}
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data.get("contract")
        else:
            raise Exception(f"Failed to retrieve contract details: {data.get('errorMessage')}")

        
        
    #Order platzieren
    @ensure_token
    def place_order(self, account_id, contract_id, order_type, side, size, limit_price=None, stop_price=None, trail_price=None, custom_tag=None, linked_order_id=None):
        url = f"{self.base_url}/api/Order/place"
        headers = {"Authorization": f"Bearer {self.token}"}
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
        headers = {"Authorization": f"Bearer {self.token}"}
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
        headers = {"Authorization": f"Bearer {self.token}"}
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
        headers = {"Authorization": f"Bearer {self.token}"}
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
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("success", False)




