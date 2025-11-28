"""Position tracking for EdgeX and Lighter exchanges."""
import asyncio
import json
import logging
import requests
import sys
from decimal import Decimal


class PositionTracker:
    """Tracks positions on both exchanges."""

    def __init__(self, ticker: str, edgex_client, edgex_contract_id: str,
                 lighter_base_url: str, account_index: int, logger: logging.Logger):
        """Initialize position tracker."""
        self.ticker = ticker
        self.edgex_client = edgex_client
        self.edgex_contract_id = edgex_contract_id
        self.lighter_base_url = lighter_base_url
        self.account_index = account_index
        self.logger = logger

        self.edgex_position = Decimal('0')
        self.lighter_position = Decimal('0')

    async def get_edgex_position(self) -> Decimal:
        """Get EdgeX position."""
        if not self.edgex_client:
            raise Exception("EdgeX client not initialized")

        positions_data = await self.edgex_client.get_account_positions()
        if not positions_data or 'data' not in positions_data:
            self.logger.warning("No positions or failed to get positions")
            return Decimal('0')

        positions = positions_data.get('data', {}).get('positionList', [])
        if positions:
            for p in positions:
                if isinstance(p, dict) and p.get('contractId') == self.edgex_contract_id:
                    return Decimal(p.get('openSize', 0))
        return Decimal('0')

    async def get_lighter_position(self) -> Decimal:
        """Get Lighter position."""
        url = f"{self.lighter_base_url}/api/v1/account"
        headers = {"accept": "application/json"}

        current_position = None
        parameters = {"by": "index", "value": self.account_index}
        attempts = 0
        while current_position is None and attempts < 10:
            try:
                response = requests.get(url, headers=headers, params=parameters, timeout=10)
                response.raise_for_status()

                if not response.text.strip():
                    self.logger.warning("⚠️ Empty response from Lighter API for position check")
                    return self.lighter_position

                data = response.json()

                if 'accounts' not in data or not data['accounts']:
                    self.logger.warning(f"⚠️ Unexpected response format from Lighter API: {data}")
                    return self.lighter_position

                positions = data['accounts'][0].get('positions', [])
                for position in positions:
                    if position.get('symbol') == self.ticker:
                        current_position = Decimal(position['position']) * position['sign']
                        break
                if current_position is None:
                    current_position = 0

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"⚠️ Network error getting position: {e}")
            except json.JSONDecodeError as e:
                self.logger.warning(f"⚠️ JSON parsing error in position response: {e}")
                self.logger.warning(f"Response text: {response.text[:200]}...")
            except Exception as e:
                self.logger.warning(f"⚠️ Unexpected error getting position: {e}")
            finally:
                attempts += 1
                await asyncio.sleep(1)

        if current_position is None:
            self.logger.error(f"❌ Failed to get Lighter position after {attempts} attempts")
            sys.exit(1)

        return current_position

    def update_edgex_position(self, delta: Decimal):
        """Update EdgeX position by delta."""
        self.edgex_position += delta

    def update_lighter_position(self, delta: Decimal):
        """Update Lighter position by delta."""
        self.lighter_position += delta

    def get_current_edgex_position(self) -> Decimal:
        """Get current EdgeX position (cached)."""
        return self.edgex_position

    def get_current_lighter_position(self) -> Decimal:
        """Get current Lighter position (cached)."""
        return self.lighter_position

    def get_net_position(self) -> Decimal:
        """Get net position across both exchanges."""
        return self.edgex_position + self.lighter_position
