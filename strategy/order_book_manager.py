"""Order book management for EdgeX and Lighter exchanges."""
import asyncio
import logging
from decimal import Decimal
from typing import Tuple, Optional


class OrderBookManager:
    """Manages order book state for both exchanges."""

    def __init__(self, logger: logging.Logger):
        """Initialize order book manager."""
        self.logger = logger

        # EdgeX order book state
        self.edgex_order_book = {'bids': {}, 'asks': {}}
        self.edgex_best_bid: Optional[Decimal] = None
        self.edgex_best_ask: Optional[Decimal] = None
        self.edgex_order_book_ready = False

        # Lighter order book state
        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_best_bid: Optional[Decimal] = None
        self.lighter_best_ask: Optional[Decimal] = None
        self.lighter_order_book_ready = False
        self.lighter_order_book_offset = 0
        self.lighter_order_book_sequence_gap = False
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_lock = asyncio.Lock()

    # EdgeX order book methods
    def update_edgex_order_book(self, bids: list, asks: list):
        """Update EdgeX order book with new levels."""
        # Update bids
        for bid in bids:
            price = Decimal(bid['price'])
            size = Decimal(bid['size'])
            if size > 0:
                self.edgex_order_book['bids'][price] = size
            else:
                self.edgex_order_book['bids'].pop(price, None)

        # Update asks
        for ask in asks:
            price = Decimal(ask['price'])
            size = Decimal(ask['size'])
            if size > 0:
                self.edgex_order_book['asks'][price] = size
            else:
                self.edgex_order_book['asks'].pop(price, None)

        # Update best bid and ask
        if self.edgex_order_book['bids']:
            self.edgex_best_bid = max(self.edgex_order_book['bids'].keys())
        if self.edgex_order_book['asks']:
            self.edgex_best_ask = min(self.edgex_order_book['asks'].keys())

        if not self.edgex_order_book_ready:
            self.edgex_order_book_ready = True
            self.logger.info(f"📊 EdgeX order book ready - Best bid: {self.edgex_best_bid}, "
                             f"Best ask: {self.edgex_best_ask}")
        else:
            self.logger.debug(f"📊 Order book updated - Best bid: {self.edgex_best_bid}, "
                              f"Best ask: {self.edgex_best_ask}")

    def get_edgex_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Get EdgeX best bid/ask prices."""
        return self.edgex_best_bid, self.edgex_best_ask

    # Lighter order book methods
    async def reset_lighter_order_book(self):
        """Reset Lighter order book state."""
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_offset = 0
            self.lighter_order_book_sequence_gap = False
            self.lighter_snapshot_loaded = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None

    def update_lighter_order_book(self, side: str, levels: list):
        """Update Lighter order book with new levels."""
        for level in levels:
            # Handle different data structures - could be list [price, size] or dict {"price": ..., "size": ...}
            if isinstance(level, list) and len(level) >= 2:
                price = Decimal(level[0])
                size = Decimal(level[1])
            elif isinstance(level, dict):
                price = Decimal(level.get("price", 0))
                size = Decimal(level.get("size", 0))
            else:
                self.logger.warning(f"⚠️ Unexpected level format: {level}")
                continue

            if size > 0:
                self.lighter_order_book[side][price] = size
            else:
                # Remove zero size orders
                self.lighter_order_book[side].pop(price, None)

    def validate_order_book_offset(self, new_offset: int) -> bool:
        """Validate order book offset sequence."""
        if new_offset <= self.lighter_order_book_offset:
            self.logger.warning(
                f"⚠️ Out-of-order update: new_offset={new_offset}, "
                f"current_offset={self.lighter_order_book_offset}")
            return False
        return True

    def validate_order_book_integrity(self) -> bool:
        """Validate order book integrity."""
        # Check for negative prices or sizes
        for side in ["bids", "asks"]:
            for price, size in self.lighter_order_book[side].items():
                if price <= 0 or size <= 0:
                    self.logger.error(f"❌ Invalid order book data: {side} price={price}, size={size}")
                    return False
        return True

    def get_lighter_best_levels(self) -> Tuple[Optional[Tuple[Decimal, Decimal]],
                                               Optional[Tuple[Decimal, Decimal]]]:
        """Get best bid and ask levels from Lighter order book."""
        best_bid = None
        best_ask = None

        if self.lighter_order_book["bids"]:
            best_bid_price = max(self.lighter_order_book["bids"].keys())
            best_bid_size = self.lighter_order_book["bids"][best_bid_price]
            best_bid = (best_bid_price, best_bid_size)

        if self.lighter_order_book["asks"]:
            best_ask_price = min(self.lighter_order_book["asks"].keys())
            best_ask_size = self.lighter_order_book["asks"][best_ask_price]
            best_ask = (best_ask_price, best_ask_size)

        return best_bid, best_ask

    def get_lighter_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Get Lighter best bid/ask prices."""
        return self.lighter_best_bid, self.lighter_best_ask

    def get_lighter_mid_price(self) -> Decimal:
        """Get mid price from Lighter order book."""
        best_bid, best_ask = self.get_lighter_best_levels()

        if best_bid is None or best_ask is None:
            raise Exception("Cannot calculate mid price - missing order book data")

        mid_price = (best_bid[0] + best_ask[0]) / Decimal('2')
        return mid_price

    def update_lighter_bbo(self):
        """Update Lighter best bid/ask from order book."""
        best_bid, best_ask = self.get_lighter_best_levels()
        if best_bid is not None:
            self.lighter_best_bid = best_bid[0]
        if best_ask is not None:
            self.lighter_best_ask = best_ask[0]
