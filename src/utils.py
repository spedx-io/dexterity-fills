import os
from typing import List

from solders.pubkey import Pubkey
from dexteritysdk.utils.aob.state import Side
from dataclasses import dataclass
from decimal import *
from anchorpy import Event
from dotenv import load_dotenv
from anchorpy import EventParser, Idl, Program, Provider

load_dotenv()

def decimal_from_int(num) -> Decimal:
    return Decimal(num).scaleb(-8)

idl_path = os.path.join(os.path.dirname(__file__), "./dex.json")
with open(idl_path, "r") as f:
    idl = Idl.from_json(f.read())
dex_program_id = os.getenv("DEX_PROGRAM_ID", Pubkey.from_string("FUfpR31LmcP1VSbz5zDaM7nxnH55iBHkpwusgrnhaFjL"))
program = Program(idl, dex_program_id, provider=Provider.readonly())
event_parser = EventParser(Pubkey.from_string(dex_program_id), program.coder)

@dataclass
class OrderFillEvent:
    market_product_group: Pubkey
    product: str
    maker_trader_risk_group: Pubkey
    maker_order_id: int
    maker_order_nonce: int
    maker_client_order_id: int
    maker_fee: Decimal
    taker_trader_risk_group: Pubkey
    taker_order_id: int
    taker_order_nonce: int
    taker_client_order_id: int
    taker_fee: Decimal
    taker_side: Side
    price: Decimal
    base_size: Decimal
    quote_size: Decimal

    @classmethod
    def from_event(cls, event: Event):
        assert event.name == cls.__name__
        return cls(
            market_product_group=event.data.market_product_group,
            product=bytes(event.data.product).decode("utf-8").strip(),
            maker_trader_risk_group=event.data.maker_trader_risk_group,
            maker_order_id=event.data.maker_order_id,
            maker_order_nonce=event.data.maker_order_nonce,
            maker_client_order_id=event.data.maker_client_order_id,
            maker_fee=decimal_from_int(event.data.maker_fee),
            taker_trader_risk_group=event.data.taker_trader_risk_group,
            taker_order_id=event.data.taker_order_id,
            taker_order_nonce=event.data.taker_order_nonce,
            taker_client_order_id=event.data.taker_client_order_id,
            taker_fee=decimal_from_int(event.data.taker_fee),
            taker_side=Side(event.data.taker_side.index),
            price=decimal_from_int(event.data.price),
            base_size=decimal_from_int(event.data.base_size),
            quote_size=decimal_from_int(event.data.quote_size),
        )


def parse_events_from_logs(logs: List[str]):
    parsed = []
    events = []
    logs = [x for x in logs if not x.startswith("Program log: ")]
    def handle_event(event):
        parsed.append(event)
    event_parser.parse_logs(logs, handle_event)
    for event in parsed:
        if event.name.startswith(OrderFillEvent.__name__):
                events.append(OrderFillEvent.from_event(event))
        else:
            pass
    return events