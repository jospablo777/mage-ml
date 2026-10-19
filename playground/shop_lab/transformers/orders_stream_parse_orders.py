from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def parse_orders(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    """
    One clean fact per order. An order with a negative quantity raises: with the dead-letter
    sink, that order goes there with the error and the others flow on.
    """
    facts = []
    for order in messages:
        for item in order['items']:
            if item['quantity'] <= 0:
                raise ValueError(f"order {order['order_id']}: quantity {item['quantity']}")
        total = sum(i['quantity'] * i['unit_price'] for i in order['items'])
        facts.append(dict(
            order_id=order['order_id'],
            customer=order['customer'],
            items=len(order['items']),
            total_cents=round(total * 100),
            currency=order['currency'],
        ))
    return facts
