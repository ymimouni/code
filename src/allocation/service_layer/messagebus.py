from collections import deque
from typing import List, Dict, Callable, Type

from allocation.domain import events
from allocation.service_layer import unit_of_work, handlers


def handle(event: events.Event, uow: unit_of_work.AbstractUnitOfWork):
    results = []
    queue = deque()
    queue.append(event)
    while queue:
        event = queue.popleft()
        for handler in HANDLERS[type(event)]:
            results.append(handler(event, uow=uow))
            queue.extend(uow.collect_new_events())
    return results


HANDLERS = {
    events.BatchCreated: [handlers.add_batch],
    events.BatchQuantityChanged: [handlers.change_batch_quantity],
    events.OutOfStock: [handlers.send_out_of_stock_notification],
    events.AllocationRequired: [handlers.allocate]
}  # type: Dict[Type[events.Event], List[Callable]]
