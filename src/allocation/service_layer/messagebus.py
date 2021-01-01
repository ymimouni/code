import logging
from collections import deque
from typing import List, Dict, Callable, Type, Union, Deque

from allocation.domain import events, commands
from allocation.service_layer import unit_of_work, handlers

logger = logging.getLogger(__name__)

Message = Union[events.Event, commands.Command]


def handle(message: Message, uow: unit_of_work.AbstractUnitOfWork):
    results = []
    queue = deque()
    queue.append(message)
    while queue:
        message = queue.popleft()
        if isinstance(message, events.Event):
            handle_event(message, queue, uow)
        elif isinstance(message, commands.Command):
            cmd_result = handle_command(message, queue, uow)
            results.append(cmd_result)
        else:
            raise Exception(f'{message} was not an Event or Command')
    return results


def handle_event(
        event: events.Event,
        queue: Deque,
        uow: unit_of_work.AbstractUnitOfWork
):
    for handler in EVENT_HANDLERS[type(event)]:
        try:
            logger.debug('handling event %s with handler %s', event, handler)
            handler(event, uow=uow)
            queue.extend(uow.collect_new_events())
        except Exception:
            logger.exception('Exception handling event %s', event)
            continue


def handle_command(
        command: commands.Command,
        queue: Deque[Message],
        uow: unit_of_work.AbstractUnitOfWork
):
    logger.debug('handling command %s', command)
    try:
        handler = COMMAND_HANDLERS[type(command)]
        result = handler(command, uow=uow)
        queue.extend(uow.collect_new_events())
        return result
    except Exception:
        logger.exception('Exception handling command %s', command)
        raise


EVENT_HANDLERS = {
    events.OutOfStock: [handlers.send_out_of_stock_notification],
    events.Allocated: [
        handlers.publish_allocated_event,
        handlers.add_allocation_to_read_model
    ],
    events.Deallocated: [
        handlers.remove_allocation_from_read_model,
        handlers.reallocate
    ]
}  # type: Dict[Type[events.Event], List[Callable]]

COMMAND_HANDLERS = {
    commands.Allocate: handlers.allocate,
    commands.CreateBatch: handlers.add_batch,
    commands.ChangeBatchQuantity: handlers.change_batch_quantity,
}  # type: Dict[Type[commands.Command], Callable]
