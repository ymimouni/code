import logging
from collections import deque
from typing import List, Dict, Callable, Type, Union

from allocation.domain import events, commands
from allocation.service_layer import unit_of_work

logger = logging.getLogger(__name__)

Message = Union[events.Event, commands.Command]


class MessageBus:

    def __init__(self,
                 uow: unit_of_work.AbstractUnitOfWork,
                 event_handlers: Dict[Type[events.Event], List[Callable]],
                 command_handlers: Dict[Type[commands.Command], Callable]):
        self.uow = uow
        self.event_handlers = event_handlers
        self.command_handlers = command_handlers
        self.queue = None

    def handle(self, message: Message):
        self.queue = deque()
        self.queue.append(message)
        while self.queue:
            message = self.queue.popleft()
            if isinstance(message, events.Event):
                self.handle_event(message)
            elif isinstance(message, commands.Command):
                self.handle_command(message)
            else:
                raise Exception(f'{message} was not an Event or Command')

    def handle_event(self, event: events.Event):
        for handler in self.event_handlers[type(event)]:
            try:
                logger.debug('handling event %s with handler %s', event, handler)
                handler(event)
                self.queue.extend(self.uow.collect_new_events())
            except Exception:
                logger.exception('Exception handling event %s', event)
                continue

    def handle_command(self, command: commands.Command):
        logger.debug('handling command %s', command)
        try:
            handler = self.command_handlers[type(command)]
            handler(command)
            self.queue.extend(self.uow.collect_new_events())
        except Exception:
            logger.exception('Exception handling command %s', command)
            raise
