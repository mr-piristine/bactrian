import abc
from dataclasses import dataclass, field
from itertools import count
from typing import Any, Dict, List, Type

class ExchangeIdGenerator:
    """Generates a thread-safe, auto-incrementing, arbitrarily large identifier."""
    _counter = count(1)
    _prefix = "EXCH"

    @classmethod
    def next_id(cls) -> str:
        return f"{cls._prefix}-{next(cls._counter)}"

@dataclass
class Exchange:
    """
    The immutable-style container that encapsulates message data, 
    metadata headers, exceptions, and unique tracking lineage identifiers.
    """
    body: Any
    headers: Dict[str, Any] = field(default_factory=dict)
    exception: Any = None
    id: str = field(default_factory=ExchangeIdGenerator.next_id)

    def set_header(self, key: str, value: Any) -> None:
        self.headers[key] = value

    def get_header(self, key: str, default: Any = None) -> Any:
        return self.headers.get(key, default)

    def derive_new(self, new_body: Any, extra_headers: Dict[str, Any] = None) -> 'Exchange':
        """
        Creates a brand new Exchange object.
        Preserves original headers, applies updates, and stamps a new sequence ID 
        while embedding the structural ancestor/parent reference for complete data lineage.
        """
        copied_headers = self.headers.copy()
        if extra_headers:
            copied_headers.update(extra_headers)
            
        # Lineage and audit tracking
        copied_headers["parent_exchange_id"] = self.id
        
        return Exchange(body=new_body, headers=copied_headers)

class Processor(abc.ABC):
    """Abstract Base Class that all individual functional steps must implement."""
    @abc.abstractmethod
    def process(self, exchange: Exchange) -> Exchange:
        """Consumes an Exchange and explicitly returns a brand new derived Exchange."""
        pass

class RouteRegistry:
    """Centralized mapping registry pairing events with an ordered processing sequence."""
    _mappings: Dict[str, List[Type[Processor]]] = {}

    @classmethod
    def register_route(cls, event_type: str, processors: List[Type[Processor]]) -> None:
        cls._mappings[event_type] = processors

    @classmethod
    def get_route(cls, event_type: str) -> List[Type[Processor]]:
        return cls._mappings.get(event_type, [])

class EventEngine:
    """The central execution context that sequentially routes Exchanges down the processing pipe."""
    
    def route(self, event_type: str, initial_exchange: Exchange) -> Exchange:
        processor_classes = RouteRegistry.get_route(event_type)
        
        if not processor_classes:
            print(f"[Bactrian Engine] [Warning] No route mapped for event: '{event_type}'")
            return initial_exchange
        
        current_exchange = initial_exchange
        
        for processor_cls in processor_classes:
            try:
                # Instantiate and run the execution step
                processor_instance = processor_cls()
                next_exchange = processor_instance.process(current_exchange)
                
                print(f"  [Engine Log] Step: {processor_cls.__name__} | Evolution: {current_exchange.id} >>> {next_exchange.id}")
                current_exchange = next_exchange
                
            except Exception as e:
                print(f"  [Engine Log] [Critical Error] Pipeline failed at {processor_cls.__name__}: {e}")
                current_exchange.exception = e
                break
                
        return current_exchange