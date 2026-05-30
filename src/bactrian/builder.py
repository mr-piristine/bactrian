from typing import List, Type, Self
from bactrian.core import Processor, RouteRegistry
from typing import List, Type, Self
from .core import Processor, RouteRegistry

class RouteBuilder:
    """The DSL for fluidly building Bactrian processing routes using method chaining."""
    
    def __init__(self) -> None:
        self._event_type: str = ""
        self._processors: List[Type[Processor]] = []

    def from_event(self, event_type: str) -> Self:
        """Specifies the incoming market event trigger."""
        self._event_type = event_type
        return self

    def to(self, processor_cls: Type[Processor]) -> Self:
        """Appends a processing stage to the linear engine pipeline."""
        self._processors.append(processor_cls)
        return self

    def pretty_print(self) -> Self:
        """
        Prints a clean visual ASCII graph layout representing the configured pipeline.
        Can be chained anywhere after processors are added.
        """
        if not self._event_type:
            print("  [Event Pipeline View] Source: (Not configured yet)")
            return self
            
        # print(f"\n┌───[ Event Topology ]─────────────────────────────────────────")
        print(f"🌐 {self._event_type}")
        # print(f"   ├───[ Processor Sequence ]")
        
        if not self._processors:
            print("  │   (No processors mapped)")
        else:
            for i, processor in enumerate(self._processors, 1):
                is_last = (i == len(self._processors))
                prefix = "└───▶" if is_last else "├───▶"
                print(f"   {prefix} [{i}] {processor.__name__}")
                
        # print(f"└────────────────────────────────────────────────────────────")
        return self  # Return self to allow chaining to continue if desired

    def register(self) -> None:
        """Finalizes the route design and registers it with Bactrian's engine mapping layer."""
        if not self._event_type:
            raise ValueError("Route Build Error: Cannot register a route without a 'from_event' source.")
        if not self._processors:
            raise ValueError("Route Build Error: Cannot register an empty route pipeline.")
            
        RouteRegistry.register_route(self._event_type, self._processors)