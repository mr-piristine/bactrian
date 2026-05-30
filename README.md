# bactrian

[![PyPI - Version](https://img.shields.io/pypi/v/bactrian.svg)](https://pypi.org/project/bactrian)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/bactrian.svg)](https://pypi.org/project/bactrian)

-----


# Bactrian Core Module Documentation

**Bactrian** is an lightweight, event-driven integration and processing framework inspired by Apache Camel’s Exchange pattern. It is explicitly designed for data-intensive workflows where processing states must be tracked lineally, and each operational step derives a new immutable-style context state rather than modifying data in place.

---

## 1. Architecture Overview

Bactrian processes streaming data sequentially via decoupled functional blocks. The data lifecycle relies on an **evolutionary chain** where every processing block inputs a structured context, extracts or mutates data payload arrays (e.g., using Polars or Pandas), and outputs a brand new tracking instance.

* **Exchange:** The atomic unit of execution containing metadata headers and data payload chunks.
* **Processor:** Stateless functional steps executing a single discrete operation.
* **RouteRegistry:** The mapping layer linking incoming event triggers to processing pipes.
* **RouteBuilder:** The programmatic pipeline layout definition component.
* **EventEngine:** The state router that instantiates processing steps and pipes data changes forward.

---

## 2. API Reference

### Class: `ExchangeIdGenerator`

An internal thread-safe identity allocator managing high-precision, sequential integer assignments.

#### Methods

* `next_id(cls) -> str`
* **Description:** Increments the counter and returns a string identifier prefixed with `EXCH-` (e.g., `EXCH-1`, `EXCH-2`). Python natively handles arbitrarily large integers, eliminating big-int integer overflow risk.



---

### Class: `Exchange`

The envelope class carrying data across the application state. Once generated, an Exchange is typically treated as transient; any updates yield a derived Exchange with a progressive tracking identity.

#### Properties

* `body: Any` — The target data payload (e.g., a List of records, a Polars DataFrame, or Matplotlib figure metadata).
* `headers: Dict[str, Any]` — Key-value dictionary containing structural data flags and routing markers.
* `exception: Any` — Slot for tracking system errors or runtime crash stack traces.
* `id: str` — Automatically generated identifier on instantiation.

#### Methods

* `set_header(key: str, value: Any) -> None`
* Assigns or overwrites metadata headers in the active Exchange block.


* `get_header(key: str, default: Any = None) -> Any`
* Retrieves a key from the headers mapping, falling back to a default value if missing.


* `derive_new(new_body: Any, extra_headers: Dict[str, Any] = None) -> Exchange`
* **Description:** Creates and returns a fresh, downstream `Exchange` object instance.
* **Mechanics:** Shallow-copies previous headers, layers on any `extra_headers`, and tracks audit metrics by setting the key `"parent_exchange_id"` to its own active identity string before stamping a new auto-incremented ID.



---

### Class: `Processor` (Abstract)

The interface defining an independent processing unit in the routing sequence.

#### Abstract Methods

* `process(self, exchange: Exchange) -> Exchange`
* **Description:** Must be overridden by concrete classes. It ingests an incoming `Exchange`, interacts with or replaces its `.body` payload, and **must return a brand new Exchange** generated via `exchange.derive_new()`.



---

### Class: `RouteRegistry`

The storage layer managing event associations. It isolates your functional data operations from the logical pathways of your system.

#### Methods

* `register_route(cls, event_type: str, processors: List[Type[Processor]]) -> None`
* Stores an ordered sequence list of `Processor` classes corresponding to a string event trigger name.


* `get_route(cls, event_type: str) -> List[Type[Processor]]`
* Retrieves the exact list sequence mapped to the requested event identifier string.



---

### Class: `RouteBuilder`

The configuration utility used to cleanly define pipelines and visually inspect topography configurations before registering them with the engine backend.

#### Methods

* `from_event(self, event_type: str) -> Self`
* Initializes a route layout definition and updates the destination target source key.


* `to(self, processor_cls: Type[Processor]) -> Self`
* Appends a chosen `Processor` definition class to the tracking route chain.


* `pretty_print(self) -> Self`
* **Description:** Outputs a clean, structured ASCII topology tree to stdout showing the path data takes through processors.


* `register(self) -> None`
* Validates configurations and writes them to the underlying `RouteRegistry`. Raises a `ValueError` if the source event or processor pipeline lists are missing.



---

### Class: `EventEngine`

The central execution engine responsible for driving the event workflow pipeline.

#### Methods

* `route(self, event_type: str, initial_exchange: Exchange) -> Exchange`
* **Description:** Looks up the processors configured for the specified `event_type`. It instantiates the first `Processor`, fires its logic, passes the outputted derived `Exchange` into the second `Processor`, and so on.
* **Error Handling:** If any step raises an unhandled exception, execution breaks to preserve integrity. The error context is bound to `exchange.exception` and the final state before the failure is returned.



---

## 3. Implementation Blueprint Code Example

Below is an explicit setup demonstrating how to map and execute functional processing nodes using the **Bactrian Core** infrastructure without the fluent wording.

```python
import polars as pl
from bactrian import Exchange, Processor, RouteBuilder, EventEngine

# 1. Implement decoupled components 
class TickerValidationProcessor(Processor):
    def process(self, exchange: Exchange) -> Exchange:
        # Expecting raw streaming ticks list in body
        df = pl.DataFrame(exchange.body)
        
        # Clean null values
        cleaned_df = df.drop_nulls()
        
        # Derive a fresh exchange tracking stage transitions
        return exchange.derive_new(
            new_body=cleaned_df,
            extra_headers={"validated": True}
        )

class MovingAverageProcessor(Processor):
    def process(self, exchange: Exchange) -> Exchange:
        df = exchange.body
        
        # Execute rolling analytical computations via Polars
        analyzed_df = df.with_columns(
            pl.col("price").mean().over("ticker").alias("ticker_avg")
        )
        
        return exchange.derive_new(
            new_body=analyzed_df,
            extra_headers={"metrics_calculated": True}
        )

# 2. Configure mappings cleanly using RouteBuilder
builder = RouteBuilder()
builder.from_event("BINANCE_AGG_STREAM")
builder.to(TickerValidationProcessor)
builder.to(MovingAverageProcessor)

# Inspect the topology layout and write to the registry layer
builder.pretty_print()
builder.register()

# 3. Handle live market events via EventEngine
if __name__ == "__main__":
    bactrian_engine = EventEngine()
    
    mock_market_ticks = [
        {"ticker": "BTC", "price": 68500.0},
        {"ticker": "ETH", "price": 3800.0},
        {"ticker": "BTC", "price": None}, # Will be dropped by step 1
    ]
    
    # Initialize initial payload exchange wrapper
    seed_exchange = Exchange(body=mock_market_ticks, headers={"feed": "websocket_live"})
    
    # Process event
    final_state = bactrian_engine.route("BINANCE_AGG_STREAM", seed_exchange)

```