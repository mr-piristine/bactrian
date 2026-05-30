import polars as pl
import matplotlib.pyplot as plt
from bactrian import Exchange, Processor, EventEngine, RouteBuilder

# --- Step 1: Define Reusable, Isolated Financial Processors ---

class MarketAnalyticsProcessor(Processor):
    def process(self, exchange: Exchange) -> Exchange:
        # Turn incoming raw tick list into a fast Polars DataFrame
        df = pl.DataFrame(exchange.body)
        
        # Calculate moving average / metrics group
        aggregated = df.group_by("ticker").agg(pl.col("price").mean().alias("avg_price"))
        
        # Derive a new Exchange passing forward the analytical state
        return exchange.derive_new(
            new_body=aggregated,
            extra_headers={"analytics_processed": True, "engine": "Polars"}
        )

class ChartGenerationProcessor(Processor):
    def process(self, exchange: Exchange) -> Exchange:
        df = exchange.body
        
        # Generate indicator charts using Matplotlib
        if isinstance(df, pl.DataFrame):
            plt.figure(figsize=(4, 2))
            plt.bar(df["ticker"].to_list(), df["avg_price"].to_list(), color="navy")
            plt.title("Hobby Platform - Market Snapshot")
            plt.close()
            
        return exchange.derive_new(
            new_body=df,
            extra_headers={"chart_generated": True}
        )

# --- Step 2: Use the builder to wire up the core Bactrian engine ---
print("--- Initializing Pipelines via RouteBuilder ---")
RouteBuilder()\
    .from_event("BINANCE_TICK_STREAM")\
    .to(MarketAnalyticsProcessor)\
    .to(ChartGenerationProcessor)\
    .pretty_print()\
    .register()

# --- Step 3: Instantiate Core Engine and Run Simulated Stream ---
if __name__ == "__main__":
    engine = EventEngine()
    
    # Mock data incoming from market sockets
    raw_ticks = [
        {"ticker": "BTC", "price": 62000},
        {"ticker": "ETH", "price": 3400},
        {"ticker": "BTC", "price": 63000},
    ]
    
    # Fire the platform pipeline
    starting_exchange = Exchange(body=raw_ticks, headers={"feed_source": "Websocket"})
    print(f"\n--- Running Pipeline for Initial ID: {starting_exchange.id} ---")
    
    final_exchange = engine.route("BINANCE_TICK_STREAM", starting_exchange)
    
    print("\n--- Audit Trace Verification ---")
    print(f"Final Exchange ID: {final_exchange.id}")
    print(f"Final Headers:     {final_exchange.headers}")