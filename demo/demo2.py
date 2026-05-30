import time 
import polars as pl

# 1. CRITICAL: Configure the non-interactive backend BEFORE importing pyplot
import matplotlib
matplotlib.use('Agg')  # Switch to headless rendering mode
import matplotlib.pyplot as plt


from bactrian import Exchange, QueuedBactrianEngine, Processor, RouteBuilder

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
        
        if isinstance(df, pl.DataFrame):
            # 2. Explicitly manage figures to prevent memory leaks in a loop
            fig, ax = plt.subplots(figsize=(4, 2))
            
            ax.bar(df["ticker"].to_list(), df["avg_price"].to_list(), color="navy")
            ax.set_title("Hobby Platform - Market Snapshot")
            
            # 3. Save the asset quietly to disk without spawning a window
            fig.savefig("market_snapshot.png", bbox_inches='tight')
            
            # 4. CRITICAL: Close the figure explicitly to free up memory 
            plt.close(fig)
            
        return exchange.derive_new(
            new_body=df,
            extra_headers={"chart_generated": True}
        )
if __name__ == "__main__":

    # --- Step 2: Use the builder to wire up the core Bactrian engine ---
    print("--- Initializing Pipelines via RouteBuilder ---")
    RouteBuilder()\
        .from_event("BINANCE_TICK_STREAM")\
        .to(MarketAnalyticsProcessor)\
        .to(ChartGenerationProcessor)\
        .pretty_print()\
        .register()


    # 1. Setup the Engine Wrapper
    queued_engine = QueuedBactrianEngine(max_queue_size=1000)
    queued_engine.start()

    # 2. Simulate your Ingestion Layer (Oasis) receiving data rapidly
    print("\n--- Ingesting Market Data Rapidly ---")
    for i in range(1, 4):
        mock_ticks = [{"ticker": "BTC", "price": 60000 + i}]
        exch = Exchange(body=mock_ticks, headers={"batch": i})
        
        # This function returns instantly! The ingestion thread never waits.
        queued_engine.publish("BINANCE_TICK_STREAM", exch)
        print(f" -> [Oasis Feed] Pushed Batch #{i} to Queue.")
        time.sleep(0.1) # Simulate super fast network intervals

    # Allow background processing loop a brief window to process the remainder
    time.sleep(1.0)
    
    # 3. Shutdown gracefully
    queued_engine.stop()