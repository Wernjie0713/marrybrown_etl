# Work Log - 2nd December

## Summary

Today was focused on solving the performance bottleneck for historical data replication. We initially explored SSIS as a high-performance solution but ultimately pivoted back to optimizing our existing Python infrastructure due to complexity and compatibility issues.

## 1. SSIS Exploration (Morning - Mid Afternoon)

- **Objective:** Attempted to set up an SSIS package to handle the bulk load of historical data, hoping to leverage its native speed and in-memory capabilities.
- **Outcome:** **Failed / Abandoned.**
- **Reasons:**
  - Encountered significant friction in setting up the environment and dependencies.
  - The learning curve and debugging time required were higher than anticipated.
  - Ultimately decided that the maintenance overhead of introducing a new technology stack (SSIS) wasn't worth the potential performance gains compared to optimizing code we already own.

## 2. Python Optimization Strategy (Late Afternoon)

- **Decision:** Pivot back to using `scripts/replicate_monthly_parallel.py` but fundamentally change its architecture.
- **Problem with Original Script:** It was "Disk I/O Bound". It would download data -> write to Parquet files -> read Parquet files -> upload to SQL. This double-handling of data on the disk was the primary bottleneck.
- **New Approach (Direct Streaming):**
  - Refactored the script to use **Direct In-Memory Streaming**.
  - **Flow:** `Source DB (Xilnex)` -> `Memory Buffer (Pandas Chunk)` -> `Target DB (Local SQL)`.
  - **Benefits:**
    - Eliminates disk writes/reads entirely.
    - Reduces latency.
    - Keeps the existing parallel month processing logic.
    - Retains the robust connection handling (reconnecting on lost connection) that we already built.
- **Status:** The script `scripts/replicate_monthly_parallel.py` has been updated with this new streaming logic and is ready for testing on the historical load.

## 3. Initial Test Results (Evening)

- **Performance:**
  - Processed **Jan 2024 - Aug 2024 (8 months)** in **under 1.5 hours**.
  - This is a significant improvement over the previous disk-based method (which took ~2 hours for a similar volume).
- **Issues Encountered:**
  - **Deadlocks:** When increasing `max-workers` to 5, we encountered SQL Server deadlock errors (`Transaction ... was deadlocked on lock resources`).
  - This suggests that 5 concurrent writers might be too aggressive for the target table/indexes, or we need to tune the transaction isolation level/locking.
- **Next Steps:**
  - Investigate deadlock resolution (reduce workers or tune locking).
  - Continue enhancements tomorrow.
