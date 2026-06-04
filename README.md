# End-to-End Enterprise Logistics & Real-Time Fleet Analytics Platform

## 📌 Project Overview
This project demonstrates a production-grade, lambda-style data architecture built entirely within **Microsoft Fabric**. It bridges the gap between historical batch processing and low-latency streaming analytics by ingesting, transforming, and securing data from a simulated global fleet logistics network. 

The architecture processes heavy batch supply chain orders alongside live vehicle telemetry stream inputs, optimizing data modeling for downstream business intelligence.

---

## 🏗️ Technical Architecture
The platform is engineered using a modular, multi-layered data strategy inside an isolated active capacity workspace:

1. **Batch Ingestion & Processing Layer:** Data ingestion handled via Dataflow Gen2 into a Centralized Lakehouse (`Lh_Logistics_core`). PySpark notebooks transform raw delta files into refined Gold-layer tables.
2. **Data Warehousing & Security Layer:** High-performance analytical querying using Fabric Data Warehouse (`Dw Global Distribution`). Advanced security protocols include **Row-Level Security (RLS)** and **Dynamic Data Masking (DDM)** to protect PII data.
3. **Real-Time Intelligence Layer:** Live event capture via Fabric Eventstream (`Es_Fleet_Telemetry`), routing streaming data directly into an Eventhouse KQL Database (`Eh_fleet_analytics`) for sub-second time-series analytical tracking.

---

## 🛠️ Step-by-Step Implementation & Phase Progress

### Phase 1: Batch Data Engineering & Medallion Pipeline
* **Ingestion:** Deployed **Dataflow Gen2** pipelines to ingest multi-layered supply chain datasets into a native delta-parquet format within the Bronze layer of `Lh_Logistics_core`.
* **Bronze Data Processing:** PySpark notebooks handle data cleaning and generation of sample records to simulate heavy supply chain throughput.

![Bronze Layer Processing](broNze%20cleaning%20Data%20.png)

* **Gold Schema Refinement:** Orchestrated **PySpark Notebooks** (`nb_batch_data_pipeline`) to clean, structurally map, and refine raw tracking details into highly optimized Gold dimensional and fact tables (`gold_fact_orders`, `gold_dim_trucks`).

![Gold Star Schema Script Complete](Gold%20Star%20Schema%20.png)

---

### Phase 2: Enterprise Data Warehousing & T-SQL Relational Modeling
* High-performance relational **Data Warehouse** (`Dw Global Distribution`) initialized as a dedicated analytical compute endpoint.

![Fabric Data Warehouse Deployment](DW%20Warehouse%20.png)

* Data was structurally migrated from the open Lakehouse storage tier into the formal Relational Data Warehouse schemas using high-performance cross-database T-SQL abstraction scripts.

![Data Warehouse Target Write Script](Connect%20Data%20to%20Warehouse%20.png)

---

### Phase 3: Advanced Data Security & Governance
To mimic strict corporate compliance environments, security infrastructure was deployed directly onto the Gold data warehouse tables:
* **Row-Level Security (RLS):** Implemented an inline security function and filter predicate (`Security.fn_fleetSecurityFilter`) to restrict regional operational vehicle visibility based on system execution contexts (`USER_NAME()`).
* **Dynamic Data Masking (DDM):** Applied partial column masking on confidential identity strings to ensure data privacy without hindering report engine processing:
  ```sql
  ALTER TABLE dbo.gold_fact_orders 
  ALTER COLUMN CustomerID_Key ADD MASKED WITH (FUNCTION = 'partial(2, "XXXX", 2)');
Phase 4: Real-Time Streaming & Low-Latency Analytics
Streaming Engine: Wired a native Fabric Eventstream instance (Es_Fleet_Telemetry) to a continuous stream generator simulating operational real-time telemetric payloads.

Eventhouse Destination: Provisioned an Eventhouse KQL Database (Eh_fleet_analytics) utilizing Direct Ingestion to achieve maximum operational ingestion speeds.

🧠 Engineering Log: Technical Bottlenecks & Solutions
1. Legacy PySpark Storage Write Failure
The Problem: The batch processing notebook failed with an unmapped attribute exception: DataFrameWriter object has no attribute synapsesql.

The Root Cause: The codebase utilized legacy Azure Synapse syntax structures which are unsupported within native, unified Fabric Spark runtimes.

The Solution: Decoupled the notebook write phase by leveraging Fabric's seamless cross-item capabilities. Handled table creation and record insertion natively via the Synapse T-SQL engine using explicit SELECT * INTO targets directly from the unified Lakehouse endpoint.

2. Silent Streaming Drops due to Case-Sensitive Schema Mismatches
The Problem: Real-time data streams failed to populate target tables, reporting a persistent empty state (0 rows processed).

The Root Cause: Kusto Query Language (KQL) engines are strictly case-sensitive. The incoming JSON stream passed condensed, lowercase keys (trip_distance), whereas manual table definitions expected standard CamelCase fields (Trip_Distance). The engine silently ignored records due to the schema shift.

The Solution: Dropped the manual target tables and reconfigured the Eventhouse destination node using the native Fabric Data Ingestion Wizard. The wizard scanned a live data slice from the Eventstream and automatically generated accurate schema-to-payload properties under a low-latency execution pathway.

📊 Real-Time KQL Insights & Analytics
With the ingestion mappings aligned, custom Kusto scripts were developed to run low-latency diagnostics on active fleet metrics:

1. Fleet Route Deviation & Long-Haul Tracking
Code snippet
RealTimeFleetTelemetry
| where trip_distance > 4.0
| project EventTime = tpep_pickup_datetime, FleetVehicleID = VendorID, LongRouteDistance = trip_distance, passenger_count
| order by LongRouteDistance desc
2. Time-Series Revenue Throughput Visual
Code snippet
RealTimeFleetTelemetry
| summarize TotalActiveFleetRevenue = sum(Total_Amount), TotalTrips = count() by bin(tpep_pickup_datetime, 1m)
| render timechart
Performance Insight: The time-series visualization updates continuously on the fly, rendering aggregates over thousands of active records in just 0.096 seconds.
