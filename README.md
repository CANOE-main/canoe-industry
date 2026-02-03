# Industry ETL Pipeline (with Atlantic Split)

A modular ETL pipeline that builds the **Industry** sector tables for the CANOE database.  
It integrates **NRCan** (Comprehensive Energy Use Database), **CER** (Energy Futures macro indicators), and **Statistics Canada Table 25-10-0029-01** to allocate **Atlantic provinces** (PEI, NB, NS, NLLAB).

This README documents the `src/*_industry.py` modules from the professional refactor.

---

## Table of Contents
- [Overview](#overview)
- [Project Layout](#project-layout)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration (`params.yaml`)](#configuration-paramsyaml)
- [Data Sources](#data-sources)
- [What the Pipeline Builds](#what-the-pipeline-builds)
- [Logging](#logging)
- [Re-running & Caching](#re-running--caching)
- [Troubleshooting](#troubleshooting)
- [Extending / Customization](#extending--customization)
- [Reproducibility Notes](#reproducibility-notes)

---

## Overview

The pipeline orchestrates these steps:

1. **Setup**: Create fresh SQLite DB from schema; introspect table structures to prepare empty DataFrames (`comb_dict`).  
2. **Technology & Commodity**: Add Industry technologies and commodities, including demand commodities.  
3. **External Data**: Fetch or load cached **NRCan** (industry aggregated tables) and **CER** (macro indicators).  
4. **Atlantic Shares (StatCan)**: Load ATL shares to distribute Atlantic aggregate values into **PEI, NB, NS, NLLAB**.  
5. **Demand & ExistingCapacity**:  
   - Base year demand values from NRCan.  
   - Later years scaled by **CER GDP growth**.  
   - ATL provinces receive shares via StatCan allocation.  
6. **LimitTechInputSplitAnnual**: Build fuel input splits per sector using NRCan share tables; handle `n.a.`/`X` as remainder-to-100% and trim >100%.  
7. **Efficiency**: Derived from `LimitTechInputSplitAnnual` (efficiency=1), mapping `tech` → `output_comm` (`I_d_*`).  
8. **Costs**: Seed `CostInvest` rows (placeholders).  
9. **Post-processing**: Add `DataSet` and `DataSource` metadata rows (`[I1]` NRCan, `[I2]` CER, `[I3]` StatCan).  
10. **Persist**: Write all DataFrames into SQLite DB.

---

## Project Layout

```
project/
├─ input/
│  └─ params.yaml
├─ outputs/
├─ cache/
├─ schema/
│  └─ schema_3_1.sql              # or schema_{X}.sql per params.yaml
├─ common.py
├─ setup_industry.py
├─ techcom_industry.py
├─ data_scraper_industry.py
├─ statcan_atl.py
├─ demands_industry.py
├─ costs_industry.py
├─ techinput_industry.py
├─ efficiency_industry.py
├─ post_processing_industry.py
├─ aggregator_industry.py
└─ requirements.txt
```

---

## Prerequisites

- **Python 3.10+**
- Install dependencies from `requirements.txt`:

```bash
pip install -r requirements.txt
```

### requirements.txt
```
pandas
pyyaml
requests
```

---

## Quick Start

1. Configure `input/params.yaml`.  
2. Ensure schema file exists in `schema/` (e.g., `schema_3_1.sql`).  
3. Run the aggregator:

```bash
python aggregator.py --db-name CAN_industry.sqlite
```

Output DB is written to `outputs/CAN_industry.sqlite`.

---

## Configuration (`params.yaml`)

Minimal example:

```yaml
version: 1
schema_version: [31]
periods: [2025, 2030, 2035]
NRCan_year: 2022
```

- **version**: dataset version ID suffix (e.g., `GENINDHRAB001`). We use 001 to allow for more versions into the future.
- **schema_version**: selects schema file (e.g., `schema_3_1.sql`).  
- **periods**: first period = base year, later periods scaled by CER GDP.  
- **NRCan_year**: base year for NRCan table pulls.

---

## Data Sources

- **NRCan CEUD (Comprehensive Energy Use Database)** — industry aggregated tables per province, plus ATL aggregate.  
- **CER Energy Futures (Macro Indicators 2023)** — GDP time series for Global Net-zero scenario.  
- **StatCan Table 25-10-0029-01** — splits ATL aggregate into provincial shares for **PEI, NB, NS, NLLAB**.

> All downloads cached in `cache/`.

---

## What the Pipeline Builds

- `Technology` — industry technology rows.  
- `Commodity` — industry commodities plus demand commodities.  
- `Demand` — demand timeseries (base NRCan, scaled CER GDP, ATL split).  
- `ExistingCapacity` — baseline capacities (2021), ATL split by StatCan.  
- `LimitTechInputSplitAnnual` — commodity shares, with NA fill and >100% trim.  
- `Efficiency` — efficiency=1 rows, derived from techinput.  
- `CostInvest` — placeholder investment costs.  
- `DataSet`/`DataSource` — metadata for provenance.

---

## Logging

- Configured in `common.py`.  
- Format: `YYYY-MM-DD HH:MM:SS | LEVEL | logger | message`.  
- Adjust log level in `setup_logging()`.

---

## Re-running & Caching

- **DB recreation**: existing SQLite DB with same name is deleted on each run.  
- **Cache**:  
  - `dataframes.pkl` — NRCan tables  
  - `pop_df.pkl` — CER macro indicators  
  - `statcan_atl.pkl` — StatCan ATL shares  

Delete files in `cache/` to force fresh fetch.

---

## Troubleshooting

**Common issues**

1. **Schema mismatch** — ensure SQL schema matches ETL expectations.  
2. **HTTP/Parsing errors** — data sources may change format; update parsers accordingly.  
3. **Empty tables** — pipeline skips empty DataFrames; check logs.  
4. **Atlantic split missing** — verify `statcan_atl.pkl` has non-zero shares for 2023.

---

## Extending / Customization

- Parameterize sector mappings (`SECTOR_TABLE_MAP`, `COM_TO_COL`).  
- Add/remove industry subsectors.  
- Expose more CLI args (e.g., `--force-download`).  
- Add tests for GDP scaling and ATL allocation.

---

## Reproducibility Notes

- Controlled by `params.yaml` (schema, version, periods).  
- Deterministic given fixed cached data + schema.  
- Base period = direct NRCan; later periods scaled via CER GDP.  
- Atlantic provinces = StatCan shares of ATL aggregate (2023 baseline).

---

## Aggregator options & toggles

The industry aggregator is modular — you can **enable/disable steps** by commenting or uncommenting single lines in `aggregator.py`. This is useful when you want to build only part of the pipeline (e.g., skip costs or time-index scaffolding).

> The default orchestrated run does: Technology/Commodity → External Data → Atlantic Shares → Demand → Tech Input Splits → Efficiency → Post-processing → Update IDs → Persist. See the code in `aggregator.py` for the exact order.

### Where to edit
Open **`aggregator.py`** and look at the calls inside `main()` (search for the numbered comments 1)–9) ).

### Toggle reference

- **Technology & Commodity scaffolding** (required)
  ```python
  comb_dict = build_technology_and_commodity_industry(comb_dict)
  ```
  Builds `Technology` and `Commodity` (incl. demand commodities). Keep this **enabled** for any run. (Defined in `techcom.py`.)

- **External data fetch (NRCan & CER)** (required for demand/techinput)
  ```python
  loaded_df, pop_df = load_cached_or_fetch_industry(cfg.nrcan_year, project_paths()['cache'])
  ```
  Pulls/caches NRCan CEUD tables and CER macro indicators. (Defined in `data_scraper.py`.)

- **Atlantic shares (StatCan)** (required if you include ATL provinces)
  ```python
  atl_shares = load_statcan_atl_shares(project_paths()['cache'])
  ```
  Loads ATL allocation shares for **PEI, NB, NS, NLLAB**. (Defined in `statcan.py`.)

- **Demand (and optional ExistingCapacity)** (optional if you only want structure)
  ```python
  comb_dict = build_demand_and_capacity_industry(comb_dict, loaded_df, pop_df, atl_shares)
  ```
  Appends `Demand`. In `demands.py`, the `ExistingCapacity` block is present but commented—uncomment inside **`demands.py`** if you also want `ExistingCapacity` rows.

- **LimitTechInputSplitAnnual (fuel shares)** (optional but recommended; enables Efficiency)
  ```python
  comb_dict = build_limit_tech_input_split_industry(comb_dict, loaded_df, atl_shares)
  ```
  Builds input splits from NRCan shares (handles `n.a.`/`X` as remainder-to-100%, trims >100%). `Efficiency` depends on this being present.

- **Efficiency** (optional; depends on LimitTechInputSplitAnnual)
  ```python
  comb_dict = build_efficiency_industry(comb_dict)
  ```
  Generates `Efficiency` with output commodities derived from technology names.

- **Costs (CostInvest seeding)** (fully optional; OFF by default)
  ```python
  # comb_dict = build_cost_invest_industry(comb_dict)
  ```
  Placeholder `CostInvest` rows. **Uncomment** to include. (Function is in `costs.py`.)

- **Post-processing: DataSet & DataSource** (recommended)
  ```python
  comb_dict = add_datasets_and_sources_industry(comb_dict)
  ```
  Adds dataset metadata and citations `[I1]–[I4]`.

- **Time/Region scaffolding (TEMOA-style indices)** (optional; OFF by default)
  ```python
  # comb_dict = add_time_ind(comb_dict)
  ```
  Populates `TimeOfDay`, `TimePeriod`, `TimeSeason`, `SeasonLabel`, `TimeSegmentFraction`, and `Region`. **Uncomment** if you want full time-index tables.

- **ID update for region-/sector-specific rows** (optional but recommended)
  ```python
  comb_dict = update_ids(comb_dict)
  ```
  Assigns `data_id`s to `Technology`, `Demand`, `Efficiency`, and `LimitTechInputSplitAnnual` based on region & sector naming.

### Dependency tips
- `Efficiency` **requires** `LimitTechInputSplitAnnual` to be present (it is derived from it).
- `Demand` uses both **NRCan** (base) and **CER** (GDP scaling) plus **StatCan** (ATL split).
- If you **exclude Demand**, you can still build structure (`Technology`, `Commodity`) and fuel shares/efficiency.
- If you **exclude ATL shares** but keep ATL provinces in `params.yaml`, ATL demand/splits will be missing for those provinces.

### Example builds

- **Structure only (no data rows):**
  - Keep: `build_technology_and_commodity_industry`
  - Comment out: demand, techinput, efficiency, costs, post-processing, time, update_ids

- **Demand-only (no splits/efficiency/costs):**
  - Keep: tech/commodity, external data, atl shares, demand, post-processing, update_ids
  - Comment out: techinput, efficiency, costs, time

- **Full run with costs and time indices:**
  - Uncomment costs and time lines as shown above.

