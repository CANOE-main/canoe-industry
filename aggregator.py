# -*- coding: utf-8 -*-
"""
Created on Sun Aug 17 13:36:00 2025

@author: david
"""
from __future__ import annotations
import argparse
import sqlite3
from typing import Dict
import pandas as pd

from common import setup_logging, project_paths
from setup import load_runtime_agri
from techcom import build_technology_and_commodity_agri
from data_scraper import load_cached_or_fetch_agri
from statcan import load_statcan_agri_shares
from demands import build_demand_and_capacity_agri
#from costs import build_cost_invest_agri
from techinput import build_limit_tech_input_split_agri
from efficiency import build_efficiency_agri
from post_processing import add_datasets_and_sources_agri
from post_processing import add_time_agri

logger = setup_logging()

def write_comb_dict_to_db(db_path, tables, comb_dict: Dict[str, pd.DataFrame]) -> None:
    with sqlite3.connect(db_path) as conn:
        for table in tables:
            df = comb_dict.get(table)
            if df is None or df.empty:
                logger.warning("Skipping empty table: %s", table)
                continue
            df.to_sql(table, conn, if_exists='append', index=False)
            logger.info("Wrote %d rows to %s", len(df), table)


def main() -> None:
    parser = argparse.ArgumentParser(description="Agriculture ETL Aggregator (with ATL split)")
    parser.add_argument("--db-name", default="CAN_agriculture.sqlite", help="Output SQLite filename")
    args = parser.parse_args()

    # Init runtime
    db_path, cfg, tables, comb_dict = load_runtime_agri(temp_db_name=args.db_name)

    # 1) Technology/Commodity scaffolding
    comb_dict = build_technology_and_commodity_agri(comb_dict)

    # 2) External data
    loaded_df, pop_df = load_cached_or_fetch_agri(cfg.nrcan_year, project_paths()['cache'])

    # 3) StatCan ATL shares (agriculture)
    atl_shares = load_statcan_agri_shares(project_paths()['cache'])

    # 4) Demand + ExistingCapacity (GDP scaling + ATL split)
    comb_dict = build_demand_and_capacity_agri(comb_dict, loaded_df, pop_df, atl_shares)

    # 5) LimitTechInputSplitAnnual from NRCan shares (ATL uses ATL table)
    comb_dict = build_limit_tech_input_split_agri(comb_dict, loaded_df)

    # 6) Efficiency (derived from techinput)
    comb_dict = build_efficiency_agri(comb_dict)

    # 7) Costs
    #comb_dict = build_cost_invest_agri(comb_dict)

    # 8) Post-processing
    comb_dict = add_datasets_and_sources_agri(comb_dict)
    #9) Testing purposes, add region and times in
    #comb_dict = add_time_agri(comb_dict)

    # 10) Persist
    write_comb_dict_to_db(db_path, tables, comb_dict)
    logger.info("Done. SQLite written to %s", db_path)

if __name__ == "__main__":
    main()