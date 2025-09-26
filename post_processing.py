# -*- coding: utf-8 -*-
"""
Created on Sun Aug 17 13:04:16 2025

@author: david
"""
from __future__ import annotations
from typing import Dict
import pandas as pd
from common import setup_logging

logger = setup_logging()


def add_datasets_and_sources_industry(comb_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    dom = comb_dict['__domain__']
    ids = comb_dict['__ids__']

    province_list = dom['province_list']
    version = comb_dict['__version__']

    ds_rows = []
    for pro in province_list:
        ds_rows.append([
            ids[pro], f"{pro} - industry - high resolution", f"v{version}",
            "2025 annual update", "active",
            "David Turnbull - david.turnbull1@ucalgary.ca", "08-2025", "",
            "Original sector design", "",
        ])

    ds_df = pd.DataFrame(ds_rows, columns=comb_dict['DataSet'].columns)
    comb_dict['DataSet'] = pd.concat([comb_dict['DataSet'], ds_df], ignore_index=True)

    src_rows = [
        ['[I1]', 'NRCan Comprehensive Database', 'Used the appropriate tables for each sector and province', ids['CAN']],
        ['[I2]', 'CER Canada Energy Futures report', 'Global net zero macro-economic indicators', ids['CAN']],
        ['[I3]', 'Statistics Canada 25-10-0029-01', 'ATL presence shares for sectoral allocation', ids['CAN']],
    ]
    src_df = pd.DataFrame(src_rows, columns=comb_dict['DataSource'].columns)
    comb_dict['DataSource'] = pd.concat([comb_dict['DataSource'], src_df], ignore_index=True)

    logger.info("Post-processing: %d DataSet, %d DataSource", len(ds_rows), len(src_rows))
    return comb_dict