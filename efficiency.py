# -*- coding: utf-8 -*-
"""
Created on Fri Aug 15 16:02:06 2025

@author: david
"""
from __future__ import annotations
import pandas as pd
from typing import Dict
from common import setup_logging

logger = setup_logging()

def _to_output_comm(tech: str) -> str | None:
    parts = tech.split('_', 1)
    if len(parts) == 2:
        prefix, name = parts
        return f"{prefix}_d_{name.lower()}"
    return None


def build_efficiency_agri(comb_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    src = comb_dict['LimitTechInputSplitAnnual'][['region','input_comm','tech','period','data_id']].copy()
    eff_df = comb_dict['Efficiency'].copy()
    if eff_df.empty:
        eff_df = pd.DataFrame(columns=comb_dict['Efficiency'].columns)

    eff_df = pd.concat([
        eff_df,
        pd.DataFrame({
            'region': src['region'],
            'input_comm': src['input_comm'],
            'tech': src['tech'],
            'vintage': src['period'],
            'output_comm': src['tech'].apply(_to_output_comm),
            'efficiency': 1.0,
            'notes': 'All technologies assumed efficiency=1; commodities from NRCan Comp DB',
            'data_source': '[A1]',
            'data_id': src['data_id'],
            'dq_cred': '', 'dq_geog': '', 'dq_struc': '', 'dq_tech': '', 'dq_time': ''
        })
    ], ignore_index=True)

    comb_dict['Efficiency'] = eff_df
    logger.info("Efficiency rows: %d", len(eff_df))
    return comb_dict