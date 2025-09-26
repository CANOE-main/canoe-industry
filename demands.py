# -*- coding: utf-8 -*-
"""
Created on Fri Aug 15 14:21:58 2025

@author: david

"""
from __future__ import annotations
import pandas as pd
from typing import Dict
from common import setup_logging

logger = setup_logging()

ATL_MAP = { 'PEI': 'Prince Edward Island', 'NS': 'Nova Scotia', 'NB': 'New Brunswick', 'NLLAB': 'Newfoundland and Labrador' }


def _gdp_scalers(pop_df: pd.DataFrame, periods: list[int]) -> dict[int, float]:
    df = pop_df.copy()
    df = df[df['Year'].isin(periods)]
    df = df[df['Variable'] == 'Real Gross Domestic Product ($2012 Millions)']
    df = df[df['Scenario'] == 'Global Net-zero']
    df = df.sort_values('Year').reset_index(drop=True)
    scalers: dict[int, float] = {}
    for i, row in df.iterrows():
        year = int(row['Year'])
        if i == 0:
            scalers[year] = 1.0
        else:
            prev = float(df.loc[i-1, 'Value'])
            cur = float(row['Value'])
            scalers[year] = cur / prev if prev else 1.0
    return scalers


def _safe_loaded_value(loaded_df, prov: str, rn: int, year: str, x_index: int) -> float | None:
    try:
        val = loaded_df[prov][rn][year][x_index]
        if val in (None, ''):
            return None
        if val == '0':
            return 0.0
        return float(val)
    except Exception:
        return None


def _apply_atl_split(temp_val: float, dem_code: str, province: str, shares: dict[str, dict[str, float]], dem_map: dict[str, str]) -> float | None:
    key = dem_map.get(dem_code)
    if not key:
        return None
    region_name = ATL_MAP.get(province)
    if not region_name:
        return None
    try:
        share = float(shares[key][region_name])
        return float(temp_val) * share
    except Exception:
        return None


def build_demand_and_capacity_industry(
    comb_dict: Dict[str, pd.DataFrame],
    loaded_df: dict[str, dict[int, pd.DataFrame]],
    pop_df: pd.DataFrame,
    atl_shares: dict[str, dict[str, float]],
) -> Dict[str, pd.DataFrame]:
    dom = comb_dict["__domain__"]
    ids = comb_dict["__ids__"]
    dem_map = comb_dict["__canoe_dem_to_sec__"]

    sector_abv = dom['sector_abv']
    province_list = dom['province_list']
    sector_list = dom['sector_list']
    periods = dom['periods']
    atl_pro = set(dom['atl_pro'])

    demand_com_list = ["D_" + s for s in sector_list]
    gdp_scale = _gdp_scalers(pop_df, periods)

    # baseline pull: rn=2..11 correspond to these sector demands (as per your script)
    ab = {}; on = {}; mb = {}; qc = {}; bc = {}; sk = {}; atl = {}
    t = 0
    for x in range(2, 12):
        dem = demand_com_list[t]
        ab[dem]  = _safe_loaded_value(loaded_df, 'AB',  x, '2022', x)
        on[dem]  = _safe_loaded_value(loaded_df, 'ON',  x, '2022', x)
        mb[dem]  = _safe_loaded_value(loaded_df, 'MB',  x, '2022', x)
        qc[dem]  = _safe_loaded_value(loaded_df, 'QC',  x, '2022', x)
        bc[dem]  = _safe_loaded_value(loaded_df, 'BC',  x, '2022', x)
        sk[dem]  = _safe_loaded_value(loaded_df, 'SK',  x, '2022', x)
        atl[dem] = _safe_loaded_value(loaded_df, 'ATL', x, '2022', x)
        t += 1

    # Demand rows
    demand_rows = []
    for pro in province_list:
        for year in periods:
            for dem in demand_com_list:
                if year == min(periods):
                    notes, ref = (
                        'Value is taken from NRCan Comprehensive Energy Database, the latest value available',
                        '[I1]'
                    )
                    if pro == 'AB': val = ab.get(dem)
                    elif pro == 'ON': val = on.get(dem)
                    elif pro == 'BC': val = bc.get(dem)
                    elif pro == 'QC': val = qc.get(dem)
                    elif pro == 'MB': val = mb.get(dem)
                    elif pro == 'SK': val = sk.get(dem)
                    elif pro in atl_pro:
                        temp = atl.get(dem)
                        if temp in (None, 0.0):
                            continue
                        split = _apply_atl_split(temp, dem, pro, atl_shares, dem_map)
                        if split in (None, 0.0):
                            continue
                        val, ref = split, '[I1][I3]'
                    else:
                        val = None
                else:
                    notes, ref = (
                        'A scaling factor derived from the GDP growth from CER CEF report',
                        '[I1][I2]'
                    )
                    scale = gdp_scale.get(year, 1.0)
                    base = (
                        ab if pro=='AB' else on if pro=='ON' else bc if pro=='BC' else qc if pro=='QC' else mb if pro=='MB' else sk if pro=='SK' else atl
                    ).get(dem)
                    if pro in atl_pro:
                        if base in (None, 0.0):
                            continue
                        split = _apply_atl_split(base, dem, pro, atl_shares, dem_map)
                        if split in (None, 0.0):
                            continue
                        val, ref = float(split) * scale, '[I1][I2][I3]'
                    else:
                        val = None if base is None else float(base) * scale

                if val is None:
                    continue

                demand_rows.append([
                    pro,
                    int(year),
                    sector_abv + dem.lower(),
                    float(val),
                    'PJ',
                    notes,
                    ref,
                    1, 1, 2, 3, 2,
                    ids[pro],
                ])

    demand_df = pd.DataFrame(demand_rows, columns=comb_dict['Demand'].columns)
    comb_dict['Demand'] = pd.concat([comb_dict['Demand'], demand_df], ignore_index=True)
    logger.info("Demand rows: %d", len(demand_rows))

    # ExistingCapacity from 2021 baseline
    ab = {}; on = {}; mb = {}; qc = {}; bc = {}; sk = {}; atl = {}
    t = 0
    for x in range(2, 12):
        dem = demand_com_list[t]
        ab[dem]  = _safe_loaded_value(loaded_df, 'AB',  x, '2021', x)
        on[dem]  = _safe_loaded_value(loaded_df, 'ON',  x, '2021', x)
        mb[dem]  = _safe_loaded_value(loaded_df, 'MB',  x, '2021', x)
        qc[dem]  = _safe_loaded_value(loaded_df, 'QC',  x, '2021', x)
        bc[dem]  = _safe_loaded_value(loaded_df, 'BC',  x, '2021', x)
        sk[dem]  = _safe_loaded_value(loaded_df, 'SK',  x, '2021', x)
        atl[dem] = _safe_loaded_value(loaded_df, 'ATL', x, '2021', x)
        t += 1

    cap_rows = []
    for pro in province_list:
        for sec in sector_list:
            dem_key = f"D_{sec}"
            base = (
                ab if pro=='AB' else on if pro=='ON' else bc if pro=='BC' else qc if pro=='QC' else mb if pro=='MB' else sk if pro=='SK' else atl
            ).get(dem_key)
            if pro in atl_pro:
                if base in (None, 0.0):
                    continue
                split = _apply_atl_split(base, dem_key, pro, atl_shares, dem_map)
                if split in (None, 0.0):
                    continue
                val, ref = split, '[I1][I3]'
            else:
                val, ref = base, '[I1]'

            if val is None:
                continue

            cap_rows.append([
                pro,
                sector_abv + sec,
                2021,
                float(val),
                'PJ',
                'Existing capacity from NRCan (previous year baseline)',
                ref,
                1, 1, 2, 3, 2,
                ids[pro],
            ])

    cap_df = pd.DataFrame(cap_rows, columns=comb_dict['ExistingCapacity'].columns)
    comb_dict['ExistingCapacity'] = pd.concat([comb_dict['ExistingCapacity'], cap_df], ignore_index=True)
    logger.info("ExistingCapacity rows: %d", len(cap_rows))

    return comb_dict