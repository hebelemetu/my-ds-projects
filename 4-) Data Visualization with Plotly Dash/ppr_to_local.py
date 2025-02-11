import os
from datetime import date, timedelta, datetime

import pandas as pd
import numpy as np

import sqlite3

from ppr_oop_local import PPR
from ppr_dbase_local import Database

def excel_to_db(path,db,date):
    for file in os.listdir(path):
        if "$" not in  file and "Mass" in file and str(date) in file:
            print(file)
            ss_date = file.split("_")[0]
            table_name = "tbl_PPR_"+str(file.split("_")[3].split(".")[0])
            print(ss_date)
            print(table_name)
            df = pd.read_excel(path+str(file))
            lista = [row for row in df.iterrows()]
            for item in lista:
                db.insert_snapshot(table_name,ss_date,
                                   item[1].values[0],
                                   item[1].values[1],
                                   item[1].values[2],
                                   item[1].values[3],
                                   item[1].values[4],
                                   item[1].values[5],
                                   item[1].values[6],
                                   item[1].values[7],
                                   item[1].values[8],
                                   item[1].values[9],
                                   item[1].values[10],
                                   item[1].values[11],
                                   item[1].values[12],
                                   item[1].values[13],
                                   item[1].values[14],
                                   item[1].values[15]
                                   ,item[1].values[16]
                                   ,item[1].values[17],
                                   item[1].values[18],
                                   item[1].values[19],
                                   item[1].values[20],
                                   item[1].values[21],
                                   item[1].values[22],
                                   item[1].values[23])


db = Database("ppr_local.db")

this_year = date.today().year
next_year = date.today().year + 1
next_yearplus1 = date.today().year + 2
today = date.today()
today_dot = today.strftime("%d.%m.%Y")

# SAVE SNAPSHOTS TO A FOLDER AND DATABASE
path = "C:/Users/cihat.sari/PycharmProjects/pythonProject/PPR/Local/data/"

file_dates = []
for file in os.listdir(path):
    ss_date = file.split("_")[0]
    if "$" not in file and "Mass" in file and ss_date not in file_dates:
        file_dates.append(ss_date)
for date in file_dates:
    if db.db_check_date(date) == 0:
        excel_to_db(path,db,date)

# DATABASE CONNECTION
conn = sqlite3.connect("ppr_local.db")
# report_date = sql date - 1
# Snapshot_date report_date+1

ppr = PPR()
def make_report(snapshot_date,conn):
    energy = ppr.data_prep(
        pd.DataFrame(pd.read_sql_query(f"SELECT * FROM tbl_PPR_Energy WHERE snapshot_date='{snapshot_date}'", conn)))
    energyfx = ppr.data_prep(
        pd.DataFrame(pd.read_sql_query(f"SELECT * FROM tbl_PPR_EnergyFX WHERE snapshot_date='{snapshot_date}'", conn)))
    yekdemunitcost = ppr.data_prep(
        pd.DataFrame(pd.read_sql_query(f"SELECT * FROM tbl_PPR_YekdemUnitCost WHERE snapshot_date='{snapshot_date}'", conn)))
    yekdemdap = ppr.data_prep(
        pd.DataFrame(pd.read_sql_query(f"SELECT * FROM tbl_PPR_YekdemDAP WHERE snapshot_date='{snapshot_date}'", conn)))
    yekdemfx = ppr.data_prep(
        pd.DataFrame(pd.read_sql_query(f"SELECT * FROM tbl_PPR_YekdemFX WHERE snapshot_date='{snapshot_date}'", conn)))

    energy_group = ppr.data_group(energy) *3/2*13/17
    energyfx_group = ppr.data_group(energyfx)*3/2*13/17
    yekdemunitcost_group = ppr.data_group(yekdemunitcost)*3/2*13/17
    yekdemdap_group = ppr.data_group(yekdemdap)*3/2*13/17
    yekdemfx_group = ppr.data_group(yekdemfx)*3/2*13/17

    # # -----------B2B--------

    # ENERGY DATA
    book_groups_energy = energy["BookGroupName"].unique()
    # DEMAND
    book_b2b_fixed_demand = [item for item in book_groups_energy if all(x in str(item) for x in ["Fixed", "B2B", "Demand"])]
    book_b2b_p2s_demand = [item for item in book_groups_energy if all(x in str(item) for x in ["P2S", "B2B", "Demand"])]
    book_b2b_tedas_demand = [item for item in book_groups_energy if all(x in str(item) for x in ["Tedas", "B2B", "Demand"])]

    book_list_demand = [book_b2b_fixed_demand, book_b2b_p2s_demand, book_b2b_tedas_demand]
    # OTC
    book_b2b_fixed_otc = [item for item in book_groups_energy if all(x in str(item) for x in ["Fixed", "B2B", "OTC"])]
    book_b2b_p2s_otc = [item for item in book_groups_energy if all(x in str(item) for x in ["P2S", "B2B", "OTC"])]
    book_b2b_tedas_otc = [item for item in book_groups_energy if all(x in str(item) for x in ["Tedas", "B2B", "OTC"])]

    book_list_otc = [book_b2b_fixed_otc, book_b2b_p2s_otc, book_b2b_tedas_otc]
    # DAP Hedge
    book_b2b_fixed_daphedge = [item for item in book_groups_energy if
                               all(x in str(item) for x in ["Fixed", "B2B", "DAP Hedge"])]
    book_b2b_p2s_daphedge = [item for item in book_groups_energy if
                             all(x in str(item) for x in ["P2S", "B2B", "DAP Hedge"])]
    book_b2b_tedas_daphedge = [item for item in book_groups_energy if
                               all(x in str(item) for x in ["Tedas", "B2B", "DAP Hedge"])]

    book_list_daphedge = [book_b2b_fixed_daphedge, book_b2b_p2s_daphedge, book_b2b_tedas_daphedge]
    # Total Energy
    book_b2b_fixed_totalenergy = [item for item in book_groups_energy if
                                  all(x in str(item) for x in ["Fixed", "B2B", "Total Energy"])]
    book_b2b_p2s_totalenergy = [item for item in book_groups_energy if
                                all(x in str(item) for x in ["P2S", "B2B", "Total Energy"])]
    book_b2b_tedas_totalenergy = [item for item in book_groups_energy if
                                  all(x in str(item) for x in ["Tedas", "B2B", "Total Energy"])]

    book_list_totalenergy = [book_b2b_fixed_totalenergy, book_b2b_p2s_totalenergy, book_b2b_tedas_totalenergy]
    # ENERGY FX DATA
    book_groups_energyfx = energyfx["BookGroupName"].unique()

    book_b2b_energy_fx = [item for item in book_groups_energyfx if all(x in str(item) for x in ["FX", "B2B", "Energy"])]
    # YEKDEM UNIT COST DATA
    book_groups_yekdemunitcost = yekdemunitcost["BookGroupName"].unique()

    book_b2b_fixed_yekdemunitcost = [item for item in book_groups_yekdemunitcost if
                                     all(x in str(item) for x in ["Fixed", "B2B", "Yekdem Unit Cost"])]
    book_b2b_p2s_yekdemunitcost = [item for item in book_groups_yekdemunitcost if
                                   all(x in str(item) for x in ["P2S", "B2B", "Yekdem Unit Cost"])]
    book_b2b_tedas_yekdemunitcost = [item for item in book_groups_yekdemunitcost if
                                     all(x in str(item) for x in ["Tedas", "B2B", "Yekdem Unit Cost"])]

    book_list_yekdemunitcost = [book_b2b_fixed_yekdemunitcost, book_b2b_p2s_yekdemunitcost, book_b2b_tedas_yekdemunitcost]
    # YEKDEM DAP DATA
    book_groups_yekdemdap = yekdemdap["BookGroupName"].unique()

    book_b2b_fixed_yekdemdap = [item for item in book_groups_yekdemdap if
                                all(x in str(item) for x in ["Fixed", "B2B", "Yekdem DAP"])]
    book_b2b_p2s_yekdemdap = [item for item in book_groups_yekdemdap if
                              all(x in str(item) for x in ["P2S", "B2B", "Yekdem DAP"])]
    book_b2b_tedas_yekdemdap = [item for item in book_groups_yekdemdap if
                                all(x in str(item) for x in ["Tedas", "B2B", "Yekdem DAP"])]

    book_list_yekdemdap = [book_b2b_fixed_yekdemdap, book_b2b_p2s_yekdemdap, book_b2b_tedas_yekdemdap]
    # YEKDEM FX DATA
    book_groups_yekdemfx = yekdemfx["BookGroupName"].unique()

    # YEKDEM FX OTC
    book_b2b_fixed_yekdemfx_otc = [item for item in book_groups_yekdemfx if
                                   all(x in str(item) for x in ["Fixed", "B2B", "OTC"])]
    book_b2b_p2s_yekdemfx_otc = [item for item in book_groups_yekdemfx if
                                 all(x in str(item) for x in ["P2S", "B2B", "OTC"])]
    book_b2b_tedas_yekdemfx_otc = [item for item in book_groups_yekdemfx if
                                   all(x in str(item) for x in ["Tedas", "B2B", "OTC"])]

    book_list_yekdemfxotc = [book_b2b_fixed_yekdemfx_otc, book_b2b_p2s_yekdemfx_otc, book_b2b_tedas_yekdemfx_otc]
    # YEKDEM FX DEMAND
    book_b2b_fixed_yekdemfx_demand = [item for item in book_groups_yekdemfx if
                                      all(x in str(item) for x in ["Fixed", "B2B", "Demand"])]
    book_b2b_p2s_yekdemfx_demand = [item for item in book_groups_yekdemfx if
                                    all(x in str(item) for x in ["P2S", "B2B", "Demand"])]
    book_b2b_tedas_yekdemfx_demand = [item for item in book_groups_yekdemfx if
                                      all(x in str(item) for x in ["Tedas", "B2B", "Demand"])]

    book_list_yekdemfxdemand = [book_b2b_fixed_yekdemfx_demand, book_b2b_p2s_yekdemfx_demand,
                                book_b2b_tedas_yekdemfx_demand]

    # # ENERGY DATA

    # B2B DEMAND GWH
    b2b_demand_gwh = ppr.create_df(book_list_demand, energy_group, "TotalNetVolume")
    b2b_demand_gwh = ppr.add_normal_sum(b2b_demand_gwh)
    # B2B DEMAND MW
    b2b_demand_mw = ppr.gwh_to_mw(b2b_demand_gwh)
    ppr.ara_tablolar["b2b_energy"]["b2b_demand_gwh"] = b2b_demand_gwh
    ppr.ara_tablolar["b2b_energy"]["b2b_demand_mw"] = b2b_demand_mw

    # B2B OTC GWH
    b2b_otc_gwh = ppr.create_df(book_list_otc, energy_group, "TotalNetVolume")
    b2b_otc_gwh = ppr.add_normal_sum(b2b_otc_gwh)
    # B2B OTC MW
    b2b_otc_mw = ppr.gwh_to_mw(b2b_otc_gwh)
    ppr.ara_tablolar["b2b_energy"]["b2b_otc_gwh"] = b2b_otc_gwh
    ppr.ara_tablolar["b2b_energy"]["b2b_otc_mw"] = b2b_otc_mw

    # DAP HEDGE MW
    b2b_daphedge_gwh = ppr.create_df(book_list_daphedge, energy_group, "TotalNetVolume")
    b2b_daphedge_gwh = ppr.add_normal_sum(b2b_daphedge_gwh)
    ppr.ara_tablolar["b2b_energy"]["b2b_daphedge_gwh"] = b2b_daphedge_gwh

    # TOTAL HEDGE MW
    total_hedge_mw = pd.DataFrame()
    for col in b2b_otc_gwh.columns:
        col_new = str(col).replace("OTC", "Total Hedge")
        col_dap = str(col).replace("OTC", "DAP Hedge")
        total_hedge_mw[str(col_new)] = b2b_otc_gwh[str(col)] + b2b_daphedge_gwh[str(col_dap)]
    total_hedge_mw = ppr.gwh_to_mw(total_hedge_mw)
    ppr.ara_tablolar["b2b_energy"]["b2b_total_hedge_mw"] = total_hedge_mw

    # B2B NET POSITION MW
    net_position_mw = pd.DataFrame()
    for col in total_hedge_mw.columns:
        col_new = str(col).replace("Total Hedge", "Net Position")
        col_demand = str(col).replace("Total Hedge", "Demand")
        net_position_mw[str(col_new)] = total_hedge_mw[str(col)] - (b2b_demand_mw[str(col_demand)] * -1)
    ppr.ara_tablolar["b2b_energy"]["b2b_net_position_mw"] = net_position_mw

    # # ENERGY FX DATA

    # ENERGY FX VOLUME
    b2b_energyfx_volume = ppr.create_df([book_b2b_energy_fx], energyfx_group, "TotalNetVolume")
    b2b_energyfx_volume = b2b_energyfx_volume.iloc[1:]
    b2b_energyfx_volume = ppr.add_normal_sum(b2b_energyfx_volume)
    ppr.ara_tablolar["b2b_energyfx"]["b2b_energyfx_volume"] = b2b_energyfx_volume

    # ENERGY FX OTC MTL
    b2b_energyfx_otcmtl = ppr.create_df([book_b2b_energy_fx], energyfx_group, "TotalPL")
    b2b_energyfx_otcmtl = b2b_energyfx_otcmtl.iloc[1:]
    b2b_energyfx_otcmtl = ppr.add_normal_sum(b2b_energyfx_otcmtl)
    ppr.ara_tablolar["b2b_energyfx"]["b2b_energyfx_otcmtl"] = b2b_energyfx_otcmtl

    # BUY PRICE TL/MWH
    b2b_energyfx_buyprice = ppr.create_df([book_b2b_energy_fx], energyfx_group, "BuyCost")
    b2b_energyfx_buypricesum = pd.DataFrame()

    for col in b2b_energyfx_buyprice.columns:
        b2b_energyfx_buyprice[str(col)] = b2b_energyfx_buyprice[str(col)] / (b2b_energyfx_volume[str(col)])
        b2b_energyfx_buypricesum[str(col)] = b2b_energyfx_buyprice[str(col)] * (b2b_energyfx_volume[str(col)])

    b2b_energyfx_buypricesum = ppr.add_normal_sum(b2b_energyfx_buypricesum)

    b2b_energyfx_volume_sums = b2b_energyfx_volume.copy().loc[:, b2b_energyfx_volume.columns.str.contains("Sum")]
    b2b_energyfx_buypricesums = b2b_energyfx_buypricesum.copy().loc[:, b2b_energyfx_buypricesum.columns.str.contains("Sum")]

    for col in b2b_energyfx_volume_sums.columns:
        b2b_energyfx_buypricesums[str(col)] = b2b_energyfx_buypricesums[str(col)] / (b2b_energyfx_volume_sums[str(col)])

    b2b_energyfx_buypricesums = b2b_energyfx_buypricesums.iloc[1:]
    b2b_energyfx_buyprice = b2b_energyfx_buyprice.iloc[1:].reindex(sorted(b2b_energyfx_buyprice.columns), axis=1)
    b2b_energyfx_buyprice = pd.concat([b2b_energyfx_buyprice.fillna(0), b2b_energyfx_buypricesums.fillna(0)], axis=1)
    ppr.ara_tablolar["b2b_energyfx"]["b2b_energyfx_buyprice"] = b2b_energyfx_buyprice

    # # ENERGY DATA CONTINUING

    # Total Energy MTL
    b2b_total_energy_mtl = ppr.create_df(book_list_totalenergy, energy_group, "TotalPL")
    for col in b2b_energyfx_otcmtl.loc[:, ~b2b_energyfx_otcmtl.columns.str.contains("Sum")].columns:
        col_new = str(col).replace("Energy FX", "Fixed Total Energy")
        b2b_total_energy_mtl[str(col_new)] = b2b_total_energy_mtl[str(col_new)].values + (
            b2b_energyfx_otcmtl[str(col)].values)
    b2b_total_energy_mtl = ppr.add_normal_sum(b2b_total_energy_mtl)
    ppr.ara_tablolar["b2b_energy"]["b2b_total_energy_mtl"] = b2b_total_energy_mtl

    # Sell Price TL/MWh
    b2b_energy_sellprice = pd.DataFrame()
    b2b_energy_sellprice_product_gwh = pd.DataFrame()
    b2b_energy_sellprice_sums = pd.DataFrame()
    for book in book_list_demand:
        for item in book:
            sell_cost = energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            sell_volume_peak = energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "SellVolumePeak"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            sell_volume_offpeak = energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "SellVolumeOffpeak"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            b2b_energy_sellprice[str(item)] = \
            energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            b2b_energy_sellprice = b2b_energy_sellprice.add(sell_cost, fill_value=0)
            b2b_energy_sellprice = b2b_energy_sellprice.add(sell_volume_peak, fill_value=0)
            b2b_energy_sellprice = b2b_energy_sellprice.add(sell_volume_offpeak, fill_value=0)
            b2b_energy_sellprice[str(item)] = b2b_energy_sellprice["SellCost"] / (
                        b2b_energy_sellprice["SellVolumePeak"] + b2b_energy_sellprice["SellVolumeOffpeak"])
            b2b_energy_sellprice = b2b_energy_sellprice.drop(["SellCost", "SellVolumePeak", "SellVolumeOffpeak"], axis=1)

    for col in [col for col in b2b_demand_gwh.copy().columns if all(x not in str(col) for x in ["Sum", "P2S"])]:
        b2b_energy_sellprice_product_gwh[str(col)] = b2b_energy_sellprice[str(col)] * b2b_demand_gwh[str(col)]

    b2b_energy_sellprice_product_gwh = ppr.add_normal_sum(b2b_energy_sellprice_product_gwh)

    b2b_demand_gwh_sums = b2b_demand_gwh.copy().loc[:, b2b_demand_gwh.columns.str.contains("Sum")]
    b2b_energy_sellprice_product_gwh_sums = b2b_energy_sellprice_product_gwh.copy().loc[:,
                                            b2b_energy_sellprice_product_gwh.columns.str.contains("Sum")]

    for col in b2b_energy_sellprice_product_gwh_sums:
        b2b_energy_sellprice_sums[str(col)] = b2b_energy_sellprice_product_gwh_sums[str(col)] / b2b_demand_gwh_sums[
            str(col)]

    b2b_energy_sellprice = pd.concat([b2b_energy_sellprice.fillna(0), b2b_energy_sellprice_sums.fillna(0)], axis=1)
    ppr.ara_tablolar["b2b_energy"]["b2b_energy_sellprice"] = b2b_energy_sellprice

    # ENERGY BUY PRICE
    b2b_energy_buyprice = pd.DataFrame()
    b2b_energy_buyvolume = pd.DataFrame()
    b2b_energy_buyprice_product_mw = pd.DataFrame()
    b2b_energy_buyprice_sums = pd.DataFrame()
    otc_buycost = pd.DataFrame()
    daphedge_buycost = pd.DataFrame()
    otc_totalbuyvolume = pd.DataFrame()
    daphedge_totalbuyvolume = pd.DataFrame()
    # buy cost
    for item in sorted(book_b2b_fixed_otc):
        col_total_hedge = str(item).replace("OTC", "Total Hedge")
        otc_buycost[str(item)] = energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "BuyCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
    # buy cost
    for item in sorted(book_b2b_fixed_daphedge):
        col_total_hedge = str(item).replace("DAP", "Total")
        daphedge_buycost[str(item)] = energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "BuyCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
    b2b_energy_buyprice = pd.concat([otc_buycost, daphedge_buycost], axis=1)
    b2b_energy_buyprice = otc_buycost.fillna(0).add(daphedge_buycost.fillna(0).values, fill_value=0)
    b2b_energy_buyprice.columns = [str(item).replace("OTC", "Total Hedge") for item in b2b_energy_buyprice.columns]
    # energy fx
    for col in b2b_energyfx_otcmtl.loc[:, ~b2b_energyfx_otcmtl.columns.str.contains("Sum")].columns:
        col_new = str(col).replace("Energy FX", "Fixed Total Hedge")
        b2b_energy_buyprice[str(col_new)] = b2b_energy_buyprice[str(col_new)].copy() - (
                    b2b_energyfx_otcmtl[str(col)].values * 1000)

    # total buy volume
    for item1, item2 in zip(sorted(book_b2b_fixed_otc), sorted(book_b2b_fixed_daphedge)):
        col_total_volume = str(item1).replace("OTC", "All Total Buy Volume")
        col_total_volume2 = str(item2).replace("DAP Hedge", "All Total Buy Volume")
        otc_totalbuyvolume[str(col_total_volume)] = \
        energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item1)])][
            "TotalBuyVolume"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
        daphedge_totalbuyvolume[str(col_total_volume2)] = \
        energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item2)])][
            "TotalBuyVolume"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
    b2b_energy_buyvolume = otc_totalbuyvolume.fillna(0).add(daphedge_totalbuyvolume.fillna(0), fill_value=0)

    b2b_energy_buyprice = b2b_energy_buyprice.reindex(sorted(b2b_energy_buyprice.columns), axis=1)
    b2b_energy_buyprice = b2b_energy_buyprice / b2b_energy_buyvolume.values

    # BUY PRICE SUM

    for col in [col for col in total_hedge_mw.copy().columns if all(x not in str(col) for x in ["Sum", "P2S"])]:
        b2b_energy_buyprice_product_mw[str(col)] = b2b_energy_buyprice[str(col)] * total_hedge_mw[str(col)]

    b2b_energy_buyprice_product_mw = ppr.add_normal_sum(b2b_energy_buyprice_product_mw)

    total_hedge_mw_sums = total_hedge_mw.copy().loc[:, total_hedge_mw.columns.str.contains("Sum")]
    b2b_energy_buyprice_product_mw_sums = b2b_energy_buyprice_product_mw.copy().loc[:,
                                          b2b_energy_buyprice_product_mw.columns.str.contains("Sum")]

    for col in b2b_energy_buyprice_product_mw_sums:
        b2b_energy_buyprice_sums[str(col)] = b2b_energy_buyprice_product_mw_sums[str(col)] / total_hedge_mw_sums[str(col)]

    b2b_energy_buyprice = pd.concat([b2b_energy_buyprice, b2b_energy_buyprice_sums], axis=1)

    ppr.ara_tablolar["b2b_energy"]["b2b_energy_buyprice"] = b2b_energy_buyprice

    b2b_energy_buypeak_demand = ppr.create_df(book_list_demand, energy_group, "BuyVolumePeak")
    b2b_energy_sellpeak_demand = ppr.create_df(book_list_demand, energy_group, "SellVolumePeak")
    b2b_energy_peak_demand = b2b_energy_buypeak_demand - b2b_energy_sellpeak_demand.values
    b2b_energy_peak_demand = ppr.gwh_to_mw(b2b_energy_peak_demand) * 2
    b2b_energy_peak_demand = ppr.add_normal_sum(b2b_energy_peak_demand)

    b2b_energy_buyoffpeak_demand = ppr.create_df(book_list_demand, energy_group, "BuyVolumeOffpeak")
    b2b_energy_selloffpeak_demand = ppr.create_df(book_list_demand, energy_group, "SellVolumeOffpeak")
    b2b_energy_offpeak_demand = b2b_energy_buyoffpeak_demand - b2b_energy_selloffpeak_demand.values
    b2b_energy_offpeak_demand = ppr.gwh_to_mw(b2b_energy_offpeak_demand) * 2
    b2b_energy_offpeak_demand = ppr.add_normal_sum(b2b_energy_offpeak_demand)

    b2b_energy_buypeak_otc = ppr.create_df(book_list_otc, energy_group, "BuyVolumePeak")
    b2b_energy_sellpeak_otc = ppr.create_df(book_list_otc, energy_group, "SellVolumePeak")
    b2b_energy_peak_otc = b2b_energy_buypeak_otc - b2b_energy_sellpeak_otc.values
    b2b_energy_peak_otc = ppr.gwh_to_mw(b2b_energy_peak_otc) * 2
    b2b_energy_peak_otc = ppr.add_normal_sum(b2b_energy_peak_otc)

    b2b_energy_buyoffpeak_otc = ppr.create_df(book_list_otc, energy_group, "BuyVolumeOffpeak")
    b2b_energy_selloffpeak_otc = ppr.create_df(book_list_otc, energy_group, "SellVolumeOffpeak")
    b2b_energy_offpeak_otc = b2b_energy_buyoffpeak_otc - b2b_energy_selloffpeak_otc.values
    b2b_energy_offpeak_otc = ppr.gwh_to_mw(b2b_energy_offpeak_otc) * 2
    b2b_energy_offpeak_otc = ppr.add_normal_sum(b2b_energy_offpeak_otc)

    b2b_energy_buypeak_daphedge = ppr.create_df(book_list_daphedge, energy_group, "BuyVolumePeak")
    b2b_energy_sellpeak_daphedge = ppr.create_df(book_list_daphedge, energy_group, "SellVolumePeak")
    b2b_energy_peak_daphedge = b2b_energy_buypeak_daphedge - b2b_energy_sellpeak_daphedge.values
    b2b_energy_peak_daphedge = ppr.gwh_to_mw(b2b_energy_peak_daphedge) * 2
    b2b_energy_peak_daphedge = ppr.add_normal_sum(b2b_energy_peak_daphedge)

    b2b_energy_buyoffpeak_daphedge = ppr.create_df(book_list_daphedge, energy_group, "BuyVolumeOffpeak")
    b2b_energy_selloffpeak_daphedge = ppr.create_df(book_list_daphedge, energy_group, "SellVolumeOffpeak")
    b2b_energy_offpeak_daphedge = b2b_energy_buyoffpeak_daphedge - b2b_energy_selloffpeak_daphedge.values
    b2b_energy_offpeak_daphedge = ppr.gwh_to_mw(b2b_energy_offpeak_daphedge) * 2
    b2b_energy_offpeak_daphedge = ppr.add_normal_sum(b2b_energy_offpeak_daphedge)

    b2b_energy_total_peak = b2b_energy_peak_demand + b2b_energy_peak_otc + b2b_energy_peak_daphedge
    b2b_energy_total_offpeak = b2b_energy_offpeak_demand + b2b_energy_offpeak_otc + b2b_energy_offpeak_daphedge

    ppr.ara_tablolar["b2b_energy"]["b2b_energy_peak_demand"] = b2b_energy_peak_demand
    ppr.ara_tablolar["b2b_energy"]["b2b_energy_offpeak_demand"] = b2b_energy_offpeak_demand
    ppr.ara_tablolar["b2b_energy"]["b2b_energy_peak_otc"] = b2b_energy_peak_otc
    ppr.ara_tablolar["b2b_energy"]["b2b_energy_offpeak_otc"] = b2b_energy_offpeak_otc
    ppr.ara_tablolar["b2b_energy"]["b2b_energy_peak_daphedge"] = b2b_energy_peak_daphedge
    ppr.ara_tablolar["b2b_energy"]["b2b_energy_offpeak_daphedge"] = b2b_energy_offpeak_daphedge
    ppr.final_tablolar["b2b"]["b2b_energy_total_peak"] = b2b_energy_total_peak["Sum"]
    ppr.final_tablolar["b2b"]["b2b_energy_total_offpeak"] = b2b_energy_total_offpeak["Sum"]

    # # YEKDEM UNIT COST DATA

    b2b_yekdemunitcostdemand_gwh = ppr.create_df(book_list_yekdemunitcost, yekdemunitcost_group, "TotalSellVolume")
    b2b_yekdemunitcostdemand_gwh = ppr.add_normal_sum(b2b_yekdemunitcostdemand_gwh)
    ppr.ara_tablolar["b2b_yekdemunitcost"]["b2b_yekdemunitcostdemand_gwh"] = b2b_yekdemunitcostdemand_gwh

    b2b_yekdemunitcosthedge_gwh = ppr.create_df(book_list_yekdemunitcost, yekdemunitcost_group, "TotalBuyVolume")
    b2b_yekdemunitcosthedge_gwh = ppr.add_normal_sum(b2b_yekdemunitcosthedge_gwh)
    ppr.ara_tablolar["b2b_yekdemunitcost"]["b2b_yekdemunitcosthedge_gwh"] = b2b_yekdemunitcosthedge_gwh

    b2b_yekdemunitcostmtl = ppr.create_df(book_list_yekdemunitcost, yekdemunitcost_group, "TotalPL")
    b2b_yekdemunitcostmtl = ppr.add_normal_sum(b2b_yekdemunitcostmtl)
    ppr.ara_tablolar["b2b_yekdemunitcost"]["b2b_yekdemunitcostmtl"] = b2b_yekdemunitcostmtl

    b2b_yekdemunitcost_sellprice = pd.DataFrame()
    b2b_yekdemunitcostdemand_sp_gwh = pd.DataFrame()
    yekdemunitcost_totalsellvolume_b2b = pd.DataFrame()
    for item in sorted(book_b2b_fixed_yekdemunitcost):
        yekdemunitcost_totalsellvolume_b2b[str(item)] = \
        yekdemunitcost_group[yekdemunitcost_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "TotalSellVolume"].reset_index(drop=True)
        b2b_yekdemunitcost_sellprice[str(item)] = \
        yekdemunitcost_group[yekdemunitcost_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
        b2b_yekdemunitcost_sellprice[str(item)] = b2b_yekdemunitcost_sellprice[str(item)] / \
                                                  yekdemunitcost_totalsellvolume_b2b[str(item)].values
    for item in sorted(book_b2b_p2s_yekdemunitcost):
        yekdemunitcost_totalsellvolume_b2b[str(item)] = \
        yekdemunitcost_group[yekdemunitcost_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "TotalSellVolume"].reset_index(drop=True)
        b2b_yekdemunitcost_sellprice[str(item)] = \
        yekdemunitcost_group[yekdemunitcost_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
        b2b_yekdemunitcost_sellprice[str(item)] = b2b_yekdemunitcost_sellprice[str(item)] / \
                                                  yekdemunitcost_totalsellvolume_b2b[str(item)].values
    for item in sorted(book_b2b_tedas_yekdemunitcost):
        yekdemunitcost_totalsellvolume_b2b[str(item)] = \
        yekdemunitcost_group[yekdemunitcost_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "TotalSellVolume"].reset_index(drop=True)
        b2b_yekdemunitcost_sellprice[str(item)] = \
        yekdemunitcost_group[yekdemunitcost_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
        b2b_yekdemunitcost_sellprice[str(item)] = b2b_yekdemunitcost_sellprice[str(item)] / \
                                                  yekdemunitcost_totalsellvolume_b2b[str(item)].values

    for col in [col for col in b2b_yekdemunitcostdemand_gwh.copy().columns if
                all(x not in str(col) for x in ["Sum", "P2S"])]:
        b2b_yekdemunitcostdemand_sp_gwh[str(col)] = b2b_yekdemunitcostdemand_gwh[str(col)] * b2b_yekdemunitcost_sellprice[
            str(col)]

    b2b_yekdemunitcostdemand_sp_gwh = ppr.add_normal_sum(b2b_yekdemunitcostdemand_sp_gwh)

    b2b_yekdemunitcostdemand_gwh_sums = b2b_yekdemunitcostdemand_gwh.copy().loc[:,
                                        b2b_yekdemunitcostdemand_gwh.columns.str.contains("Sum")]
    b2b_yekdemunitcost_sellprice_sums = b2b_yekdemunitcost_sellprice.copy().loc[:,
                                        b2b_yekdemunitcost_sellprice.columns.str.contains("Sum")]
    b2b_yekdemunitcostdemand_sp_gwh_sums = b2b_yekdemunitcostdemand_sp_gwh.copy().loc[:,
                                           b2b_yekdemunitcostdemand_sp_gwh.columns.str.contains("Sum")]

    for col in b2b_yekdemunitcostdemand_sp_gwh_sums:
        b2b_yekdemunitcost_sellprice_sums[str(col)] = b2b_yekdemunitcostdemand_sp_gwh_sums[str(col)] / \
                                                      b2b_yekdemunitcostdemand_gwh_sums[str(col)]

    b2b_yekdemunitcost_sellprice = pd.concat(
        [b2b_yekdemunitcost_sellprice.fillna(0), b2b_yekdemunitcost_sellprice_sums.fillna(0)], axis=1)

    ppr.ara_tablolar["b2b_yekdemunitcost"]["b2b_yekdemunitcost_sellprice"] = b2b_yekdemunitcost_sellprice

    # # YEKDEM DAP DATA


    # YEKDEM DAP GWH
    b2b_yekdemdap_gwh = ppr.create_df(book_list_yekdemdap, yekdemdap_group, "TotalNetVolume")
    b2b_yekdemdap_gwh = ppr.add_normal_sum(b2b_yekdemdap_gwh)
    ppr.ara_tablolar["b2b_yekdemdap"]["b2b_yekdemdap_gwh"] = b2b_yekdemdap_gwh

    # YEKDEM DAP MTL
    b2b_yekdemdap_mtl = ppr.create_df(book_list_yekdemdap, yekdemdap_group, "TotalPL")
    b2b_yekdemdap_mtl = ppr.add_normal_sum(b2b_yekdemdap_mtl)
    ppr.ara_tablolar["b2b_yekdemdap"]["b2b_yekdemdap_mtl"] = b2b_yekdemdap_mtl

    # YEKDEM DAP SELL PRICE TL/MWh
    b2b_yekdemdap_sellprice = pd.DataFrame()
    b2b_yekdemdap_sellprice_product_gwh = pd.DataFrame()

    for book in book_list_yekdemdap:
        for item in book:
            yekdemdap_totalsellvolume = \
            yekdemdap_group[yekdemdap_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "TotalSellVolume"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            b2b_yekdemdap_sellpricex = \
            yekdemdap_group[yekdemdap_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            b2b_yekdemdap_sellprice[str(item)] = \
            yekdemdap_group[yekdemdap_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            b2b_yekdemdap_sellprice = b2b_yekdemdap_sellprice.add(b2b_yekdemdap_sellpricex, fill_value=0)
            b2b_yekdemdap_sellprice = b2b_yekdemdap_sellprice.add(yekdemdap_totalsellvolume, fill_value=0)
            b2b_yekdemdap_sellprice[str(item)] = b2b_yekdemdap_sellprice["SellCost"] / b2b_yekdemdap_sellprice[
                "TotalSellVolume"]
            b2b_yekdemdap_sellprice = b2b_yekdemdap_sellprice.drop(["SellCost", "TotalSellVolume"], axis=1)

    for col in [col for col in b2b_yekdemdap_gwh.copy().columns if all(x not in str(col) for x in ["Sum", "P2S"])]:
        b2b_yekdemdap_sellprice_product_gwh[str(col)] = b2b_yekdemdap_gwh[str(col)] * b2b_yekdemdap_sellprice[str(col)]

    b2b_yekdemdap_sellprice_product_gwh = ppr.add_normal_sum(b2b_yekdemdap_sellprice_product_gwh)

    b2b_yekdemdap_gwh_sums = b2b_yekdemdap_gwh.copy().loc[:, b2b_yekdemdap_gwh.columns.str.contains("Sum")]
    b2b_yekdemdap_sellprice_sums = b2b_yekdemdap_sellprice.copy().loc[:,
                                   b2b_yekdemdap_sellprice.columns.str.contains("Sum")]
    b2b_yekdemdap_sellprice_product_gwh_sums = b2b_yekdemdap_sellprice_product_gwh.copy().loc[:,
                                               b2b_yekdemdap_sellprice_product_gwh.columns.str.contains("Sum")]

    for col in b2b_yekdemdap_sellprice_product_gwh_sums:
        b2b_yekdemdap_sellprice_sums[str(col)] = b2b_yekdemdap_sellprice_product_gwh_sums[str(col)] / \
                                                 b2b_yekdemdap_gwh_sums[str(col)]

    b2b_yekdemdap_sellprice = pd.concat([b2b_yekdemdap_sellprice.fillna(0), b2b_yekdemdap_sellprice_sums.fillna(0)], axis=1)

    ppr.ara_tablolar["b2b_yekdemdap"]["b2b_yekdemdap_sellprice"] = b2b_yekdemdap_sellprice

    # # YEKDEM FX DATA


    # YEKDEM FX OTC GWH
    b2b_yekdemfx_otc_gwh = ppr.create_df(book_list_yekdemfxotc, yekdemfx_group, "TotalNetVolume")
    b2b_yekdemfx_otc_gwh = ppr.add_normal_sum(b2b_yekdemfx_otc_gwh)
    ppr.ara_tablolar["b2b_yekdemfx"]["b2b_yekdemfx_otc_gwh"] = b2b_yekdemfx_otc_gwh

    # YEKDEM FX OTC MTL
    b2b_yekdemfx_otc_mtl = ppr.create_df(book_list_yekdemfxotc, yekdemfx_group, "TotalPL")
    b2b_yekdemfx_otc_mtl = ppr.add_normal_sum(b2b_yekdemfx_otc_mtl)
    ppr.ara_tablolar["b2b_yekdemfx"]["b2b_yekdemfx_otc_mtl"] = b2b_yekdemfx_otc_mtl

    # YEKDEM FX BUY PRICE TL/MWh
    b2b_yekdemfx_buyprice = ppr.create_df(book_list_yekdemfxotc, yekdemfx_group, "BuyCost")
    b2b_yekdemfx_buyprice_product_gwh = pd.DataFrame()

    for col in b2b_yekdemfx_buyprice.loc[:, ~b2b_yekdemfx_buyprice.columns.str.contains("Sum")].columns:
        b2b_yekdemfx_buyprice[str(col)] = b2b_yekdemfx_buyprice[str(col)] / b2b_yekdemfx_otc_gwh[str(col)]

    for col in [col for col in b2b_yekdemfx_otc_gwh.copy().columns if all(x not in str(col) for x in ["Sum", "P2S"])]:
        b2b_yekdemfx_buyprice_product_gwh[str(col)] = b2b_yekdemfx_otc_gwh[str(col)] * b2b_yekdemfx_buyprice[str(col)]

    b2b_yekdemfx_buyprice_product_gwh = ppr.add_normal_sum(b2b_yekdemfx_buyprice_product_gwh)

    b2b_yekdemfx_otc_gwh_sums = b2b_yekdemfx_otc_gwh.copy().loc[:, b2b_yekdemfx_otc_gwh.columns.str.contains("Sum")]
    b2b_yekdemfx_buyprice_sums = b2b_yekdemfx_buyprice.copy().loc[:, b2b_yekdemfx_buyprice.columns.str.contains("Sum")]
    b2b_yekdemfx_buyprice_product_gwh_sums = b2b_yekdemfx_buyprice_product_gwh.copy().loc[:,
                                             b2b_yekdemfx_buyprice_product_gwh.columns.str.contains("Sum")]

    for col in b2b_yekdemfx_buyprice_product_gwh_sums:
        b2b_yekdemfx_buyprice_sums[str(col)] = b2b_yekdemfx_buyprice_product_gwh_sums[str(col)] / b2b_yekdemfx_otc_gwh_sums[
            str(col)]

    b2b_yekdemfx_buyprice = pd.concat([b2b_yekdemfx_buyprice.fillna(0), b2b_yekdemfx_buyprice_sums.fillna(0)], axis=1)

    ppr.ara_tablolar["b2b_yekdemfx"]["b2b_yekdemfx_buyprice"] = b2b_yekdemfx_buyprice

    # YEKDEM FX DEMAND GWH
    b2b_yekdemfx_demand_gwh = ppr.create_df(book_list_yekdemfxdemand, yekdemfx_group, "TotalNetVolume")
    b2b_yekdemfx_demand_gwh = ppr.add_normal_sum(b2b_yekdemfx_demand_gwh)
    ppr.ara_tablolar["b2b_yekdemfx"]["b2b_yekdemfx_demand_gwh"] = b2b_yekdemfx_demand_gwh

    # YEKDEM FX SELL PRICE TL/MWh
    b2b_yekdemfx_sellprice = ppr.create_df(book_list_yekdemfxdemand, yekdemfx_group, "SellCost")
    b2b_yekdemfx_sellprice_product_gwh = pd.DataFrame()

    for col in b2b_yekdemfx_sellprice.loc[:, ~b2b_yekdemfx_sellprice.copy().columns.str.contains("Sum")].columns:
        b2b_yekdemfx_sellprice[str(col)] = b2b_yekdemfx_sellprice[str(col)] / (b2b_yekdemfx_demand_gwh[str(col)] * -1)

    for col in [col for col in b2b_yekdemfx_demand_gwh.copy().columns if all(x not in str(col) for x in ["Sum", "P2S"])]:
        b2b_yekdemfx_sellprice_product_gwh[str(col)] = b2b_yekdemfx_demand_gwh[str(col)] * b2b_yekdemfx_sellprice[str(col)]

    b2b_yekdemfx_sellprice_product_gwh = ppr.add_normal_sum(b2b_yekdemfx_sellprice_product_gwh)

    b2b_yekdemfx_demand_gwh_sums = b2b_yekdemfx_demand_gwh.copy().loc[:,
                                   b2b_yekdemfx_demand_gwh.columns.str.contains("Sum")]
    b2b_yekdemfx_sellprice_sums = b2b_yekdemfx_sellprice.copy().loc[:, b2b_yekdemfx_sellprice.columns.str.contains("Sum")]
    b2b_yekdemfx_sellprice_product_gwh_sums = b2b_yekdemfx_sellprice_product_gwh.copy().loc[:,
                                              b2b_yekdemfx_sellprice_product_gwh.columns.str.contains("Sum")]

    for col in b2b_yekdemfx_sellprice_product_gwh_sums:
        b2b_yekdemfx_sellprice_sums[str(col)] = b2b_yekdemfx_sellprice_product_gwh_sums[str(col)] / \
                                                b2b_yekdemfx_demand_gwh_sums[str(col)]

    b2b_yekdemfx_sellprice = pd.concat([b2b_yekdemfx_sellprice.fillna(0), b2b_yekdemfx_sellprice_sums.fillna(0)], axis=1)

    ppr.ara_tablolar["b2b_yekdemfx"]["b2b_yekdemfx_sellprice"] = b2b_yekdemfx_sellprice

    # # ----BULK----

    # ENERGY DATA
    book_groups_energy = energy["BookGroupName"].unique()
    # DEMAND
    book_Bulk_fixed_demand = [item for item in book_groups_energy if
                              all(x in str(item) for x in ["Fixed", "Bulk", "Demand"])]
    book_Bulk_Free_demand = [item for item in book_groups_energy if all(x in str(item) for x in ["Free", "Bulk", "Demand"])]
    book_Bulk_tedas_demand = [item for item in book_groups_energy if
                              all(x in str(item) for x in ["Tedas", "Bulk", "Demand"])]

    book_list_demand_bulk = [book_Bulk_fixed_demand, book_Bulk_Free_demand, book_Bulk_tedas_demand]
    # OTC
    book_Bulk_fixed_otc = [item for item in book_groups_energy if all(x in str(item) for x in ["Fixed", "Bulk", "OTC"])]
    book_Bulk_Free_otc = [item for item in book_groups_energy if all(x in str(item) for x in ["Free", "Bulk", "OTC"])]
    book_Bulk_tedas_otc = [item for item in book_groups_energy if all(x in str(item) for x in ["Tedas", "Bulk", "OTC"])]

    book_list_otc_bulk = [book_Bulk_fixed_otc, book_Bulk_Free_otc, book_Bulk_tedas_otc]
    # DAP Hedge
    book_Bulk_fixed_daphedge = [item for item in book_groups_energy if
                                all(x in str(item) for x in ["Fixed", "Bulk", "DAP Hedge"])]
    book_Bulk_Free_daphedge = [item for item in book_groups_energy if
                               all(x in str(item) for x in ["Free", "Bulk", "DAP Hedge"])]
    book_Bulk_tedas_daphedge = [item for item in book_groups_energy if
                                all(x in str(item) for x in ["Tedas", "Bulk", "DAP Hedge"])]

    book_list_daphedge_bulk = [book_Bulk_fixed_daphedge, book_Bulk_Free_daphedge, book_Bulk_tedas_daphedge]
    # Total Energy
    book_Bulk_fixed_totalenergy = [item for item in book_groups_energy if
                                   all(x in str(item) for x in ["Fixed", "Bulk", "Total Energy"])]
    book_Bulk_Free_totalenergy = [item for item in book_groups_energy if
                                  all(x in str(item) for x in ["Free", "Bulk", "Total Energy"])]
    book_Bulk_tedas_totalenergy = [item for item in book_groups_energy if
                                   all(x in str(item) for x in ["Tedas", "Bulk", "Total Energy"])]

    book_list_totalenergy_bulk = [book_Bulk_fixed_totalenergy, book_Bulk_Free_totalenergy, book_Bulk_tedas_totalenergy]

    # Firm
    book_Bulk_fixed_firm = [item for item in book_groups_energy if all(x in str(item) for x in ["Fixed", "Bulk", "Firm"])]
    book_Bulk_Free_firm = [item for item in book_groups_energy if all(x in str(item) for x in ["Free", "Bulk", "Firm"])]
    book_Bulk_tedas_firm = [item for item in book_groups_energy if all(x in str(item) for x in ["Tedas", "Bulk", "Firm"])]

    book_list_firm_bulk = [book_Bulk_fixed_firm, book_Bulk_Free_firm, book_Bulk_tedas_firm]

    # ENERGY FX DATA
    book_groups_energyfx = energyfx["BookGroupName"].unique()

    book_Bulk_energy_fx = [item for item in book_groups_energyfx if all(x in str(item) for x in ["FX", "Bulk", "Energy"])]
    # YEKDEM UNIT COST DATA
    book_groups_yekdemunitcost = yekdemunitcost["BookGroupName"].unique()

    book_Bulk_fixed_yekdemunitcost = [item for item in book_groups_yekdemunitcost if
                                      all(x in str(item) for x in ["Fixed", "Bulk", "Yekdem Unit Cost"])]
    book_Bulk_Free_yekdemunitcost = [item for item in book_groups_yekdemunitcost if
                                     all(x in str(item) for x in ["Free", "Bulk", "Yekdem Unit Cost"])]
    book_Bulk_tedas_yekdemunitcost = [item for item in book_groups_yekdemunitcost if
                                      all(x in str(item) for x in ["Tedas", "Bulk", "Yekdem Unit Cost"])]

    book_list_yekdemunitcost_bulk = [book_Bulk_fixed_yekdemunitcost, book_Bulk_Free_yekdemunitcost,
                                     book_Bulk_tedas_yekdemunitcost]
    # YEKDEM DAP DATA
    book_groups_yekdemdap = yekdemdap["BookGroupName"].unique()

    book_Bulk_fixed_yekdemdap = [item for item in book_groups_yekdemdap if
                                 all(x in str(item) for x in ["Fixed", "Bulk", "Yekdem DAP"])]
    book_Bulk_Free_yekdemdap = [item for item in book_groups_yekdemdap if
                                all(x in str(item) for x in ["Free", "Bulk", "Yekdem DAP"])]
    book_Bulk_tedas_yekdemdap = [item for item in book_groups_yekdemdap if
                                 all(x in str(item) for x in ["Tedas", "Bulk", "Yekdem DAP"])]

    book_list_yekdemdap_bulk = [book_Bulk_fixed_yekdemdap, book_Bulk_Free_yekdemdap, book_Bulk_tedas_yekdemdap]
    # YEKDEM FX DATA
    book_groups_yekdemfx = yekdemfx["BookGroupName"].unique()

    # YEKDEM FX OTC
    book_Bulk_fixed_yekdemfx_otc = [item for item in book_groups_yekdemfx if
                                    all(x in str(item) for x in ["Fixed", "Bulk", "OTC"])]
    book_Bulk_Free_yekdemfx_otc = [item for item in book_groups_yekdemfx if
                                   all(x in str(item) for x in ["Free", "Bulk", "OTC"])]
    book_Bulk_tedas_yekdemfx_otc = [item for item in book_groups_yekdemfx if
                                    all(x in str(item) for x in ["Tedas", "Bulk", "OTC"])]

    book_list_yekdemfxotc_bulk = [book_Bulk_fixed_yekdemfx_otc, book_Bulk_Free_yekdemfx_otc, book_Bulk_tedas_yekdemfx_otc]
    # YEKDEM FX DEMAND
    book_Bulk_fixed_yekdemfx_demand = [item for item in book_groups_yekdemfx if
                                       all(x in str(item) for x in ["Fixed", "Bulk", "Demand"])]
    book_Bulk_Free_yekdemfx_demand = [item for item in book_groups_yekdemfx if
                                      all(x in str(item) for x in ["Free", "Bulk", "Demand"])]
    book_Bulk_tedas_yekdemfx_demand = [item for item in book_groups_yekdemfx if
                                       all(x in str(item) for x in ["Tedas", "Bulk", "Demand"])]

    book_list_yekdemfxdemand_bulk = [book_Bulk_fixed_yekdemfx_demand, book_Bulk_Free_yekdemfx_demand,
                                     book_Bulk_tedas_yekdemfx_demand]

    # Bulk DEMAND GWH
    Bulk_demand_gwh = ppr.create_df(book_list_demand_bulk, energy_group, "TotalNetVolume")
    Bulk_demand_gwh = ppr.add_normal_sum(Bulk_demand_gwh)
    ppr.ara_tablolar["bulk_energy"]["Bulk_demand_gwh"] = Bulk_demand_gwh
    # Bulk DEMAND MW
    Bulk_demand_mw = ppr.gwh_to_mw(Bulk_demand_gwh)
    ppr.ara_tablolar["bulk_energy"]["Bulk_demand_mw"] = Bulk_demand_mw
    # Bulk OTC GWH
    Bulk_otc_gwh = ppr.create_df(book_list_otc_bulk, energy_group, "TotalNetVolume")
    Bulk_otc_gwh = ppr.add_normal_sum(Bulk_otc_gwh)
    ppr.ara_tablolar["bulk_energy"]["Bulk_otc_gwh"] = Bulk_otc_gwh
    # Bulk OTC MW
    Bulk_otc_mw = ppr.gwh_to_mw(Bulk_otc_gwh)
    ppr.ara_tablolar["bulk_energy"]["Bulk_otc_mw"] = Bulk_otc_mw
    # DAP HEDGE MW
    Bulk_daphedge_gwh = ppr.create_df(book_list_daphedge_bulk, energy_group, "TotalNetVolume")
    Bulk_daphedge_gwh = ppr.add_normal_sum(Bulk_daphedge_gwh)
    ppr.ara_tablolar["bulk_energy"]["Bulk_daphedge_gwh"] = Bulk_daphedge_gwh
    # TOTAL HEDGE MW
    bulk_total_hedge_mw = pd.DataFrame()
    bulk_parameters = ["Fixed", "Free", "Tedas"]
    dict_otc_bulk = {}
    dict_dap_bulk = {}
    for parameter in bulk_parameters:
        dict_dap_bulk[f'{parameter}_bulk'] = [col for col in
                                              Bulk_daphedge_gwh.loc[:, Bulk_daphedge_gwh.columns.str.contains(parameter)]]
        dict_otc_bulk[f'{parameter}_bulk'] = [col2 for col2 in
                                              Bulk_otc_gwh.loc[:, Bulk_otc_gwh.columns.str.contains(parameter)]]

    for parameter in bulk_parameters:
        if len(dict_otc_bulk[f'{parameter}_bulk']) != 0 and len(dict_dap_bulk[f'{parameter}_bulk']) != 0:
            for col1, col2 in zip(dict_otc_bulk[f'{parameter}_bulk'], dict_dap_bulk[f'{parameter}_bulk']):
                col_new = col1.replace("OTC", "Total Hedge")
                bulk_total_hedge_mw[col_new] = Bulk_otc_gwh[str(col1)] + Bulk_daphedge_gwh[str(col2)]
        elif len(dict_otc_bulk[f'{parameter}_bulk']) == 0 and len(dict_dap_bulk[f'{parameter}_bulk']) != 0:
            for col in dict_dap_bulk[f'{parameter}_bulk']:
                col_new = col.replace("DAP Hedge", "Total Hedge")
                bulk_total_hedge_mw[col_new] = Bulk_daphedge_gwh[str(col)]
        elif len(dict_otc_bulk[f'{parameter}_bulk']) != 0 and len(dict_dap_bulk[f'{parameter}_bulk']) == 0:
            for col in dict_otc_bulk[f'{parameter}_bulk']:
                col_new = col.replace("OTC", "Total Hedge")
                bulk_total_hedge_mw[col_new] = Bulk_otc_gwh[str(col)]
    bulk_total_hedge_mw = ppr.gwh_to_mw(bulk_total_hedge_mw)
    bulk_total_hedge_mw = ppr.add_normal_sum(bulk_total_hedge_mw)
    ppr.ara_tablolar["bulk_energy"]["bulk_total_hedge_mw"] = bulk_total_hedge_mw
    # Bulk NET POSITION MW
    bulk_net_position_mw = pd.DataFrame()
    for col in bulk_total_hedge_mw.columns:
        col_new = str(col).replace("Total Hedge", "Net Position")
        col_demand = str(col).replace("Total Hedge", "Demand")
        bulk_net_position_mw[str(col_new)] = bulk_total_hedge_mw[str(col)] - (Bulk_demand_mw[str(col_demand)] * -1)
    ppr.ara_tablolar["bulk_energy"]["bulk_net_position_mw"] = bulk_net_position_mw

    # ENERGY FX VOLUME
    Bulk_energyfx_volume = ppr.create_df([book_Bulk_energy_fx], energyfx_group, "TotalNetVolume")
    Bulk_energyfx_volume = Bulk_energyfx_volume.iloc[1:]
    Bulk_energyfx_volume = ppr.add_normal_sum(Bulk_energyfx_volume)
    ppr.ara_tablolar["bulk_energyfx"]["Bulk_energyfx_volume"] = Bulk_energyfx_volume
    # ENERGY FX OTC MTL
    Bulk_energyfx_otcmtl = ppr.create_df([book_Bulk_energy_fx], energyfx_group, "TotalPL")
    Bulk_energyfx_otcmtl = Bulk_energyfx_otcmtl.iloc[1:]
    Bulk_energyfx_otcmtl = ppr.add_normal_sum(Bulk_energyfx_otcmtl)
    ppr.ara_tablolar["bulk_energyfx"]["Bulk_energyfx_otcmtl"] = Bulk_energyfx_otcmtl
    # BUY PRICE TL/MWH
    Bulk_energyfx_buyprice = ppr.create_df([book_Bulk_energy_fx], energyfx_group, "BuyCost")
    Bulk_energyfx_buypricesum = pd.DataFrame()

    for col in Bulk_energyfx_buyprice.columns:
        Bulk_energyfx_buyprice[str(col)] = Bulk_energyfx_buyprice[str(col)] / (Bulk_energyfx_volume[str(col)])
        Bulk_energyfx_buypricesum[str(col)] = Bulk_energyfx_buyprice[str(col)] * (Bulk_energyfx_volume[str(col)])
    Bulk_energyfx_buypricesum = ppr.add_normal_sum(Bulk_energyfx_buypricesum)

    Bulk_energyfx_volume_sums = Bulk_energyfx_volume.copy().loc[:, Bulk_energyfx_volume.columns.str.contains("Sum")]
    Bulk_energyfx_buypricesums = Bulk_energyfx_buypricesum.copy().loc[:,
                                 Bulk_energyfx_buypricesum.columns.str.contains("Sum")]

    for col in Bulk_energyfx_volume_sums.columns:
        Bulk_energyfx_buypricesums[str(col)] = Bulk_energyfx_buypricesums[str(col)] / (Bulk_energyfx_volume_sums[str(col)])

    Bulk_energyfx_buypricesums = Bulk_energyfx_buypricesums.iloc[1:]
    Bulk_energyfx_buyprice = Bulk_energyfx_buyprice.iloc[1:].reindex(sorted(Bulk_energyfx_buyprice.columns), axis=1)
    Bulk_energyfx_buyprice = pd.concat([Bulk_energyfx_buyprice, Bulk_energyfx_buypricesums], axis=1).fillna(0)
    ppr.ara_tablolar["bulk_energyfx"]["Bulk_energyfx_buyprice"] = Bulk_energyfx_buyprice

    # Bulk Firm MTL
    Bulk_firm_mtl = ppr.create_df(book_list_firm_bulk, energy_group, "TotalPL")
    Bulk_firm_mtl = ppr.add_normal_sum(Bulk_firm_mtl)
    ppr.ara_tablolar["bulk_energy"]["Bulk_firm_mtl"] = Bulk_firm_mtl
    # Total Energy MTL
    Bulk_total_energy_mtl = ppr.create_df(book_list_totalenergy_bulk, energy_group, "TotalPL")

    for col in Bulk_energyfx_otcmtl.loc[:, ~Bulk_energyfx_otcmtl.columns.str.contains("Sum")].columns:
        col_new = str(col).replace("Energy FX", "Fixed Total Energy")
        Bulk_total_energy_mtl[str(col_new)] = Bulk_total_energy_mtl[str(col_new)] + (Bulk_energyfx_otcmtl[str(col)].values)

    for col in Bulk_firm_mtl.loc[:, ~Bulk_firm_mtl.columns.str.contains("Sum")].columns:
        col_new = str(col).replace("Firm", "Total Energy")
        Bulk_total_energy_mtl[str(col_new)] = Bulk_total_energy_mtl[str(col_new)] + (Bulk_firm_mtl[str(col)].values)

    Bulk_total_energy_mtl = ppr.add_normal_sum(Bulk_total_energy_mtl)
    ppr.ara_tablolar["bulk_energy"]["Bulk_total_energy_mtl"] = Bulk_total_energy_mtl

    # Sell Price TL/MWh
    Bulk_energy_sellprice = pd.DataFrame()
    Bulk_energy_sellprice_product_gwh = pd.DataFrame()
    Bulk_energy_sellprice_sums = pd.DataFrame()
    for book in book_list_firm_bulk:
        for item in book:
            sell_cost = energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            sell_volume_peak = energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "SellVolumePeak"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            sell_volume_offpeak = energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "SellVolumeOffpeak"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            Bulk_energy_sellprice[str(item)] = \
            energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            Bulk_energy_sellprice = Bulk_energy_sellprice.add(sell_cost, fill_value=0)
            Bulk_energy_sellprice = Bulk_energy_sellprice.add(sell_volume_peak, fill_value=0)
            Bulk_energy_sellprice = Bulk_energy_sellprice.add(sell_volume_offpeak, fill_value=0)
            Bulk_energy_sellprice[str(item)] = Bulk_energy_sellprice["SellCost"] / (
                        Bulk_energy_sellprice["SellVolumePeak"] + Bulk_energy_sellprice["SellVolumeOffpeak"])
            Bulk_energy_sellprice = Bulk_energy_sellprice.drop(["SellCost", "SellVolumePeak", "SellVolumeOffpeak"], axis=1)

    for col in [col for col in Bulk_demand_gwh.copy().columns if all(x not in str(col) for x in ["Sum", "P2S"])]:
        col_new = str(col).replace("Demand", "Firm")
        Bulk_energy_sellprice_product_gwh[str(col_new)] = Bulk_energy_sellprice[str(col_new)] * Bulk_demand_gwh[str(col)]

    Bulk_energy_sellprice_product_gwh = ppr.add_normal_sum(Bulk_energy_sellprice_product_gwh)

    Bulk_demand_gwh_sums = Bulk_demand_gwh.copy().loc[:, Bulk_demand_gwh.columns.str.contains("Sum")]
    Bulk_energy_sellprice_product_gwh_sums = Bulk_energy_sellprice_product_gwh.copy().loc[:,
                                             Bulk_energy_sellprice_product_gwh.columns.str.contains("Sum")]

    for col in Bulk_energy_sellprice_product_gwh_sums:
        Bulk_energy_sellprice_sums[str(col)] = Bulk_energy_sellprice_product_gwh_sums[str(col)] / Bulk_demand_gwh_sums[
            str(col)]

    Bulk_energy_sellprice = pd.concat([Bulk_energy_sellprice, Bulk_energy_sellprice_sums], axis=1).fillna(0)
    ppr.ara_tablolar["bulk_energy"]["Bulk_energy_sellprice"] = Bulk_energy_sellprice

    # ENERGY BUY PRICE
    Bulk_energy_buyprice = pd.DataFrame()
    Bulk_energy_buyvolume = pd.DataFrame()
    Bulk_energy_buyprice_product_mw = pd.DataFrame()
    Bulk_energy_buyprice_sums = pd.DataFrame()
    otc_buycost = pd.DataFrame()
    daphedge_buycost = pd.DataFrame()
    otc_totalbuyvolume = pd.DataFrame()
    daphedge_totalbuyvolume = pd.DataFrame()
    # buy cost
    for book in book_list_otc_bulk:
        for item in sorted(book):
            col_total_hedge = str(item).replace("OTC", "Total Hedge")
            otc_buycost[str(col_total_hedge)] = \
            energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "BuyCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
    # buy cost
    for book in book_list_daphedge_bulk:
        for item in sorted(book):
            col_total_hedge = str(item).replace("DAP", "Total")
            daphedge_buycost[str(col_total_hedge)] = \
            energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "BuyCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)

    otc_buycost = otc_buycost.fillna(0)
    daphedge_buycost = daphedge_buycost.fillna(0)

    Bulk_energy_buyprice_fixed = otc_buycost.loc[:, otc_buycost.columns.str.contains("Fixed")].add(
        daphedge_buycost.loc[:, daphedge_buycost.columns.str.contains("Fixed")].values, fill_value=0)
    Bulk_energy_buyprice = pd.concat([Bulk_energy_buyprice, Bulk_energy_buyprice_fixed], axis=1)

    otcy = otc_buycost.loc[:, otc_buycost.columns.str.contains("Free")].reset_index(drop=True).shape[1]
    daphedgey = daphedge_buycost.loc[:, daphedge_buycost.columns.str.contains("Free")].reset_index(drop=True).shape[1]

    otcy2 = otc_buycost.loc[:, otc_buycost.columns.str.contains("Tedas")].reset_index(drop=True).shape[1]
    daphedgey2 = daphedge_buycost.loc[:, daphedge_buycost.columns.str.contains("Tedas")].reset_index(drop=True).shape[1]

    if otcy != 0 and daphedgey != 0:
        Bulk_energy_buyprice_free = otc_buycost.loc[:, otc_buycost.columns.str.contains("Free")].add(
            daphedge_buycost.loc[:, daphedge_buycost.columns.str.contains("Free")].values, fill_value=0)
        Bulk_energy_buyprice = pd.concat([Bulk_energy_buyprice, Bulk_energy_buyprice_free], axis=1)
    elif otcy == 0 and daphedgey != 0:
        Bulk_energy_buyprice_free = daphedge_buycost.loc[:, daphedge_buycost.columns.str.contains("Free")]
        Bulk_energy_buyprice = pd.concat([Bulk_energy_buyprice, Bulk_energy_buyprice_free], axis=1)
    elif otcy != 0 and daphedgey == 0:
        Bulk_energy_buyprice_free = otc_buycost.loc[:, otc_buycost.columns.str.contains("Free")]
        Bulk_energy_buyprice = pd.concat([Bulk_energy_buyprice, Bulk_energy_buyprice_free], axis=1)

    if otcy2 != 0 and daphedgey2 != 0:
        Bulk_energy_buyprice_tedas = otc_buycost.loc[:, otc_buycost.columns.str.contains("Tedas")].add(
            daphedge_buycost.loc[:, daphedge_buycost.columns.str.contains("Free")].values, fill_value=0)
        Bulk_energy_buyprice = pd.concat([Bulk_energy_buyprice, Bulk_energy_buyprice_tedas], axis=1)
    elif otcy2 == 0 and daphedgey2 != 0:
        Bulk_energy_buyprice_tedas = daphedge_buycost.loc[:, daphedge_buycost.columns.str.contains("Tedas")]
        Bulk_energy_buyprice = pd.concat([Bulk_energy_buyprice, Bulk_energy_buyprice_tedas], axis=1)
    elif otcy2 != 0 and daphedgey2 == 0:
        Bulk_energy_buyprice_tedas = otc_buycost.loc[:, otc_buycost.columns.str.contains("Tedas")]
        Bulk_energy_buyprice = pd.concat([Bulk_energy_buyprice, Bulk_energy_buyprice_tedas], axis=1)

    # energy fx
    for col in Bulk_energyfx_otcmtl.loc[:, ~Bulk_energyfx_otcmtl.columns.str.contains("Sum")].columns:
        col_new = str(col).replace("Energy FX", "Fixed Total Hedge")
        Bulk_energy_buyprice[str(col_new)] = Bulk_energy_buyprice[str(col_new)].copy() - (
                    Bulk_energyfx_otcmtl[str(col)].values * 1000)

    # buy volume
    otc_buyvolume = pd.DataFrame()
    daphedge_buyvolume = pd.DataFrame()
    Bulk_energy_buyvolume = pd.DataFrame()
    for book in book_list_otc_bulk:
        for item in sorted(book):
            col_total_hedge = str(item).replace("OTC", "All Total Buy Volume")
            otc_buyvolume[str(col_total_hedge)] = \
            energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "TotalBuyVolume"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
    # buy cost
    for book in book_list_daphedge_bulk:
        for item in sorted(book):
            col_total_hedge = str(item).replace("DAP Hedge", "All Total Buy Volume")
            daphedge_buyvolume[str(col_total_hedge)] = \
            energy_group[energy_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "TotalBuyVolume"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)

    otc_buyvolume = otc_buyvolume.fillna(0)
    daphedge_buyvolume = daphedge_buyvolume.fillna(0)

    Bulk_energy_buyvolume_fixed = otc_buyvolume.loc[:, otc_buyvolume.columns.str.contains("Fixed")].add(
        daphedge_buyvolume.loc[:, daphedge_buyvolume.columns.str.contains("Fixed")].values, fill_value=0)
    Bulk_energy_buyvolume = pd.concat([Bulk_energy_buyvolume, Bulk_energy_buyvolume_fixed], axis=1)

    otcy_volume = otc_buyvolume.loc[:, otc_buyvolume.columns.str.contains("Free")].reset_index(drop=True).shape[1]
    daphedgey_volume = \
    daphedge_buyvolume.loc[:, daphedge_buyvolume.columns.str.contains("Free")].reset_index(drop=True).shape[1]

    otcy_volume2 = otc_buyvolume.loc[:, otc_buyvolume.columns.str.contains("Tedas")].reset_index(drop=True).shape[1]
    daphedgey_volume2 = \
    daphedge_buyvolume.loc[:, daphedge_buyvolume.columns.str.contains("Tedas")].reset_index(drop=True).shape[1]

    if otcy_volume != 0 and daphedgey_volume != 0:
        Bulk_energy_buyvolume_free = otc_buyvolume.loc[:, otc_buyvolume.columns.str.contains("Free")].add(
            daphedge_buyvolume.loc[:, daphedge_buyvolume.columns.str.contains("Free")].values, fill_value=0)
        Bulk_energy_buyvolume = pd.concat([Bulk_energy_buyvolume, Bulk_energy_buyvolume_free], axis=1)
    elif otcy_volume == 0 and daphedgey_volume != 0:
        Bulk_energy_buyvolume_free = daphedge_buyvolume.loc[:, daphedge_buyvolume.columns.str.contains("Free")]
        Bulk_energy_buyvolume = pd.concat([Bulk_energy_buyvolume, Bulk_energy_buyvolume_free], axis=1)
    elif otcy_volume != 0 and daphedgey_volume == 0:
        Bulk_energy_buyvolume_free = otc_buyvolume.loc[:, otc_buyvolume.columns.str.contains("Free")]
        Bulk_energy_buyvolume = pd.concat([Bulk_energy_buyvolume, Bulk_energy_buyvolume_free], axis=1)

    if otcy_volume2 != 0 and daphedgey_volume2 != 0:
        Bulk_energy_buyvolume_tedas = otc_buyvolume.loc[:, otc_buyvolume.columns.str.contains("Tedas")].add(
            daphedge_buyvolume.loc[:, daphedge_buyvolume.columns.str.contains("Free")].values, fill_value=0)
        Bulk_energy_buyvolume = pd.concat([Bulk_energy_buyvolume, Bulk_energy_buyvolume_tedas], axis=1)
    elif otcy_volume2 == 0 and daphedgey_volume2 != 0:
        Bulk_energy_buyvolume_tedas = daphedge_buyvolume.loc[:, daphedge_buyvolume.columns.str.contains("Tedas")]
        Bulk_energy_buyvolume = pd.concat([Bulk_energy_buyvolume, Bulk_energy_buyvolume_tedas], axis=1)
    elif otcy_volume2 != 0 and daphedgey_volume2 == 0:
        Bulk_energy_buyvolume_tedas = otc_buyvolume.loc[:, otc_buyvolume.columns.str.contains("Tedas")]
        Bulk_energy_buyvolume = pd.concat([Bulk_energy_buyvolume, Bulk_energy_buyvolume_tedas], axis=1)

    Bulk_energy_buyprice = Bulk_energy_buyprice.reindex(sorted(Bulk_energy_buyprice.columns), axis=1)
    Bulk_energy_buyprice = Bulk_energy_buyprice / Bulk_energy_buyvolume.values
    Bulk_energy_buyprice = Bulk_energy_buyprice.fillna(0)

    # BUY PRICE SUM
    for col in [col for col in bulk_total_hedge_mw.copy().columns if all(x not in str(col) for x in ["Sum", "P2S"])]:
        Bulk_energy_buyprice_product_mw[str(col)] = Bulk_energy_buyprice[str(col)] * bulk_total_hedge_mw[str(col)]

    Bulk_energy_buyprice_product_mw = ppr.add_normal_sum(Bulk_energy_buyprice_product_mw)

    total_hedge_mw_sums = bulk_total_hedge_mw.copy().loc[:, bulk_total_hedge_mw.columns.str.contains("Sum")]
    Bulk_energy_buyprice_product_mw_sums = Bulk_energy_buyprice_product_mw.copy().loc[:,
                                           Bulk_energy_buyprice_product_mw.columns.str.contains("Sum")]

    for col in Bulk_energy_buyprice_product_mw_sums:
        Bulk_energy_buyprice_sums[str(col)] = Bulk_energy_buyprice_product_mw_sums[str(col)] / total_hedge_mw_sums[str(col)]

    Bulk_energy_buyprice = pd.concat([Bulk_energy_buyprice, Bulk_energy_buyprice_sums], axis=1)

    ppr.ara_tablolar["bulk_energy"]["Bulk_energy_buyprice"] = Bulk_energy_buyprice

    Bulk_energy_buypeak_demand = ppr.create_df(book_list_demand_bulk, energy_group, "BuyVolumePeak")
    Bulk_energy_sellpeak_demand = ppr.create_df(book_list_demand_bulk, energy_group, "SellVolumePeak")
    Bulk_energy_peak_demand = Bulk_energy_buypeak_demand - Bulk_energy_sellpeak_demand.values
    Bulk_energy_peak_demand = ppr.gwh_to_mw(Bulk_energy_peak_demand) * 2
    Bulk_energy_peak_demand = ppr.add_normal_sum(Bulk_energy_peak_demand)

    Bulk_energy_buyoffpeak_demand = ppr.create_df(book_list_demand_bulk, energy_group, "BuyVolumeOffpeak")
    Bulk_energy_selloffpeak_demand = ppr.create_df(book_list_demand_bulk, energy_group, "SellVolumeOffpeak")
    Bulk_energy_offpeak_demand = Bulk_energy_buyoffpeak_demand - Bulk_energy_selloffpeak_demand.values
    Bulk_energy_offpeak_demand = ppr.gwh_to_mw(Bulk_energy_offpeak_demand) * 2
    Bulk_energy_offpeak_demand = ppr.add_normal_sum(Bulk_energy_offpeak_demand)

    Bulk_energy_buypeak_otc = ppr.create_df(book_list_otc_bulk, energy_group, "BuyVolumePeak")
    Bulk_energy_sellpeak_otc = ppr.create_df(book_list_otc_bulk, energy_group, "SellVolumePeak")
    Bulk_energy_peak_otc = Bulk_energy_buypeak_otc - Bulk_energy_sellpeak_otc.values
    Bulk_energy_peak_otc = ppr.gwh_to_mw(Bulk_energy_peak_otc) * 2
    Bulk_energy_peak_otc = ppr.add_normal_sum(Bulk_energy_peak_otc)

    Bulk_energy_buyoffpeak_otc = ppr.create_df(book_list_otc_bulk, energy_group, "BuyVolumeOffpeak")
    Bulk_energy_selloffpeak_otc = ppr.create_df(book_list_otc_bulk, energy_group, "SellVolumeOffpeak")
    Bulk_energy_offpeak_otc = Bulk_energy_buyoffpeak_otc - Bulk_energy_selloffpeak_otc.values
    Bulk_energy_offpeak_otc = ppr.gwh_to_mw(Bulk_energy_offpeak_otc) * 2
    Bulk_energy_offpeak_otc = ppr.add_normal_sum(Bulk_energy_offpeak_otc)

    Bulk_energy_buypeak_daphedge = ppr.create_df(book_list_daphedge_bulk, energy_group, "BuyVolumePeak")
    Bulk_energy_sellpeak_daphedge = ppr.create_df(book_list_daphedge_bulk, energy_group, "SellVolumePeak")
    Bulk_energy_peak_daphedge = Bulk_energy_buypeak_daphedge - Bulk_energy_sellpeak_daphedge.values
    Bulk_energy_peak_daphedge = ppr.gwh_to_mw(Bulk_energy_peak_daphedge) * 2
    Bulk_energy_peak_daphedge = ppr.add_normal_sum(Bulk_energy_peak_daphedge)

    Bulk_energy_buyoffpeak_daphedge = ppr.create_df(book_list_daphedge_bulk, energy_group, "BuyVolumeOffpeak")
    Bulk_energy_selloffpeak_daphedge = ppr.create_df(book_list_daphedge_bulk, energy_group, "SellVolumeOffpeak")
    Bulk_energy_offpeak_daphedge = Bulk_energy_buyoffpeak_daphedge - Bulk_energy_selloffpeak_daphedge.values
    Bulk_energy_offpeak_daphedge = ppr.gwh_to_mw(Bulk_energy_offpeak_daphedge) * 2
    Bulk_energy_offpeak_daphedge = ppr.add_normal_sum(Bulk_energy_offpeak_daphedge)

    Bulk_energy_peak_total = Bulk_energy_peak_demand + Bulk_energy_peak_otc + Bulk_energy_peak_daphedge
    Bulk_energy_offpeak_total = Bulk_energy_offpeak_demand + Bulk_energy_offpeak_otc + Bulk_energy_offpeak_daphedge

    ppr.ara_tablolar["bulk_energy"]["Bulk_energy_peak_demand"] = Bulk_energy_peak_demand
    ppr.ara_tablolar["bulk_energy"]["Bulk_energy_offpeak_demand"] = Bulk_energy_offpeak_demand
    ppr.ara_tablolar["bulk_energy"]["Bulk_energy_peak_otc"] = Bulk_energy_peak_otc
    ppr.ara_tablolar["bulk_energy"]["Bulk_energy_offpeak_otc"] = Bulk_energy_offpeak_otc
    ppr.ara_tablolar["bulk_energy"]["Bulk_energy_peak_daphedge"] = Bulk_energy_peak_daphedge
    ppr.ara_tablolar["bulk_energy"]["Bulk_energy_offpeak_daphedge"] = Bulk_energy_offpeak_daphedge
    ppr.final_tablolar["mass"]["Bulk_energy_peak_total"] = Bulk_energy_peak_total["Sum"]
    ppr.final_tablolar["mass"]["Bulk_energy_offpeak_total"] = Bulk_energy_offpeak_total["Sum"]

    # YEKDEM UNIT COST DATA
    Bulk_yekdemunitcostdemand_gwh = ppr.create_df(book_list_yekdemunitcost_bulk, yekdemunitcost_group, "TotalSellVolume")
    Bulk_yekdemunitcostdemand_gwh = ppr.add_normal_sum(Bulk_yekdemunitcostdemand_gwh)
    ppr.ara_tablolar["bulk_yekdemunitcost"]["Bulk_yekdemunitcostdemand_gwh"] = Bulk_yekdemunitcostdemand_gwh

    # YEKDEM UNIT COST HEDGE GWH
    Bulk_yekdemunitcosthedge_gwh = ppr.create_df(book_list_yekdemunitcost_bulk, yekdemunitcost_group, "TotalBuyVolume")
    Bulk_yekdemunitcosthedge_gwh = ppr.add_normal_sum(Bulk_yekdemunitcosthedge_gwh)
    ppr.ara_tablolar["bulk_yekdemunitcost"]["Bulk_yekdemunitcosthedge_gwh"] = Bulk_yekdemunitcosthedge_gwh
    # YEKDEM UNIT COST MTL
    Bulk_yekdemunitcostmtl = ppr.create_df(book_list_yekdemunitcost_bulk, yekdemunitcost_group, "TotalPL")
    Bulk_yekdemunitcostmtl = ppr.add_normal_sum(Bulk_yekdemunitcostmtl)
    ppr.ara_tablolar["bulk_yekdemunitcost"]["Bulk_yekdemunitcostmtl"] = Bulk_yekdemunitcostmtl

    # YEKDEM UNIT COST SELL PRICE
    Bulk_yekdemunitcost_sellprice = pd.DataFrame()
    Bulk_yekdemunitcostdemand_sp_gwh = pd.DataFrame()
    Bulk_yekdemunitcost_sellprice = pd.DataFrame()
    Bulk_yekdemunitcostdemand_sp_gwh = pd.DataFrame()
    yekdemunitcost_totalsellvolume = pd.DataFrame()
    for item in sorted(book_Bulk_fixed_yekdemunitcost):
        yekdemunitcost_totalsellvolume[str(item)] = \
        yekdemunitcost_group[yekdemunitcost_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "TotalSellVolume"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
        Bulk_yekdemunitcost_sellprice[str(item)] = \
        yekdemunitcost_group[yekdemunitcost_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
        Bulk_yekdemunitcost_sellprice[str(item)] = Bulk_yekdemunitcost_sellprice[str(item)] / \
                                                   yekdemunitcost_totalsellvolume[str(item)].values
    for item in sorted(book_Bulk_Free_yekdemunitcost):
        yekdemunitcost_totalsellvolume[str(item)] = \
        yekdemunitcost_group[yekdemunitcost_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "TotalSellVolume"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
        Bulk_yekdemunitcost_sellprice[str(item)] = \
        yekdemunitcost_group[yekdemunitcost_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
        Bulk_yekdemunitcost_sellprice[str(item)] = Bulk_yekdemunitcost_sellprice[str(item)] / \
                                                   yekdemunitcost_totalsellvolume[str(item)].values
    for item in sorted(book_Bulk_tedas_yekdemunitcost):
        yekdemunitcost_totalsellvolume[str(item)] = \
        yekdemunitcost_group[yekdemunitcost_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "TotalSellVolume"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
        Bulk_yekdemunitcost_sellprice[str(item)] = \
        yekdemunitcost_group[yekdemunitcost_group.index.get_level_values('BookGroupName').isin([str(item)])][
            "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
        Bulk_yekdemunitcost_sellprice[str(item)] = Bulk_yekdemunitcost_sellprice[str(item)] / \
                                                   yekdemunitcost_totalsellvolume[str(item)].values

    for col in [col for col in Bulk_yekdemunitcostdemand_gwh.copy().columns if
                all(x not in str(col) for x in ["Sum", "P2S"])]:
        Bulk_yekdemunitcostdemand_sp_gwh[str(col)] = Bulk_yekdemunitcostdemand_gwh[str(col)] * \
                                                     Bulk_yekdemunitcost_sellprice[str(col)]

    Bulk_yekdemunitcostdemand_sp_gwh = ppr.add_normal_sum(Bulk_yekdemunitcostdemand_sp_gwh)

    Bulk_yekdemunitcostdemand_gwh_sums = Bulk_yekdemunitcostdemand_gwh.copy().loc[:,
                                         Bulk_yekdemunitcostdemand_gwh.columns.str.contains("Sum")]
    Bulk_yekdemunitcost_sellprice_sums = Bulk_yekdemunitcost_sellprice.copy().loc[:,
                                         Bulk_yekdemunitcost_sellprice.columns.str.contains("Sum")]
    Bulk_yekdemunitcostdemand_sp_gwh_sums = Bulk_yekdemunitcostdemand_sp_gwh.copy().loc[:,
                                            Bulk_yekdemunitcostdemand_sp_gwh.columns.str.contains("Sum")]

    for col in Bulk_yekdemunitcostdemand_sp_gwh_sums:
        Bulk_yekdemunitcost_sellprice_sums[str(col)] = Bulk_yekdemunitcostdemand_sp_gwh_sums[str(col)] / \
                                                       Bulk_yekdemunitcostdemand_gwh_sums[str(col)]

    Bulk_yekdemunitcost_sellprice = pd.concat([Bulk_yekdemunitcost_sellprice, Bulk_yekdemunitcost_sellprice_sums],
                                              axis=1).fillna(0)
    ppr.ara_tablolar["bulk_yekdemunitcost"]["Bulk_yekdemunitcost_sellprice"] = Bulk_yekdemunitcost_sellprice

    # YEKDEM DAP GWH
    Bulk_yekdemdap_gwh = ppr.create_df(book_list_yekdemdap_bulk, yekdemdap_group, "TotalNetVolume")
    Bulk_yekdemdap_gwh = ppr.add_normal_sum(Bulk_yekdemdap_gwh)
    ppr.ara_tablolar["bulk_yekdemdap"]["Bulk_yekdemdap_gwh"] = Bulk_yekdemdap_gwh

    # YEKDEM DAP MTL
    Bulk_yekdemdap_mtl = ppr.create_df(book_list_yekdemdap_bulk, yekdemdap_group, "TotalPL")
    Bulk_yekdemdap_mtl = ppr.add_normal_sum(Bulk_yekdemdap_mtl)
    ppr.ara_tablolar["bulk_yekdemdap"]["Bulk_yekdemdap_mtl"] = Bulk_yekdemdap_mtl

    # YEKDEM DAP SELL PRICE TL/MWh
    Bulk_yekdemdap_sellprice = pd.DataFrame()
    Bulk_yekdemdap_sellprice_product_gwh = pd.DataFrame()

    for book in book_list_yekdemdap_bulk:
        for item in sorted(book):
            yekdemdap_totalsellvolume = \
            yekdemdap_group[yekdemdap_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "TotalSellVolume"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            Bulk_yekdemdap_sellpricex = \
            yekdemdap_group[yekdemdap_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            Bulk_yekdemdap_sellprice[str(item)] = \
            yekdemdap_group[yekdemdap_group.index.get_level_values('BookGroupName').isin([str(item)])][
                "SellCost"].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
            Bulk_yekdemdap_sellprice = Bulk_yekdemdap_sellprice.add(Bulk_yekdemdap_sellpricex, fill_value=0)
            Bulk_yekdemdap_sellprice = Bulk_yekdemdap_sellprice.add(yekdemdap_totalsellvolume, fill_value=0)
            Bulk_yekdemdap_sellprice[str(item)] = Bulk_yekdemdap_sellprice["SellCost"] / Bulk_yekdemdap_sellprice[
                "TotalSellVolume"]
            Bulk_yekdemdap_sellprice = Bulk_yekdemdap_sellprice.drop(["SellCost", "TotalSellVolume"], axis=1)

    for col in [col for col in Bulk_yekdemdap_gwh.copy().columns if all(x not in str(col) for x in ["Sum", "P2S"])]:
        Bulk_yekdemdap_sellprice_product_gwh[str(col)] = Bulk_yekdemdap_gwh[str(col)] * Bulk_yekdemdap_sellprice[str(col)]

    Bulk_yekdemdap_sellprice_product_gwh = ppr.add_normal_sum(Bulk_yekdemdap_sellprice_product_gwh)

    Bulk_yekdemdap_gwh_sums = Bulk_yekdemdap_gwh.copy().loc[:, Bulk_yekdemdap_gwh.columns.str.contains("Sum")]
    Bulk_yekdemdap_sellprice_sums = Bulk_yekdemdap_sellprice.copy().loc[:,
                                    Bulk_yekdemdap_sellprice.columns.str.contains("Sum")]
    Bulk_yekdemdap_sellprice_product_gwh_sums = Bulk_yekdemdap_sellprice_product_gwh.copy().loc[:,
                                                Bulk_yekdemdap_sellprice_product_gwh.columns.str.contains("Sum")]

    for col in Bulk_yekdemdap_sellprice_product_gwh_sums:
        Bulk_yekdemdap_sellprice_sums[str(col)] = Bulk_yekdemdap_sellprice_product_gwh_sums[str(col)] / \
                                                  Bulk_yekdemdap_gwh_sums[str(col)]

    Bulk_yekdemdap_sellprice = pd.concat([Bulk_yekdemdap_sellprice, Bulk_yekdemdap_sellprice_sums], axis=1).fillna(0)
    ppr.ara_tablolar["bulk_yekdemdap"]["Bulk_yekdemdap_sellprice"] = Bulk_yekdemdap_sellprice

    # YEKDEM FX OTC GWH
    Bulk_yekdemfx_otc_gwh = ppr.create_df(book_list_yekdemfxotc_bulk, yekdemfx_group, "TotalNetVolume")
    Bulk_yekdemfx_otc_gwh = ppr.add_normal_sum(Bulk_yekdemfx_otc_gwh)
    ppr.ara_tablolar["bulk_yekdemfx"]["Bulk_yekdemfx_otc_gwh"] = Bulk_yekdemfx_otc_gwh

    # YEKDEM FX OTC MTL
    Bulk_yekdemfx_otc_mtl = ppr.create_df(book_list_yekdemfxotc_bulk, yekdemfx_group, "TotalPL")
    Bulk_yekdemfx_otc_mtl = ppr.add_normal_sum(Bulk_yekdemfx_otc_mtl)
    ppr.ara_tablolar["bulk_yekdemfx"]["Bulk_yekdemfx_otc_mtl"] = Bulk_yekdemfx_otc_mtl

    # YEKDEM FX BUY PRICE TL/MWh
    Bulk_yekdemfx_buyprice = ppr.create_df(book_list_yekdemfxotc_bulk, yekdemfx_group, "BuyCost")
    Bulk_yekdemfx_buyprice_product_gwh = pd.DataFrame()

    for col in Bulk_yekdemfx_buyprice.loc[:, ~Bulk_yekdemfx_buyprice.columns.str.contains("Sum")].columns:
        Bulk_yekdemfx_buyprice[str(col)] = Bulk_yekdemfx_buyprice[str(col)] / Bulk_yekdemfx_otc_gwh[str(col)]

    for col in [col for col in Bulk_yekdemfx_otc_gwh.copy().columns if all(x not in str(col) for x in ["Sum", "P2S"])]:
        Bulk_yekdemfx_buyprice_product_gwh[str(col)] = Bulk_yekdemfx_otc_gwh[str(col)] * Bulk_yekdemfx_buyprice[str(col)]

    Bulk_yekdemfx_buyprice_product_gwh = ppr.add_normal_sum(Bulk_yekdemfx_buyprice_product_gwh)

    Bulk_yekdemfx_otc_gwh_sums = Bulk_yekdemfx_otc_gwh.copy().loc[:, Bulk_yekdemfx_otc_gwh.columns.str.contains("Sum")]
    Bulk_yekdemfx_buyprice_sums = Bulk_yekdemfx_buyprice.copy().loc[:, Bulk_yekdemfx_buyprice.columns.str.contains("Sum")]
    Bulk_yekdemfx_buyprice_product_gwh_sums = Bulk_yekdemfx_buyprice_product_gwh.copy().loc[:,
                                              Bulk_yekdemfx_buyprice_product_gwh.columns.str.contains("Sum")]

    for col in Bulk_yekdemfx_buyprice_product_gwh_sums:
        Bulk_yekdemfx_buyprice_sums[str(col)] = Bulk_yekdemfx_buyprice_product_gwh_sums[str(col)] / \
                                                Bulk_yekdemfx_otc_gwh_sums[str(col)]

    Bulk_yekdemfx_buyprice = pd.concat([Bulk_yekdemfx_buyprice, Bulk_yekdemfx_buyprice_sums], axis=1)
    ppr.ara_tablolar["bulk_yekdemfx"]["Bulk_yekdemfx_buyprice"] = Bulk_yekdemfx_buyprice

    # YEKDEM FX DEMAND GWH
    Bulk_yekdemfx_demand_gwh = ppr.create_df(book_list_yekdemfxdemand_bulk, yekdemfx_group, "TotalNetVolume")
    Bulk_yekdemfx_demand_gwh = ppr.add_normal_sum(Bulk_yekdemfx_demand_gwh)
    ppr.ara_tablolar["bulk_yekdemfx"]["Bulk_yekdemfx_demand_gwh"] = Bulk_yekdemfx_demand_gwh

    # YEKDEM FX SELL PRICE TL/MWh
    Bulk_yekdemfx_sellprice = ppr.create_df(book_list_yekdemfxdemand_bulk, yekdemfx_group, "SellCost")
    Bulk_yekdemfx_sellprice_product_gwh = pd.DataFrame()

    for col in Bulk_yekdemfx_sellprice.loc[:, ~Bulk_yekdemfx_sellprice.copy().columns.str.contains("Sum")].columns:
        Bulk_yekdemfx_sellprice[str(col)] = Bulk_yekdemfx_sellprice[str(col)] / (Bulk_yekdemfx_demand_gwh[str(col)] * -1)

    for col in [col for col in Bulk_yekdemfx_demand_gwh.copy().columns if all(x not in str(col) for x in ["Sum", "P2S"])]:
        Bulk_yekdemfx_sellprice_product_gwh[str(col)] = Bulk_yekdemfx_demand_gwh[str(col)] * Bulk_yekdemfx_sellprice[
            str(col)]

    Bulk_yekdemfx_sellprice_product_gwh = ppr.add_normal_sum(Bulk_yekdemfx_sellprice_product_gwh)

    Bulk_yekdemfx_demand_gwh_sums = Bulk_yekdemfx_demand_gwh.copy().loc[:,
                                    Bulk_yekdemfx_demand_gwh.columns.str.contains("Sum")]
    Bulk_yekdemfx_sellprice_sums = Bulk_yekdemfx_sellprice.copy().loc[:,
                                   Bulk_yekdemfx_sellprice.columns.str.contains("Sum")]
    Bulk_yekdemfx_sellprice_product_gwh_sums = Bulk_yekdemfx_sellprice_product_gwh.copy().loc[:,
                                               Bulk_yekdemfx_sellprice_product_gwh.columns.str.contains("Sum")]

    for col in Bulk_yekdemfx_sellprice_product_gwh_sums:
        Bulk_yekdemfx_sellprice_sums[str(col)] = Bulk_yekdemfx_sellprice_product_gwh_sums[str(col)] / \
                                                 Bulk_yekdemfx_demand_gwh_sums[str(col)]

    Bulk_yekdemfx_sellprice = pd.concat([Bulk_yekdemfx_sellprice, Bulk_yekdemfx_sellprice_sums], axis=1)
    ppr.ara_tablolar["bulk_yekdemfx"]["Bulk_yekdemfx_sellprice"] = Bulk_yekdemfx_sellprice

    # # B2B'22

    b2b_this_year_energy = ppr.add_yearsum_column(b2b_total_energy_mtl, this_year, 'ENERGY PL-MTL')

    b2b_this_year_unitcost = ppr.add_yearsum_column(b2b_yekdemunitcostmtl, this_year, 'YEKDEM PL-MTL UNIT COST')
    b2b_this_year_dap = ppr.add_yearsum_column(b2b_yekdemdap_mtl, this_year, 'YEKDEM PL-MTL DAP')
    b2b_this_year_fx = ppr.add_yearsum_column(b2b_yekdemfx_otc_mtl, this_year, 'YEKDEM PL-MTL FX')
    b2b_this_year_total_yekdem = pd.DataFrame(
        data=b2b_this_year_unitcost.values + b2b_this_year_dap.values + b2b_this_year_fx.values,
        index=b2b_this_year_unitcost.index, columns=['YEKDEM PL-MTL TOTAL'])

    b2b_this_year_salesmw = ppr.add_yearaverage_column(b2b_demand_mw, this_year, 'SALES-MW TOTAL')
    b2b_this_year_hedgemw = ppr.add_yearaverage_column(total_hedge_mw, this_year, 'HEDGE-MW TOTAL')

    b2b_this_year_open_baseload = pd.DataFrame(data=b2b_this_year_salesmw.values + b2b_this_year_hedgemw.values,
                                               index=b2b_this_year_energy.index, columns=['OPEN POS-MW BASELOAD'])

    b2b_this_year_buyprice_total = sum(
        ppr.add_yearsum_column(b2b_energy_buyprice, this_year, f'{this_year}')[:-1].values * b2b_this_year_hedgemw[
                                                                                             :-1].values / np.sum(
            b2b_this_year_hedgemw[:-1].values))
    b2b_this_year_buyprice = ppr.add_yearsum_column(b2b_energy_buyprice, this_year, f'TL/MWh BUY PRICE')[:-1]
    b2b_this_year_buyprice.loc[f'{this_year}'] = b2b_this_year_buyprice_total

    b2b_this_year_sellprice_total = sum(
        ppr.add_yearsum_column(b2b_energy_sellprice, this_year, f'{this_year}')[:-1].values * b2b_this_year_salesmw[
                                                                                              :-1].values / np.sum(
            b2b_this_year_salesmw[:-1].values))
    b2b_this_year_sellprice = ppr.add_yearsum_column(b2b_energy_sellprice, this_year, f'TL/MWh SELL PRICE')[:-1]
    b2b_this_year_sellprice.loc[f'{this_year}'] = b2b_this_year_sellprice_total

    b2b_this_year = pd.concat(
        [b2b_this_year_energy, b2b_this_year_unitcost, b2b_this_year_dap, b2b_this_year_fx, b2b_this_year_total_yekdem,
         b2b_this_year_salesmw, b2b_this_year_hedgemw, b2b_this_year_open_baseload, b2b_this_year_buyprice,
         b2b_this_year_sellprice], axis=1)

    b2b_pl_this_year = np.sum(b2b_this_year_energy[:-1].values + b2b_this_year_total_yekdem[:-1].values)
    ppr.final_tablolar["b2b"]["b2b_this_year"] = b2b_this_year

    # # B2B'23

    b2b_next_year_energy = ppr.add_yearsum_column(b2b_total_energy_mtl, next_year, 'ENERGY PL-MTL')

    b2b_next_year_unitcost = ppr.add_yearsum_column(b2b_yekdemunitcostmtl, next_year, 'YEKDEM PL-MTL UNIT COST')
    b2b_next_year_dap = ppr.add_yearsum_column(b2b_yekdemdap_mtl, next_year, 'YEKDEM PL-MTL DAP')
    b2b_next_year_fx = ppr.add_yearsum_column(b2b_yekdemfx_otc_mtl, next_year, 'YEKDEM PL-MTL FX')
    b2b_next_year_total_yekdem = pd.DataFrame(
        data=b2b_next_year_unitcost.values + b2b_next_year_dap.values + b2b_next_year_fx.values,
        index=b2b_next_year_unitcost.index, columns=['YEKDEM PL-MTL TOTAL'])

    b2b_next_year_salesmw = ppr.add_yearaverage_column(b2b_demand_mw, next_year, 'SALES-MW TOTAL')
    b2b_next_year_hedgemw = ppr.add_yearaverage_column(total_hedge_mw, next_year, 'HEDGE-MW TOTAL')

    b2b_next_year_open_baseload = pd.DataFrame(data=b2b_next_year_salesmw.values + b2b_next_year_hedgemw.values,
                                               index=b2b_next_year_energy.index, columns=['OPEN POS-MW BASELOAD'])

    b2b_next_year_buyprice_total = sum(
        ppr.add_yearsum_column(b2b_energy_buyprice, next_year, 'TOTAL')[:-1].values * b2b_next_year_hedgemw[
                                                                                      :-1].values / np.sum(
            b2b_next_year_hedgemw[:-1].values))
    b2b_next_year_buyprice = ppr.add_yearsum_column(b2b_energy_buyprice, next_year, 'TL/MWh BUY PRICE')[:-1]
    b2b_next_year_buyprice.loc[f'{next_year}'] = b2b_next_year_buyprice_total

    b2b_next_year_sellprice_total = sum(
        ppr.add_yearsum_column(b2b_energy_sellprice, next_year, 'TOTAL')[:-1].values * b2b_next_year_salesmw[
                                                                                       :-1].values / np.sum(
            b2b_next_year_salesmw[:-1].values))
    b2b_next_year_sellprice = ppr.add_yearsum_column(b2b_energy_sellprice, next_year, 'TL/MWh SELL PRICE')[:-1]
    b2b_next_year_sellprice.loc[f'{next_year}'] = b2b_next_year_sellprice_total

    b2b_next_year = pd.concat(
        [b2b_next_year_energy, b2b_next_year_unitcost, b2b_next_year_dap, b2b_next_year_fx, b2b_next_year_total_yekdem,
         b2b_next_year_salesmw, b2b_next_year_hedgemw, b2b_next_year_open_baseload, b2b_next_year_buyprice,
         b2b_next_year_sellprice], axis=1)

    b2b_pl_next_year = np.sum(b2b_next_year_energy[:-1].values + b2b_next_year_total_yekdem[:-1].values)
    ppr.final_tablolar["b2b"]["b2b_next_year"] = b2b_next_year

    # # FX'22

    fx_this_year = pd.DataFrame()
    fx_this_year_b2b_demandmusd = ppr.add_yearsum_column(b2b_yekdemfx_demand_gwh, this_year, 'DEMAND M-USD B2B') / 1000
    fx_this_year_mass_demandmusd = ppr.add_yearsum_column(Bulk_yekdemfx_demand_gwh, this_year, 'DEMAND M-USD MASS') / 1000
    fx_this_year_total_demand = pd.DataFrame(data=fx_this_year_b2b_demandmusd.values + fx_this_year_mass_demandmusd.values,
                                             index=fx_this_year_mass_demandmusd.index, columns=['DEMAND M-USD TOTAL'])

    fx_this_year_b2b_hedgemusd = ppr.add_yearsum_column(b2b_yekdemfx_otc_gwh, this_year, 'HEDGE M-USD B2B') / 1000
    fx_this_year_mass_hedgemusd = ppr.add_yearsum_column(Bulk_yekdemfx_otc_gwh, this_year, 'HEDGE M-USD MASS') / 1000
    fx_this_year_total_hedge = pd.DataFrame(data=fx_this_year_b2b_hedgemusd.values + fx_this_year_mass_hedgemusd.values,
                                            index=fx_this_year_mass_hedgemusd.index, columns=['HEDGE M-USD TOTAL'])

    demand_plus_hedge_b2b = pd.DataFrame(data=fx_this_year_b2b_demandmusd.values + fx_this_year_b2b_hedgemusd.values,
                                         index=fx_this_year_b2b_demandmusd.index, columns=["OPEN M-USD B2B"])
    demand_plus_hedge_mass = pd.DataFrame(data=fx_this_year_mass_demandmusd.values + fx_this_year_mass_hedgemusd.values,
                                          index=fx_this_year_mass_demandmusd.index, columns=["OPEN M-USD MASS"])
    fx_this_year_total_open = pd.DataFrame(data=demand_plus_hedge_b2b.values + demand_plus_hedge_mass.values,
                                           index=demand_plus_hedge_mass.index, columns=['OPEN M-USD TOTAL'])

    fx_this_year_b2b_hedgeplmtl = pd.DataFrame(data=b2b_this_year_fx.values, index=fx_this_year_b2b_demandmusd.index,
                                               columns=["HEDGE PL-MTL B2B"])
    fx_this_year_mass_hedgeplmtl = ppr.add_yearsum_column(Bulk_yekdemfx_otc_mtl, this_year, "HEDGE PL-MTL MASS")

    fx_this_year_dbsm1 = ppr.add_next_year_jan(b2b_energyfx_volume, this_year, next_year, "fx_this_year_dbsm1")
    fx_this_year_dbsm2 = ppr.add_next_year_jan(Bulk_energyfx_volume, this_year, next_year, "fx_this_year_dbsm2")
    fx_this_year_dbsm = pd.DataFrame(data=fx_this_year_dbsm1.values / 1000 + fx_this_year_dbsm2.values / 1000,
                                     columns=["DOLLAR BASED SOURCING MUSD*"])

    fx_this_year_dbfxrate1 = ppr.add_next_year_jan(b2b_energyfx_buyprice, this_year, next_year, "fx_this_year_dbfxrate1")
    fx_this_year_dbfxrate2 = ppr.add_next_year_jan(Bulk_energyfx_buyprice, this_year, next_year, "fx_this_year_dbfxrate2")
    fx_this_year_dbfxrate = pd.DataFrame(data=((
                                                           fx_this_year_dbsm1.values * fx_this_year_dbfxrate1.values + fx_this_year_dbsm2.values * fx_this_year_dbfxrate2.values) / (
                                                           fx_this_year_dbsm1.values + fx_this_year_dbsm2.values)),
                                         columns=["DOLLAR BASED SOURCING FX RATE*"])[:-1]
    fx_this_year_dbfxrate.loc["Sum"] = (
                np.sum(fx_this_year_dbsm[:-1].values * fx_this_year_dbfxrate.values) / fx_this_year_dbsm.iloc[-1].values[0])

    fx_this_year_dbsm.index = fx_this_year_b2b_demandmusd.index
    fx_this_year_dbfxrate.index = fx_this_year_b2b_demandmusd.index

    fx_this_year = pd.concat(
        [fx_this_year_total_demand, fx_this_year_b2b_demandmusd, fx_this_year_mass_demandmusd, fx_this_year_total_hedge,
         fx_this_year_b2b_hedgemusd, fx_this_year_mass_hedgemusd, fx_this_year_total_open, demand_plus_hedge_b2b,
         demand_plus_hedge_mass, fx_this_year_b2b_hedgeplmtl, fx_this_year_mass_hedgeplmtl, fx_this_year_dbsm,
         fx_this_year_dbfxrate], axis=1)

    fx_pl_this_year = np.sum(fx_this_year_b2b_hedgeplmtl[:-1].values + fx_this_year_mass_hedgeplmtl[:-1].values)
    ppr.final_tablolar["fx"]["fx_this_year"] = fx_this_year

    # # FX'23

    fx_next_year = pd.DataFrame()
    fx_next_year_b2b_demandmusd = ppr.add_yearsum_column(b2b_yekdemfx_demand_gwh, next_year, 'DEMAND M-USD B2B') / 1000
    fx_next_year_mass_demandmusd = ppr.add_yearsum_column(Bulk_yekdemfx_demand_gwh, next_year, 'DEMAND M-USD MASS') / 1000
    fx_next_year_total_demand = pd.DataFrame(data=fx_next_year_b2b_demandmusd.values + fx_next_year_mass_demandmusd.values,
                                             index=fx_next_year_mass_demandmusd.index, columns=['DEMAND M-USD TOTAL'])

    fx_next_year_b2b_hedgemusd = ppr.add_yearsum_column(b2b_yekdemfx_otc_gwh, next_year, 'HEDGE M-USD B2B') / 1000
    fx_next_year_mass_hedgemusd = ppr.add_yearsum_column(Bulk_yekdemfx_otc_gwh, next_year, 'HEDGE M-USD MASS') / 1000
    fx_next_year_total_hedge = pd.DataFrame(data=fx_next_year_b2b_hedgemusd.values + fx_next_year_mass_hedgemusd.values,
                                            index=fx_next_year_mass_hedgemusd.index, columns=['HEDGE M-USD TOTAL'])

    demand_plus_hedge_b2b = pd.DataFrame(data=fx_next_year_b2b_demandmusd.values + fx_next_year_b2b_hedgemusd.values,
                                         index=fx_next_year_b2b_demandmusd.index, columns=["OPEN M-USD B2B"])
    demand_plus_hedge_mass = pd.DataFrame(data=fx_next_year_mass_demandmusd.values + fx_next_year_mass_hedgemusd.values,
                                          index=fx_next_year_mass_demandmusd.index, columns=["MASS M-USD B2B"])
    fx_next_year_total_open = pd.DataFrame(data=demand_plus_hedge_b2b.values + demand_plus_hedge_mass.values,
                                           index=demand_plus_hedge_mass.index, columns=['OPEN M-USD TOTAL'])

    fx_next_year_b2b_hedgeplmtl = pd.DataFrame(data=b2b_next_year_fx.values, index=fx_next_year_b2b_demandmusd.index,
                                               columns=["HEDGE PL-MTL B2B"])
    fx_next_year_mass_hedgeplmtl = ppr.add_yearsum_column(Bulk_yekdemfx_otc_mtl, next_year, "HEDGE PL-MTL MASS")

    fx_next_year_dbsm1 = ppr.add_next_year_janfx(b2b_energyfx_volume, next_year, next_yearplus1, "fx_next_year_dbsm1")
    fx_next_year_dbsm2 = ppr.add_next_year_janfx(Bulk_energyfx_volume, next_year, next_yearplus1, "fx_next_year_dbsm2")
    fx_next_year_dbsm = pd.DataFrame(data=fx_next_year_dbsm1.values / 1000 + fx_next_year_dbsm2.values / 1000,
                                     columns=["DOLLAR BASED SOURCING MUSD*"])

    fx_next_year_dbfxrate1 = ppr.add_next_year_janfx(b2b_energyfx_buyprice, next_year, next_yearplus1,
                                                     "fx_next_year_dbfxrate1")
    fx_next_year_dbfxrate2 = ppr.add_next_year_janfx(Bulk_energyfx_buyprice, next_year, next_yearplus1,
                                                     "fx_next_year_dbfxrate2")
    fx_next_year_dbfxrate = pd.DataFrame(data=((
                                                           fx_next_year_dbsm1.values * fx_next_year_dbfxrate1.values + fx_next_year_dbsm2.values * fx_next_year_dbfxrate2.values) / (
                                                           fx_next_year_dbsm1.values + fx_next_year_dbsm2.values)),
                                         columns=["DOLLAR BASED SOURCING FX RATE*"])[:-1]
    fx_next_year_dbfxrate.loc["Sum"] = (
                np.sum(fx_next_year_dbsm[:-1].values * fx_next_year_dbfxrate.values) / fx_next_year_dbsm.iloc[-1].values[0])

    fx_next_year_dbsm.index = fx_next_year_b2b_demandmusd.index
    fx_next_year_dbfxrate.index = fx_next_year_b2b_demandmusd.index

    fx_next_year = pd.concat(
        [fx_next_year_total_demand, fx_next_year_b2b_demandmusd, fx_next_year_mass_demandmusd, fx_next_year_total_hedge,
         fx_next_year_b2b_hedgemusd, fx_next_year_mass_hedgemusd, fx_next_year_total_open, demand_plus_hedge_b2b,
         demand_plus_hedge_mass, fx_next_year_b2b_hedgeplmtl, fx_next_year_mass_hedgeplmtl, fx_next_year_dbsm,
         fx_next_year_dbfxrate], axis=1)

    fx_pl_next_year = np.sum(fx_next_year_b2b_hedgeplmtl[:-1].values + fx_next_year_mass_hedgeplmtl[:-1].values)
    ppr.final_tablolar["fx"]["fx_next_year"] = fx_next_year

    # # MASS'22

    mass_this_year = pd.DataFrame()
    mass_thisyear_energy = ppr.add_yearsum_column(Bulk_total_energy_mtl, this_year, 'ENERGY PL-MTL ENERGY')

    mass_thisyear_unitcost = ppr.add_yearsum_column(Bulk_yekdemunitcostmtl, this_year, 'YEKDEM PL-MTL UNITCOST')
    mass_thisyear_dap = ppr.add_yearsum_column(Bulk_yekdemdap_mtl, this_year, 'YEKDEM PL-MTL DAP')
    mass_thisyear_fx = ppr.add_yearsum_column(Bulk_yekdemfx_otc_mtl, this_year, 'YEKDEM PL-MTL FX')
    mass_this_year_total_yekdem = pd.DataFrame(
        data=(mass_thisyear_unitcost.values + mass_thisyear_dap.values + mass_thisyear_fx.values),
        index=mass_thisyear_fx.index, columns=['YEKDEM PL-MTL TOTAL'])

    mass_thisyear_salestotal = ppr.add_yearaverage_column(Bulk_demand_mw, this_year, 'SALES-MW TOTAL')

    mass_thisyear_hedgetotal = ppr.add_yearaverage_column(bulk_total_hedge_mw, this_year, 'HEDGE-MW TOTAL')

    mass_thisyear_baseload = pd.DataFrame(data=(mass_thisyear_salestotal.values + mass_thisyear_hedgetotal.values),
                                          index=mass_thisyear_fx.index, columns=['OPEN POS-MW BASELOAD'])

    mass_thisyear_buy_price = ppr.add_yearsum_column(Bulk_energy_buyprice, this_year, 'TL/MWh BUY PRICE')[:-1]
    mass_total_buy_price_this = np.sum(mass_thisyear_hedgetotal[:-1].values * mass_thisyear_buy_price.values) / np.sum(
        mass_thisyear_hedgetotal[:-1].values)
    mass_thisyear_buy_price.loc[f'{this_year}'] = mass_total_buy_price_this

    mass_thisyear_sell_price = ppr.add_yearsum_column(Bulk_energy_sellprice, this_year, 'TL/MWh SELL PRICE')[:-1]
    mass_total_sell_price_this = np.sum(mass_thisyear_salestotal[:-1].values * mass_thisyear_sell_price.values) / np.sum(
        mass_thisyear_salestotal[:-1].values)
    mass_thisyear_sell_price.loc[f'{this_year}'] = mass_total_sell_price_this

    mass_this_year = pd.concat(
        [mass_thisyear_energy, mass_thisyear_unitcost, mass_thisyear_dap, mass_thisyear_fx, mass_this_year_total_yekdem,
         mass_thisyear_salestotal, mass_thisyear_hedgetotal, mass_thisyear_baseload, mass_thisyear_buy_price,
         mass_thisyear_sell_price], axis=1)

    mass_pl_this_year = np.sum(mass_thisyear_energy[:-1].values + mass_this_year_total_yekdem[:-1].values)
    ppr.final_tablolar["mass"]["mass_this_year"] = mass_this_year

    # # MASS'23

    mass_next_year = pd.DataFrame()
    mass_nextyear_energy = ppr.add_yearsum_column(Bulk_total_energy_mtl, next_year, 'ENERGY PL-MTL ENERGY')

    mass_nextyear_unitcost = ppr.add_yearsum_column(Bulk_yekdemunitcostmtl, next_year, 'YEKDEM PL-MTL UNITCOST')
    mass_nextyear_dap = ppr.add_yearsum_column(Bulk_yekdemdap_mtl, next_year, 'YEKDEM PL-MTL DAP')
    mass_nextyear_fx = ppr.add_yearsum_column(Bulk_yekdemfx_otc_mtl, next_year, 'YEKDEM PL-MTL FX')
    mass_next_year_total_yekdem = pd.DataFrame(
        data=(mass_nextyear_unitcost.values + mass_nextyear_dap.values + mass_nextyear_fx.values),
        index=mass_nextyear_fx.index, columns=['YEKDEM PL-MTL TOTAL'])

    mass_nextyear_salestotal = ppr.add_yearaverage_column(Bulk_demand_mw, next_year, 'SALES-MW TOTAL')

    mass_nextyear_hedgetotal = ppr.add_yearaverage_column(bulk_total_hedge_mw, next_year, 'HEDGE-MW TOTAL')

    mass_nextyear_baseload = pd.DataFrame(data=(mass_nextyear_salestotal.values + mass_nextyear_hedgetotal.values),
                                          index=mass_nextyear_fx.index, columns=['OPEN POS-MW BASELOAD'])

    mass_nextyear_buy_price = ppr.add_yearsum_column(Bulk_energy_buyprice, next_year, 'TL/MWh BUY PRICE')[:-1]
    mass_total_buy_price_next = np.sum(mass_nextyear_hedgetotal[:-1].values * mass_nextyear_buy_price.values) / np.sum(
        mass_nextyear_hedgetotal[:-1].values)
    mass_nextyear_buy_price.loc[f'{next_year}'] = mass_total_buy_price_next

    mass_nextyear_sell_price = ppr.add_yearsum_column(Bulk_energy_sellprice, next_year, 'TL/MWh SELL PRICE')[:-1]
    mass_total_sell_price_next = np.sum(mass_nextyear_salestotal[:-1].values * mass_nextyear_sell_price.values) / np.sum(
        mass_nextyear_salestotal[:-1].values)
    mass_nextyear_sell_price.loc[f'{next_year}'] = mass_total_sell_price_next

    mass_next_year = pd.concat(
        [mass_nextyear_energy, mass_nextyear_unitcost, mass_nextyear_dap, mass_nextyear_fx, mass_next_year_total_yekdem,
         mass_nextyear_salestotal, mass_nextyear_hedgetotal, mass_nextyear_baseload, mass_nextyear_buy_price,
         mass_nextyear_sell_price], axis=1)

    mass_pl_next_year = np.sum(mass_nextyear_energy[:-1].values + mass_next_year_total_yekdem[:-1].values)
    ppr.final_tablolar["mass"]["mass_next_year"] = mass_next_year

    # # TOTAL'22

    total_this_year_energy = pd.DataFrame(data=mass_thisyear_energy.values + b2b_this_year_energy.values,
                                          index=mass_thisyear_energy.index, columns=['PL-MTL ENERGY'])
    total_this_year_yekdem = pd.DataFrame(data=mass_this_year_total_yekdem.values + b2b_this_year_total_yekdem.values,
                                          index=total_this_year_energy.index, columns=['PL-MTL YEKDEM'])
    total_this_year_plmtl_total = pd.DataFrame(data=total_this_year_energy.values + total_this_year_yekdem.values,
                                               index=total_this_year_energy.index, columns=['PL-MTL TOTAL'])

    total_this_year_salespeak_b2b = ppr.add_yearaverage_column(b2b_energy_peak_demand, this_year, 'SALES-MW PEAK B2B')
    total_this_year_salespeak_bulk = ppr.add_yearaverage_column(Bulk_energy_peak_demand, this_year, 'SALES-MW PEAK BULK')
    total_this_year_salespeak = pd.DataFrame(
        data=total_this_year_salespeak_b2b.values + total_this_year_salespeak_bulk.values,
        index=total_this_year_energy.index, columns=['SALES-MW PEAK'])

    total_this_year_salesoffpeak_b2b = ppr.add_yearaverage_column(b2b_energy_offpeak_demand, this_year,
                                                                  'SALES-MW OFF PEAK B2B')
    total_this_year_salesoffpeak_bulk = ppr.add_yearaverage_column(Bulk_energy_offpeak_demand, this_year,
                                                                   'SALES-MW OFF PEAK BULK')
    total_this_year_salesoffpeak = pd.DataFrame(
        data=total_this_year_salesoffpeak_b2b.values + total_this_year_salesoffpeak_bulk.values,
        index=total_this_year_energy.index, columns=['SALES-MW OFFPEAK'])

    total_this_year_salestotal = pd.DataFrame(
        data=(total_this_year_salespeak.values + total_this_year_salesoffpeak.values) / 2,
        index=total_this_year_energy.index, columns=['SALES-MW TOTAL'])[:-1]
    total_this_year_salestotal.loc[f'{this_year}'] = np.average(total_this_year_salestotal.values)

    total_this_year_hedgepeak_b2b = pd.DataFrame(
        data=ppr.add_yearaverage_column(b2b_energy_peak_otc, this_year, "1").values + ppr.add_yearaverage_column(
            b2b_energy_peak_daphedge, this_year, "2").values, index=total_this_year_energy.index,
        columns=['HEDGE-MW PEAK B2B'])
    total_this_year_hedgepeak_bulk = pd.DataFrame(
        data=ppr.add_yearaverage_column(Bulk_energy_peak_otc, this_year, "1").values + ppr.add_yearaverage_column(
            Bulk_energy_peak_daphedge, this_year, "2").values, index=total_this_year_energy.index,
        columns=['HEDGE-MW PEAK BULK'])
    total_this_year_hedgepeak = pd.DataFrame(
        data=(total_this_year_hedgepeak_b2b.values + total_this_year_hedgepeak_bulk.values),
        index=total_this_year_energy.index, columns=['HEDGE-MW PEAK'])[:-1]
    total_this_year_hedgepeak.loc[f'{this_year}'] = np.average(total_this_year_hedgepeak.values)

    total_this_year_hedgeoffpeak_b2b = pd.DataFrame(
        data=ppr.add_yearaverage_column(b2b_energy_offpeak_otc, this_year, "1").values + ppr.add_yearaverage_column(
            b2b_energy_offpeak_daphedge, this_year, "2").values, index=total_this_year_energy.index,
        columns=['HEDGE-MW OFFPEAK B2B'])
    total_this_year_hedgeoffpeak_bulk = pd.DataFrame(
        data=ppr.add_yearaverage_column(Bulk_energy_offpeak_otc, this_year, "1").values + ppr.add_yearaverage_column(
            Bulk_energy_offpeak_daphedge, this_year, "2").values, index=total_this_year_energy.index,
        columns=['HEDGE-MW OFFPEAK BULK'])
    total_this_year_hedgeoffpeak = pd.DataFrame(
        data=(total_this_year_hedgeoffpeak_b2b.values + total_this_year_hedgeoffpeak_bulk.values),
        index=total_this_year_energy.index, columns=['HEDGE-MW OFFPEAK'])[:-1]
    total_this_year_hedgeoffpeak.loc[f'{this_year}'] = np.average(total_this_year_hedgeoffpeak.values)

    total_this_year_hedgetotal = pd.DataFrame(
        data=(total_this_year_hedgepeak.values + total_this_year_hedgeoffpeak.values) / 2,
        index=total_this_year_energy.index, columns=['HEDGE-MW TOTAL'])[:-1]
    total_this_year_hedgetotal.loc[f'{this_year}'] = np.average(total_this_year_hedgetotal.values)

    total_this_year_open_baseload = pd.DataFrame(data=total_this_year_hedgetotal.values + total_this_year_salestotal.values,
                                                 index=mass_thisyear_energy.index, columns=['OPEN POSITION-MW BASELOAD'])
    total_this_year_open_peak = pd.DataFrame(data=total_this_year_hedgepeak.values + total_this_year_salespeak.values,
                                             index=mass_thisyear_energy.index, columns=['OPEN POSITION-MW PEAK'])
    total_this_year_open_offpeak = pd.DataFrame(
        data=total_this_year_hedgeoffpeak.values + total_this_year_salesoffpeak.values, index=mass_thisyear_energy.index,
        columns=['OPEN POSITION-MW OFFPEAK'])

    total_this_year = pd.concat(
        [total_this_year_plmtl_total, total_this_year_energy, total_this_year_yekdem, total_this_year_salestotal,
         total_this_year_salespeak, total_this_year_salesoffpeak, total_this_year_hedgetotal, total_this_year_hedgepeak,
         total_this_year_hedgeoffpeak, total_this_year_open_baseload, total_this_year_open_peak,
         total_this_year_open_offpeak], axis=1)

    hedge_percent_this_year = round(
        np.sum(total_this_year_hedgetotal.values) / np.sum(total_this_year_salestotal.values) * (-1) * 100, 0)

    total_pl_this_year = round(np.sum(total_this_year_energy[:-1].values) + np.sum(total_this_year_yekdem[:-1].values), 1)
    ppr.final_tablolar["total"]["total_this_year"] = total_this_year

    total_next_year_energy = pd.DataFrame(data=mass_nextyear_energy.values + b2b_next_year_energy.values,
                                          index=mass_nextyear_energy.index, columns=['PL-MTL ENERGY'])
    total_next_year_yekdem = pd.DataFrame(data=mass_next_year_total_yekdem.values + b2b_next_year_total_yekdem.values,
                                          index=total_next_year_energy.index, columns=['PL-MTL YEKDEM'])
    total_next_year_plmtl_total = pd.DataFrame(data=total_next_year_energy.values + total_next_year_yekdem.values,
                                               index=total_next_year_energy.index, columns=['PL-MTL TOTAL'])

    total_next_year_salespeak_b2b = ppr.add_yearaverage_column(b2b_energy_peak_demand, next_year, 'SALES-MW PEAK B2B')
    total_next_year_salespeak_bulk = ppr.add_yearaverage_column(Bulk_energy_peak_demand, next_year, 'SALES-MW PEAK BULK')
    total_next_year_salespeak = pd.DataFrame(
        data=total_next_year_salespeak_b2b.values + total_next_year_salespeak_bulk.values,
        index=total_next_year_energy.index, columns=['SALES-MW PEAK'])

    total_next_year_salesoffpeak_b2b = ppr.add_yearaverage_column(b2b_energy_offpeak_demand, next_year,
                                                                  'SALES-MW OFF PEAK B2B')
    total_next_year_salesoffpeak_bulk = ppr.add_yearaverage_column(Bulk_energy_offpeak_demand, next_year,
                                                                   'SALES-MW OFF PEAK BULK')
    total_next_year_salesoffpeak = pd.DataFrame(
        data=total_next_year_salesoffpeak_b2b.values + total_next_year_salesoffpeak_bulk.values,
        index=total_next_year_energy.index, columns=['SALES-MW OFFPEAK'])

    total_next_year_salestotal = pd.DataFrame(
        data=(total_next_year_salespeak.values + total_next_year_salesoffpeak.values) / 2,
        index=total_next_year_energy.index, columns=['SALES-MW TOTAL'])[:-1]
    total_next_year_salestotal.loc[f'{next_year}'] = np.average(total_next_year_salestotal.values)

    total_next_year_hedgepeak_b2b = pd.DataFrame(
        data=ppr.add_yearaverage_column(b2b_energy_peak_otc, next_year, "1").values + ppr.add_yearaverage_column(
            b2b_energy_peak_daphedge, next_year, "2").values, index=total_next_year_energy.index,
        columns=['HEDGE-MW PEAK B2B'])
    total_next_year_hedgepeak_bulk = pd.DataFrame(
        data=ppr.add_yearaverage_column(Bulk_energy_peak_otc, next_year, "1").values + ppr.add_yearaverage_column(
            Bulk_energy_peak_daphedge, next_year, "2").values, index=total_next_year_energy.index,
        columns=['HEDGE-MW PEAK BULK'])
    total_next_year_hedgepeak = pd.DataFrame(
        data=(total_next_year_hedgepeak_b2b.values + total_next_year_hedgepeak_bulk.values),
        index=total_next_year_energy.index, columns=['HEDGE-MW PEAK'])[:-1]
    total_next_year_hedgepeak.loc[f'{next_year}'] = np.average(total_next_year_hedgepeak.values)

    total_next_year_hedgeoffpeak_b2b = pd.DataFrame(
        data=ppr.add_yearaverage_column(b2b_energy_offpeak_otc, next_year, "1").values + ppr.add_yearaverage_column(
            b2b_energy_offpeak_daphedge, next_year, "2").values, index=total_next_year_energy.index,
        columns=['HEDGE-MW OFFPEAK B2B'])
    total_next_year_hedgeoffpeak_bulk = pd.DataFrame(
        data=ppr.add_yearaverage_column(Bulk_energy_offpeak_otc, next_year, "1").values + ppr.add_yearaverage_column(
            Bulk_energy_offpeak_daphedge, next_year, "2").values, index=total_next_year_energy.index,
        columns=['HEDGE-MW OFFPEAK BULK'])
    total_next_year_hedgeoffpeak = pd.DataFrame(
        data=(total_next_year_hedgeoffpeak_b2b.values + total_next_year_hedgeoffpeak_bulk.values),
        index=total_next_year_energy.index, columns=['HEDGE-MW OFFPEAK'])[:-1]
    total_next_year_hedgeoffpeak.loc[f'{next_year}'] = np.average(total_next_year_hedgeoffpeak.values)

    total_next_year_hedgetotal = pd.DataFrame(
        data=(total_next_year_hedgepeak.values + total_next_year_hedgeoffpeak.values) / 2,
        index=total_next_year_energy.index, columns=['HEDGE-MW TOTAL'])[:-1]
    total_next_year_hedgetotal.loc[f'{next_year}'] = np.average(total_next_year_hedgetotal.values)

    total_next_year_open_baseload = pd.DataFrame(data=total_next_year_hedgetotal.values + total_next_year_salestotal.values,
                                                 index=total_next_year_energy.index, columns=['OPEN POSITION-MW BASELOAD'])
    total_next_year_open_peak = pd.DataFrame(data=total_next_year_hedgepeak.values + total_next_year_salespeak.values,
                                             index=total_next_year_energy.index, columns=['OPEN POSITION-MW PEAK'])
    total_next_year_open_offpeak = pd.DataFrame(
        data=total_next_year_hedgeoffpeak.values + total_next_year_salesoffpeak.values, index=total_next_year_energy.index,
        columns=['OPEN POSITION-MW OFFPEAK'])

    total_next_year = pd.concat(
        [total_next_year_plmtl_total, total_next_year_energy, total_next_year_yekdem, total_next_year_salestotal,
         total_next_year_salespeak, total_next_year_salesoffpeak, total_next_year_hedgetotal, total_next_year_hedgepeak,
         total_next_year_hedgeoffpeak, total_next_year_open_baseload, total_next_year_open_peak,
         total_next_year_open_offpeak], axis=1)

    hedge_percent_next_year = round(
        np.sum(total_next_year_hedgetotal.values) / np.sum(total_next_year_salestotal.values) * (-1) * 100, 0)

    total_pl_next_year = round(np.sum(total_next_year_energy[:-1].values) + np.sum(total_next_year_yekdem[:-1].values), 1)
    ppr.final_tablolar["total"]["total_next_year"] = total_next_year

    print(f"TOTAL PL for {snapshot_date} {this_year}: {total_pl_this_year}\nHEDGE% {this_year}: {hedge_percent_this_year} ")
    print(f"TOTAL PL for {snapshot_date} {next_year}: {total_pl_next_year}\nHEDGE% {next_year}: {hedge_percent_next_year} ")

    #PARAMETERS
    parameters = pd.DataFrame()
    param_BidPricePeak = energy_group[energy_group.index.get_level_values('BookGroupName') == str("Bulk Fixed OTC")][
        str("BidPricePeak")].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
    param_BidPriceOffpeak = energy_group[energy_group.index.get_level_values('BookGroupName') == str("Bulk Fixed OTC")][
        str("BidPriceOffpeak")].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
    param_BidPriceBL = pd.DataFrame(data=(param_BidPricePeak.values + param_BidPriceOffpeak.values) / 2,
                                    columns=["BIDBASELOAD"], index=param_BidPricePeak.index)
    param_OfferPricePeak = energy_group[energy_group.index.get_level_values('BookGroupName') == str("Bulk Fixed OTC")][
        str("OfferPricePeak")].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
    param_OfferPriceOffpeak = \
    energy_group[energy_group.index.get_level_values('BookGroupName') == str("Bulk Fixed OTC")][
        str("OfferPriceOffpeak")].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
    param_OfferPriceBL = pd.DataFrame(data=(param_OfferPricePeak.values + param_OfferPriceOffpeak.values) / 2,
                                      columns=["OFFERBASELOAD"], index=param_OfferPricePeak.index)
    param_PB = pd.DataFrame(data=(param_OfferPricePeak.values / param_OfferPriceBL.values), columns=["P/B"],
                            index=param_BidPricePeak.index)
    param_Spread = pd.DataFrame(data=(param_OfferPriceBL.values - param_BidPriceBL.values), columns=["Spread"],
                                index=param_BidPricePeak.index)

    param_yekdem_UnitCost_data = yekdemunitcost_group[
        yekdemunitcost_group.index.get_level_values('BookGroupName') == str("B2B Fixed Yekdem Unit Cost")][
        str("OfferPricePeak")].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
    param_yekdem_UnitCost = pd.DataFrame(data=param_yekdem_UnitCost_data.values, columns=["UNIT COST"],
                                         index=param_BidPricePeak.index)

    param_yekdem_FX_data = \
    yekdemfx_group[yekdemfx_group.index.get_level_values('BookGroupName') == str("Bulk Fixed FX OTC")][
        str("OfferPricePeak")].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
    param_yekdem_FX = pd.DataFrame(data=param_yekdem_FX_data.values, columns=["FX FWC"], index=param_BidPricePeak.index)

    parameters = pd.concat(
        [parameters, param_BidPricePeak, param_BidPriceOffpeak, param_BidPriceBL, param_OfferPricePeak,
         param_OfferPriceOffpeak, param_OfferPriceBL, param_PB, param_Spread, param_yekdem_UnitCost, param_yekdem_FX],
        axis=1)

    parameter_electricity_fwc_bid = pd.concat(
        [param_BidPriceBL, param_BidPricePeak.rename(columns={"BidPricePeak": "BIDPEAK"}),
         param_BidPriceOffpeak.rename(columns={"BidPriceOffpeak": "BIDOFFPEAK"})], axis=1)
    parameter_electricity_fwc_bid_ty = parameter_electricity_fwc_bid[
        parameter_electricity_fwc_bid.index.get_level_values('Year') == this_year].copy()
    parameter_electricity_fwc_bid_ty.loc[f'{this_year}'] = parameter_electricity_fwc_bid_ty.iloc[:, :].mean()
    parameter_electricity_fwc_bid_ny = parameter_electricity_fwc_bid[
        parameter_electricity_fwc_bid.index.get_level_values('Year') == next_year].copy()
    parameter_electricity_fwc_bid_ny.loc[f'{next_year}'] = parameter_electricity_fwc_bid_ny.iloc[:, :].mean()

    parameter_electricity_fwc_parameters = pd.concat([param_Spread, param_PB.rename(columns={"P/B": "PEAK/BASE"})],
                                                     axis=1)
    parameter_electricity_fwc_parameters_ty = parameter_electricity_fwc_parameters[
        parameter_electricity_fwc_parameters.index.get_level_values('Year') == this_year].copy()
    parameter_electricity_fwc_parameters_ty.loc[f'{this_year}'] = parameter_electricity_fwc_parameters_ty.iloc[:,
                                                                  :].mean()
    parameter_electricity_fwc_parameters_ny = parameter_electricity_fwc_parameters[
        parameter_electricity_fwc_parameters.index.get_level_values('Year') == next_year].copy()
    parameter_electricity_fwc_parameters_ny.loc[f'{next_year}'] = parameter_electricity_fwc_parameters_ny.iloc[:,
                                                                  :].mean()

    parameter_electricity_fwc_offer = pd.concat(
        [param_OfferPriceBL, param_OfferPricePeak.rename(columns={"OfferPricePeak": "OFFERPEAK"}),
         param_OfferPriceOffpeak.rename(columns={"OfferPriceOffpeak": "OFFEROFFPEAK"})], axis=1)
    parameter_electricity_fwc_offer_ty = parameter_electricity_fwc_offer[
        parameter_electricity_fwc_offer.index.get_level_values('Year') == this_year].copy()
    parameter_electricity_fwc_offer_ty.loc[f'{this_year}'] = parameter_electricity_fwc_offer_ty.iloc[:, :].mean()
    parameter_electricity_fwc_offer_ny = parameter_electricity_fwc_offer[
        parameter_electricity_fwc_offer.index.get_level_values('Year') == next_year].copy()
    parameter_electricity_fwc_offer_ny.loc[f'{next_year}'] = parameter_electricity_fwc_offer_ny.iloc[:, :].mean()

    parameter_yekdem_fwc = pd.concat([param_yekdem_UnitCost, param_yekdem_FX], axis=1)
    parameter_yekdem_fwc_ty = parameter_yekdem_fwc[
        parameter_yekdem_fwc.index.get_level_values('Year') == this_year].copy()
    parameter_yekdem_fwc_ty.loc[f'{this_year}'] = parameter_yekdem_fwc_ty.iloc[:, :].mean()
    parameter_yekdem_fwc_ny = parameter_yekdem_fwc[
        parameter_yekdem_fwc.index.get_level_values('Year') == next_year].copy()
    parameter_yekdem_fwc_ny.loc[f'{next_year}'] = parameter_yekdem_fwc_ny.iloc[:, :].mean()

    parameters_this_year = pd.concat(
        [parameter_electricity_fwc_bid_ty, parameter_electricity_fwc_parameters_ty, parameter_electricity_fwc_offer_ty,
         parameter_yekdem_fwc_ty], axis=1)
    parameters_next_year = pd.concat(
        [parameter_electricity_fwc_bid_ny, parameter_electricity_fwc_parameters_ny, parameter_electricity_fwc_offer_ny,
         parameter_yekdem_fwc_ny], axis=1)

    ppr.ara_tablolar["parameters"]["parameters"] = parameters
    ppr.final_tablolar["parameters"]["parameters_this_year"] = parameters_this_year
    ppr.final_tablolar["parameters"]["parameters_next_year"] = parameters_next_year


    snapshot_date = datetime.strptime(snapshot_date,"%d.%m.%Y")
    db.insert_total_result(snapshot_date,
                       total_pl_this_year,
                       hedge_percent_this_year,
                       total_pl_next_year,
                       hedge_percent_next_year
                       )

    db.insert_b2b_result(snapshot_date,
                       b2b_pl_this_year,
                       b2b_pl_next_year
                       )

    db.insert_mass_result(snapshot_date,
                       mass_pl_this_year,
                       mass_pl_next_year
                       )

    db.insert_fx_result(snapshot_date,
                       fx_pl_this_year,
                       fx_pl_next_year
                       )
def insert_total_this_year(snapshot_date):
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_total_this_year(snapshot_date) == 0:
        lista_this_year = [row for row in ppr.final_tablolar["total"]["total_this_year"].iterrows()]
        for item in lista_this_year:
            db.insert_totals_this_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1],
                                   item[1].values[2],
                                   item[1].values[3],
                                   item[1].values[4],
                                   item[1].values[5],
                                   item[1].values[6],
                                   item[1].values[7],
                                   item[1].values[8],
                                   item[1].values[9],
                                   item[1].values[10],
                                   item[1].values[11]
                               )
        print(f"insert_total_this_year is done for {snapshot_date}")

def insert_total_next_year(snapshot_date):
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_total_next_year(snapshot_date) == 0:
        lista_next_year = [row for row in ppr.final_tablolar["total"]["total_next_year"] .iterrows()]
        for item in lista_next_year:
            db.insert_totals_next_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1],
                                   item[1].values[2],
                                   item[1].values[3],
                                   item[1].values[4],
                                   item[1].values[5],
                                   item[1].values[6],
                                   item[1].values[7],
                                   item[1].values[8],
                                   item[1].values[9],
                                   item[1].values[10],
                                   item[1].values[11]
                               )
        print(f"insert_total_next_year is done for {snapshot_date}")

def insert_b2b_this_year(snapshot_date):
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_b2b_this_year(snapshot_date) == 0:
        lista_next_year = [row for row in ppr.final_tablolar["b2b"]["b2b_this_year"].iterrows()]
        for item in lista_next_year:
            db.insert_b2b_this_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1],
                                   item[1].values[2],
                                   item[1].values[3],
                                   item[1].values[4],
                                   item[1].values[5],
                                   item[1].values[6],
                                   item[1].values[7],
                                   item[1].values[8],
                                   item[1].values[9]
                               )
        print(f"insert_b2b_this_year is done for {snapshot_date}")

def insert_b2b_next_year(snapshot_date):
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_b2b_next_year(snapshot_date) == 0:
        lista_next_year = [row for row in ppr.final_tablolar["b2b"]["b2b_next_year"].iterrows()]
        for item in lista_next_year:
            db.insert_b2b_next_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1],
                                   item[1].values[2],
                                   item[1].values[3],
                                   item[1].values[4],
                                   item[1].values[5],
                                   item[1].values[6],
                                   item[1].values[7],
                                   item[1].values[8],
                                   item[1].values[9]
                               )
        print(f"insert_b2b_next_year is done for {snapshot_date}")

def insert_fx_this_year(snapshot_date):
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_fx_this_year(snapshot_date) == 0:
        lista_next_year = [row for row in ppr.final_tablolar["fx"]["fx_this_year"].iterrows()]
        for item in lista_next_year:
            db.insert_fx_this_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1],
                                   item[1].values[2],
                                   item[1].values[3],
                                   item[1].values[4],
                                   item[1].values[5],
                                   item[1].values[6],
                                   item[1].values[7],
                                   item[1].values[8],
                                   item[1].values[9],
                                   item[1].values[10],
                                   item[1].values[11],
                                   item[1].values[12]
                               )
        print(f"insert_fx_this_year is done for {snapshot_date}")

def insert_fx_next_year(snapshot_date):
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_fx_next_year(snapshot_date) == 0:
        lista_next_year = [row for row in ppr.final_tablolar["fx"]["fx_next_year"].iterrows()]
        for item in lista_next_year:
            db.insert_fx_next_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1],
                                   item[1].values[2],
                                   item[1].values[3],
                                   item[1].values[4],
                                   item[1].values[5],
                                   item[1].values[6],
                                   item[1].values[7],
                                   item[1].values[8],
                                   item[1].values[9],
                                   item[1].values[10],
                                   item[1].values[11],
                                   item[1].values[12]
                               )
        print(f"insert_fx_next_year is done for {snapshot_date}")

def insert_mass_this_year(snapshot_date):
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_mass_this_year(snapshot_date) == 0:
        lista_next_year = [row for row in ppr.final_tablolar["mass"]["mass_this_year"].iterrows()]
        for item in lista_next_year:
            db.insert_mass_this_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1],
                                   item[1].values[2],
                                   item[1].values[3],
                                   item[1].values[4],
                                   item[1].values[5],
                                   item[1].values[6],
                                   item[1].values[7],
                                   item[1].values[8],
                                   item[1].values[9]
                               )
        print(f"insert_mass_this_year is done for {snapshot_date}")

def insert_mass_next_year(snapshot_date):
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_mass_next_year(snapshot_date) == 0:
        lista_next_year = [row for row in ppr.final_tablolar["mass"]["mass_next_year"].iterrows()]
        for item in lista_next_year:
            db.insert_mass_next_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1],
                                   item[1].values[2],
                                   item[1].values[3],
                                   item[1].values[4],
                                   item[1].values[5],
                                   item[1].values[6],
                                   item[1].values[7],
                                   item[1].values[8],
                                   item[1].values[9]
                               )
        print(f"insert_mass_next_year is done for {snapshot_date}")

def insert_b2b_op_this_year(snapshot_date):
    df = pd.concat([ppr.final_tablolar["b2b"]["b2b_energy_total_peak"], ppr.final_tablolar["b2b"]["b2b_energy_total_offpeak"]],axis=1)
    df = df[df.index.get_level_values("Year") == this_year]
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_b2b_op_this_year(snapshot_date) == 0:
        lista_next_year = [row for row in df.iterrows()]
        for item in lista_next_year:
            db.insert_b2b_op_this_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1]
                               )
        print(f"insert_b2b_op_this_year is done for {snapshot_date}")

def insert_b2b_op_next_year(snapshot_date):
    df = pd.concat([ppr.final_tablolar["b2b"]["b2b_energy_total_peak"], ppr.final_tablolar["b2b"]["b2b_energy_total_offpeak"]],axis=1)
    df = df[df.index.get_level_values("Year") == next_year]
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_b2b_op_next_year(snapshot_date) == 0:
        lista_next_year = [row for row in df.iterrows()]
        for item in lista_next_year:
            db.insert_b2b_op_next_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1]
                               )
        print(f"insert_b2b_op_next_year is done for {snapshot_date}")

def insert_mass_op_this_year(snapshot_date):
    df = pd.concat([ppr.final_tablolar["mass"]["Bulk_energy_peak_total"], ppr.final_tablolar["mass"]["Bulk_energy_offpeak_total"]],axis=1)
    df = df[df.index.get_level_values("Year") == this_year]
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_mass_op_this_year(snapshot_date) == 0:
        lista_next_year = [row for row in df.iterrows()]
        for item in lista_next_year:
            db.insert_mass_op_this_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1]
                               )
        print(f"insert_mass_op_this_year is done for {snapshot_date}")

def insert_mass_op_next_year(snapshot_date):
    df = pd.concat([ppr.final_tablolar["mass"]["Bulk_energy_peak_total"],ppr.final_tablolar["mass"]["Bulk_energy_offpeak_total"]], axis=1)
    df = df[df.index.get_level_values("Year") == next_year]
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_mass_op_next_year(snapshot_date) == 0:
        lista_next_year = [row for row in df.iterrows()]
        for item in lista_next_year:
            db.insert_mass_op_next_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1]
                               )
        print(f"insert_mass_op_next_year is done for {snapshot_date}")

def insert_fx_op_this_year(snapshot_date):
    openb2b = ppr.final_tablolar["fx"]["fx_this_year"]["OPEN M-USD B2B"][:-1]
    openmass = ppr.final_tablolar["fx"]["fx_this_year"]["OPEN M-USD MASS"][:-1]
    fx_open = pd.concat([openb2b, openmass], axis=1)
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_fx_op_this_year(snapshot_date) == 0:
        lista_next_year = [row for row in fx_open.iterrows()]
        for item in lista_next_year:
            db.insert_fx_op_this_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1]
                               )
        print(f"insert_fx_op_this_year is done for {snapshot_date}")

def insert_fx_op_next_year(snapshot_date):
    openb2b = ppr.final_tablolar["fx"]["fx_next_year"]["OPEN M-USD B2B"][:-1]
    openmass = ppr.final_tablolar["fx"]["fx_next_year"]["MASS M-USD B2B"][:-1]
    fx_open = pd.concat([openb2b, openmass], axis=1)
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_fx_op_next_year(snapshot_date) == 0:
        lista_next_year = [row for row in fx_open.iterrows()]
        for item in lista_next_year:
            db.insert_fx_op_next_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1]
                               )
        print(f"insert_fx_op_next_year is done for {snapshot_date}")

def insert_parameters_this_year(snapshot_date):
    params = ppr.final_tablolar["parameters"]["parameters_this_year"]
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_parameters_this_year(snapshot_date) == 0:
        lista = [row for row in params.iterrows()]
        for item in lista:
            db.insert_param_this_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1],
                                   item[1].values[2],
                                   item[1].values[3],
                                   item[1].values[4],
                                   item[1].values[5],
                                   item[1].values[6],
                                   item[1].values[7],
                                   item[1].values[8],
                                   item[1].values[9]
                               )
        print(f"insert_parameters_this_year is done for {snapshot_date}")

def insert_parameters_next_year(snapshot_date):
    params = ppr.final_tablolar["parameters"]["parameters_next_year"]
    snapshot_date = datetime.strptime(snapshot_date, "%d.%m.%Y")
    if db.db_check_parameters_next_year(snapshot_date) == 0:
        lista = [row for row in params.iterrows()]
        for item in lista:
            db.insert_param_next_year(snapshot_date,
                                   item[1].values[0],
                                   item[1].values[1],
                                   item[1].values[2],
                                   item[1].values[3],
                                   item[1].values[4],
                                   item[1].values[5],
                                   item[1].values[6],
                                   item[1].values[7],
                                   item[1].values[8],
                                   item[1].values[9]
                               )
        print(f"insert_parameters_next_year is done for {snapshot_date}")

if __name__ == "__main__":
    print(file_dates)
    error_dates = []
    for date in file_dates:
        try:
            make_report(date, conn)
            insert_total_this_year(date)
            insert_total_next_year(date)
            insert_b2b_this_year(date)
            insert_b2b_next_year(date)
            insert_fx_this_year(date)
            insert_fx_next_year(date)
            insert_mass_this_year(date)
            insert_mass_next_year(date)
            insert_b2b_op_this_year(date)
            insert_b2b_op_next_year(date)
            insert_mass_op_this_year(date)
            insert_mass_op_next_year(date)
            insert_fx_op_this_year(date)
            insert_fx_op_next_year(date)
            insert_parameters_this_year(date)
            insert_parameters_next_year((date))
        except KeyError:
            print(f"KeyError for {date}")
            error_dates.append(date)
    print(error_dates)






"""
for key,value in ppr.ara_tablolar.items():
    print(f"Writing to {key} excel...")
    with pd.ExcelWriter(f"Tablolar/{key}.xlsx") as writer:
        for key1,value1 in ppr.ara_tablolar[key].items():
            value1.to_excel(writer, sheet_name=f"{key1}", index=False)
            print(f"{key1} page is created in {key} excel...")

for key,value in ppr.final_tablolar.items():
    print(f"Writing to {key} excel...")
    with pd.ExcelWriter(f"Tablolar/final_tablo{key}.xlsx") as writer:
        for key1,value1 in ppr.final_tablolar[key].items():
            value1.to_excel(writer, sheet_name=f"{key1}", index=False)
            print(f"{key1} page is created in {key} excel...")
"""

