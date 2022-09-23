import sqlite3



class Database:
    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()

    def insert_snapshot(self,table_name,date,OfferVersionId,BidVersionId,SnapshotName,BookGroupName,Year,Month,BuyVolumePeak,BuyVolumeOffpeak,SellVolumePeak,SellVolumeOffpeak,BuyPricePeak,BuyPriceOffpeak,SellPricePeak,SellPriceOffpeak,BidPricePeak,BidPriceOffpeak,OfferPricePeak,OfferPriceOffpeak,PositionPeak,PositionOffpeak,LockedInPLPeak,LockedInPLOffpeak,FloatingPLPeak,FloatingPLOffpeak):
        self.cur.execute(f"INSERT INTO {table_name} VALUES (NULL ,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                         (date,OfferVersionId,BidVersionId,SnapshotName,BookGroupName,Year,Month,BuyVolumePeak,BuyVolumeOffpeak,SellVolumePeak,SellVolumeOffpeak,BuyPricePeak,BuyPriceOffpeak,SellPricePeak,SellPriceOffpeak,BidPricePeak,BidPriceOffpeak,OfferPricePeak,OfferPriceOffpeak,PositionPeak,PositionOffpeak,LockedInPLPeak,LockedInPLOffpeak,FloatingPLPeak,FloatingPLOffpeak))
        self.conn.commit()

    def insert_total_result(self,date,totalpl_thisyear,hedgepercent_thisyear,totalpl_nextyear,hedgepercent_nextyear):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_Total_Results)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        if count == 0:
            self.cur.execute(f"INSERT INTO tbl_PPR_Total_Results VALUES (NULL ,?,?,?,?,?)",
                             (date,totalpl_thisyear,hedgepercent_thisyear,totalpl_nextyear,hedgepercent_nextyear))
            self.conn.commit()

    def insert_b2b_result(self,date,b2bpl_thisyear,b2bpl_nextyear):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_b2b_Results)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        if count == 0:
            self.cur.execute(f"INSERT INTO tbl_PPR_b2b_Results VALUES (NULL ,?,?,?)",
                             (date,b2bpl_thisyear,b2bpl_nextyear))
            self.conn.commit()

    def insert_mass_result(self,date,masspl_thisyear,masspl_nextyear):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_mass_Results)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        if count == 0:
            self.cur.execute(f"INSERT INTO tbl_PPR_mass_Results VALUES (NULL ,?,?,?)",
                             (date,masspl_thisyear,masspl_nextyear))
            self.conn.commit()

    def insert_fx_result(self,date,fxpl_thisyear,fxpl_nextyear):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_fx_Results)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        if count == 0:
            self.cur.execute(f"INSERT INTO tbl_PPR_fx_Results VALUES (NULL ,?,?,?)",
                             (date,fxpl_thisyear,fxpl_nextyear))
            self.conn.commit()

    def insert_totals_this_year(self,snapshot_date,plmtltotal,plmtlenergy,plmtlyekdem,salesmwtotal,salesmwpeak,salesmwoffpeak,hedgemwtotal,hedgemwpeak,hedgemwoffpeak,openpositionmwbaseload,openpositionmwpeak,openpositionmwpeakoffpeak):
        self.cur.execute(f"INSERT INTO tbl_PPR_Total_This_Year VALUES (NULL ,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                         (snapshot_date,plmtltotal,plmtlenergy,plmtlyekdem,salesmwtotal,salesmwpeak,salesmwoffpeak,hedgemwtotal,hedgemwpeak,hedgemwoffpeak,openpositionmwbaseload,openpositionmwpeak,openpositionmwpeakoffpeak))
        self.conn.commit()

    def insert_totals_next_year(self,snapshot_date,plmtltotal,plmtlenergy,plmtlyekdem,salesmwtotal,salesmwpeak,salesmwoffpeak,hedgemwtotal,hedgemwpeak,hedgemwoffpeak,openpositionmwbaseload,openpositionmwpeak,openpositionmwpeakoffpeak):
        self.cur.execute(f"INSERT INTO tbl_PPR_Total_Next_Year VALUES (NULL ,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                         (snapshot_date,plmtltotal,plmtlenergy,plmtlyekdem,salesmwtotal,salesmwpeak,salesmwoffpeak,hedgemwtotal,hedgemwpeak,hedgemwoffpeak,openpositionmwbaseload,openpositionmwpeak,openpositionmwpeakoffpeak))
        self.conn.commit()
        
    def insert_b2b_this_year(self,snapshot_date,energy_pl_mtl, yekdem_pl_mtl_unit_cost, yekdem_pl_mtl_dap,yekdem_pl_mtl_fx, yekdem_pl_mtl_total, sales_mw_total,hedge_mw_total, open_pos_mw_baseload, tl_mwh_buy_price,tl_mwh_sell_price):
        self.cur.execute(f"INSERT INTO tbl_PPR_b2b_this_year VALUES (NULL ,?,?,?,?,?,?,?,?,?,?,?)",
                                        (snapshot_date,energy_pl_mtl, yekdem_pl_mtl_unit_cost, yekdem_pl_mtl_dap,yekdem_pl_mtl_fx, yekdem_pl_mtl_total, sales_mw_total,hedge_mw_total, open_pos_mw_baseload, tl_mwh_buy_price,tl_mwh_sell_price))
        self.conn.commit()

    def insert_b2b_next_year(self,snapshot_date,energy_pl_mtl, yekdem_pl_mtl_unit_cost, yekdem_pl_mtl_dap,yekdem_pl_mtl_fx, yekdem_pl_mtl_total, sales_mw_total,hedge_mw_total, open_pos_mw_baseload, tl_mwh_buy_price,tl_mwh_sell_price):
        self.cur.execute(f"INSERT INTO tbl_PPR_b2b_next_year VALUES (NULL ,?,?,?,?,?,?,?,?,?,?,?)",
                         (snapshot_date,energy_pl_mtl, yekdem_pl_mtl_unit_cost, yekdem_pl_mtl_dap,yekdem_pl_mtl_fx, yekdem_pl_mtl_total, sales_mw_total,hedge_mw_total, open_pos_mw_baseload, tl_mwh_buy_price,tl_mwh_sell_price))
        self.conn.commit()

    def insert_fx_this_year(self,snapshot_date,demand_m_usd_total, demand_m_usd_b2b, demand_m_usd_mass,hedge_m_usd_total, hedge_m_usd_b2b, hedge_m_usd_mass,open_m_usd_total, open_m_usd_b2b, open_m_usd_mass,hedge_pl_mtl_b2b, hedge_pl_mtl_mass, dollar_based_sourcing_musd,dollar_based_sourcing_fx_rate):
        self.cur.execute(f"INSERT INTO tbl_PPR_fx_this_year VALUES (NULL ,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                         (snapshot_date,demand_m_usd_total, demand_m_usd_b2b, demand_m_usd_mass,hedge_m_usd_total, hedge_m_usd_b2b, hedge_m_usd_mass,open_m_usd_total, open_m_usd_b2b, open_m_usd_mass,hedge_pl_mtl_b2b, hedge_pl_mtl_mass, dollar_based_sourcing_musd,dollar_based_sourcing_fx_rate))
        self.conn.commit()

    def insert_fx_next_year(self,snapshot_date,demand_m_usd_total, demand_m_usd_b2b, demand_m_usd_mass,hedge_m_usd_total, hedge_m_usd_b2b, hedge_m_usd_mass,open_m_usd_total, open_m_usd_b2b, open_m_usd_mass,hedge_pl_mtl_b2b, hedge_pl_mtl_mass, dollar_based_sourcing_musd,dollar_based_sourcing_fx_rate):
        self.cur.execute(f"INSERT INTO tbl_PPR_fx_next_year VALUES (NULL ,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                         (snapshot_date,demand_m_usd_total, demand_m_usd_b2b, demand_m_usd_mass,hedge_m_usd_total, hedge_m_usd_b2b, hedge_m_usd_mass,open_m_usd_total, open_m_usd_b2b, open_m_usd_mass,hedge_pl_mtl_b2b, hedge_pl_mtl_mass, dollar_based_sourcing_musd,dollar_based_sourcing_fx_rate))
        self.conn.commit()

    def insert_mass_this_year(self,snapshot_date,energy_pl_mtl_energy, yekdem_pl_mtl_unitcost, yekdem_pl_mtl_dap,yekdem_pl_mtl_fx, yekdem_pl_mtl_total, sales_mw_total,hedge_mw_total, open_pos_mw_baseload, tl_mwh_buy_price,tl_mwh_sell_price):
        self.cur.execute(f"INSERT INTO tbl_PPR_mass_this_year VALUES (NULL ,?,?,?,?,?,?,?,?,?,?,?)",
                         (snapshot_date,energy_pl_mtl_energy, yekdem_pl_mtl_unitcost, yekdem_pl_mtl_dap,yekdem_pl_mtl_fx, yekdem_pl_mtl_total, sales_mw_total,hedge_mw_total, open_pos_mw_baseload, tl_mwh_buy_price,tl_mwh_sell_price))
        self.conn.commit()

    def insert_mass_next_year(self,snapshot_date,energy_pl_mtl_energy, yekdem_pl_mtl_unitcost, yekdem_pl_mtl_dap,yekdem_pl_mtl_fx, yekdem_pl_mtl_total, sales_mw_total,hedge_mw_total, open_pos_mw_baseload, tl_mwh_buy_price,tl_mwh_sell_price):
        self.cur.execute(f"INSERT INTO tbl_PPR_mass_next_year VALUES (NULL ,?,?,?,?,?,?,?,?,?,?,?)",
                         (snapshot_date,energy_pl_mtl_energy, yekdem_pl_mtl_unitcost, yekdem_pl_mtl_dap,yekdem_pl_mtl_fx, yekdem_pl_mtl_total, sales_mw_total,hedge_mw_total, open_pos_mw_baseload, tl_mwh_buy_price,tl_mwh_sell_price))
        self.conn.commit()

    def insert_b2b_op_this_year(self,snapshot_date,peak,offpeak):
        self.cur.execute(f"INSERT INTO tbl_PPR_b2b_op_this_year VALUES (NULL ,?,?,?)",
                         (snapshot_date,peak,offpeak))
        self.conn.commit()

    def insert_b2b_op_next_year(self,snapshot_date,peak,offpeak):
        self.cur.execute(f"INSERT INTO tbl_PPR_b2b_op_next_year VALUES (NULL ,?,?,?)",
                         (snapshot_date,peak,offpeak))
        self.conn.commit()

    def insert_mass_op_this_year(self,snapshot_date,peak,offpeak):
        self.cur.execute(f"INSERT INTO tbl_PPR_mass_op_this_year VALUES (NULL ,?,?,?)",
                         (snapshot_date,peak,offpeak))
        self.conn.commit()

    def insert_mass_op_next_year(self,snapshot_date,peak,offpeak):
        self.cur.execute(f"INSERT INTO tbl_PPR_mass_op_next_year VALUES (NULL ,?,?,?)",
                         (snapshot_date,peak,offpeak))
        self.conn.commit()

    def insert_fx_op_this_year(self,snapshot_date,openb2b,openmass):
        self.cur.execute(f"INSERT INTO tbl_PPR_fx_op_this_year VALUES (NULL ,?,?,?)",
                         (snapshot_date,openb2b,openmass))
        self.conn.commit()

    def insert_fx_op_next_year(self,snapshot_date,openb2b,openmass):
        self.cur.execute(f"INSERT INTO tbl_PPR_fx_op_next_year VALUES (NULL ,?,?,?)",
                         (snapshot_date,openb2b,openmass))
        self.conn.commit()

    def insert_param_this_year(self,snapshot_date,bid_baseload,bid_peak,bid_offpeak,spread,peak_over_base,offer_baseload,offer_peak,offer_offpeak,unit_cost,fx_fwc):
        self.cur.execute(f"INSERT INTO tbl_PPR_Parameters_This_Year VALUES (NULL ,?,?,?,?,?,?,?,?,?,?,?)",
                         (snapshot_date,bid_baseload,bid_peak,bid_offpeak,spread,peak_over_base,offer_baseload,offer_peak,offer_offpeak,unit_cost,fx_fwc))
        self.conn.commit()

    def insert_param_next_year(self,snapshot_date,bid_baseload,bid_peak,bid_offpeak,spread,peak_over_base,offer_baseload,offer_peak,offer_offpeak,unit_cost,fx_fwc):
        self.cur.execute(f"INSERT INTO tbl_PPR_Parameters_Next_Year VALUES (NULL ,?,?,?,?,?,?,?,?,?,?,?)",
                         (snapshot_date,bid_baseload,bid_peak,bid_offpeak,spread,peak_over_base,offer_baseload,offer_peak,offer_offpeak,unit_cost,fx_fwc))
        self.conn.commit()

    def db_check_date(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_Energy)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_total_this_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_Total_This_Year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_total_next_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_Total_Next_Year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_b2b_this_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_b2b_this_year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_b2b_next_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_b2b_next_year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_fx_this_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_fx_this_year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_fx_next_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_fx_next_year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_mass_this_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_mass_this_year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_mass_next_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_mass_next_year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_b2b_op_this_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_b2b_op_this_year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_b2b_op_next_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_b2b_op_next_year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_mass_op_this_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_mass_op_this_year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_mass_op_next_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_mass_op_next_year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_fx_op_this_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_fx_op_this_year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_fx_op_next_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_fx_op_next_year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_parameters_this_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_Parameters_This_Year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count

    def db_check_parameters_next_year(self,date):
        self.cur.execute(f"SELECT COUNT(1) FROM (SELECT DISTINCT snapshot_date FROM tbl_PPR_Parameters_Next_Year)t WHERE snapshot_date = '{date}'")
        count = self.cur.fetchall()[0][0]
        return count


if __name__ == '__main__':
    db = Database()
    for row in db.db_get_table_names():
        print(row[2])


