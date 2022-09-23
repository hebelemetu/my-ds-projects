from datetime import date
import pandas as pd


class PPR:
    def __init__(self):
        self.regions = ["Ayedas", "Baskent", "Toroslar"]
        self.ara_tablolar = {"b2b_energy": {},
                             "b2b_energyfx": {},
                             "b2b_yekdemunitcost": {},
                             "b2b_yekdemdap": {},
                             "b2b_yekdemfx": {},
                             "bulk_energy": {},
                             "bulk_energyfx": {},
                             "bulk_yekdemunitcost": {},
                             "bulk_yekdemdap": {},
                             "bulk_yekdemfx": {},
                             "parameters": {}}

        self.final_tablolar = {"b2b": {},
                               "fx": {},
                               "mass": {},
                               "total": {},
                               "parameters": {}}

    def data_prep(self, df):
        df["TotalBuyVolume"] = df["BuyVolumePeak"].copy() + df["BuyVolumeOffpeak"].copy()
        df["TotalSellVolume"] = df["SellVolumePeak"].copy() + df["SellVolumeOffpeak"].copy()
        df["TotalNetVolume"] = df["TotalBuyVolume"].copy() - df["TotalSellVolume"].copy()
        df["AvgBuyPrice"] = (df["BuyVolumePeak"].copy() * df["BuyPricePeak"].copy() + df["BuyVolumeOffpeak"].copy() *
                             df["BuyPriceOffpeak"].copy()) / df["TotalBuyVolume"]
        df["AvgSellPrice"] = (df["SellVolumePeak"].copy() * df["SellPricePeak"].copy() + df[
            "SellVolumeOffpeak"].copy() * df["SellPriceOffpeak"].copy()) / df["TotalSellVolume"]
        df["BuyCost"] = (df["BuyVolumePeak"].copy() * df["BuyPricePeak"].copy() + df["BuyVolumeOffpeak"].copy() * df[
            "BuyPriceOffpeak"].copy())
        df["SellCost"] = (
                    df["SellVolumePeak"].copy() * df["SellPricePeak"].copy() + df["SellVolumeOffpeak"].copy() * df[
                "SellPriceOffpeak"].copy())
        df["TotalPL"] = df["LockedInPLPeak"].copy() + df["LockedInPLOffpeak"].copy() + df["FloatingPLPeak"].copy() + df[
            "FloatingPLOffpeak"]
        df = df.fillna(0)
        return df

    def data_group(self, df):
        df = df.sort_index()
        df = df.groupby(["BookGroupName", "Year", "Month"]).sum()
        df_hours = [pd.Timestamp(item[1], item[2], 1).days_in_month * 24 for item in list(df.index)]
        df.index = [[item[0] for item in df.index], [item[1] for item in df.index], [item[2] for item in df.index],
                    df_hours]
        df.index.names = ["BookGroupName", "Year", "Month", "Hour"]
        df = df.fillna(0)
        return df

    def create_df(self, book_list, book_group, column_name):
        df = pd.DataFrame()
        for book in book_list:
            for item in sorted(book):
                df_item = book_group[book_group.index.get_level_values('BookGroupName') == str(item)][
                    str(column_name)].to_frame().reset_index(level=[0]).drop("BookGroupName", axis=1)
                df_item = df_item.rename(columns={str(column_name): str(item)})
                df = pd.concat([df, df_item], axis=1)
        df = df.fillna(0)
        return df

    def add_normal_sum(self, df):
        df = df.reindex(sorted(df.columns), axis=1)
        df["Sum"] = df[
            [col for col in df.columns if all(x not in str(col) for x in self.regions) and "P2S" not in str(col)]].sum(
            axis=1)
        for i in range(len(self.regions)):
            df[f"{self.regions[i]} Sum"] = df[
                [col for col in df.columns if self.regions[i] in str(col) and "P2S" not in str(col)]].sum(axis=1)
        df = df.fillna(0)
        return df

    def gwh_to_mw(self, df):
        df = df.apply(lambda x: x * 1000 / df.index.get_level_values('Hour'))
        df = df.fillna(0)
        return df

    def add_yearsum_column(self, df, year, column_name):
        df_new = df[df.index.get_level_values('Year') == year][
            [col for col in df.columns if all(x not in str(col) for x in self.regions)]]
        df_new = df_new.iloc[:, df_new.columns.str.contains("Sum")].reset_index(level=[2]).drop("Hour", axis=1)
        df_new.rename(columns={'Sum': column_name}, inplace=True)
        df_new.loc[f'{year}'] = df_new.iloc[:, :].sum()
        return df_new

    def add_next_year_jan(self, df, year1, year2, column_name):
        df_new = df[df.index.get_level_values('Year') == year1][
            [col for col in df.columns if all(x not in str(col) for x in self.regions)]]
        df_new = df_new.iloc[:, df_new.columns.str.contains("Sum")].reset_index(level=[2]).drop("Hour", axis=1)
        df_new.rename(columns={'Sum': column_name}, inplace=True)

        df_new2 = df[(df.index.get_level_values('Year') == year2) & (df.index.get_level_values('Month') == 1)][
            [col for col in df.columns if all(x not in str(col) for x in self.regions)]]
        df_new2 = df_new2.iloc[:, df_new2.columns.str.contains("Sum")].reset_index(level=[2]).drop("Hour", axis=1)
        df_new2.rename(columns={'Sum': column_name}, inplace=True)

        df_new = df_new.append(df_new2)

        df_new.loc[f'{year1}'] = df_new.iloc[:, :].sum()
        return df_new

    def add_next_year_janfx(self, df, year1, year2, column_name):
        df_new = df[df.index.get_level_values('Year') == year1][
            [col for col in df.columns if all(x not in str(col) for x in self.regions)]]
        df_new = df_new.iloc[:, df_new.columns.str.contains("Sum")].reset_index(level=[2]).drop("Hour", axis=1)
        df_new.rename(columns={'Sum': column_name}, inplace=True)

        df_new2 = df[(df.index.get_level_values('Year') == year2) & (df.index.get_level_values('Month') == 1)][
            [col for col in df.columns if all(x not in str(col) for x in self.regions)]]
        df_new2 = df_new2.iloc[:, df_new2.columns.str.contains("Sum")].reset_index(level=[2]).drop("Hour", axis=1)
        df_new2.rename(columns={'Sum': column_name}, inplace=True)

        df_new = df_new[1:]
        df_new = df_new.append(df_new2)

        df_new.loc[f'{year1}'] = df_new.iloc[:, :].sum()
        return df_new

    def add_yearaverage_column(self, df, year, column_name):
        df_new = df[df.index.get_level_values('Year') == year][
            [col for col in df.columns if all(x not in str(col) for x in self.regions)]]
        df_new = df_new.iloc[:, df_new.columns.str.contains("Sum")].reset_index(level=[2]).drop("Hour", axis=1)
        df_new.rename(columns={'Sum': column_name}, inplace=True)
        df_new.loc[f'{year}'] = df_new.iloc[:, :].mean()
        return df_new