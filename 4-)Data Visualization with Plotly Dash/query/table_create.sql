CREATE TABLE [tbl_PPR_Parameters_This_Year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [bid_baseload] FLOAT NULL
	, [bid_peak] FLOAT NULL
	, [bid_offpeak] FLOAT NULL
	, [spread] FLOAT NULL
	, [peak_over_base] FLOAT NULL
	, [offer_baseload] FLOAT NULL
	, [offer_peak] FLOAT NULL
	, [offer_offpeak] FLOAT NULL
	, [unit_cost] FLOAT NULL
	, [fx_fwc] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83FD63EE08A] PRIMARY KEY ([id] ASC)
)

CREATE TABLE [tbl_PPR_Parameters_Next_Year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [bid_baseload] FLOAT NULL
	, [bid_peak] FLOAT NULL
	, [bid_offpeak] FLOAT NULL
	, [spread] FLOAT NULL
	, [peak_over_base] FLOAT NULL
	, [offer_baseload] FLOAT NULL
	, [offer_peak] FLOAT NULL
	, [offer_offpeak] FLOAT NULL
	, [unit_cost] FLOAT NULL
	, [fx_fwc] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F6C02BD88] PRIMARY KEY ([id] ASC)
)

CREATE TABLE [tbl_PPR_b2b_Results]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [b2bpl_thisyear] FLOAT NULL
	, [b2bpl_nextyear] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83FDC102E38] PRIMARY KEY ([id] ASC)
)

CREATE TABLE [tbl_PPR_mass_Results]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [masspl_thisyear] FLOAT NULL
	, [masspl_nextyear] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83FDFB88490] PRIMARY KEY ([id] ASC)
)

CREATE TABLE [tbl_PPR_fx_Results]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [fxpl_thisyear] FLOAT NULL
	, [fxpl_nextyear] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F278E0863] PRIMARY KEY ([id] ASC)
)

CREATE TABLE [tbl_PPR_Energy]
(
	  [id] INT NOT NULL
	, [snapshot_date] VARCHAR(50)
	, [OfferVersionId] INT NULL
	, [BidVersionId] INT NULL
	, [SnapshotName] VARCHAR(50)
	, [BookGroupName] VARCHAR(50)
	, [Year] INT NULL
	, [Month] INT NULL
	, [BuyVolumePeak] FLOAT NULL
	, [BuyVolumeOffpeak] FLOAT NULL
	, [SellVolumePeak] FLOAT NULL
	, [SellVolumeOffpeak] FLOAT NULL
	, [BuyPricePeak] FLOAT NULL
	, [BuyPriceOffpeak] FLOAT NULL
	, [SellPricePeak] FLOAT NULL
	, [SellPriceOffpeak] FLOAT NULL
	, [BidPricePeak] FLOAT NULL
	, [BidPriceOffpeak] FLOAT NULL
	, [OfferPricePeak] FLOAT NULL
	, [OfferPriceOffpeak] FLOAT NULL
	, [PositionPeak] FLOAT NULL
	, [PositionOffpeak] FLOAT NULL
	, [LockedInPLPeak] FLOAT NULL
	, [LockedInPLOffpeak] FLOAT NULL
	, [FloatingPLPeak] FLOAT NULL
	, [FloatingPLOffpeak] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83FF0DEA59D] PRIMARY KEY ([id] ASC)
)

CREATE TABLE [tbl_PPR_EnergyFX]
(
	  [id] INT NOT NULL
	, [snapshot_date] VARCHAR(50)
	, [OfferVersionId] INT NULL
	, [BidVersionId] INT NULL
	, [SnapshotName] VARCHAR(50)
	, [BookGroupName] VARCHAR(50)
	, [Year] INT NULL
	, [Month] INT NULL
	, [BuyVolumePeak] FLOAT NULL
	, [BuyVolumeOffpeak] FLOAT NULL
	, [SellVolumePeak] FLOAT NULL
	, [SellVolumeOffpeak] FLOAT NULL
	, [BuyPricePeak] FLOAT NULL
	, [BuyPriceOffpeak] FLOAT NULL
	, [SellPricePeak] FLOAT NULL
	, [SellPriceOffpeak] FLOAT NULL
	, [BidPricePeak] FLOAT NULL
	, [BidPriceOffpeak] FLOAT NULL
	, [OfferPricePeak] FLOAT NULL
	, [OfferPriceOffpeak] FLOAT NULL
	, [PositionPeak] FLOAT NULL
	, [PositionOffpeak] FLOAT NULL
	, [LockedInPLPeak] FLOAT NULL
	, [LockedInPLOffpeak] FLOAT NULL
	, [FloatingPLPeak] FLOAT NULL
	, [FloatingPLOffpeak] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F1D5B96C3] PRIMARY KEY ([id] ASC)
)

CREATE TABLE [tbl_PPR_YekdemDAP]
(
	  [id] INT NOT NULL
	, [snapshot_date] VARCHAR(50)
	, [OfferVersionId] INT NULL
	, [BidVersionId] INT NULL
	, [SnapshotName] VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
	, [BookGroupName] VARCHAR(50)
	, [Year] INT NULL
	, [Month] INT NULL
	, [BuyVolumePeak] FLOAT NULL
	, [BuyVolumeOffpeak] FLOAT NULL
	, [SellVolumePeak] FLOAT NULL
	, [SellVolumeOffpeak] FLOAT NULL
	, [BuyPricePeak] FLOAT NULL
	, [BuyPriceOffpeak] FLOAT NULL
	, [SellPricePeak] FLOAT NULL
	, [SellPriceOffpeak] FLOAT NULL
	, [BidPricePeak] FLOAT NULL
	, [BidPriceOffpeak] FLOAT NULL
	, [OfferPricePeak] FLOAT NULL
	, [OfferPriceOffpeak] FLOAT NULL
	, [PositionPeak] FLOAT NULL
	, [PositionOffpeak] FLOAT NULL
	, [LockedInPLPeak] FLOAT NULL
	, [LockedInPLOffpeak] FLOAT NULL
	, [FloatingPLPeak] FLOAT NULL
	, [FloatingPLOffpeak] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F75A32B9B] PRIMARY KEY ([id] ASC)
)

CREATE TABLE [tbl_PPR_YekdemFX]
(
	  [id] INT NOT NULL
	, [snapshot_date] VARCHAR(50)
	, [OfferVersionId] INT NULL
	, [BidVersionId] INT NULL
	, [SnapshotName] VARCHAR(50)
	, [BookGroupName] VARCHAR(50)
	, [Year] INT NULL
	, [Month] INT NULL
	, [BuyVolumePeak] FLOAT NULL
	, [BuyVolumeOffpeak] FLOAT NULL
	, [SellVolumePeak] FLOAT NULL
	, [SellVolumeOffpeak] FLOAT NULL
	, [BuyPricePeak] FLOAT NULL
	, [BuyPriceOffpeak] FLOAT NULL
	, [SellPricePeak] FLOAT NULL
	, [SellPriceOffpeak] FLOAT NULL
	, [BidPricePeak] FLOAT NULL
	, [BidPriceOffpeak] FLOAT NULL
	, [OfferPricePeak] FLOAT NULL
	, [OfferPriceOffpeak] FLOAT NULL
	, [PositionPeak] FLOAT NULL
	, [PositionOffpeak] FLOAT NULL
	, [LockedInPLPeak] FLOAT NULL
	, [LockedInPLOffpeak] FLOAT NULL
	, [FloatingPLPeak] FLOAT NULL
	, [FloatingPLOffpeak] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F98B7C52E] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_YekdemUnitCost]
(
	  [id] INT NOT NULL
	, [snapshot_date] VARCHAR(50)
	, [OfferVersionId] INT NULL
	, [BidVersionId] INT NULL
	, [SnapshotName] VARCHAR(50)
	, [BookGroupName] VARCHAR(50)
	, [Year] INT NULL
	, [Month] INT NULL
	, [BuyVolumePeak] FLOAT NULL
	, [BuyVolumeOffpeak] FLOAT NULL
	, [SellVolumePeak] FLOAT NULL
	, [SellVolumeOffpeak] FLOAT NULL
	, [BuyPricePeak] FLOAT NULL
	, [BuyPriceOffpeak] FLOAT NULL
	, [SellPricePeak] FLOAT NULL
	, [SellPriceOffpeak] FLOAT NULL
	, [BidPricePeak] FLOAT NULL
	, [BidPriceOffpeak] FLOAT NULL
	, [OfferPricePeak] FLOAT NULL
	, [OfferPriceOffpeak] FLOAT NULL
	, [PositionPeak] FLOAT NULL
	, [PositionOffpeak] FLOAT NULL
	, [LockedInPLPeak] FLOAT NULL
	, [LockedInPLOffpeak] FLOAT NULL
	, [FloatingPLPeak] FLOAT NULL
	, [FloatingPLOffpeak] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F715D0253] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_Total_Results]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [totalpl_thisyear] FLOAT NULL
	, [hedgepercent_thisyear] FLOAT NULL
	, [totalpl_nextyear] FLOAT NULL
	, [hedgepercent_nextyear] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F01466C6E] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_Total_This_Year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [plmtltotal] FLOAT NULL
	, [plmtlenergy] FLOAT NULL
	, [plmtlyekdem] FLOAT NULL
	, [salesmwtotal] FLOAT NULL
	, [salesmwpeak] FLOAT NULL
	, [salesmwoffpeak] FLOAT NULL
	, [hedgemwtotal] FLOAT NULL
	, [hedgemwpeak] FLOAT NULL
	, [hedgemwoffpeak] FLOAT NULL
	, [openpositionmwbaseload] FLOAT NULL
	, [openpositionmwpeak] FLOAT NULL
	, [openpositionmwpeakoffpeak] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F81792D0D] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_Total_Next_Year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [plmtltotal] FLOAT NULL
	, [plmtlenergy] FLOAT NULL
	, [plmtlyekdem] FLOAT NULL
	, [salesmwtotal] FLOAT NULL
	, [salesmwpeak] FLOAT NULL
	, [salesmwoffpeak] FLOAT NULL
	, [hedgemwtotal] FLOAT NULL
	, [hedgemwpeak] FLOAT NULL
	, [hedgemwoffpeak] FLOAT NULL
	, [openpositionmwbaseload] FLOAT NULL
	, [openpositionmwpeak] FLOAT NULL
	, [openpositionmwpeakoffpeak] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F450245C9] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_b2b_this_year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [energy_pl_mtl] FLOAT NULL
	, [yekdem_pl_mtl_unit_cost] FLOAT NULL
	, [yekdem_pl_mtl_dap] FLOAT NULL
	, [yekdem_pl_mtl_fx] FLOAT NULL
	, [yekdem_pl_mtl_total] FLOAT NULL
	, [sales_mw_total] FLOAT NULL
	, [hedge_mw_total] FLOAT NULL
	, [open_pos_mw_baseload] FLOAT NULL
	, [tl_mwh_buy_price] FLOAT NULL
	, [tl_mwh_sell_price] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83FE0F69392] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_b2b_next_year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [energy_pl_mtl] FLOAT NULL
	, [yekdem_pl_mtl_unit_cost] FLOAT NULL
	, [yekdem_pl_mtl_dap] FLOAT NULL
	, [yekdem_pl_mtl_fx] FLOAT NULL
	, [yekdem_pl_mtl_total] FLOAT NULL
	, [sales_mw_total] FLOAT NULL
	, [hedge_mw_total] FLOAT NULL
	, [open_pos_mw_baseload] FLOAT NULL
	, [tl_mwh_buy_price] FLOAT NULL
	, [tl_mwh_sell_price] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83FC7AAB11B] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_fx_this_year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [demand_m_usd_total] FLOAT NULL
	, [demand_m_usd_b2b] FLOAT NULL
	, [demand_m_usd_mass] FLOAT NULL
	, [hedge_m_usd_total] FLOAT NULL
	, [hedge_m_usd_b2b] FLOAT NULL
	, [hedge_m_usd_mass] FLOAT NULL
	, [open_m_usd_total] FLOAT NULL
	, [open_m_usd_b2b] FLOAT NULL
	, [open_m_usd_mass] FLOAT NULL
	, [hedge_pl_mtl_b2b] FLOAT NULL
	, [hedge_pl_mtl_mass] FLOAT NULL
	, [dollar_based_sourcing_musd] FLOAT NULL
	, [dollar_based_sourcing_fx_rate] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F1A3A7364] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_fx_next_year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [demand_m_usd_total] FLOAT NULL
	, [demand_m_usd_b2b] FLOAT NULL
	, [demand_m_usd_mass] FLOAT NULL
	, [hedge_m_usd_total] FLOAT NULL
	, [hedge_m_usd_b2b] FLOAT NULL
	, [hedge_m_usd_mass] FLOAT NULL
	, [open_m_usd_total] FLOAT NULL
	, [open_m_usd_b2b] FLOAT NULL
	, [mass_m_usd_b2b] FLOAT NULL
	, [hedge_pl_mtl_b2b] FLOAT NULL
	, [hedge_pl_mtl_mass] FLOAT NULL
	, [dollar_based_sourcing_musd] FLOAT NULL
	, [dollar_based_sourcing_fx_rate] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F5DF390C3] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_mass_this_year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [energy_pl_mtl_energy] FLOAT NULL
	, [yekdem_pl_mtl_unitcost] FLOAT NULL
	, [yekdem_pl_mtl_dap] FLOAT NULL
	, [yekdem_pl_mtl_fx] FLOAT NULL
	, [yekdem_pl_mtl_total] FLOAT NULL
	, [sales_mw_total] FLOAT NULL
	, [hedge_mw_total] FLOAT NULL
	, [open_pos_mw_baseload] FLOAT NULL
	, [tl_mwh_buy_price] FLOAT NULL
	, [tl_mwh_sell_price] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F30F1054F] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_mass_next_year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [energy_pl_mtl_energy] FLOAT NULL
	, [yekdem_pl_mtl_unitcost] FLOAT NULL
	, [yekdem_pl_mtl_dap] FLOAT NULL
	, [yekdem_pl_mtl_fx] FLOAT NULL
	, [yekdem_pl_mtl_total] FLOAT NULL
	, [sales_mw_total] FLOAT NULL
	, [hedge_mw_total] FLOAT NULL
	, [open_pos_mw_baseload] FLOAT NULL
	, [tl_mwh_buy_price] FLOAT NULL
	, [tl_mwh_sell_price] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F2CF63BC4] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_b2b_op_this_year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [peak] FLOAT NULL
	, [offpeak] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F3D9E2981] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_mass_op_this_year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [peak] FLOAT NULL
	, [offpeak] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83FD200A37E] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_b2b_op_next_year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [peak] FLOAT NULL
	, [offpeak] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83FC461DB8E] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_mass_op_next_year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [peak] FLOAT NULL
	, [offpeak] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F86341502] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_fx_op_this_year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [openb2b] FLOAT NULL
	, [openmass] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F0477978D] PRIMARY KEY ([id] ASC)
)
CREATE TABLE [tbl_PPR_fx_op_next_year]
(
	  [id] INT NOT NULL
	, [snapshot_date] DATE NULL
	, [openb2b] FLOAT NULL
	, [openmass] FLOAT NULL
	, CONSTRAINT [PK__tbl_PPR___3213E83F9ACB75E6] PRIMARY KEY ([id] ASC)
)
