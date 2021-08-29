
for sector in sector_dict.keys():
    industry_df = pd.DataFrame()
    date_df = pd.DataFrame({'Date':dates})
    date_df['Industry'] = sector
    for date in tqdm(dates):
        date_df_all = financials_factor[financials_factor['Date']==date]
        sector_df = date_df_all[date_df_all.Ticker.isin(sector_dict[sector])]
        sector_df_mc = sector_df.market_cap.sum()
        columns = ['current_ratio', 'quick_ratio', 'cfo_ratio', 'receivable_to',
         'inventory_to', 'asset_to', 'ROE', 'ROA', 'net_margin', 'gross_margin',
         'op_margin', 'eps', 'payout', 'debt_asset', 'debt_equity',
         'financial_leverage', 'adj_close_price', 'div_yield', 'PER', 'BM',
         'PSR', 'P_FCF', 'ev_ebitda']

        sector_df['weight'] = sector_df['market_cap']/sector_df_mc
        daily_sum = sector_df[columns].apply(lambda x: x*sector_df['weight']).sum().to_frame().T
        daily_sum['Date'] = date
        industry_df = pd.concat([industry_df,daily_sum])