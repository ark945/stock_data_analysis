import pandas as pd
import numpy as np

def run_comparison():
    p1 = r'D:\MyProject\stock_data_analysis\output\api_absr1_2026-08-26_2026-08-26.parquet'
    p2 = r'D:\MyProject\stock_data_analysis\output\finmind_2026-08-26.parquet'

    df1 = pd.read_parquet(p1)
    df2 = pd.read_parquet(p2)

    print('=' * 60)
    print('【1. 基本結構與資料維度比較】')
    print('=' * 60)
    print(f'API (ABSR1) 檔案形狀: {df1.shape} (行數: {len(df1):,}, 欄位數: {df1.shape[1]})')
    print(f'API 欄位清單: {df1.columns.tolist()}')
    print('-' * 40)
    print(f'FinMind 檔案形狀: {df2.shape} (行數: {len(df2):,}, 欄位數: {df2.shape[1]})')
    print(f'FinMind 欄位清單: {df2.columns.tolist()}')

    # FinMind 是逐筆分價資料 (by price)，需要 aggregate 成 (stock_id, securities_trader_id)
    df2['buy_amt'] = df2['buy'] * df2['price']
    df2['sell_amt'] = df2['sell'] * df2['price']

    fin_agg = df2.groupby(['stock_id', 'securities_trader_id', 'date']).agg(
        fin_buy_vol=('buy', 'sum'),
        fin_sell_vol=('sell', 'sum'),
        fin_buy_amt=('buy_amt', 'sum'),
        fin_sell_amt=('sell_amt', 'sum'),
        price_levels=('price', 'count')
    ).reset_index()

    fin_agg['fin_buy_avg_price'] = np.where(fin_agg['fin_buy_vol'] > 0, fin_agg['fin_buy_amt'] / fin_agg['fin_buy_vol'], 0.0)
    fin_agg['fin_sell_avg_price'] = np.where(fin_agg['fin_sell_vol'] > 0, fin_agg['fin_sell_amt'] / fin_agg['fin_sell_vol'], 0.0)

    print('\n' + '=' * 60)
    print('【2. 標的 (Symbol / Stock ID) 涵蓋比較】')
    print('=' * 60)
    sym1 = set(df1['symbol'].unique())
    sym2 = set(df2['stock_id'].unique())
    print(f'API 標的檔數: {len(sym1):,}')
    print(f'FinMind 標的檔數: {len(sym2):,}')
    print(f'兩者共有標的檔數: {len(sym1 & sym2):,}')
    print(f'僅在 API 出現的標的檔數: {len(sym1 - sym2):,}')
    if len(sym1 - sym2) > 0:
        print(f'  範例: {list(sym1 - sym2)[:10]}')
    print(f'僅在 FinMind 出現的標的檔數: {len(sym2 - sym1):,}')
    if len(sym2 - sym1) > 0:
        print(f'  範例: {list(sym2 - sym1)[:10]}')

    print('\n' + '=' * 60)
    print('【3. 券商分點 (Broker ID) 涵蓋比較】')
    print('=' * 60)
    brk1 = set(df1['broker_id'].unique())
    brk2 = set(df2['securities_trader_id'].unique())
    print(f'API 券商分點數: {len(brk1):,}')
    print(f'FinMind 券商分點數: {len(brk2):,}')
    print(f'兩者共有券商分點數: {len(brk1 & brk2):,}')
    print(f'僅在 API 出現的分點數: {len(brk1 - brk2):,}')
    if len(brk1 - brk2) > 0:
        print(f'  範例: {list(brk1 - brk2)[:10]}')
    print(f'僅在 FinMind 出現的分點數: {len(brk2 - brk1):,}')
    if len(brk2 - brk1) > 0:
        print(f'  範例: {list(brk2 - brk1)[:10]}')

    print('\n' + '=' * 60)
    print('【4. 同一標的 + 分點交易紀錄 (Symbol, Broker ID) 比對】')
    print('=' * 60)
    merged = pd.merge(
        df1,
        fin_agg,
        left_on=['symbol', 'broker_id'],
        right_on=['stock_id', 'securities_trader_id'],
        how='outer',
        indicator=True
    )
    print('交集狀態統計:')
    print(merged['_merge'].value_counts())

    both = merged[merged['_merge'] == 'both'].copy()
    both['buy_vol_diff'] = both['buy_vol'] - both['fin_buy_vol']
    both['sell_vol_diff'] = both['sell_vol'] - both['fin_sell_vol']
    both['buy_price_diff'] = both['buy_avg_price'] - both['fin_buy_avg_price']
    both['sell_price_diff'] = both['sell_avg_price'] - both['fin_sell_avg_price']

    total_both = len(both)
    print(f'\n在雙方皆有紀錄的 {total_both:,} 筆 (標的, 券商分點) 中：')
    print(f'1. 買進股數 (buy_vol) 完全一致: {(both["buy_vol_diff"] == 0).sum():,} 筆 ({(both["buy_vol_diff"] == 0).mean():.4%})')
    print(f'2. 賣出股數 (sell_vol) 完全一致: {(both["sell_vol_diff"] == 0).sum():,} 筆 ({(both["sell_vol_diff"] == 0).mean():.4%})')
    print(f'3. 買進均價 (buy_avg_price) 絕對誤差 < 0.001: {(both["buy_price_diff"].abs() < 0.001).sum():,} 筆 ({(both["buy_price_diff"].abs() < 0.001).mean():.4%})')
    print(f'4. 賣出均價 (sell_avg_price) 絕對誤差 < 0.001: {(both["sell_price_diff"].abs() < 0.001).sum():,} 筆 ({(both["sell_price_diff"].abs() < 0.001).mean():.4%})')

    # 若有不一致，檢查統計
    diff_buy = both[both['buy_vol_diff'] != 0]
    diff_sell = both[both['sell_vol_diff'] != 0]
    if len(diff_buy) > 0:
        print(f'\n買進股數不一致筆數: {len(diff_buy):,}')
        print(diff_buy[['symbol', 'broker_id', 'buy_vol', 'fin_buy_vol', 'buy_vol_diff']].head(5))
    if len(diff_sell) > 0:
        print(f'\n賣出股數不一致筆數: {len(diff_sell):,}')
        print(diff_sell[['symbol', 'broker_id', 'sell_vol', 'fin_sell_vol', 'sell_vol_diff']].head(5))

    # 檢查總成交股數與金額對比
    print('\n' + '=' * 60)
    print('【5. 全市場總量比較 (Total Market Volume & Amount)】')
    print('=' * 60)
    print(f'API 總買進股數: {df1["buy_vol"].sum():,.0f} 股')
    print(f'FinMind 總買進股數: {df2["buy"].sum():,.0f} 股')
    print(f'總買進股數差異 (API - FinMind): {df1["buy_vol"].sum() - df2["buy"].sum():,.0f} 股')
    print(f'API 總賣出股數: {df1["sell_vol"].sum():,.0f} 股')
    print(f'FinMind 總賣出股數: {df2["sell"].sum():,.0f} 股')
    print(f'總賣出股數差異 (API - FinMind): {df1["sell_vol"].sum() - df2["sell"].sum():,.0f} 股')

if __name__ == '__main__':
    run_comparison()
