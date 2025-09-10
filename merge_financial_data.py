#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票歷史價格與財務成長數據合併腳本
將financialGrowth.csv（季度數據）左並到historicalPriceFull.csv（每日數據）
"""

import pandas as pd
from datetime import datetime, timedelta
import numpy as np

def get_quarter_date_range(year, quarter):
    """
    根據年份和季度計算該季度的日期範圍
    Q1: 1月1日至3月31日
    Q2: 4月1日至6月30日
    Q3: 7月1日至9月30日
    Q4: 10月1日至12月31日
    """
    if quarter == 'Q1':
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 3, 31)
    elif quarter == 'Q2':
        start_date = datetime(year, 4, 1)
        end_date = datetime(year, 6, 30)
    elif quarter == 'Q3':
        start_date = datetime(year, 7, 1)
        end_date = datetime(year, 9, 30)
    elif quarter == 'Q4':
        start_date = datetime(year, 10, 1)
        end_date = datetime(year, 12, 31)
    else:
        raise ValueError(f"Invalid quarter: {quarter}")
    
    return start_date, end_date

def merge_financial_data(historical_file='data/historicalPriceFull.csv', 
                        financial_file='data/financialGrowth.csv',
                        output_file='merged_financial_data.csv'):
    """
    合併歷史價格數據和財務成長數據
    """
    
    # 讀取數據
    print("正在讀取數據文件...")
    historical_df = pd.read_csv(historical_file)
    financial_df = pd.read_csv(financial_file)
    
    # 轉換日期格式
    print("正在處理日期格式...")
    historical_df['date'] = pd.to_datetime(historical_df['date'])
    financial_df['date'] = pd.to_datetime(financial_df['date'])
    
    # 為每一行歷史數據添加對應的財務數據
    print("正在合併數據...")
    
    # 初始化結果DataFrame
    result_df = historical_df.copy()
    
    # 獲取financialGrowth的所有列（除了date和symbol）
    financial_cols = [col for col in financial_df.columns if col not in ['date', 'symbol']]
    
    # 為結果DataFrame添加財務數據列
    for col in financial_cols:
        result_df[col] = np.nan
    
    # 按symbol分組處理
    for symbol in historical_df['symbol'].unique():
        print(f"正在處理股票: {symbol}")
        
        # 獲取該股票的歷史數據和財務數據
        hist_symbol = historical_df[historical_df['symbol'] == symbol].copy()
        fin_symbol = financial_df[financial_df['symbol'] == symbol].copy()
        
        # 為每一行歷史數據找到對應的財務數據
        for idx, hist_row in hist_symbol.iterrows():
            hist_date = hist_row['date']
            
            # 找到包含這個日期的季度財務數據
            for _, fin_row in fin_symbol.iterrows():
                year = fin_row['calendarYear']
                quarter = fin_row['period']
                
                # 計算該季度的日期範圍
                try:
                    quarter_start, quarter_end = get_quarter_date_range(year, quarter)
                    
                    # 檢查歷史日期是否在該季度範圍內
                    if quarter_start <= hist_date <= quarter_end:
                        # 將財務數據複製到結果DataFrame
                        for col in financial_cols:
                            result_df.loc[idx, col] = fin_row[col]
                        break
                        
                except ValueError as e:
                    print(f"警告: {e} - 跳過該記錄")
                    continue
    
    # 保存結果
    print(f"正在保存合併後的數據到: {output_file}")
    result_df.to_csv(output_file, index=False)
    
    # 顯示統計信息
    total_rows = len(result_df)
    rows_with_financial = len(result_df.dropna(subset=financial_cols, how='all'))
    
    print(f"\n合併完成!")
    print(f"總行數: {total_rows}")
    print(f"包含財務數據的行數: {rows_with_financial}")
    print(f"財務數據覆蓋率: {rows_with_financial/total_rows*100:.2f}%")
    
    return result_df

def main():
    """
    主函數
    """
    try:
        # 執行合併
        merged_data = merge_financial_data()
        
        # 顯示數據基本信息
        print(f"\n數據維度: {merged_data.shape}")
        print(f"列名: {list(merged_data.columns)}")
        
    except FileNotFoundError as e:
        print(f"錯誤: 找不到文件 - {e}")
    except Exception as e:
        print(f"執行過程中發生錯誤: {e}")

if __name__ == "__main__":
    main()