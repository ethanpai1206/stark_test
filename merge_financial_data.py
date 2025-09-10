#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票歷史價格與財務數據合併腳本
支援選擇性合併多個財務數據表到historicalPriceFull.csv（每日數據）
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

def merge_quarterly_data_to_historical(historical_df, quarterly_df, quarterly_name):
    """
    將季度數據合併到歷史數據中
    
    Args:
        historical_df: 歷史價格數據DataFrame
        quarterly_df: 季度財務數據DataFrame
        quarterly_name: 季度數據的名稱（用於顯示）
    
    Returns:
        合併後的DataFrame
    """
    print(f"正在合併 {quarterly_name} 數據...")
    
    # 獲取季度數據的所有列（除了date和symbol）
    quarterly_cols = [col for col in quarterly_df.columns if col not in ['date', 'symbol']]
    
    # 批量添加新列，避免逐一添加導致的性能問題
    new_cols = [col for col in quarterly_cols if col not in historical_df.columns]
    if new_cols:
        # 創建包含新列的DataFrame並一次性合併
        new_cols_df = pd.DataFrame(index=historical_df.index, columns=new_cols)
        new_cols_df[:] = np.nan
        historical_df = pd.concat([historical_df, new_cols_df], axis=1)
    
    # 按symbol分組處理
    for symbol in historical_df['symbol'].unique():
        if symbol not in quarterly_df['symbol'].values:
            continue
            
        print(f"  處理股票 {symbol} 的 {quarterly_name} 數據...")
        
        # 獲取該股票的歷史數據和季度數據
        hist_symbol_idx = historical_df['symbol'] == symbol
        quarterly_symbol = quarterly_df[quarterly_df['symbol'] == symbol].copy()
        
        # 為每一行歷史數據找到對應的季度數據
        for idx in historical_df[hist_symbol_idx].index:
            hist_date = historical_df.loc[idx, 'date']
            
            # 找到包含這個日期的季度數據
            for _, quarterly_row in quarterly_symbol.iterrows():
                year = quarterly_row['calendarYear']
                quarter = quarterly_row['period']
                
                # 計算該季度的日期範圍
                try:
                    quarter_start, quarter_end = get_quarter_date_range(year, quarter)
                    
                    # 檢查歷史日期是否在該季度範圍內
                    if quarter_start <= hist_date <= quarter_end:
                        # 將季度數據複製到結果DataFrame
                        for col in quarterly_cols:
                            historical_df.loc[idx, col] = quarterly_row[col]
                        break
                        
                except ValueError as e:
                    print(f"    警告: {e} - 跳過該記錄")
                    continue
    
    return historical_df

def merge_selected_data(selected_tables, historical_file='data/historicalPriceFull.csv'):
    """
    合併用戶選擇的數據表
    
    Args:
        selected_tables: 選擇的表格列表，每個元素為 (文件路徑, 表格名稱)
        historical_file: 歷史價格數據文件路徑
    
    Returns:
        合併後的DataFrame
    """
    
    # 讀取歷史價格數據
    print("正在讀取歷史價格數據...")
    result_df = pd.read_csv(historical_file)
    result_df['date'] = pd.to_datetime(result_df['date'])
    
    print(f"歷史數據維度: {result_df.shape}")
    
    # 逐個合併選擇的表格
    for file_path, table_name in selected_tables:
        print(f"\n=== 開始合併 {table_name} ===")
        
        try:
            # 讀取季度數據
            quarterly_df = pd.read_csv(file_path)
            quarterly_df['date'] = pd.to_datetime(quarterly_df['date'])
            
            print(f"{table_name} 數據維度: {quarterly_df.shape}")
            
            # 合併數據
            result_df = merge_quarterly_data_to_historical(result_df, quarterly_df, table_name)
            
            print(f"{table_name} 合併完成")
            
        except FileNotFoundError:
            print(f"警告: 找不到文件 {file_path}，跳過 {table_name}")
        except Exception as e:
            print(f"合併 {table_name} 時發生錯誤: {e}")
    
    return result_df

def main():
    """
    主函數
    """
    # 定義可用的數據表
    available_tables = {
        '1': ('data/financialGrowth.csv', '財務成長數據'),
        '2': ('data/ratios.csv', '財務比率數據'),
        '3': ('data/cashFlowStatementGrowth.csv', '現金流量表成長數據'),
        '4': ('data/incomeStatementGrowth.csv', '損益表成長數據'),
        # 未來可以在這裡新增更多表格
        # '5': ('data/newTable.csv', '新的財務數據'),
    }
    
    print("=== 股票數據合併工具 ===")
    print("可用的數據表:")
    for key, (_, name) in available_tables.items():
        print(f"{key}. {name}")
    
    print("\n請選擇要合併的數據表（可多選）:")
    print("例如: 輸入 '1,2' 表示同時合併財務成長數據和財務比率數據")
    print("或輸入 '1' 表示只合併財務成長數據")
    
    user_input = input("\n請輸入選擇（用逗號分隔）: ").strip()
    
    if not user_input:
        print("未選擇任何數據表，程式結束")
        return
    
    try:
        # 解析用戶輸入
        selected_keys = [key.strip() for key in user_input.split(',')]
        selected_tables = []
        
        for key in selected_keys:
            if key in available_tables:
                selected_tables.append(available_tables[key])
            else:
                print(f"警告: 無效的選擇 '{key}'，已忽略")
        
        if not selected_tables:
            print("沒有有效的選擇，程式結束")
            return
        
        print(f"\n將合併以下數據表:")
        for _, name in selected_tables:
            print(f"- {name}")
        
        # 生成輸出文件名
        table_names = [name.replace('數據', '') for _, name in selected_tables]
        output_file = f"merged_{'_'.join(table_names)}_data.csv"
        
        print(f"\n開始合併數據...")
        
        # 執行合併
        result_df = merge_selected_data(selected_tables)
        
        # 保存結果
        print(f"\n正在保存合併後的數據到: {output_file}")
        result_df.to_csv(output_file, index=False)
        
        # 顯示統計信息
        print(f"\n=== 合併完成! ===")
        print(f"最終數據維度: {result_df.shape}")
        print(f"輸出文件: {output_file}")
        
        # 顯示數據覆蓋率統計
        historical_cols = ['date', 'symbol', 'open', 'high', 'low', 'close', 'adjClose', 
                          'volume', 'unadjustedVolume', 'change', 'changePercent', 
                          'vwap', 'label', 'changeOverTime']
        financial_cols = [col for col in result_df.columns if col not in historical_cols]
        
        if financial_cols:
            total_rows = len(result_df)
            rows_with_financial = len(result_df.dropna(subset=financial_cols, how='all'))
            print(f"總行數: {total_rows}")
            print(f"包含財務數據的行數: {rows_with_financial}")
            print(f"財務數據覆蓋率: {rows_with_financial/total_rows*100:.2f}%")
        
    except Exception as e:
        print(f"執行過程中發生錯誤: {e}")

if __name__ == "__main__":
    main()