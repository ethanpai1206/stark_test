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

def merge_daily_data_to_historical(historical_df, daily_df, daily_name):
    """
    將日資料合併到歷史數據中（基於日期直接匹配）
    
    Args:
        historical_df: 歷史價格數據DataFrame
        daily_df: 日技術指標數據DataFrame
        daily_name: 日資料的名稱（用於顯示）
    
    Returns:
        合併後的DataFrame
    """
    print(f"正在合併 {daily_name} 數據...")
    
    # 設定日期為索引以便進行合併
    historical_df = historical_df.set_index(['date', 'symbol'])
    daily_df = daily_df.set_index(['date', 'symbol'])
    
    # 獲取日資料的所有列（除了date和symbol）
    daily_cols = [col for col in daily_df.columns]
    
    print(f"  將合併 {len(daily_cols)} 個 {daily_name} 欄位")
    
    # 使用left join合併數據
    result_df = historical_df.join(daily_df, how='left')
    
    # 重設索引
    result_df = result_df.reset_index()
    
    # 計算數據覆蓋率
    non_null_count = result_df[daily_cols].notna().any(axis=1).sum()
    total_count = len(result_df)
    coverage = (non_null_count / total_count) * 100
    
    print(f"  {daily_name} 數據覆蓋率: {coverage:.2f}% ({non_null_count}/{total_count})")
    
    return result_df

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

def merge_selected_data(selected_tables, daily_tables=None, historical_file='data/historicalPriceFull.csv'):
    """
    合併用戶選擇的數據表
    
    Args:
        selected_tables: 選擇的季度表格列表，每個元素為 (文件路徑, 表格名稱)
        daily_tables: 選擇的日資料表格列表，每個元素為 (文件路徑, 表格名稱)
        historical_file: 歷史價格數據文件路徑
    
    Returns:
        合併後的DataFrame
    """
    
    # 讀取歷史價格數據
    print("正在讀取歷史價格數據...")
    result_df = pd.read_csv(historical_file)
    result_df['date'] = pd.to_datetime(result_df['date'])
    
    print(f"歷史數據維度: {result_df.shape}")
    
    # 首先合併季度表格
    for file_path, table_name in selected_tables:
        print(f"\n=== 開始合併 {table_name} (季度數據) ===")
        
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
    
    # 然後合併日資料表格
    if daily_tables:
        for file_path, table_name in daily_tables:
            print(f"\n=== 開始合併 {table_name} (日資料) ===")
            
            try:
                # 讀取日資料
                daily_df = pd.read_csv(file_path)
                daily_df['date'] = pd.to_datetime(daily_df['date'])
                
                print(f"{table_name} 數據維度: {daily_df.shape}")
                
                # 合併日資料
                result_df = merge_daily_data_to_historical(result_df, daily_df, table_name)
                
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
        '5': ('data/balanceSheetStatementGrowth.csv', '資產負債表成長數據'),
        # 未來可以在這裡新增更多表格
        # '6': ('data/newTable.csv', '新的財務數據'),
    }
    
    # 定義可用的日資料表
    daily_tables_available = {
        'd1': ('data/tech5.csv', 'Tech5技術指標數據'),
        'd2': ('data/tech20.csv', 'Tech20技術指標數據'),
        'd3': ('data/tech60.csv', 'Tech60技術指標數據'),
        'd4': ('data/tech252.csv', 'Tech252技術指標數據'),
        # 未來可以在這裡新增更多日資料表格
    }
    
    print("=== 股票數據合併工具 ===")
    print("\n📊 主表說明:")
    print("• 主表: data/historicalPriceFull.csv (每日歷史股價數據)")
    print("• 合併方式: 優先合併季度財務數據，然後合併日技術指標數據")
    print("• 季度數據會填入對應季度內的所有交易日")
    print("• 日資料數據基於日期直接匹配合併")
    
    print("📋 可用的財務數據表 (季度數據):")
    for key, (_, name) in available_tables.items():
        print(f"{key}. {name}")
    
    print("\n� 可用的技術指標表 (日資料):")
    for key, (_, name) in daily_tables_available.items():
        print(f"{key}. {name}")
    
    print("\n�🔧 使用說明:")
    print("• 財務數據: 請選擇要合併的財務數據表（可多選）")
    print("  例如: 輸入 '1,2' 表示同時合併財務成長數據和財務比率數據")
    print("• 技術指標: 請選擇要合併的技術指標表（可多選）")
    print("  例如: 輸入 'd1,d2' 表示同時合併Tech5和Tech20數據")
    print("• 可以同時選擇財務數據和技術指標")
    print("  例如: 財務選擇 '1,2'，技術指標選擇 'd1' 表示合併財務成長、財務比率和Tech5數據")
    
    # 選擇財務數據表
    financial_input = input("\n請選擇財務數據表（用逗號分隔，留空表示不選擇）: ").strip()
    
    # 選擇技術指標表
    daily_input = input("請選擇技術指標表（用逗號分隔，留空表示不選擇）: ").strip()
    
    # 檢查是否都為空
    if not financial_input and not daily_input:
        print("未選擇任何數據表，程式結束")
        return
    
    try:
        selected_tables = []
        selected_daily_tables = []
        
        # 解析財務數據選擇
        if financial_input:
            financial_keys = [key.strip() for key in financial_input.split(',')]
            for key in financial_keys:
                if key in available_tables:
                    selected_tables.append(available_tables[key])
                else:
                    print(f"警告: 無效的財務數據選擇 '{key}'，已忽略")
        
        # 解析技術指標選擇
        if daily_input:
            daily_keys = [key.strip() for key in daily_input.split(',')]
            for key in daily_keys:
                if key in daily_tables_available:
                    selected_daily_tables.append(daily_tables_available[key])
                else:
                    print(f"警告: 無效的技術指標選擇 '{key}'，已忽略")
        
        if not selected_tables and not selected_daily_tables:
            print("沒有有效的選擇，程式結束")
            return
        
        # 顯示將要合併的表格
        if selected_tables:
            print(f"\n將合併以下財務數據表:")
            for _, name in selected_tables:
                print(f"- {name}")
        
        if selected_daily_tables:
            print(f"\n將合併以下技術指標表:")
            for _, name in selected_daily_tables:
                print(f"- {name}")
        
        # 生成輸出文件名
        all_table_names = []
        if selected_tables:
            all_table_names.extend([name.replace('數據', '') for _, name in selected_tables])
        if selected_daily_tables:
            all_table_names.extend([name.replace('數據', '') for _, name in selected_daily_tables])
        
        output_file = f"merged_{'_'.join(all_table_names)}_data.csv"
        
        print(f"\n開始合併數據...")
        
        # 執行合併（包含日資料和季度數據）
        result_df = merge_selected_data(selected_tables, selected_daily_tables)
        
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
        
        # 分別統計財務數據和技術指標的覆蓋率
        if selected_tables:
            financial_cols = []
            for _, name in selected_tables:
                # 根據不同的財務數據表，添加相應的列
                if '財務成長' in name:
                    financial_cols.extend([col for col in result_df.columns if any(x in col.lower() for x in ['growth', 'revenue', 'income', 'profit'])])
                elif '比率' in name:
                    financial_cols.extend([col for col in result_df.columns if any(x in col.lower() for x in ['ratio', 'margin', 'return'])])
            
            financial_cols = list(set(financial_cols))  # 去重
            if financial_cols:
                total_rows = len(result_df)
                rows_with_financial = len(result_df.dropna(subset=financial_cols, how='all'))
                print(f"財務數據覆蓋率: {rows_with_financial/total_rows*100:.2f}% ({rows_with_financial}/{total_rows})")
        
        if selected_daily_tables:
            tech_cols = [col for col in result_df.columns if any(x in col.lower() for x in ['tech5', 'tech20', 'tech60', 'tech252', 'sma', 'ema', 'rsi'])]
            if tech_cols:
                total_rows = len(result_df)
                rows_with_tech = len(result_df.dropna(subset=tech_cols, how='all'))
                print(f"技術指標覆蓋率: {rows_with_tech/total_rows*100:.2f}% ({rows_with_tech}/{total_rows})")
        
    except Exception as e:
        print(f"執行過程中發生錯誤: {e}")

if __name__ == "__main__":
    main()