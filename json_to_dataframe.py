import json
import sys
import os
import pandas as pd


def process_historical_price_full(json_file_path):
    """
    將 JSON 檔案中的 historicalPriceFull 數據轉換為 DataFrame
    
    Args:
        json_file_path (str): JSON 檔案路徑
    
    Returns:
        pd.DataFrame: 轉換後的 DataFrame
    """
    
    # 讀取 JSON 檔案
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 取得歷史價格數據
    historical_data = data['historicalPriceFull']['historical']
    
    # 創建 DataFrame
    df_data = []
    
    for record in historical_data:
        row = {
            'date': record.get('date'),
            'symbol': record.get('symbol'),
            'open': record.get('open'),
            'high': record.get('high'),
            'low': record.get('low'),
            'close': record.get('close'),
            'adjClose': record.get('adjClose'),
            'volume': record.get('volume'),
            'unadjustedVolume': record.get('unadjustedVolume'),
            'change': record.get('change'),
            'changePercent': record.get('changePercent'),
            'vwap': record.get('vwap'),
            'label': record.get('label'),
            'changeOverTime': record.get('changeOverTime')
        }
        df_data.append(row)
    
    # 創建 DataFrame
    df = pd.DataFrame(df_data)
    
    # 將 date 欄位轉換為日期格式
    df['date'] = pd.to_datetime(df['date'])
    
    # 按日期排序（由舊到新）
    df = df.sort_values('date').reset_index(drop=True)
    
    return df

def process_financial_growth(json_file_path):
    """
    將 JSON 檔案中的 financialGrowth 數據轉換為 DataFrame
    
    Args:
        json_file_path (str): JSON 檔案路徑
    
    Returns:
        pd.DataFrame: 轉換後的 DataFrame
    """
    
    # 讀取 JSON 檔案
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 取得財務成長數據
    financial_growth_data = data['financialGrowth']
    
    # 創建 DataFrame
    df_data = []
    
    for record in financial_growth_data:
        row = {
            'date': record.get('date'),
            'symbol': record.get('symbol'),
            'calendarYear': record.get('calendarYear'),
            'period': record.get('period'),
            'revenueGrowth': record.get('revenueGrowth'),
            'grossProfitGrowth': record.get('grossProfitGrowth'),
            'ebitgrowth': record.get('ebitgrowth'),
            'operatingIncomeGrowth': record.get('operatingIncomeGrowth'),
            'netIncomeGrowth': record.get('netIncomeGrowth'),
            'epsgrowth': record.get('epsgrowth'),
            'epsdilutedGrowth': record.get('epsdilutedGrowth'),
            'weightedAverageSharesGrowth': record.get('weightedAverageSharesGrowth'),
            'weightedAverageSharesDilutedGrowth': record.get('weightedAverageSharesDilutedGrowth'),
            'dividendsperShareGrowth': record.get('dividendsperShareGrowth'),
            'operatingCashFlowGrowth': record.get('operatingCashFlowGrowth'),
            'freeCashFlowGrowth': record.get('freeCashFlowGrowth'),
            'tenYRevenueGrowthPerShare': record.get('tenYRevenueGrowthPerShare'),
            'fiveYRevenueGrowthPerShare': record.get('fiveYRevenueGrowthPerShare'),
            'threeYRevenueGrowthPerShare': record.get('threeYRevenueGrowthPerShare'),
            'tenYOperatingCFGrowthPerShare': record.get('tenYOperatingCFGrowthPerShare'),
            'fiveYOperatingCFGrowthPerShare': record.get('fiveYOperatingCFGrowthPerShare'),
            'threeYOperatingCFGrowthPerShare': record.get('threeYOperatingCFGrowthPerShare'),
            'tenYNetIncomeGrowthPerShare': record.get('tenYNetIncomeGrowthPerShare'),
            'fiveYNetIncomeGrowthPerShare': record.get('fiveYNetIncomeGrowthPerShare'),
            'threeYNetIncomeGrowthPerShare': record.get('threeYNetIncomeGrowthPerShare'),
            'tenYShareholdersEquityGrowthPerShare': record.get('tenYShareholdersEquityGrowthPerShare'),
            'fiveYShareholdersEquityGrowthPerShare': record.get('fiveYShareholdersEquityGrowthPerShare'),
            'threeYShareholdersEquityGrowthPerShare': record.get('threeYShareholdersEquityGrowthPerShare'),
            'tenYDividendperShareGrowthPerShare': record.get('tenYDividendperShareGrowthPerShare')
        }
        df_data.append(row)
    
    # 創建 DataFrame
    df = pd.DataFrame(df_data)
    
    # 將 date 欄位轉換為日期格式（如果存在）
    if 'date' in df.columns and df['date'].notna().any():
        df['date'] = pd.to_datetime(df['date'])
        # 按日期排序（由新到舊）
        df = df.sort_values('date', ascending=False).reset_index(drop=True)
    
    return df

def load_json_data(file_path: str) -> dict:
    """讀取 JSON 檔案並回傳字典"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def add_common_columns(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """加入共用欄位 symbol"""
    df['symbol'] = symbol
    return df

def save_dataframe(df: pd.DataFrame, filename: str, output_dir: str = "/home/ubuntu"):
    """儲存 DataFrame 為 CSV 檔案"""
    output_path = os.path.join(output_dir, filename)
    df.to_csv(output_path, index=False)
    print(f"已儲存為 CSV 檔案: {output_path}")

def process_all_data(json_file_path: str) -> dict:
    """處理所有 key 的數據，回傳包含所有 DataFrame 的字典"""
    data = load_json_data(json_file_path)
    result = {}
    
    # 處理 historicalPriceFull
    if 'historicalPriceFull' in data and data['historicalPriceFull']['historical']:
        df = process_historical_price_full(json_file_path)
        result['historicalPriceFull'] = df
    
    # 處理 financialGrowth
    if 'financialGrowth' in data and data['financialGrowth']:
        df = process_financial_growth(json_file_path)
        result['financialGrowth'] = df
    
    return result

def main():
    # 指定 JSON 檔案路徑
    json_file_path = '/home/ubuntu/workspace/stark_test/output_data.json'
    try:
        # 可選擇處理全部 key 或單一 key
        all_dataframes = process_all_data(json_file_path)
        
        # 取得 historicalPriceFull 的 DataFrame
        price_df = all_dataframes['historicalPriceFull'] if 'historicalPriceFull' in all_dataframes else None
        if price_df is not None:
            print("\n--- price_df 範例 ---")
            print(price_df.head())
            save_dataframe(price_df, f"historicalPriceFull.csv")
        
        # 取得 financialGrowth 的 DataFrame
        growth_df = all_dataframes['financialGrowth'] if 'financialGrowth' in all_dataframes else None
        if growth_df is not None:
            print("\n--- financialGrowth_df 範例 ---")
            print(growth_df.head())
            save_dataframe(growth_df, f"financialGrowth.csv")
        
        return all_dataframes
    except Exception as e:
        print(f"錯誤: {e}")
        return None

if __name__ == "__main__":
    df = main()