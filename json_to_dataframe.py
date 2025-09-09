import json
import sys
import os
import pandas as pd


def convert_json_to_dataframe(json_file_path, symbol="1101.TW"):
    """
    將 JSON 檔案中的 historicalPriceFull 數據轉換為 DataFrame
    
    Args:
        json_file_path (str): JSON 檔案路徑
        symbol (str): 股票代號，預設為 "1101.TW"
    
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
            'date': record['date'],
            'symbol': symbol,
            'open': record['open'],
            'high': record['high'],
            'low': record['low'],
            'close': record['close'],
            'adjClose': record['adjClose'],
            'volume': record['volume'],
            'unadjustedVolume': record['unadjustedVolume'],
            'change': record['change'],
            'changePercent': record['changePercent'],
            'vwap': record['vwap'],
            'label': record['label'],
            'changeOverTime': record['changeOverTime']
        }
        df_data.append(row)
    
    # 創建 DataFrame
    df = pd.DataFrame(df_data)
    
    # 將 date 欄位轉換為日期格式
    df['date'] = pd.to_datetime(df['date'])
    
    # 按日期排序（由舊到新）
    df = df.sort_values('date').reset_index(drop=True)
    
    return df

def main():
    # 指定 JSON 檔案路徑
    json_file_path = '/home/ubuntu/workspace/output_data.json'
    
    try:
        # 轉換為 DataFrame
        df = convert_json_to_dataframe(json_file_path)
        
        # 顯示基本資訊
        print(f"資料筆數: {len(df)}")
        print(f"日期範圍: {df['date'].min()} 到 {df['date'].max()}")
        print("\n前 5 筆資料:")
        print(df.head())
        
        # 顯示欄位資訊
        print("\n欄位資訊:")
        print(df.info())
        
        # 儲存為 CSV 檔案
        output_csv_path = '/home/ubuntu/historical_price_data.csv'
        df.to_csv(output_csv_path, index=False)
        print(f"\n已儲存為 CSV 檔案: {output_csv_path}")
        
        # 顯示範例資料格式
        print("\n範例資料 (前 3 筆):")
        for i in range(min(3, len(df))):
            row = df.iloc[i]
            print(f'"{row["date"].strftime("%Y-%m-%d")}", "{row["symbol"]}", {row["open"]}, {row["high"]}, {row["low"]}, {row["close"]}, {row["adjClose"]}, {row["volume"]}, {row["unadjustedVolume"]}, {row["change"]}, {row["changePercent"]}, {row["vwap"]}, "{row["label"]}", {row["changeOverTime"]}')
        
        return df
        
    except Exception as e:
        print(f"錯誤: {e}")
        return None

if __name__ == "__main__":
    df = main()