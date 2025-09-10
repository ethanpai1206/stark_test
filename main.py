#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票數據處理主流程
整合JSON數據獲取、轉換成CSV和合併財務數據的完整流程
"""

import os
import sys
import subprocess
from datetime import datetime

# 導入各個模組
from get_json_data import fetch_json_from_url
import json_to_dataframe
import merge_financial_data

def print_step_header(step_num, step_name):
    """打印步驟標題"""
    print("\n" + "="*60)
    print(f"步驟 {step_num}: {step_name}")
    print("="*60)

def check_file_exists(file_path):
    """檢查文件是否存在"""
    return os.path.exists(file_path)

def main():
    """
    主流程函數
    """
    print("🚀 股票數據處理主流程啟動")
    print(f"執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # ===== 步驟 1: 從指定網址獲取 JSON 數據 =====
        print_step_header(1, "從網址獲取 JSON 數據")
        
        # 你的 Notion URL（可以在這裡修改或從用戶輸入獲取）
        notion_url = "https://file.notion.so/f/f/d70b900c-92f2-4d32-870b-1fa0d80e953b/2cc1982f-a835-4d84-9002-318758475632/output_clean_date_technical.json?table=block&id=f447ef6f-695d-45bb-9e49-f6a9c2e5ddd0&spaceId=d70b900c-92f2-4d32-870b-1fa0d80e953b&expirationTimestamp=1757541600000&signature=tMAguhh67Khr95BrN0qd39SPDMimj3gtXdsbwGQNwmA&downloadName=output_clean_date_technical.json"
        
        # 詢問是否使用預設 URL 或輸入新的 URL
        use_default = input(f"是否使用預設的 Notion URL？(y/n，預設為 y): ").strip().lower()
        
        if use_default not in ['y', 'yes', '']:
            notion_url = input("請輸入 JSON 數據的 URL: ").strip()
        
        print(f"正在從 URL 獲取 JSON 數據...")
        print(f"URL: {notion_url[:100]}...")  # 只顯示前100個字符
        
        # 獲取 JSON 數據
        data = fetch_json_from_url(notion_url)
        
        if not data:
            print("❌ 無法獲取 JSON 數據，程式結束")
            return
        
        # 保存 JSON 數據到本地
        json_file_path = '/home/ubuntu/workspace/stark_test/output_data.json'
        with open(json_file_path, 'w', encoding='utf-8') as f:
            import json
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print("✅ JSON 數據獲取成功!")
        print(f"數據已保存到: {json_file_path}")
        
        # 顯示 JSON 結構
        print("\n📋 JSON 數據結構:")
        for key in data.keys():
            print(f"- {key}: {type(data[key])}")
            if isinstance(data[key], list):
                print(f"  長度: {len(data[key])}")
            elif isinstance(data[key], dict):
                print(f"  鍵數量: {len(data[key])}")
        
        # ===== 步驟 2: 將 JSON 轉換成基礎 CSV 文件 =====
        print_step_header(2, "將 JSON 轉換成基礎 CSV 文件")
        
        # 確保 data 目錄存在
        os.makedirs('/home/ubuntu/workspace/stark_test/data', exist_ok=True)
        
        print("正在處理所有數據並轉換為 CSV...")
        
        # 調用 json_to_dataframe 的 process_all_data 函數
        all_dataframes = json_to_dataframe.process_all_data(json_file_path)
        
        if not all_dataframes:
            print("❌ JSON 轉 CSV 失敗，程式結束")
            return
        
        # 保存所有 DataFrame 為 CSV 文件
        saved_files = []
        
        for key, df in all_dataframes.items():
            if df is not None and not df.empty:
                if key == 'historicalPriceFull':
                    file_path = f"data/{key}.csv"
                else:
                    file_path = f"data/{key}.csv"
                
                json_to_dataframe.save_dataframe(df, file_path)
                saved_files.append(file_path)
        
        print("✅ JSON 轉 CSV 完成!")
        print(f"已生成 {len(saved_files)} 個 CSV 文件:")
        for file in saved_files:
            print(f"  - {file}")
        
        # 檢查必要的文件是否存在
        required_file = 'data/historicalPriceFull.csv'
        if not check_file_exists(required_file):
            print(f"❌ 缺少必要文件: {required_file}")
            print("無法進行下一步合併，程式結束")
            return
        
        # ===== 步驟 3: 詢問合併需求並執行合併 =====
        print_step_header(3, "財務數據合併")
        
        print("即將啟動財務數據合併工具...")
        print("您可以選擇要合併的財務數據表和技術指標表")
        
        # 詢問是否要繼續進行合併
        continue_merge = input("\n是否要繼續進行數據合併？(y/n，預設為 y): ").strip().lower()
        
        if continue_merge in ['n', 'no']:
            print("✅ 數據處理完成，跳過合併步驟")
            print(f"您可以稍後手動執行: python merge_financial_data.py")
            return
        
        print("\n🔄 啟動數據合併工具...")
        
        # 調用 merge_financial_data 的主函數
        merge_financial_data.main()
        
        print("\n🎉 主流程執行完成!")
        print("所有步驟已成功完成:")
        print("  ✅ JSON 數據獲取")
        print("  ✅ 數據轉換為 CSV")
        print("  ✅ 財務數據合併")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  用戶中止程式執行")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 執行過程中發生錯誤: {e}")
        print("請檢查錯誤信息並重新執行")
        sys.exit(1)

if __name__ == "__main__":
    main()