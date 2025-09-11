# 股票數據處理系統 (Stock Data Processing System)

這是一個完整的股票數據處理流水線，可以從指定的 URL 獲取 JSON 格式的股票數據，轉換成 CSV 格式，並提供靈活的財務數據合併功能，適合用於機器學習模型訓練和金融數據分析。

## 🚀 快速開始

### 1. 啟動 Docker Compose
```bash
docker compose up -d
```

### 2. 進入工作目錄
```bash
cd /home/ubuntu/workspace/stark_test
```

### 3. 啟動主程式
```bash
python main.py
```

## 📋 功能特點

### 🔄 完整的數據處理流水線
- **步驟 1**: 從指定 URL 獲取 JSON 數據
- **步驟 2**: 將 JSON 數據轉換成基礎 CSV 文件
- **步驟 3**: 提供靈活的數據合併選項

### 📊 支援的數據類型

#### 基礎數據
- `historicalPriceFull.csv` - 每日歷史股價數據

#### 財務數據 (季度數據)
- `financialGrowth.csv` - 財務成長數據
- `ratios.csv` - 財務比率數據
- `cashFlowStatementGrowth.csv` - 現金流量表成長數據
- `incomeStatementGrowth.csv` - 損益表成長數據
- `balanceSheetStatementGrowth.csv` - 資產負債表成長數據

#### 技術指標 (日資料)
- `tech5.csv` - 5日技術指標
- `tech20.csv` - 20日技術指標
- `tech60.csv` - 60日技術指標
- `tech252.csv` - 252日技術指標

## 🛠 系統架構

```
main.py                   # 主流程控制
├── get_json_data.py      # JSON 數據獲取模組
├── json_to_dataframe.py  # JSON 轉 CSV 轉換模組
└── merge_financial_data.py # 數據合併模組
```

## 📁 目錄結構

```
stark_test/
├── main.py                            # 主程式入口
├── get_json_data.py                   # JSON 數據獲取
├── json_to_dataframe.py               # 數據轉換
├── merge_financial_data.py            # 數據合併
├── output_data.json                   # 下載的原始 JSON 數據
├── data/                              # CSV 數據目錄
│   ├── historicalPriceFull.csv
│   ├── financialGrowth.csv
│   ├── ratios.csv
│   ├── tech5.csv
│   ├── tech20.csv
│   ├── tech60.csv
│   └── tech252.csv
└── merged_*.csv                       # 合併後的輸出文件
```

## 🔧 使用說明

### 主流程 (main.py)
一鍵完成所有步驟：
```bash
python main.py
```

### 個別模組使用

#### 1. 單獨獲取 JSON 數據
```bash
python get_json_data.py
```

#### 2. 單獨轉換 JSON 為 CSV
```bash
python json_to_dataframe.py
```

#### 3. 單獨進行數據合併
```bash
python merge_financial_data.py
```

## 📈 數據合併功能

### 合併方式
- **季度數據**: 填入對應季度內的所有交易日
- **日資料**: 基於日期直接匹配合併
- **合併順序**: 優先合併季度財務數據，然後合併日技術指標數據

### 選擇範例
- 財務數據選擇: `1,2` (財務成長 + 財務比率)
- 技術指標選擇: `d1,d2` (Tech5 + Tech20)
- 可同時選擇財務數據和技術指標

### 數據清理選項
- **保留所有數據**: 包含 NaN 值的完整數據集
- **移除 NaN**: 適合機器學習模型訓練的乾淨數據集

### 輸出格式
- 按日期由新到舊排序
- 自動生成描述性文件名
- 清理過的文件會加上 `_cleaned` 後綴

## 📊 數據覆蓋率統計

系統會自動計算並顯示：
- 財務數據覆蓋率
- 技術指標覆蓋率
- 數據清理前後的統計信息

## 🔍 範例輸出

### 合併後的文件命名範例
```
merged_財務成長_財務比率_Tech5技術指標_data.csv
merged_財務成長_財務比率_Tech5技術指標_data_cleaned.csv
```

### 統計信息範例
```
=== 合併完成! ===
最終數據維度: (1500, 120)
財務數據覆蓋率: 85.23% (1278/1500)
技術指標覆蓋率: 92.67% (1390/1500)
```

## ⚠️ 注意事項

1. **網絡連接**: 需要網絡連接以獲取遠程 JSON 數據
2. **磁碟空間**: 確保有足夠的磁碟空間儲存數據文件
3. **數據時效**: Notion URL 中的簽名可能會過期，需要更新
4. **依賴套件**: 需要 `pandas`, `requests`, `json` 等 Python 套件

## 🐛 疑難排解

### 常見問題
- **JSON 獲取失敗**: 檢查網絡連接和 URL 有效性
- **文件不存在**: 確保 `data/` 目錄存在且包含必要文件
- **記憶體不足**: 處理大量數據時可能需要更多記憶體

### 錯誤處理
- 系統包含完整的錯誤處理機制
- 每個步驟都會顯示詳細的進度和狀態信息
- 支援 Ctrl+C 中止執行

## 📄 授權

本項目用於教育和研究目的。

## 🤝 貢獻

歡迎提交 Issue 和 Pull Request 來改進這個系統。