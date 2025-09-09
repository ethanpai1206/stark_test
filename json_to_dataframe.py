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
    symbol = data['historicalPriceFull'].get('symbol', None)
    # 創建 DataFrame
    df_data = []
    
    for record in historical_data:
        row = {
            'date': record.get('date'),
            'symbol': symbol,
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

def process_ratios(json_file_path):
    """
    將 JSON 檔案中的 ratios 數據轉換為 DataFrame
    
    Args:
        json_file_path (str): JSON 檔案路徑
    
    Returns:
        pd.DataFrame: 轉換後的 DataFrame
    """
    
    # 讀取 JSON 檔案
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 取得比率數據
    ratios_data = data['ratios']
    
    # 創建 DataFrame
    df_data = []
    
    for record in ratios_data:
        row = {
            'date': record.get('date'),
            'symbol': record.get('symbol'),
            'calendarYear': record.get('calendarYear'),
            'period': record.get('period'),
            'currentRatio': record.get('currentRatio'),
            'quickRatio': record.get('quickRatio'),
            'cashRatio': record.get('cashRatio'),
            'daysOfSalesOutstanding': record.get('daysOfSalesOutstanding'),
            'daysOfInventoryOutstanding': record.get('daysOfInventoryOutstanding'),
            'operatingCycle': record.get('operatingCycle'),
            'daysOfPayablesOutstanding': record.get('daysOfPayablesOutstanding'),
            'cashConversionCycle': record.get('cashConversionCycle'),
            'grossProfitMargin': record.get('grossProfitMargin'),
            'operatingProfitMargin': record.get('operatingProfitMargin'),
            'pretaxProfitMargin': record.get('pretaxProfitMargin'),
            'netProfitMargin': record.get('netProfitMargin'),
            'effectiveTaxRate': record.get('effectiveTaxRate'),
            'returnOnAssets': record.get('returnOnAssets'),
            'returnOnEquity': record.get('returnOnEquity'),
            'returnOnCapitalEmployed': record.get('returnOnCapitalEmployed'),
            'netIncomePerEBT': record.get('netIncomePerEBT'),
            'ebtPerEbit': record.get('ebtPerEbit'),
            'ebitPerRevenue': record.get('ebitPerRevenue'),
            'debtRatio': record.get('debtRatio'),
            'debtEquityRatio': record.get('debtEquityRatio'),
            'longTermDebtToCapitalization': record.get('longTermDebtToCapitalization'),
            'totalDebtToCapitalization': record.get('totalDebtToCapitalization'),
            'interestCoverage': record.get('interestCoverage'),
            'cashFlowToDebtRatio': record.get('cashFlowToDebtRatio'),
            'companyEquityMultiplier': record.get('companyEquityMultiplier'),
            'receivablesTurnover': record.get('receivablesTurnover'),
            'payablesTurnover': record.get('payablesTurnover'),
            'inventoryTurnover': record.get('inventoryTurnover'),
            'fixedAssetTurnover': record.get('fixedAssetTurnover'),
            'assetTurnover': record.get('assetTurnover'),
            'operatingCashFlowPerShare': record.get('operatingCashFlowPerShare'),
            'freeCashFlowPerShare': record.get('freeCashFlowPerShare'),
            'cashPerShare': record.get('cashPerShare'),
            'payoutRatio': record.get('payoutRatio'),
            'operatingCashFlowSalesRatio': record.get('operatingCashFlowSalesRatio'),
            'freeCashFlowOperatingCashFlowRatio': record.get('freeCashFlowOperatingCashFlowRatio'),
            'cashFlowCoverageRatios': record.get('cashFlowCoverageRatios'),
            'shortTermCoverageRatios': record.get('shortTermCoverageRatios'),
            'capitalExpenditureCoverageRatio': record.get('capitalExpenditureCoverageRatio'),
            'dividendPaidAndCapexCoverageRatio': record.get('dividendPaidAndCapexCoverageRatio'),
            'dividendPayoutRatio': record.get('dividendPayoutRatio'),
            'priceBookValueRatio': record.get('priceBookValueRatio'),
            'priceToBookRatio': record.get('priceToBookRatio'),
            'priceToSalesRatio': record.get('priceToSalesRatio'),
            'priceEarningsRatio': record.get('priceEarningsRatio'),
            'priceToFreeCashFlowsRatio': record.get('priceToFreeCashFlowsRatio'),
            'priceToOperatingCashFlowsRatio': record.get('priceToOperatingCashFlowsRatio'),
            'priceCashFlowRatio': record.get('priceCashFlowRatio'),
            'priceEarningsToGrowthRatio': record.get('priceEarningsToGrowthRatio'),
            'priceSalesRatio': record.get('priceSalesRatio'),
            'dividendYield': record.get('dividendYield'),
            'enterpriseValueMultiple': record.get('enterpriseValueMultiple'),
            'priceFairValue': record.get('priceFairValue')
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

def process_cash_flow_growth(json_file_path):
    """
    將 JSON 檔案中的 cashFlowStatementGrowth 數據轉換為 DataFrame
    
    Args:
        json_file_path (str): JSON 檔案路徑
    
    Returns:
        pd.DataFrame: 轉換後的 DataFrame
    """
    
    # 讀取 JSON 檔案
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 取得現金流量成長數據
    cash_flow_growth_data = data['cashFlowStatementGrowth']
    
    # 創建 DataFrame
    df_data = []
    
    for record in cash_flow_growth_data:
        row = {
            'date': record.get('date'),
            'symbol': record.get('symbol'),
            'calendarYear': record.get('calendarYear'),
            'period': record.get('period'),
            'growthNetIncome': record.get('growthNetIncome'),
            'growthDepreciationAndAmortization': record.get('growthDepreciationAndAmortization'),
            'growthDeferredIncomeTax': record.get('growthDeferredIncomeTax'),
            'growthStockBasedCompensation': record.get('growthStockBasedCompensation'),
            'growthChangeInWorkingCapital': record.get('growthChangeInWorkingCapital'),
            'growthAccountsReceivables': record.get('growthAccountsReceivables'),
            'growthInventory': record.get('growthInventory'),
            'growthAccountsPayables': record.get('growthAccountsPayables'),
            'growthOtherWorkingCapital': record.get('growthOtherWorkingCapital'),
            'growthOtherNonCashItems': record.get('growthOtherNonCashItems'),
            'growthNetCashProvidedByOperatingActivites': record.get('growthNetCashProvidedByOperatingActivites'),
            'growthInvestmentsInPropertyPlantAndEquipment': record.get('growthInvestmentsInPropertyPlantAndEquipment'),
            'growthAcquisitionsNet': record.get('growthAcquisitionsNet'),
            'growthPurchasesOfInvestments': record.get('growthPurchasesOfInvestments'),
            'growthSalesMaturitiesOfInvestments': record.get('growthSalesMaturitiesOfInvestments'),
            'growthOtherInvestingActivites': record.get('growthOtherInvestingActivites'),
            'growthNetCashUsedForInvestingActivites': record.get('growthNetCashUsedForInvestingActivites'),
            'growthDebtRepayment': record.get('growthDebtRepayment'),
            'growthCommonStockIssued': record.get('growthCommonStockIssued'),
            'growthCommonStockRepurchased': record.get('growthCommonStockRepurchased'),
            'growthDividendsPaid': record.get('growthDividendsPaid'),
            'growthOtherFinancingActivites': record.get('growthOtherFinancingActivites'),
            'growthNetCashUsedProvidedByFinancingActivities': record.get('growthNetCashUsedProvidedByFinancingActivities'),
            'growthEffectOfForexChangesOnCash': record.get('growthEffectOfForexChangesOnCash'),
            'growthNetChangeInCash': record.get('growthNetChangeInCash'),
            'growthCashAtEndOfPeriod': record.get('growthCashAtEndOfPeriod'),
            'growthCashAtBeginningOfPeriod': record.get('growthCashAtBeginningOfPeriod'),
            'growthOperatingCashFlow': record.get('growthOperatingCashFlow'),
            'growthCapitalExpenditure': record.get('growthCapitalExpenditure'),
            'growthFreeCashFlow': record.get('growthFreeCashFlow')
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

def process_income_growth(json_file_path):
    """
    將 JSON 檔案中的 incomeStatementGrowth 數據轉換為 DataFrame
    
    Args:
        json_file_path (str): JSON 檔案路徑
    
    Returns:
        pd.DataFrame: 轉換後的 DataFrame
    """
    
    # 讀取 JSON 檔案
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 取得損益表成長數據
    income_growth_data = data['incomeStatementGrowth']
    
    # 創建 DataFrame
    df_data = []
    
    for record in income_growth_data:
        row = {
            'date': record.get('date'),
            'symbol': record.get('symbol'),
            'calendarYear': record.get('calendarYear'),
            'period': record.get('period'),
            'growthRevenue': record.get('growthRevenue'),
            'growthCostOfRevenue': record.get('growthCostOfRevenue'),
            'growthGrossProfit': record.get('growthGrossProfit'),
            'growthGrossProfitRatio': record.get('growthGrossProfitRatio'),
            'growthResearchAndDevelopmentExpenses': record.get('growthResearchAndDevelopmentExpenses'),
            'growthGeneralAndAdministrativeExpenses': record.get('growthGeneralAndAdministrativeExpenses'),
            'growthSellingAndMarketingExpenses': record.get('growthSellingAndMarketingExpenses'),
            'growthOtherExpenses': record.get('growthOtherExpenses'),
            'growthOperatingExpenses': record.get('growthOperatingExpenses'),
            'growthCostAndExpenses': record.get('growthCostAndExpenses'),
            'growthInterestExpense': record.get('growthInterestExpense'),
            'growthDepreciationAndAmortization': record.get('growthDepreciationAndAmortization'),
            'growthEBITDA': record.get('growthEBITDA'),
            'growthEBITDARatio': record.get('growthEBITDARatio'),
            'growthOperatingIncome': record.get('growthOperatingIncome'),
            'growthOperatingIncomeRatio': record.get('growthOperatingIncomeRatio'),
            'growthTotalOtherIncomeExpensesNet': record.get('growthTotalOtherIncomeExpensesNet'),
            'growthIncomeBeforeTax': record.get('growthIncomeBeforeTax'),
            'growthIncomeBeforeTaxRatio': record.get('growthIncomeBeforeTaxRatio'),
            'growthIncomeTaxExpense': record.get('growthIncomeTaxExpense'),
            'growthNetIncome': record.get('growthNetIncome'),
            'growthNetIncomeRatio': record.get('growthNetIncomeRatio'),
            'growthEPS': record.get('growthEPS'),
            'growthEPSDiluted': record.get('growthEPSDiluted'),
            'growthWeightedAverageShsOut': record.get('growthWeightedAverageShsOut'),
            'growthWeightedAverageShsOutDil': record.get('growthWeightedAverageShsOutDil')
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

def process_balance_sheet_growth(json_file_path):
    """
    將 JSON 檔案中的 balanceSheetStatementGrowth 數據轉換為 DataFrame
    
    Args:
        json_file_path (str): JSON 檔案路徑
    
    Returns:
        pd.DataFrame: 轉換後的 DataFrame
    """
    
    # 讀取 JSON 檔案
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 取得資產負債表成長數據
    balance_sheet_growth_data = data['balanceSheetStatementGrowth']
    
    # 創建 DataFrame
    df_data = []
    
    for record in balance_sheet_growth_data:
        row = {
            'date': record.get('date'),
            'symbol': record.get('symbol'),
            'calendarYear': record.get('calendarYear'),
            'period': record.get('period'),
            'growthCashAndCashEquivalents': record.get('growthCashAndCashEquivalents'),
            'growthShortTermInvestments': record.get('growthShortTermInvestments'),
            'growthCashAndShortTermInvestments': record.get('growthCashAndShortTermInvestments'),
            'growthNetReceivables': record.get('growthNetReceivables'),
            'growthInventory': record.get('growthInventory'),
            'growthOtherCurrentAssets': record.get('growthOtherCurrentAssets'),
            'growthTotalCurrentAssets': record.get('growthTotalCurrentAssets'),
            'growthPropertyPlantEquipmentNet': record.get('growthPropertyPlantEquipmentNet'),
            'growthGoodwill': record.get('growthGoodwill'),
            'growthIntangibleAssets': record.get('growthIntangibleAssets'),
            'growthGoodwillAndIntangibleAssets': record.get('growthGoodwillAndIntangibleAssets'),
            'growthLongTermInvestments': record.get('growthLongTermInvestments'),
            'growthTaxAssets': record.get('growthTaxAssets'),
            'growthOtherNonCurrentAssets': record.get('growthOtherNonCurrentAssets'),
            'growthTotalNonCurrentAssets': record.get('growthTotalNonCurrentAssets'),
            'growthOtherAssets': record.get('growthOtherAssets'),
            'growthTotalAssets': record.get('growthTotalAssets'),
            'growthAccountPayables': record.get('growthAccountPayables'),
            'growthShortTermDebt': record.get('growthShortTermDebt'),
            'growthTaxPayables': record.get('growthTaxPayables'),
            'growthDeferredRevenue': record.get('growthDeferredRevenue'),
            'growthOtherCurrentLiabilities': record.get('growthOtherCurrentLiabilities'),
            'growthTotalCurrentLiabilities': record.get('growthTotalCurrentLiabilities'),
            'growthLongTermDebt': record.get('growthLongTermDebt'),
            'growthDeferredRevenueNonCurrent': record.get('growthDeferredRevenueNonCurrent'),
            'growthDeferrredTaxLiabilitiesNonCurrent': record.get('growthDeferrredTaxLiabilitiesNonCurrent'),
            'growthOtherNonCurrentLiabilities': record.get('growthOtherNonCurrentLiabilities'),
            'growthTotalNonCurrentLiabilities': record.get('growthTotalNonCurrentLiabilities'),
            'growthOtherLiabilities': record.get('growthOtherLiabilities'),
            'growthTotalLiabilities': record.get('growthTotalLiabilities'),
            'growthCommonStock': record.get('growthCommonStock'),
            'growthRetainedEarnings': record.get('growthRetainedEarnings'),
            'growthAccumulatedOtherComprehensiveIncomeLoss': record.get('growthAccumulatedOtherComprehensiveIncomeLoss'),
            'growthOthertotalStockholdersEquity': record.get('growthOthertotalStockholdersEquity'),
            'growthTotalStockholdersEquity': record.get('growthTotalStockholdersEquity'),
            'growthTotalLiabilitiesAndStockholdersEquity': record.get('growthTotalLiabilitiesAndStockholdersEquity'),
            'growthTotalInvestments': record.get('growthTotalInvestments'),
            'growthTotalDebt': record.get('growthTotalDebt'),
            'growthNetDebt': record.get('growthNetDebt')
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

def process_tech5(json_file_path):
    """
    將 JSON 檔案中的 tech5 數據轉換為 DataFrame
    
    Args:
        json_file_path (str): JSON 檔案路徑
    
    Returns:
        pd.DataFrame: 轉換後的 DataFrame
    """
    
    # 讀取 JSON 檔案
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 取得 tech5 數據
    tech5_data = data['tech5']
    
    # 創建 DataFrame
    df_data = []
    
    for record in tech5_data:
        row = {
            'date': record.get('date'),
            'symbol': "1101.TW",
            'open': record.get('open'),
            'high': record.get('high'),
            'low': record.get('low'),
            'close': record.get('close'),
            'volume': record.get('volume'),
            'sma': record.get('sma'),
            'ema': record.get('ema'),
            'wma': record.get('wma'),
            'dema': record.get('dema'),
            'tema': record.get('tema'),
            'williams': record.get('williams'),
            'rsi': record.get('rsi'),
            'adx': record.get('adx'),
            'standardDeviation': record.get('standardDeviation')
        }
        df_data.append(row)
    
    # 創建 DataFrame
    df = pd.DataFrame(df_data)
    
    # 將 date 欄位轉換為日期格式（如果存在）
    if 'date' in df.columns and df['date'].notna().any():
        df['date'] = pd.to_datetime(df['date'])
        # 按日期排序（由舊到新）
        df = df.sort_values('date').reset_index(drop=True)
    
    return df

def process_tech20(json_file_path):
    """
    將 JSON 檔案中的 tech20 數據轉換為 DataFrame
    
    Args:
        json_file_path (str): JSON 檔案路徑
    
    Returns:
        pd.DataFrame: 轉換後的 DataFrame
    """
    
    # 讀取 JSON 檔案
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # 取得 tech20 數據
    tech20_data = data['tech20']
    
    # 創建 DataFrame
    df_data = []
    
    for record in tech20_data:
        row = {
            'date': record.get('date'),
            'symbol': "1101.TW",
            'open': record.get('open'),
            'high': record.get('high'),
            'low': record.get('low'),
            'close': record.get('close'),
            'volume': record.get('volume'),
            'sma': record.get('sma'),
            'ema': record.get('ema'),
            'wma': record.get('wma'),
            'dema': record.get('dema'),
            'tema': record.get('tema'),
            'williams': record.get('williams'),
            'rsi': record.get('rsi'),
            'adx': record.get('adx'),
            'standardDeviation': record.get('standardDeviation')
        }
        df_data.append(row)
    
    # 創建 DataFrame
    df = pd.DataFrame(df_data)
    
    # 將 date 欄位轉換為日期格式（如果存在）
    if 'date' in df.columns and df['date'].notna().any():
        df['date'] = pd.to_datetime(df['date'])
        # 按日期排序（由舊到新）
        df = df.sort_values('date').reset_index(drop=True)
    
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
    
    # 處理 ratios
    if 'ratios' in data and data['ratios']:
        df = process_ratios(json_file_path)
        result['ratios'] = df
    
    # 處理 cashFlowStatementGrowth
    if 'cashFlowStatementGrowth' in data and data['cashFlowStatementGrowth']:
        df = process_cash_flow_growth(json_file_path)
        result['cashFlowStatementGrowth'] = df
    
    # 處理 incomeStatementGrowth
    if 'incomeStatementGrowth' in data and data['incomeStatementGrowth']:
        df = process_income_growth(json_file_path)
        result['incomeStatementGrowth'] = df
    
    # 處理 balanceSheetStatementGrowth
    if 'balanceSheetStatementGrowth' in data and data['balanceSheetStatementGrowth']:
        df = process_balance_sheet_growth(json_file_path)
        result['balanceSheetStatementGrowth'] = df
    
    # 處理 tech5
    if 'tech5' in data and data['tech5']:
        df = process_tech5(json_file_path)
        result['tech5'] = df
    
    # 處理 tech20
    if 'tech20' in data and data['tech20']:
        df = process_tech20(json_file_path)
        result['tech20'] = df
    
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
        
        # 取得 ratios 的 DataFrame
        ratios_df = all_dataframes['ratios'] if 'ratios' in all_dataframes else None
        if ratios_df is not None:
            print("\n--- ratios_df 範例 ---")
            print(ratios_df.head())
            save_dataframe(ratios_df, f"ratios.csv")
        
        # 取得 cashFlowStatementGrowth 的 DataFrame
        cashflow_growth_df = all_dataframes['cashFlowStatementGrowth'] if 'cashFlowStatementGrowth' in all_dataframes else None
        if cashflow_growth_df is not None:
            print("\n--- cashFlowStatementGrowth_df 範例 ---")
            print(cashflow_growth_df.head())
            save_dataframe(cashflow_growth_df, f"cashFlowStatementGrowth.csv")
        
        # 取得 incomeStatementGrowth 的 DataFrame
        income_growth_df = all_dataframes['incomeStatementGrowth'] if 'incomeStatementGrowth' in all_dataframes else None
        if income_growth_df is not None:
            print("\n--- incomeStatementGrowth_df 範例 ---")
            print(income_growth_df.head())
            save_dataframe(income_growth_df, f"incomeStatementGrowth.csv")
        
        # 取得 balanceSheetStatementGrowth 的 DataFrame
        balance_sheet_growth_df = all_dataframes['balanceSheetStatementGrowth'] if 'balanceSheetStatementGrowth' in all_dataframes else None
        if balance_sheet_growth_df is not None:
            print("\n--- balanceSheetStatementGrowth_df 範例 ---")
            print(balance_sheet_growth_df.head())
            save_dataframe(balance_sheet_growth_df, f"balanceSheetStatementGrowth.csv")
        
        # 取得 tech5 的 DataFrame
        tech5_df = all_dataframes['tech5'] if 'tech5' in all_dataframes else None
        if tech5_df is not None:
            print("\n--- tech5_df 範例 ---")
            print(tech5_df.head())
            save_dataframe(tech5_df, f"tech5.csv")
        
        # 取得 tech20 的 DataFrame
        tech20_df = all_dataframes['tech20'] if 'tech20' in all_dataframes else None
        if tech20_df is not None:
            print("\n--- tech20_df 範例 ---")
            print(tech20_df.head())
            save_dataframe(tech20_df, f"tech20.csv")
        
        return all_dataframes
    except Exception as e:
        print(f"錯誤: {e}")
        return None

if __name__ == "__main__":
    df = main()