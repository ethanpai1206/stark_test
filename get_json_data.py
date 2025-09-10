import requests
import json

def fetch_json_from_url(url):
    """
    從指定的 URL 獲取 JSON 資料
    
    Args:
        url (str): 要獲取 JSON 的 URL
    
    Returns:
        dict: 解析後的 JSON 資料
    """
    try:
        # 發送 GET 請求
        response = requests.get(url)
        
        # 檢查請求是否成功
        response.raise_for_status()
        
        # 解析 JSON 資料
        json_data = response.json()
        
        return json_data
        
    except requests.exceptions.RequestException as e:
        print(f"請求失敗: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON 解析失敗: {e}")
        return None

# 你的 Notion URL
notion_url = "https://file.notion.so/f/f/d70b900c-92f2-4d32-870b-1fa0d80e953b/2cc1982f-a835-4d84-9002-318758475632/output_clean_date_technical.json?table=block&id=f447ef6f-695d-45bb-9e49-f6a9c2e5ddd0&spaceId=d70b900c-92f2-4d32-870b-1fa0d80e953b&expirationTimestamp=1757541600000&signature=tMAguhh67Khr95BrN0qd39SPDMimj3gtXdsbwGQNwmA&downloadName=output_clean_date_technical.json"

# 獲取 JSON 資料
data = fetch_json_from_url(notion_url)

if data:
    print("成功獲取 JSON 資料!")
    
    # 顯示 JSON 結構
    print("\nJSON 結構:")
    for key in data.keys():
        print(f"- {key}: {type(data[key])}")
        if isinstance(data[key], list):
            print(f"  長度: {len(data[key])}")
        elif isinstance(data[key], dict):
            print(f"  鍵數量: {len(data[key])}")
    
    # 美化輸出整個 JSON (可選，如果資料很大可能會很長)
    print("\n完整 JSON 資料 (前1000個字符):")
    json_str = json.dumps(data, indent=2, ensure_ascii=False)
    print(json_str[:1000] + "..." if len(json_str) > 1000 else json_str)
    
    # 如果你想要保存到本地檔案
    with open('output_data.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print("\n資料已保存到 output_data.json")
    
else:
    print("無法獲取 JSON 資料")