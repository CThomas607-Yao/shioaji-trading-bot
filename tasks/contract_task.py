def get_valid_contracts(api, targets):
    """
    將 TARGETS 配置轉換為有效的合約物件清單
    :param api: Shioaji API 物件
    :param targets: 包含 type 與 code 的字典清單
    """
    valid_contracts = []
    print("\n--- [Step: 合約驗證] 正在初始化合約物件 ---")
    
    for t in targets:
        t_type = t.get("type")
        t_code = t.get("code")
        contract = None
        
        try:
            if t_type == "Stock":
                contract = api.Contracts.Stocks[t_code]
            elif t_type == "Future":
                # 期貨需擷取前三碼 Symbol (例如 TXFR1 取 TXF)
                symbol = t_code[:3]
                contract = api.Contracts.Futures[symbol][t_code]
                
            if contract:
                valid_contracts.append(contract)
                print(f"✅ 成功載入合約: {t_code} ({t_type})")
            else:
                print(f"⚠️ 找不到合約，已跳過: {t_code}")
                
        except KeyError:
            print(f"⚠️ 無效標的或代碼錯誤: {t_code} ({t_type})")
        except Exception as e:
            print(f"❌ 獲取合約出錯 {t_code}: {e}")

    return valid_contracts