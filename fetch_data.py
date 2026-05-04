#!/usr/bin/env python3
"""
ETF 持股監控 - 每日資料抓取
資料來源：MoneyDJ（持股）+ TWSE MIS API（即時股價）
每次執行產出 data/YYYYMMDD.json 及 data/index.json
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import os
import re
from datetime import datetime
import pytz

TAIPEI_TZ = pytz.timezone("Asia/Taipei")

# 依規模排序的前14大主動式ETF
ETF_LIST = [
    ("00981A", "主動統一台股增長"),
    ("00982A", "主動群益台灣強棒"),
    ("00991A", "主動復華未來50"),
    ("00990A", "主動元大AI新經濟"),
    ("00992A", "主動群益科技創新"),
    ("00993A", "主動安聯台灣"),
    ("00988A", "主動統一全球創新"),
    ("00980A", "主動野村臺灣優選"),
    ("00985A", "主動野村台灣50"),
    ("00995A", "主動中信台灣卓越"),
    ("00984A", "主動安聯台灣高息"),
    ("00983A", "主動中信ARK創新"),
    ("00987A", "主動台新優勢成長"),
    ("00989A", "主動摩根美國科技"),
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8",
    "Referer": "https://www.moneydj.com/",
}


def fetch_moneydj_holdings(etf_code: str) -> list[dict]:
    """從 MoneyDJ 抓取 ETF 持股明細（basic0007 頁面）

    實際表格格式（class="datalist"）：
      欄1：個股名稱  → 如「台積電(2330.TW)」
      欄2：投資比例(%) → 如「8.99」（無%符號）
      欄3：持有股數  → 如「10,039,000.00」
    """
    url = (
        "https://www.moneydj.com/etf/x/basic/basic0007.xdjhtm"
        f"?etfid={etf_code.lower()}.tw"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "lxml")

        # 找 class="datalist" 且標題含「個股名稱」的表格
        target_table = None
        for table in soup.find_all("table", class_="datalist"):
            header = table.find("tr")
            if header and "個股名稱" in header.get_text():
                target_table = table
                break

        # 備用：找含「個股名稱」的任意表格
        if not target_table:
            for table in soup.find_all("table"):
                header = table.find("tr")
                if header and "個股名稱" in header.get_text():
                    target_table = table
                    break

        if not target_table:
            print(f"    ⚠  找不到持股表格")
            return []

        holdings = []
        rows = target_table.find_all("tr")

        for row in rows[1:]:  # 略過標題列
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 2:
                continue

            name_cell = cells[0]   # 如「台積電(2330.TW)」

            # 從括號中取出股票代號，格式：(XXXX.TW) 或 (XXXXXX.TW)
            code_match = re.search(r"\((\d{4,6})\.\w+\)", name_cell)
            if not code_match:
                continue
            code = code_match.group(1)

            # 取括號前面的股票名稱
            name = name_cell[: name_cell.rfind("(")].strip()

            # 投資比例：cells[1]，如「8.99」（無%）
            weight = 0.0
            try:
                weight = float(cells[1].replace(",", "").replace("%", "").strip())
            except ValueError:
                pass

            # 持有股數：cells[2]，如「10,039,000.00」
            shares = 0
            if len(cells) >= 3:
                try:
                    shares = int(float(cells[2].replace(",", "").strip()))
                except ValueError:
                    pass

            if weight > 0:
                holdings.append(
                    {
                        "code": code,
                        "name": name,
                        "shares": shares,
                        "lots": shares // 1000,
                        "weight": round(weight, 4),
                        "price": 0.0,
                        "value": 0.0,
                    }
                )

        return holdings

    except Exception as e:
        print(f"    ✗ MoneyDJ {etf_code}: {e}")
        return []


def fetch_stock_prices(codes: list[str]) -> dict[str, float]:
    """批次從 TWSE MIS API 取得收盤/即時股價"""
    prices: dict[str, float] = {}
    batch_size = 40

    for i in range(0, len(codes), batch_size):
        batch = codes[i : i + batch_size]
        ex_ch = "|".join(f"tse_{c}.tw" for c in batch)
        url = (
            "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
            f"?ex_ch={ex_ch}&json=1&delay=0"
        )
        try:
            r = requests.get(
                url,
                headers={"User-Agent": HEADERS["User-Agent"]},
                timeout=15,
            )
            data = r.json()
            for item in data.get("msgArray", []):
                c = item.get("c", "")
                # 'z'=最新成交, 'y'=昨收
                raw = item.get("z") or item.get("y", "")
                try:
                    if raw and raw != "-":
                        prices[c] = float(raw)
                except ValueError:
                    pass
        except Exception as e:
            print(f"    ✗ 股價批次 {i // batch_size + 1}: {e}")

        time.sleep(1.5)

    return prices


def main():
    now = datetime.now(TAIPEI_TZ)
    today_str = now.strftime("%Y%m%d")

    os.makedirs("data", exist_ok=True)
    out_path = f"data/{today_str}.json"

    print(f"=== ETF 持股抓取  {now.strftime('%Y-%m-%d %H:%M:%S %Z')} ===\n")

    all_etf_data: dict[str, dict] = {}
    all_codes: set[str] = set()

    # ── 1. 抓各 ETF 持股 ──────────────────────────────
    for etf_code, etf_name in ETF_LIST:
        print(f"  [{etf_code}] {etf_name}")
        holdings = fetch_moneydj_holdings(etf_code)
        print(f"        → {len(holdings)} 檔持股")
        all_etf_data[etf_code] = {"name": etf_name, "holdings": holdings}
        for h in holdings:
            all_codes.add(h["code"])
        time.sleep(3)

    # ── 2. 批次取股價 ──────────────────────────────────
    print(f"\n  取得 {len(all_codes)} 檔股票價格...")
    prices = fetch_stock_prices(sorted(all_codes))
    print(f"  → 成功 {len(prices)} 檔\n")

    # ── 3. 計算市值 ────────────────────────────────────
    for etf_data in all_etf_data.values():
        for h in etf_data["holdings"]:
            p = prices.get(h["code"], 0.0)
            h["price"] = p
            h["value"] = round(h["shares"] * p)

    # ── 4. 儲存當日 JSON ───────────────────────────────
    output = {
        "date": now.strftime("%Y-%m-%d"),
        "timestamp": now.isoformat(),
        "etfs": all_etf_data,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))
    print(f"  ✓ 儲存：{out_path}")

    # ── 5. 更新 index.json ─────────────────────────────
    index_path = "data/index.json"
    index: dict = {"dates": []}
    if os.path.exists(index_path):
        try:
            with open(index_path, "r", encoding="utf-8") as f:
                index = json.load(f)
        except Exception:
            pass

    dates: list[str] = index.get("dates", [])
    if today_str not in dates:
        dates.insert(0, today_str)
    index["dates"] = dates[:60]  # 保留最近 60 個交易日

    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False)
    print(f"  ✓ 索引更新：{len(index['dates'])} 筆記錄")

    # ── 摘要 ───────────────────────────────────────────
    failed = [c for c, v in all_etf_data.items() if not v["holdings"]]
    total = sum(len(v["holdings"]) for v in all_etf_data.values())
    print(f"\n=== 完成  共 {total} 筆持股  失敗：{failed or '無'} ===")


if __name__ == "__main__":
    main()
