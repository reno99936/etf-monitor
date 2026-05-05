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
import urllib.request
from datetime import datetime
import pytz

TAIPEI_TZ = pytz.timezone("Asia/Taipei")

# 依規模排序的主動式ETF（含台股、全球、美股）
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
    ("00999A", "主動野村臺灣高息"),
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


def load_config() -> dict:
    """讀取 data/config.json，若不存在則回傳空 dict"""
    config_path = "data/config.json"
    if not os.path.exists(config_path):
        return {}
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"  ⚠ 讀取 config.json 失敗：{e}")
        return {}


def fetch_moneydj_holdings(etf_code: str) -> list[dict]:
    """從 MoneyDJ 抓取 ETF 全部持股明細（Basic0007B 全部持股頁面）

    實際表格格式（class="datalist"）：
      欄1：個股名稱  → 如「台積電(2330.TW)」
      欄2：投資比例(%) → 如「8.99」（無%符號）
      欄3：持有股數  → 如「10,039,000.00」
    """
    url = (
        "https://www.moneydj.com/ETF/X/Basic/Basic0007B.xdjhtm"
        f"?etfid={etf_code.upper()}.TW"
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

            name_cell = cells[0]   # 如「台積電(2330.TW)」或「NVIDIA(NVDA.US)」

            # 從括號取出代號與交易所，支援台股 (1234.TW) 及外股 (AAPL.US) 格式
            code_match = re.search(r"\(([A-Z0-9\-]{1,10})\.(\w+)\)", name_cell, re.IGNORECASE)
            if not code_match:
                continue
            code = code_match.group(1).upper()
            exchange_raw = code_match.group(2).upper()
            is_tw = bool(re.match(r"^\d{4,6}$", code)) and exchange_raw in ("TW", "TWO")

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
                        "exchange": exchange_raw,
                        "is_tw": is_tw,
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

    # ── 2. 批次取股價（僅台股，外股 price/value 維持 0）──
    tw_codes: set[str] = {
        h["code"] for etf_data in all_etf_data.values()
        for h in etf_data["holdings"] if h.get("is_tw", True)
    }
    print(f"\n  取得 {len(tw_codes)} 檔台股價格（共 {len(all_codes)} 檔含外股）...")
    prices = fetch_stock_prices(sorted(tw_codes))
    print(f"  → 成功 {len(prices)} 檔\n")

    # ── 3. 計算市值（僅台股有即時報價）───────────────────
    for etf_data in all_etf_data.values():
        for h in etf_data["holdings"]:
            if h.get("is_tw", True):
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

    # ── 6. Telegram 推播 ───────────────────────────────
    config = load_config()

    tg_token = config.get("tg_token", os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    tg_chat  = config.get("tg_chat",  os.environ.get("TELEGRAM_CHAT_ID", ""))

    if tg_token and tg_chat:
        # 讀取昨日資料做比較
        compare_data = None
        if len(index["dates"]) >= 2:
            prev_path = f"data/{index['dates'][1]}.json"
            if os.path.exists(prev_path):
                with open(prev_path, "r", encoding="utf-8") as f:
                    compare_data = json.load(f)

        tg_period  = config.get("tg_period",  int(os.environ.get("TG_PERIOD",    "1")))
        tg_min     = config.get("tg_min",     int(os.environ.get("TG_MIN_COUNT", "3")))
        tg_new_min = config.get("tg_new_min", int(os.environ.get("TG_NEW_MIN",   "1")))
        send_a     = config.get("send_a",     os.environ.get("TG_SEND_A", "true").lower() == "true")
        send_b     = config.get("send_b",     os.environ.get("TG_SEND_B", "true").lower() == "true")

        # b_etfs：config 優先（接受 list 或逗號字串），否則 fallback 到 env var
        cfg_b = config.get("b_etfs")
        if cfg_b:
            if isinstance(cfg_b, list):
                b_etfs = [e.strip() for e in cfg_b if e.strip()]
            else:
                b_etfs = [e.strip() for e in str(cfg_b).split(",") if e.strip()]
        else:
            b_etfs_raw = os.environ.get(
                "TG_ETFS_B",
                "00981A,00982A,00991A,00990A,00992A,00993A,00988A,00980A,"
                "00985A,00995A,00984A,00983A,00987A,00989A,00999A",
            )
            b_etfs = [e.strip() for e in b_etfs_raw.split(",") if e.strip()]

        if send_a:
            msg = build_telegram_a(output, compare_data, tg_period, tg_min, tg_new_min)
            ok = send_telegram(tg_token, tg_chat, msg)
            print(f"  Telegram 樣板A：{'✓' if ok else '✗'}")

        if send_b:
            for etf_code in b_etfs:
                msg = build_telegram_b(etf_code, output, compare_data)
                ok = send_telegram(tg_token, tg_chat, msg)
                print(f"  Telegram 樣板B {etf_code}：{'✓' if ok else '✗'}")
                time.sleep(1)
    else:
        print("  (未設定 TELEGRAM_BOT_TOKEN，跳過推播)")


# ── Telegram 工具函式 ──────────────────────────────────────────────────

ETF_SHORT = {
    "00981A": "統一增長", "00982A": "群益強棒", "00991A": "復華未來50",
    "00990A": "元大AI",   "00992A": "群益科技", "00993A": "安聯台灣",
    "00988A": "統一全球", "00980A": "野村優選", "00985A": "野村50",
    "00995A": "中信卓越", "00984A": "安聯高息", "00983A": "中信ARK",
    "00987A": "台新優勢", "00989A": "摩根美科", "00999A": "野村高息",
}


def send_telegram(token: str, chat_id: str, text: str) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    body = json.dumps({"chat_id": chat_id, "text": text, "parse_mode": "HTML"}).encode()
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read()).get("ok", False)
    except Exception as e:
        print(f"    Telegram 錯誤：{e}")
        return False


def build_telegram_a(
    today_data: dict,
    compare_data: dict | None,
    period: int,
    min_count: int,
    new_min: int = 1,
) -> str:
    # ── 增持統計 ──────────────────────────────────────────
    stock_map: dict[str, dict] = {}
    for etf_code, etf_t in today_data["etfs"].items():
        comp = compare_data["etfs"].get(etf_code, {}) if compare_data else {}
        comp_h = {h["code"]: h for h in comp.get("holdings", [])}
        for h in etf_t["holdings"]:
            if not h.get("is_tw", True):   # 樣板A只納入台股
                continue
            prev = comp_h.get(h["code"])
            dW = round(h["weight"] - (prev["weight"] if prev else 0), 4)
            if dW <= 0.01:
                continue
            dLots = h["lots"] - (prev["lots"] if prev else 0)
            c = h["code"]
            if c not in stock_map:
                stock_map[c] = {"code": c, "name": h["name"], "etfs": [], "total_lots": 0, "total_val": 0}
            stock_map[c]["etfs"].append({
                "code": etf_code, "dW": dW, "dLots": dLots,
                "dVal": dLots * 1000 * h.get("price", 0),
            })
            stock_map[c]["total_lots"] += dLots
            stock_map[c]["total_val"]  += dLots * 1000 * h.get("price", 0)

    filtered = [s for s in stock_map.values() if len(s["etfs"]) >= min_count]
    filtered.sort(key=lambda s: len(s["etfs"]), reverse=True)
    nums = ["①", "②", "③", "④", "⑤"]

    msg = f"📊 多家投信同步加碼 {today_data['date']}\n\n"
    msg += f"過去{period}天，{len(filtered)}檔個股被{min_count}家以上投信同步增持\n\n"

    if filtered:
        msg += "📈 增持家數 TOP5：\n"
        for i, s in enumerate(filtered[:5]):
            lots_s = f"+{s['total_lots']:,}張" if s["total_lots"] > 0 else ""
            val    = s["total_val"]
            val_s  = (f"折合市值+{val/1e8:.2f}億" if val >= 1e8
                      else f"+{val/1e4:.1f}萬" if val >= 1e4 else "")
            # 依 dLots 降序，全部列出
            all_etfs_sorted = sorted(s["etfs"], key=lambda e: e["dLots"], reverse=True)
            etf_parts = "｜".join(
                f"{ETF_SHORT.get(e['code'], e['code'])}({e['code']}) {'+' if e['dLots'] >= 0 else ''}{e['dLots']}"
                for e in all_etfs_sorted
            )
            msg += f"{nums[i]} {s['name']} ({s['code']})  {len(s['etfs'])}家\n"
            if lots_s or val_s:
                msg += f"   合計 {' ｜ '.join(x for x in [lots_s, val_s] if x)}\n"
            msg += f"   └ {etf_parts}\n\n"

    # ── 減持摘要 ───────────────────────────────────────────
    dec_map: dict[str, int] = {}
    for etf_code, etf_t in today_data["etfs"].items():
        comp = compare_data["etfs"].get(etf_code, {}) if compare_data else {}
        comp_h = {h["code"]: h for h in comp.get("holdings", [])}
        for h in etf_t["holdings"]:
            if not h.get("is_tw", True):   # 樣板A只納入台股
                continue
            prev = comp_h.get(h["code"])
            if prev and h["weight"] - prev["weight"] < -0.01:
                dec_map[h["code"]] = dec_map.get(h["code"], 0) + 1

    dec_filtered = [(c, n) for c, n in dec_map.items() if n >= min_count]
    dec_filtered.sort(key=lambda x: x[1], reverse=True)
    if dec_filtered:
        msg += "📉 減持家數 TOP3：\n"
        all_hs = {h["code"]: h for etf in today_data["etfs"].values() for h in etf["holdings"]}
        for i, (c, n) in enumerate(dec_filtered[:3]):
            name = all_hs.get(c, {}).get("name", c)
            msg += f"{nums[i]} {name} ({c})  {n}家\n"

    # ── 新建倉 & 完全出清 ──────────────────────────────────
    msg += f"\n────────────────\n"

    # 建立今日 / 昨日台股持倉 Map：stock_code -> {etf_code: lots}
    today_tw: dict[str, dict[str, int]] = {}   # {stock_code: {etf_code: lots}}
    prev_tw:  dict[str, dict[str, int]] = {}

    for etf_code, etf_t in today_data["etfs"].items():
        for h in etf_t["holdings"]:
            if not h.get("is_tw", True):
                continue
            today_tw.setdefault(h["code"], {})[etf_code] = h["lots"]
            # 股票名稱備用
            today_tw[h["code"]]["__name__"] = h["name"]   # type: ignore[assignment]

    if compare_data:
        for etf_code, etf_c in compare_data["etfs"].items():
            for h in etf_c.get("holdings", []):
                if not h.get("is_tw", True):
                    continue
                prev_tw.setdefault(h["code"], {})[etf_code] = h["lots"]
                prev_tw[h["code"]]["__name__"] = h["name"]   # type: ignore[assignment]

    # 新建倉：今日有、昨日沒有，且進場家數 >= new_min
    new_entries: list[tuple[str, str, dict[str, int]]] = []
    for code, etf_lots in today_tw.items():
        if code == "__name__":
            continue
        real_etfs = {k: v for k, v in etf_lots.items() if k != "__name__"}
        if code not in prev_tw and len(real_etfs) >= new_min:
            name = etf_lots.get("__name__", code)  # type: ignore[arg-type]
            new_entries.append((code, name, real_etfs))

    new_entries.sort(key=lambda x: len(x[2]), reverse=True)

    msg += f"\n🆕 今日新建倉（≥{new_min}家進場）：\n"
    if new_entries:
        for i, (code, name, etf_lots) in enumerate(new_entries[:5]):
            etf_parts = "｜".join(
                f"{ETF_SHORT.get(ec, ec)}({ec}) {etf_lots[ec]}張"
                for ec in sorted(etf_lots, key=lambda k: etf_lots[k], reverse=True)
            )
            msg += f"{nums[i]} {name} ({code})  {len(etf_lots)}家\n"
            msg += f"   └ {etf_parts}\n"
    else:
        msg += "無\n"

    # 完全出清：昨日有、今日沒有，且清倉家數 >= new_min
    exit_entries: list[tuple[str, str, dict[str, int]]] = []
    for code, etf_lots in prev_tw.items():
        if code == "__name__":
            continue
        real_etfs = {k: v for k, v in etf_lots.items() if k != "__name__"}
        if code not in today_tw and len(real_etfs) >= new_min:
            name = etf_lots.get("__name__", code)  # type: ignore[arg-type]
            exit_entries.append((code, name, real_etfs))

    exit_entries.sort(key=lambda x: len(x[2]), reverse=True)

    msg += f"\n🚫 今日完全出清（≥{new_min}家清倉）：\n"
    if exit_entries:
        for i, (code, name, etf_lots) in enumerate(exit_entries[:5]):
            etf_parts = "｜".join(
                f"{ETF_SHORT.get(ec, ec)}({ec})"
                for ec in sorted(etf_lots)
            )
            msg += f"{nums[i]} {name} ({code})  {len(etf_lots)}家\n"
            msg += f"   └ {etf_parts}\n"
    else:
        msg += "無\n"

    return msg


def build_telegram_b(etf_code: str, today_data: dict, compare_data: dict | None) -> str:
    etf_t = today_data["etfs"].get(etf_code)
    if not etf_t or not etf_t["holdings"]:
        return f"⚠️ {etf_code}：無持股資料"

    etf_c = compare_data["etfs"].get(etf_code) if compare_data else None

    # 只比較台股（外股不列入異動報表）
    t_map = {h["code"]: h for h in etf_t["holdings"] if h.get("is_tw", True)}
    c_map = {h["code"]: h for h in (etf_c["holdings"] if etf_c else []) if h.get("is_tw", True)}

    date_str = today_data["date"].replace("-", "/")
    msg = f"📅 {date_str} {etf_code} 異動追蹤\n\n"

    # 新增持股
    new_h = [h for code, h in t_map.items() if code not in c_map]
    msg += "🌟 新增持股：\n"
    if new_h:
        for h in new_h:
            msg += f"• {h['name']} ({h['code']})\n  └ {h['lots']:,}張 ｜ 權重 {h['weight']}%\n"
    else:
        msg += "無\n"
    msg += "\n"

    # 剔除持股
    rmvd = [h for code, h in c_map.items() if code not in t_map]
    msg += "🗑 剔除持股：\n"
    if rmvd:
        for h in rmvd:
            msg += f"• {h['name']} ({h['code']})\n"
    else:
        msg += "無\n"
    msg += "\n"

    # 計算持股異動
    changes = []
    for code, th in t_map.items():
        prev = c_map.get(code)
        if not prev:
            continue
        dW    = round(th["weight"] - prev["weight"], 4)
        dLots = th["lots"] - prev["lots"]
        changes.append({**th, "dW": dW, "dLots": dLots})

    inc = sorted([c for c in changes if c["dLots"] > 0 or c["dW"] > 0.01],
                 key=lambda c: c["dLots"], reverse=True)
    dec = sorted([c for c in changes if c["dLots"] < 0 or c["dW"] < -0.01],
                 key=lambda c: c["dLots"])

    msg += "📈 增加持股前五名：\n"
    if inc:
        for h in inc[:5]:
            sl = "+" if h["dLots"] >= 0 else ""
            sw = "+" if h["dW"] >= 0 else ""
            msg += (f"• {h['name']} ({h['code']})\n"
                    f"  └ {h['lots']:,}張 ({sl}{h['dLots']:,})"
                    f" ｜ 權重 {h['weight']}% ({sw}{h['dW']:.2f}%)\n")
    else:
        msg += "無\n"
    msg += "\n"

    msg += "📉 減少持股前五名：\n"
    if dec:
        for h in dec[:5]:
            sl = "+" if h["dLots"] >= 0 else ""
            sw = "+" if h["dW"] >= 0 else ""
            msg += (f"• {h['name']} ({h['code']})\n"
                    f"  └ {h['lots']:,}張 ({sl}{h['dLots']:,})"
                    f" ｜ 權重 {h['weight']}% ({sw}{h['dW']:.2f}%)\n")
    else:
        msg += "無\n"

    return msg


if __name__ == "__main__":
    main()
