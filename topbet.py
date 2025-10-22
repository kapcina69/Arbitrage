# topbet_scrape_parse.py
# -*- coding: utf-8 -*-
#
# Otvori TopBet → KLIK "SVE" → fokus → skrol 30x (robustan) → fokus → Ctrl+A/C → RAW
# → parsiraj u soccer-like pregled (1/X/2 popunjeni).
#
# Pokretanje:
#   pip install playwright
#   playwright install
#   python topbet_scrape_parse.py

import re
import time
from pathlib import Path
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

URL        = "https://www.topbet.rs/sportsko-kladjenje/1-offer/3-fudbal"
ORIGIN     = "https://www.topbet.rs"
RAW_TXT    = Path("topbet_sledeci_mecevi.txt")
PRETTY_TXT = Path("topbet_mecevi_pregled.txt")

# ===========================
#  A) KOPIRANJE (Playwright)
# ===========================

def wait_idle(page, ms=1200):
    try:
        page.wait_for_load_state("networkidle", timeout=ms)
    except PWTimeoutError:
        pass

def accept_cookies(page) -> None:
    labels = [
        r"Prihvatam", r"Prihvatam sve", r"Prihvati sve", r"Slažem se",
        r"Accept", r"Accept all", r"I agree", r"U redu", r"Ok",
    ]
    deadline = time.time() + 10
    while time.time() < deadline:
        clicked = False
        for pat in labels:
            try:
                page.get_by_role("button", name=re.compile(pat, re.I)).click(timeout=500)
                time.sleep(0.25); clicked = True; break
            except Exception:
                pass
        if clicked:
            return
        try:
            page.locator("button:has-text('Prihv')").first.click(timeout=500)
            time.sleep(0.25); return
        except Exception:
            pass
        time.sleep(0.2)

def click_sve(page) -> bool:
    attempts = [
        lambda: page.get_by_role("button", name=re.compile(r"^\s*SVE\s*$", re.I)).click(timeout=1200),
        lambda: page.get_by_role("tab",    name=re.compile(r"^\s*SVE\s*$", re.I)).click(timeout=1200),
        lambda: page.get_by_role("link",   name=re.compile(r"^\s*SVE\s*$", re.I)).click(timeout=1200),
        lambda: page.locator("button:has-text('SVE')").first.click(timeout=1200),
        lambda: page.locator("a:has-text('SVE')").first.click(timeout=1200),
        lambda: page.get_by_text(re.compile(r"^\s*SVE\s*$", re.I), exact=False).first.click(timeout=1200),

        lambda: page.get_by_role("button", name=re.compile(r"^\s*Sve\s*$", re.I)).click(timeout=1200),
        lambda: page.get_by_role("tab",    name=re.compile(r"^\s*Sve\s*$", re.I)).click(timeout=1200),
        lambda: page.get_by_role("link",   name=re.compile(r"^\s*Sve\s*$", re.I)).click(timeout=1200),
        lambda: page.locator("button:has-text('Sve')").first.click(timeout=1200),
        lambda: page.locator("a:has-text('Sve')").first.click(timeout=1200),
        lambda: page.get_by_text(re.compile(r"^\s*Sve\s*$", re.I), exact=False).first.click(timeout=1200),
    ]
    for fn in attempts:
        try:
            fn()
            wait_idle(page, 1500)
            time.sleep(0.3)
            return True
        except Exception:
            continue
    return False

def click_center(page):
    try:
        vp = page.viewport_size or {"width": 1200, "height": 800}
        x = int(vp["width"] // 2)
        y = int(vp["height"] // 2)
        page.mouse.move(x, y)
        page.mouse.click(x, y)
        time.sleep(0.15)
    except Exception:
        try:
            page.locator("body").click(position={"x": 20, "y": 120}, timeout=1500)
        except Exception:
            pass

def robust_scroll_30(page, pause=0.25):
    """
    Robusno skrolovanje vidljivo u headless=False:
      - fokus na centar
      - combine: mouse.wheel, window.scrollBy, PageDown fallback
      - proverava rast document.body.scrollHeight; ako je „zapeo“, prodrma fokus i ponovi
    """
    # idi na vrh prvo
    try: page.evaluate("window.scrollTo(0,0)")
    except Exception: pass
    wait_idle(page, 600)
    click_center(page)

    get_h = lambda: page.evaluate("() => document.body.scrollHeight")
    last_h = get_h()

    for step in range(1, 31):
        # 1) probaj wheel
        try:
            vp = page.viewport_size or {"width": 1200, "height": 800}
            page.mouse.move(vp["width"]//2, min(vp["height"]//2, vp["height"]-40))
            page.mouse.wheel(0, 1400)
        except Exception:
            pass

        time.sleep(pause)
        wait_idle(page, int(pause*1000))

        new_h = get_h()
        advanced = new_h > last_h + 5

        if not advanced:
            # 2) probaj programatski scrollBy
            try:
                page.evaluate("window.scrollBy(0, Math.max(window.innerHeight, 700))")
            except Exception:
                pass
            time.sleep(pause)
            wait_idle(page, int(pause*1000))
            new_h = get_h()
            advanced = new_h > last_h + 5

        if not advanced:
            # 3) PageDown fallback
            try:
                page.keyboard.press("PageDown")
            except Exception:
                pass
            time.sleep(pause)
            wait_idle(page, int(pause*1000))
            new_h = get_h()
            advanced = new_h > last_h + 5

        if not advanced:
            # 4) prodrmaj fokus i probaj ponovo kratko
            click_center(page)
            try:
                page.mouse.wheel(0, 1600)
            except Exception:
                pass
            time.sleep(pause)
            wait_idle(page, int(pause*1000))
            new_h = get_h()
            advanced = new_h > last_h + 5

        last_h = max(last_h, new_h)
        print(f"[scroll] step {step:02d}/30  advanced={advanced}")

def copy_all(page) -> str:
    click_center(page)
    try:
        page.keyboard.press("Control+A")
        time.sleep(0.8)
        page.keyboard.press("Control+C")
    except Exception:
        pass

    time.sleep(0.25)
    try:
        page.keyboard.press("Meta+A")
        time.sleep(0.8)
        page.keyboard.press("Meta+C")
    except Exception:
        pass

    time.sleep(0.3)
    try:
        txt = page.evaluate(
            "() => navigator.clipboard && navigator.clipboard.readText ? navigator.clipboard.readText() : ''"
        )
        if isinstance(txt, str) and txt.strip():
            return txt
    except Exception:
        pass

    try:
        return page.locator("body").inner_text()
    except Exception:
        return ""

def fetch_raw_topbet(headless: bool = False) -> str:
    """Otvori TopBet, klik 'SVE' → robust scroll 30 → kopiraj → vrati tekst i upiši RAW."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            locale="sr-RS",
            permissions=["clipboard-read", "clipboard-write"],
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"),
            viewport={"width": 1440, "height": 1100},
        )
        try:
            context.grant_permissions(["clipboard-read", "clipboard-write"], origin=ORIGIN)
        except Exception:
            pass

        page = context.new_page()
        try:
            page.goto(URL, wait_until="domcontentloaded", timeout=60000)
            accept_cookies(page)
            wait_idle(page, 1500)

            clicked = click_sve(page)
            if not clicked:
                print("[WARN] Nije nađen tab/dugme 'SVE' — nastavljam bez toga.")

            robust_scroll_30(page, pause=0.25)

            # vrati se na vrh radi preglednosti, fokus pa kopiraj
            try: page.evaluate("window.scrollTo(0,0)")
            except Exception: pass
            time.sleep(0.2)
            txt = copy_all(page)

            RAW_TXT.write_text(txt, encoding="utf-8")
            print(f"[OK] RAW sačuvan: {RAW_TXT.resolve()}")
            return txt
        finally:
            browser.close()

# ===========================
#  B) PARSER (TopBet RAW → Pretty)
# ===========================

TIME_RE       = re.compile(r"^([01]?\d|2[0-3]):[0-5]\d$")
PLUS_ID_RE    = re.compile(r"^\+\d+$")
FLOAT_RE      = re.compile(r"^\d+(?:[.,]\d+)?$")
DAY_HEAD_RE   = re.compile(r"^(PON|UTO|SRE|ČET|CET|PET|SUB|NED)\.\s+(\d{1,2}\.\d{1,2}\.)$", re.I)

SKIP_TOKENS = {
    "Fudbal",
    "KONAČAN ISHOD", "UKUPNO GOLOVA", "OBA TIMA DAJU GOL",
    "KONACAN ISHOD", "UKUPNO GOLOVA 2.5",
    "1", "X", "2",
    "Tiket (0)",
}

DAY_CANON = {
    "PON": "Pon", "UTO": "Uto", "SRE": "Sre", "ČET": "Čet", "CET": "Čet",
    "PET": "Pet", "SUB": "Sub", "NED": "Ned",
}

def _to_float(s: str) -> Optional[float]:
    s = s.strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def is_league_line(s: str) -> bool:
    if not s: return False
    if " - " in s: return False
    if TIME_RE.match(s): return False
    if PLUS_ID_RE.match(s): return False
    if FLOAT_RE.match(s): return False
    if s in SKIP_TOKENS: return False
    return True

def parse_topbet(text: str) -> List[Dict]:
    lines = [ln.strip().replace("\xa0", " ") for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]

    out: List[Dict] = []
    cur_league = ""
    cur_day = ""
    cur_date = ""

    i, n = 0, len(lines)
    while i < n:
        ln = lines[i].strip()

        if ln in SKIP_TOKENS:
            i += 1; continue

        mday = DAY_HEAD_RE.match(ln)
        if mday:
            cur_day  = DAY_CANON[mday.group(1).upper()]
            cur_date = mday.group(2)
            i += 1
            continue

        if is_league_line(ln):
            cur_league = ln
            i += 1
            continue

        if TIME_RE.match(ln):
            time_s = ln
            i += 1
            if i >= n: break

            if " - " not in lines[i]:
                i += 1
                continue
            teams_line = lines[i]; i += 1
            home, away = [t.strip(" .") for t in teams_line.split(" - ", 1)]

            if i + 2 >= n: break
            q1 = _to_float(lines[i]);   i += 1
            qx = _to_float(lines[i]);   i += 1
            q2 = _to_float(lines[i]);   i += 1

            match_id = ""
            if i < n and PLUS_ID_RE.match(lines[i]):
                match_id = lines[i][1:]
                i += 1

            out.append({
                "time":   time_s,
                "day":    cur_day,
                "date":   cur_date,
                "league": cur_league,
                "home":   home,
                "away":   away,
                "match_id": match_id,
                "odds": {
                    "1": q1, "X": qx, "2": q2,
                    "0-2": None, "2+": None, "3+": None,
                    "GG": None, "IGG": None, "GG&3+": None
                }
            })
            continue

        i += 1

    return out

def write_pretty(blocks: List[Dict], out_path: Path):
    def fmt(x: Optional[float]) -> str:
        if x is None: return "-"
        return str(int(x)) if float(x).is_integer() else f"{x}"

    lines: List[str] = []
    for b in blocks:
        lines.append("=" * 70)
        header = f"{b['time']}  {b.get('day','')}  {b.get('date','')}".rstrip()
        league_tag = f"  [{b['league']}]" if b.get("league") else ""
        lines.append(header + league_tag)

        id_part = f"   (ID: {b['match_id']})" if b.get("match_id") else ""
        lines.append(f"{b['home']}  vs  {b['away']}{id_part}")

        od = b["odds"]
        lines.append(f"1={fmt(od.get('1'))}   X={fmt(od.get('X'))}   2={fmt(od.get('2'))}")
        lines.append(f"0-2={fmt(od.get('0-2'))}   2+={fmt(od.get('2+'))}   3+={fmt(od.get('3+'))}")
        lines.append(f"GG={fmt(od.get('GG'))}   IGG={fmt(od.get('IGG'))}   GG&3+={fmt(od.get('GG&3+'))}")
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] Pretty sačuvan: {out_path.resolve()}")

# ===========================
#  C) MAIN
# ===========================

def main():
    # forsiramo vidljivo (headless=False) da vidiš skrol
    raw = fetch_raw_topbet(headless=False)
    blocks = parse_topbet(raw)
    write_pretty(blocks, PRETTY_TXT)
    print(f"[OK] Isparsiranih mečeva: {len(blocks)}")

if __name__ == "__main__":
    main()
