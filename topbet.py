# topbet_scrape_parse.py
# -*- coding: utf-8 -*-
#
# Otvori TopBet → klik na centar (radi fokusa) → skrol 30x → klik na centar → Ctrl+A/C → sačuvaj RAW
# → isparsiraj RAW u soccer-like pregled (1/X/2 popunjeni, ostala tržišta “-” jer ih nema u RAW).
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

URL      = "https://www.topbet.rs/sportsko-kladjenje/1-offer/3-fudbal"
ORIGIN   = "https://www.topbet.rs"
RAW_TXT  = Path("topbet_sledeci_mecevi.txt")
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

def click_center(page):
    """Klik na centar viewport-a – obezbeđuje fokus pre skrolovanja/kopiranja."""
    try:
        vp = page.viewport_size or {"width": 1200, "height": 800}
        x = int(vp["width"] // 2)
        y = int(vp["height"] // 2)
        page.mouse.move(x, y)
        page.mouse.click(x, y)
        time.sleep(0.2)
    except Exception:
        try:
            page.locator("body").click(position={"x": 20, "y": 120}, timeout=1500)
        except Exception:
            pass

def scroll_30(page, pause=0.35):
    """Glavni scroll preko window.scrollBy — tačno 30 puta."""
    for _ in range(30):
        page.evaluate("window.scrollBy(0, Math.max(window.innerHeight, 600))")
        time.sleep(pause)
        wait_idle(page, int(pause * 1000))

def copy_all(page) -> str:
    """Ctrl+A → kratko čekanje → Ctrl+C → pročitati clipboard; fallback innerText."""
    # fokus/klik na centar pre kopiranja
    click_center(page)

    # Windows/Linux
    try:
        page.keyboard.press("Control+A")
        time.sleep(0.8)
        page.keyboard.press("Control+C")
    except Exception:
        pass

    # macOS fallback
    time.sleep(0.25)
    try:
        page.keyboard.press("Meta+A")
        time.sleep(0.8)
        page.keyboard.press("Meta+C")
    except Exception:
        pass

    time.sleep(0.3)
    # Clipboard API
    try:
        txt = page.evaluate(
            "() => navigator.clipboard && navigator.clipboard.readText ? navigator.clipboard.readText() : ''"
        )
        if isinstance(txt, str) and txt.strip():
            return txt
    except Exception:
        pass

    # fallback
    try:
        return page.locator("body").inner_text()
    except Exception:
        return ""

def fetch_raw_topbet(headless: bool = True) -> str:
    """Otvori TopBet, fokus + skrol + kopiraj, vrati prekopirani tekst i upiši RAW_TXT."""
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

            # klik na centar → skrol 30x → klik na centar → kopiraj sve
            click_center(page)
            scroll_30(page, pause=0.35)
            click_center(page)
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
    """Liga je kratak tekst bez ':' i bez ' - ' između timova; npr. 'Liga Šampiona', 'Engleska 1'."""
    if not s: return False
    if " - " in s: return False           # to je par timova
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

        # preskoči opšte tokene
        if ln in SKIP_TOKENS:
            i += 1; continue

        # dan + datum (npr. "UTO. 21.10.")
        mday = DAY_HEAD_RE.match(ln)
        if mday:
            cur_day  = DAY_CANON[mday.group(1).upper()]
            cur_date = mday.group(2)
            i += 1
            continue

        # liga
        if is_league_line(ln):
            cur_league = ln
            i += 1
            continue

        # vreme → blok meča
        if TIME_RE.match(ln):
            time_s = ln
            i += 1
            if i >= n: break

            # "Home - Away"
            if " - " not in lines[i]:
                # nije validan red timova – skip
                continue
            teams_line = lines[i]; i += 1
            home, away = [t.strip(" .") for t in teams_line.split(" - ", 1)]

            # tri kvote (1, X, 2)
            if i + 2 >= n: break
            q1 = _to_float(lines[i]);   i += 1
            qx = _to_float(lines[i]);   i += 1
            q2 = _to_float(lines[i]);   i += 1

            # opcioni +ID
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

        # ako ništa od gore – sledeća linija
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
    raw = fetch_raw_topbet(headless=True)  # stavi True ako ne želiš prozor
    blocks = parse_topbet(raw)
    write_pretty(blocks, PRETTY_TXT)
    print(f"[OK] Isparsiranih mečeva: {len(blocks)}")

if __name__ == "__main__":
    main()
