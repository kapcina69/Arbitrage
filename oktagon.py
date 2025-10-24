# oktagonbet_scrape_and_parse.py
# -*- coding: utf-8 -*-

import re
import time
from pathlib import Path
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from datetime import datetime

URL       = "https://www.oktagonbet.com/ibet-web-client/#/home/leaguesWithMatches"
ORIGIN    = "https://www.oktagonbet.com"
RAW_TXT   = Path("oktagonbet_sledeci_mecevi.txt")
PRETTY_TXT= Path("oktagonbet_mecevi_pregled.txt")

# ===========================
# A) Pomoćne (Playwright)
# ===========================

def wait_idle(page, ms=1200):
    try:
        page.wait_for_load_state("networkidle", timeout=ms)
    except PWTimeoutError:
        pass

def accept_cookies(page):
    labels = [
        r"Prihvatam", r"Prihvatam sve", r"Prihvati sve", r"Slažem se",
        r"Accept", r"Accept all", r"I agree", r"U redu", r"Ok",
    ]
    deadline = time.time() + 8
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
    """Klik na centar viewport-a – često pomaže da fokus bude na glavnom prozoru."""
    try:
        vp = page.viewport_size or {"width": 1200, "height": 800}
        x = int(vp["width"] // 2)
        y = int(min(vp["height"] // 2, vp["height"] - 40))
        page.mouse.move(x, y)
        page.mouse.click(x, y)
        time.sleep(0.15)
    except Exception:
        try:
            page.locator("body").click(position={"x": 20, "y": 120}, timeout=1500)
        except Exception:
            pass

def scroll_wheel(page, steps=1, pause=0.3, wheel_delta=1600):
    vp = page.viewport_size or {"width": 1200, "height": 800}
    cx = int(vp["width"] // 2)
    cy = int(min(vp["height"] // 2, vp["height"] - 40))
    for _ in range(steps):
        try:
            page.mouse.move(cx, cy)
            page.mouse.wheel(0, wheel_delta)
        except Exception:
            try:
                page.keyboard.press("PageDown")
            except Exception:
                try:
                    page.evaluate("window.scrollBy(0, Math.max(window.innerHeight, 600))")
                except Exception:
                    pass
        time.sleep(pause)
        wait_idle(page, int(pause * 1000))

def smart_scroll(page, total_down=30, pause=0.3):
    """Skrol sa malim ‘bounce’ pokušajem kad se zaglavi."""
    def metrics():
        try:
            return page.evaluate("""() => ({
                y: window.scrollY || document.documentElement.scrollTop || document.body.scrollTop || 0,
                h: document.body.scrollHeight || document.documentElement.scrollHeight || 0
            })""")
        except Exception:
            return {"y": 0, "h": 0}

    last = metrics()
    for i in range(total_down):
        scroll_wheel(page, steps=1, pause=pause, wheel_delta=1600)
        cur = metrics()
        moved = (cur["y"] > last["y"] + 5) or (cur["h"] > last["h"] + 5)
        if not moved:
            click_center(page)
            time.sleep(0.1)
            scroll_wheel(page, steps=1, pause=pause, wheel_delta=1800)
            try: page.keyboard.press("PageDown")
            except Exception: pass
            cur = metrics()
        last = cur

        if (i + 1) % 5 == 0:
            # mini bounce
            try:
                page.mouse.wheel(0, -1200)
                time.sleep(0.18)
                page.mouse.wheel(0, 1400)
            except Exception:
                pass
            wait_idle(page, 600)

def click_500_twice(page):
    """Pronađi dugme ‘500’ i klikni ga DVA puta (radi osvežavanja liste)."""
    def _click_once():
        selectors = [
            lambda: page.get_by_role("button", name=re.compile(r"^\s*500\s*$")),
            lambda: page.locator("button:has-text('500')"),
            lambda: page.get_by_text("500", exact=True),
            lambda: page.locator("//button[normalize-space()='500']"),
            lambda: page.locator("a:has-text('500')"),
            lambda: page.locator("div:has-text('500')"),
        ]
        for mk in selectors:
            try:
                loc = mk()
                cnt = min(loc.count(), 8) if hasattr(loc, "count") else 1
                for i in range(max(cnt, 1)):
                    try:
                        target = loc.nth(i) if hasattr(loc, "nth") else loc
                        if hasattr(target, "is_visible"):
                            if not target.is_visible(timeout=1500):
                                continue
                        target.click(timeout=2000)
                        wait_idle(page, 1200)
                        return True
                    except Exception:
                        continue
            except Exception:
                continue
        return False

    ok1 = _click_once()
    time.sleep(0.4)
    ok2 = _click_once()
    time.sleep(0.4)
    ok3 = _click_once()
    return ok1 or ok2

def copy_try_ctrl(page) -> str:
    """Ctrl/Meta A+C + clipboard → fallback prazan string."""
    click_center(page)
    # Win/Linux
    try:
        page.keyboard.press("Control+A"); time.sleep(0.8)
        page.keyboard.press("Control+C")
    except Exception:
        pass
    time.sleep(0.25)
    # macOS
    try:
        page.keyboard.press("Meta+A"); time.sleep(0.8)
        page.keyboard.press("Meta+C")
    except Exception:
        pass
    time.sleep(0.35)
    try:
        txt = page.evaluate("() => navigator.clipboard && navigator.clipboard.readText ? navigator.clipboard.readText() : ''")
        if isinstance(txt, str):
            return txt
    except Exception:
        pass
    return ""

def copy_try_execcommand(page) -> str:
    """
    Textarea + document.execCommand('copy') + readText.
    Ako ne uspe, vrati prazan string (ne body ovde – to je zaseban metod).
    """
    click_center(page)
    time.sleep(0.2)
    try:
        page.evaluate("""() => {
            const text = document.body && document.body.innerText ? document.body.innerText : '';
            const ta = document.createElement('textarea');
            ta.value = text;
            ta.setAttribute('readonly','');
            ta.style.position = 'fixed';
            ta.style.top = '-10000px';
            ta.style.left = '-10000px';
            document.body.appendChild(ta);
            ta.focus();
            ta.select();
            try { document.execCommand('copy'); } catch(e) {}
            document.body.removeChild(ta);
        }""")
        time.sleep(0.3)
        txt = page.evaluate("() => navigator.clipboard && navigator.clipboard.readText ? navigator.clipboard.readText() : ''")
        if isinstance(txt, str):
            return txt
    except Exception:
        pass
    return ""

def copy_try_dom(page) -> str:
    """DOM fallback – probaj široke kontejnere pa body.innerText."""
    candidates = [
        "main", "app-root", "app-ibet", "[role='main']",
        ".content", "#content", ".container", ".wrapper",
        "body"
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible():
                t = loc.first.inner_text()
                if isinstance(t, str) and t.strip():
                    return t
        except Exception:
            continue
    try:
        return page.locator("body").inner_text()
    except Exception:
        return ""

def fetch_and_copy_oktagon(headless=False) -> str:
    """Otvori → prihvati kolačiće → skrol 4x → klikni 500 DVA PUTA → skrol 30x → kopiraj (više metoda, uzmi najduži)."""
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

            # malo skrolovanja da se pojave dugmad
            smart_scroll(page, total_down=4, pause=0.32)

            # klik “500” DVA PUTA
            if not click_500_twice(page):
                print("[!] Dugme '500' nije nađeno – nastavljam bez klika.")
            else:
                wait_idle(page, 1500)
                time.sleep(0.3)
                click_center(page)

            # skrol još 30x
            smart_scroll(page, total_down=30, pause=0.28)

            # probe kopiranja – uzmi NAJDUŽE
            candidates = []
            for fn in (copy_try_ctrl, copy_try_execcommand, copy_try_dom):
                try:
                    s = fn(page)
                    candidates.append(s if isinstance(s, str) else "")
                except Exception:
                    candidates.append("")
            best = max(candidates, key=lambda s: len(s or "")) or ""

            RAW_TXT.write_text(best, encoding="utf-8")
            print(f"[OK] RAW sačuvan: {RAW_TXT.resolve()}  (dužina: {len(best)})")
            return best
        finally:
            browser.close()

# ===========================
# B) PARSER (RAW → Pretty)
# ===========================

DATE_TIME_RE = re.compile(r"^(\d{1,2}\.\d{1,2}\.)\s+([01]?\d|2[0-3]):[0-5]\d$")
NUM_RE       = re.compile(r"^-?\d+(?:[.,]\d+)?$")  # dozvoli i '-' (nema kvote)

DAY_NAMES_SR = ["Pon", "Uto", "Sre", "Čet", "Pet", "Sub", "Ned"]

def _to_float(s: str) -> Optional[float]:
    s = s.strip().replace(",", ".")
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None

def _day_from_date(date_s: str) -> str:
    try:
        d, m = date_s.strip(". ").split(".")[:2]
        dt = datetime(datetime.now().year, int(m), int(d))
        return DAY_NAMES_SR[dt.weekday()]
    except Exception:
        return ""

def parse_oktagon(text: str) -> List[Dict]:
    """
    Blokovi:
      DD.MM. HH:MM
      Home - Away
      8 redova kvota: 1, X, 2, UG0-2, UG3+, UG4+, GG, GG3+
    Ako ima premalo kvota (npr. samo jedna realna), preskačemo meč.
    """
    lines = [ln.replace("\xa0", " ").strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]

    out: List[Dict] = []
    i, n = 0, len(lines)

    while i < n:
        mdt = DATE_TIME_RE.match(lines[i])
        if not mdt:
            i += 1
            continue

        date_s = mdt.group(1)
        # vreme je zadnja “reč” (ili group(2))
        time_s = lines[i].split()[-1]
        i += 1
        if i >= n:
            break

        # Timovi: "Home - Away"
        if " - " not in lines[i]:
            continue
        home, away = [t.strip() for t in lines[i].split(" - ", 1)]
        i += 1

        # Sledećih do 8 linija kvota
        vals: List[Optional[float]] = []
        take = 8
        j = 0
        while i < n and j < take and NUM_RE.match(lines[i]):
            vals.append(_to_float(lines[i]))
            i += 1
            j += 1

        # Preskoči meč ako ima manje od 2 realne kvote
        real_vals = [v for v in vals if v is not None]
        if len(real_vals) < 2:
            continue

        while len(vals) < 8:
            vals.append(None)

        q1, qx, q2, q_u02, q_3p, q_4p, q_gg, q_gg3 = vals[:8]

        record = {
            "time": time_s,
            "day": _day_from_date(date_s),
            "date": date_s,
            "league": "",  # ovaj RAW nema ligu u zaglavlju
            "home": home,
            "away": away,
            "match_id": "",
            "odds": {
                "1": q1, "X": qx, "2": q2,
                "0-2": q_u02,
                "2+": None,      # nema u ovom formatu
                "3+": q_3p,
                "4+": q_4p,      # dodatno – ispisujemo ako postoji
                "GG": q_gg,
                "IGG": None,
                "GG&3+": q_gg3
            }
        }
        out.append(record)

    return out

def _fmt(x: Optional[float]) -> str:
    if x is None:
        return "-"
    return str(int(x)) if float(x).is_integer() else f"{x}"

def write_pretty(blocks: List[Dict], out_path: Path):
    lines: List[str] = []
    for b in blocks:
        lines.append("=" * 70)
        header = f"{b['time']}  {b.get('day','')}  {b.get('date','')}".rstrip()
        if b.get("league"):
            header += f"  [{b['league']}]"
        lines.append(header)
        lines.append(f"{b['home']}  vs  {b['away']}")

        od = b["odds"]
        lines.append(f"1={_fmt(od.get('1'))}   X={_fmt(od.get('X'))}   2={_fmt(od.get('2'))}")
        lines.append(f"0-2={_fmt(od.get('0-2'))}   2+={_fmt(od.get('2+'))}   3+={_fmt(od.get('3+'))}")
        lines.append(f"GG={_fmt(od.get('GG'))}   IGG={_fmt(od.get('IGG'))}   GG&3+={_fmt(od.get('GG&3+'))}")
        if od.get("4+") is not None:
            lines.append(f"4+={_fmt(od.get('4+'))}")
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] Pretty sačuvan: {out_path.resolve()} (mečeva: {len(blocks)})")

# ===========================
# C) Main
# ===========================

def main():
    raw = fetch_and_copy_oktagon(headless=True)  # stavi True za rad bez prozora
    blocks = parse_oktagon(raw)
    write_pretty(blocks, PRETTY_TXT)

if __name__ == "__main__":
    main()
