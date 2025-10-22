# balkanbet_fixed_scroll.py  (BalkanBet — fiksni broj skrolova, bez "smart" logike)
# -*- coding: utf-8 -*-

import re
import time
from pathlib import Path
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ======= PODEŠAVANJA (menjaj ovde) =======
SCROLL_STEPS = 120        # ← tačno ovoliko skrol "klikova" će se izvršiti
HEADLESS = False          # ← True = headless; False = vidljivo
SCROLL_DELTA = 1500       # ← jačina točkića po koraku (pozitivan broj = naniže)
SCROLL_PAUSE = 0.20       # ← pauza između skrol koraka u sekundama
# =========================================

URL = "https://www.balkanbet.rs/sportsko-kladjenje/1-offer/18-fudbal"
ORIGIN = "https://www.balkanbet.rs"
OUT_TXT = Path("balkanbet_sledeci_mecevi.txt")       # RAW clipboard
OUT_PRETTY = Path("balkanbet_mecevi_pregled.txt")    # "soccer-like" izlaz

# -----------------------------
# POMOĆNE (cookie, čekanja, modali)
# -----------------------------

def accept_cookies(page) -> None:
    labels = [
        "Prihvatam", "Prihvatam sve", "Prihvati sve", "Slažem se",
        "Accept", "Accept all", "I agree", "U redu", "Ok"
    ]
    deadline = time.time() + 8
    while time.time() < deadline:
        for lbl in labels:
            try:
                page.get_by_role("button", name=re.compile(lbl, re.I)).click(timeout=700)
                time.sleep(0.3)
                return
            except Exception:
                pass
        try:
            page.locator("button:has-text('Prihv')").first.click(timeout=700)
            time.sleep(0.3)
            return
        except Exception:
            pass
        time.sleep(0.2)

def wait_networkidle_soft(page, ms: int = 1200):
    try:
        page.wait_for_load_state("networkidle", timeout=ms)
    except PWTimeoutError:
        pass

def dismiss_interstitials(page) -> bool:
    """Pokuša da zatvori iskačuće prozore ('Ne sada', 'Ne hvala', 'Kasnije', 'Zatvori', 'Close', '×')."""
    clicked = False
    attempts = [
        lambda: page.get_by_role("button", name=re.compile(r"^\s*Ne\s*sada\s*$", re.I)).click(timeout=400),
        lambda: page.locator("button:has-text('Ne sada')").first.click(timeout=400),
        lambda: page.get_by_role("button", name=re.compile(r"^\s*Ne\s*hvala\s*$", re.I)).click(timeout=400),
        lambda: page.locator("button:has-text('Ne hvala')").first.click(timeout=400),
        lambda: page.get_by_role("button", name=re.compile(r"^\s*Kasnije\s*$", re.I)).click(timeout=400),
        lambda: page.locator("button:has-text('Kasnije')").first.click(timeout=400),
        lambda: page.get_by_role("button", name=re.compile(r"^\s*Zatvori\s*$", re.I)).click(timeout=400),
        lambda: page.get_by_role("button", name=re.compile(r"^\s*Close\s*$", re.I)).click(timeout=400),
        lambda: page.locator("button[aria-label*='Zatvori'], button[aria-label*='Close']").first.click(timeout=400),
        lambda: page.locator("button:has-text('×'), .modal button:has-text('×'), .popup button:has-text('×')").first.click(timeout=400),
    ]
    for fn in attempts:
        try:
            fn()
            clicked = True
            time.sleep(0.15)
        except Exception:
            continue
    if clicked:
        wait_networkidle_soft(page, 800)
    return clicked

def force_close_overlays(page) -> None:
    """
    Dodatno zatvaranje pre kopiranja:
    - pokuša 'Ne sada/Ne hvala/Kasnije', X/Close
    - pritisne Escape
    - poslednje: sakrije tipične modal/pop-up elemente
    """
    try:
        dismissed = dismiss_interstitials(page)

        try:
            page.keyboard.press("Escape")
            time.sleep(0.1)
        except Exception:
            pass

        dismissed2 = dismiss_interstitials(page)

        if not (dismissed or dismissed2):
            js_hide = """
            (() => {
              const sel = [
                "[role=dialog]", ".modal", ".popup", ".popover",
                ".overlay", ".cookie", ".gdpr", ".subscribe", ".newsletter",
                ".modal-backdrop", "[class*='modal']", "[class*='popup']",
                "[id*='modal']", "[id*='popup']"
              ].join(',');
              document.querySelectorAll(sel).forEach(el => {
                try { el.style.setProperty('display', 'none', 'important'); } catch(e) {}
              });
            })();
            """
            try:
                page.evaluate(js_hide)
            except Exception:
                pass

        wait_networkidle_soft(page, 800)
    except Exception:
        pass

def early_dismiss(page, window_ms: int = 4000):
    """
    U prvih `window_ms` ms posle otvaranja, agresivno pokušava da zatvori
    'Ne sada' / 'Ne hvala' / 'Kasnije' / 'Zatvori' prozore.
    """
    deadline = time.time() + (window_ms / 1000.0)
    while time.time() < deadline:
        try:
            dismiss_interstitials(page)
            try:
                page.keyboard.press("Escape")
            except Exception:
                pass
            time.sleep(0.15)
        except Exception:
            time.sleep(0.15)
    wait_networkidle_soft(page, 800)

def click_Engleska1(page) -> None:
    """Sačekaj 3s i klikni na 'Engleska 1' (više selektora)."""
    time.sleep(3.0)
    attempts = [
        lambda: page.get_by_role("button", name=re.compile(r"^\s*Engleska\s*1\s*$", re.I)).click(timeout=1500),
        lambda: page.get_by_role("link",   name=re.compile(r"^\s*Engleska\s*1\s*$", re.I)).click(timeout=1500),
        lambda: page.locator("button:has-text('Engleska 1')").first.click(timeout=1500),
        lambda: page.locator("a:has-text('Engleska 1')").first.click(timeout=1500),
        lambda: page.locator(":text('Engleska 1')").first.click(timeout=1500),
    ]
    for fn in attempts:
        try:
            fn()
            wait_networkidle_soft(page, 2000)
            time.sleep(0.4)
            return
        except Exception:
            continue

# -----------------------------
# FIKSNO SKROLOVANJE (bez "smart")
# -----------------------------

def do_fixed_scroll(page,
                    steps: int,
                    delta: int = SCROLL_DELTA,
                    pause: float = SCROLL_PAUSE) -> int:
    """
    Jednostavno skrolovanje: tačno 'steps' puta okrene točkić miša naniže.
    Nema bounce-a, nema merenja visine, nema heuristike.
    """
    # idi na vrh strane i fokusiraj centar viewport-a (za svaki slučaj)
    try:
        page.evaluate("window.scrollTo(0, 0)")
    except Exception:
        pass
    try:
        vp = page.viewport_size or {"width": 1200, "height": 800}
        page.mouse.move(vp["width"] // 2, vp["height"] // 2)
    except Exception:
        pass

    done = 0
    for _ in range(max(0, int(steps))):
        try:
            page.mouse.wheel(0, delta)
            done += 1
        except Exception:
            time.sleep(pause)
        time.sleep(pause)
    return done

def _click_center_of_viewport(page) -> None:
    """Jedan klik u sredinu ekrana (centar viewport-a)."""
    try:
        vp = page.viewport_size or {"width": 1200, "height": 800}
        x = vp["width"] // 2
        y = vp["height"] // 2
        page.mouse.move(x, y)
        page.mouse.click(x, y)
    except Exception:
        pass

def copy_all_via_keyboard(page) -> str:
    """
    Redosled:
      Ctrl+A  -> čekaj 1s -> 'Ne sada' -> KLIK U SREDINU EKRANA -> ponovo Ctrl+A -> Ctrl+C
    (mac fallback: Meta+A/C). Zatim čita clipboard.
    """
    # fokus na body
    try:
        page.locator("body").click(position={"x": 10, "y": 10}, timeout=1500)
    except Exception:
        try:
            page.click("body", timeout=1500)
        except Exception:
            pass

    # 1) Ctrl+A
    try:
        page.keyboard.press("Control+A")
    except Exception:
        pass

    # 2) čekaj 1s
    time.sleep(1.0)

    # 3) klik 'Ne sada' (ako postoji)
    dismiss_interstitials(page)

    # 4) klik u sredinu ekrana
    _click_center_of_viewport(page)
    time.sleep(0.3)

    # 5) ponovo Ctrl+A
    try:
        page.keyboard.press("Control+A")
    except Exception:
        pass
    time.sleep(0.3)

    # 6) Ctrl+C
    try:
        page.keyboard.press("Control+C")
    except Exception:
        pass

    # macOS fallback
    time.sleep(0.25)
    try:
        page.keyboard.press("Meta+A")
        time.sleep(0.3)
        page.keyboard.press("Meta+C")
    except Exception:
        pass

    time.sleep(0.25)

    # Clipboard API
    try:
        txt = page.evaluate("() => navigator.clipboard.readText()")
        if isinstance(txt, str) and txt.strip():
            return txt
    except Exception:
        pass

    # fallback — innerText
    try:
        return page.locator("body").inner_text()
    except Exception:
        return ""

# -----------------------------
# PARSER RAW (BalkanBet) -> PRETTY
# -----------------------------

DAY_MAP = {
    "PON": "Pon", "UTO": "Uto", "SRE": "Sre",
    "ČET": "Čet", "CET": "Čet", "PET": "Pet",
    "SUB": "Sub", "NED": "Ned",
}
RE_DAY_HEADER = re.compile(r"^\s*([A-ZČĆŠĐŽ]{3})\.\s+(\d{2})\.(\d{2})\.(\d{4})\s*$", re.I)
RE_TIME = re.compile(r"^([01]?\d|2[0-3]):[0-5]\d$")
RE_ID = re.compile(r"^\+(\d+)\s*$")

def _to_float(s: str) -> Optional[float]:
    s = s.strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def parse_balkanbet_raw_to_blocks(raw: str) -> List[Dict]:
    """
    Po uzorku očekuje:
      <LEAGUE>
      <DAY HEADER>  npr "NED. 19.10.2025"
      (linija sa nazivima kolona) 1, X, 2, 2+, 3+, 4+, 0-2, GG, GG&3+, GG&4+, I GG
      <time>
      <Home> - <Away>
      11 kvota u istom redosledu
      +ID
    """
    lines = [ln.strip().replace("\xa0", " ") for ln in raw.splitlines()]
    lines = [ln for ln in lines if ln]

    current_league = ""
    cur_day = ""
    cur_date = ""
    i, n = 0, len(lines)
    out: List[Dict] = []

    def set_day_from_header(header_line: str):
        nonlocal cur_day, cur_date
        m = RE_DAY_HEADER.match(header_line)
        if not m:
            return False
        dcode, dd, mm, yyyy = m.groups()
        dcode = dcode.upper()
        cur_day = DAY_MAP.get(dcode, "")
        cur_date = f"{dd}.{mm}."
        return True

    while i < n:
        ln = lines[i]

        # liga / zaglavlja kolona
        if not RE_TIME.match(ln) and not RE_DAY_HEADER.match(ln) and not RE_ID.match(ln):
            if ln in {"1","X","2","2+","3+","4+","0-2","GG","GG&3+","GG&4+","I GG"}:
                i += 1
                continue
            lookahead = lines[i+1] if i+1 < n else ""
            if RE_DAY_HEADER.match(lookahead) or lookahead in {"1","X","2","2+","3+","4+","0-2","GG","GG&3+","GG&4+","I GG"}:
                current_league = ln
                i += 1
                continue

        if RE_DAY_HEADER.match(ln):
            set_day_from_header(ln)
            i += 1
            while i < n and lines[i] in {"1","X","2","2+","3+","4+","0-2","GG","GG&3+","GG&4+","I GG"}:
                i += 1
            continue

        if RE_TIME.match(ln):
            time_s = ln
            i += 1
            if i >= n:
                break

            teams_line = lines[i]
            i += 1

            home, away = "", ""
            mt = re.match(r"(.+?)\s*-\s*(.+)", teams_line)
            if mt:
                home = mt.group(1).strip()
                away = mt.group(2).strip()
            else:
                mt2 = re.match(r"(.+?)\s+vs\s+(.+)", teams_line, re.I)
                if mt2:
                    home = mt2.group(1).strip()
                    away = mt2.group(2).strip()
                else:
                    continue

            nums: List[float] = []
            while i < n and len(nums) < 11:
                if RE_TIME.match(lines[i]) or RE_DAY_HEADER.match(lines[i]):
                    break
                if RE_ID.match(lines[i]):
                    break
                v = _to_float(lines[i])
                if v is not None:
                    nums.append(v)
                i += 1

            match_id = ""
            if i < n and RE_ID.match(lines[i]):
                match_id = RE_ID.match(lines[i]).group(1)
                i += 1

            nums += [None] * (11 - len(nums))
            q1, qx, q2, q2p, q3p, q4p, q02, qgg, qgg3, qgg4, qigg = nums[:11]

            odds = {
                "1": q1, "X": qx, "2": q2,
                "0-2": q02,
                "2+": q2p,
                "3+": q3p,
                "4+": q4p,
                "GG": qgg,
                "IGG": qigg,
                "GG&3+": qgg3,
                "GG&4+": qgg4,
            }

            out.append({
                "time": time_s,
                "day": cur_day,
                "date": cur_date,
                "league": current_league,
                "home": home,
                "away": away,
                "match_id": match_id,
                "odds": odds,
            })
            continue

        i += 1

    return out

def _fmt(x: Optional[float]) -> str:
    if x is None:
        return "-"
    return str(int(x)) if float(x).is_integer() else f"{x}"

def write_pretty_balkanbet(blocks: List[Dict], out_path: Path):
    lines: List[str] = []
    for b in blocks:
        lines.append("=" * 70)
        league_tag = f"[{b['league']}]" if b.get("league") else ""
        header = f"{b['time']}  {b['day']}  {b['date']}  {league_tag}".rstrip()
        lines.append(header)

        id_part = f"   (ID: {b['match_id']})" if b.get("match_id") else ""
        lines.append(f"{b['home']}  vs  {b['away']}{id_part}")

        od = b["odds"]
        lines.append(f"1={_fmt(od.get('1'))}   X={_fmt(od.get('X'))}   2={_fmt(od.get('2'))}")
        lines.append(f"0-2={_fmt(od.get('0-2'))}   2+={_fmt(od.get('2+'))}   3+={_fmt(od.get('3+'))}")
        lines.append(f"GG={_fmt(od.get('GG'))}   IGG={_fmt(od.get('IGG'))}   GG&3+={_fmt(od.get('GG&3+'))}")
        if od.get("GG&4+") is not None:
            lines.append(f"GG&4+={_fmt(od.get('GG&4+'))}")
        if od.get("4+") is not None:
            lines.append(f"4+={_fmt(od.get('4+'))}")
    out_path.write_text("\n".join(lines), encoding="utf-8")

# -----------------------------
# GLAVNI TOK
# -----------------------------

def main():
    # 1) Skrol (fiksni koraci) + RAW preuzimanje (clipboard)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
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
            # Otvori stranicu
            page.goto(URL, wait_until="domcontentloaded", timeout=60000)

            # ODMAH zatvori "Ne sada"/pop-up prozorčiće
            early_dismiss(page, window_ms=5000)

            # Cookies (ako postoji) — pa opet zatvori ako nešto iskoči
            accept_cookies(page)
            early_dismiss(page, window_ms=2500)

            # (opciono) filtriraj ligu
            click_Engleska1(page)

            # FIKSNO SKROLOVANJE (bez smart heuristike)
            down = do_fixed_scroll(page, steps=SCROLL_STEPS, delta=SCROLL_DELTA, pause=SCROLL_PAUSE)
            print(f"[i] Down koraka (wheel): {down}")

            # Vrati na vrh i JOŠ JEDNOM osiguraj da ništa ne prekriva stranu
            try:
                page.evaluate("window.scrollTo(0,0)")
            except Exception:
                pass
            early_dismiss(page, window_ms=1500)

            # Kopiranje teksta: Ctrl+A -> 'Ne sada' -> klik centar -> Ctrl+A -> Ctrl/Cmd+C
            copied = copy_all_via_keyboard(page)
            OUT_TXT.write_text(copied, encoding="utf-8")
            print(f"[OK] RAW (clipboard) sačuvan u: {OUT_TXT.resolve()}")
        finally:
            browser.close()

    # 2) Parsiranje RAW -> Pretty
    raw = OUT_TXT.read_text(encoding="utf-8", errors="ignore")
    blocks = parse_balkanbet_raw_to_blocks(raw)
    write_pretty_balkanbet(blocks, OUT_PRETTY)
    print(f"[OK] Pretty: {OUT_PRETTY.resolve()}  (mečeva: {len(blocks)})")

if __name__ == "__main__":
    main()
