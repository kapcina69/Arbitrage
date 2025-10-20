# brazil.py
# -*- coding: utf-8 -*-
#
# BrazilBet → RAW tekst → PARSE u "soccer pretty" format.
# Fokus: format iz brazil_sledeci_mecevi.txt (liga → [int] → vreme → home → away → [int] → label/kvota parovi).

import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

URL = "https://www.brazilbet.rs/sr/vremenska-ponuda"
OUT_TXT = Path("brazil_sledeci_mecevi.txt")        # RAW (ceo tekst stranice)
OUT_PRETTY = Path("brazil_mecevi_pregled.txt")     # Sređen pregled

# -----------------------------
# Skrol (po potrebi – možeš preskočiti i ručno nalepiti RAW)
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
                time.sleep(0.3); return
            except Exception:
                pass
        try:
            page.locator("button:has-text('Prihv')").first.click(timeout=700)
            time.sleep(0.3); return
        except Exception:
            pass
        time.sleep(0.2)

FIND_SCROLLABLE_JS = """
() => {
  const deepCollect = (root) => {
    const out = [];
    const walk = (n) => {
      if (!n) return;
      if (n.nodeType === 1) out.push(n);
      for (const k of (n.children||[])) walk(k);
      if (n.shadowRoot) for (const el of n.shadowRoot.querySelectorAll('*')) out.push(el);
    };
    walk(document.documentElement);
    return out;
  };
  const canScroll = (el) => {
    try {
      const r = el.getBoundingClientRect();
      if (r.height < 160 || r.width < 240) return false;
      return (el.scrollHeight||0) > (el.clientHeight||0) + 4;
    } catch { return false; }
  };
  const looks = (el) => {
    const t = (el.innerText||"").toLowerCase();
    return t.includes("fudbal") || t.includes("football") || /\\b([01]?\\d|2[0-3]):[0-5]\\d\\b/.test(t);
  };
  const all = deepCollect(document);
  const c = all.filter(el => el instanceof Element && canScroll(el) && looks(el));
  if (!c.length) return null;
  c.sort((a,b)=>{
    const ra=a.getBoundingClientRect(), rb=b.getBoundingClientRect();
    const da=(a.scrollHeight-a.clientHeight)*(ra.width*ra.height);
    const db=(b.scrollHeight-b.clientHeight)*(rb.width*rb.height);
    return db-da;
  });
  return c[0];
}
"""

def find_inner_scroll_container(page):
    try:
        h = page.evaluate_handle(FIND_SCROLLABLE_JS)
        if page.evaluate("(el)=>el!==null", h):
            return h
    except Exception:
        pass
    return None

def do_down_scrolls(page, container_handle=None, steps=20, pause=0.35, wheel_down=1400, bounce_every=4, wheel_up=-1100):
    # pozicioniraj miš na panel ili viewport
    if container_handle:
        try:
            box = page.evaluate("(e)=>{const r=e.getBoundingClientRect();return {x:r.left+r.width/2,y:r.top+Math.min(r.height/2,r.height-30)};}", container_handle)
            page.mouse.move(box["x"], box["y"])
            try: container_handle.click(force=True)
            except Exception: pass
        except Exception:
            container_handle = None
    else:
        try:
            vp = page.viewport_size or {"width": 1200, "height": 800}
            page.mouse.move(vp["width"]//2, min(vp["height"]//2, vp["height"]-30))
        except Exception:
            pass

    done = 0
    while done < steps:
        if container_handle:
            page.mouse.wheel(0, wheel_down)
        else:
            page.evaluate("window.scrollBy(0, Math.max(window.innerHeight, 600))")
        done += 1
        time.sleep(pause)
        try: page.wait_for_load_state("networkidle", timeout=int(pause*1000))
        except PWTimeoutError: pass

        if bounce_every and done % bounce_every == 0 and done < steps:
            if container_handle:
                page.mouse.wheel(0, wheel_up)
            else:
                page.evaluate("window.scrollBy(0, -Math.max(window.innerHeight, 600))")
            time.sleep(max(0.25, pause-0.1))
            try: page.wait_for_load_state("networkidle", timeout=int(pause*1000))
            except PWTimeoutError: pass

# -----------------------------
# PARSER RAW → Pretty (format iz fajla)
# -----------------------------

# Liga je oblika: "COLOMBIA, Primera B - Clausura" (dozvoli slova, zarez, crticu)
LEAGUE_RE = re.compile(r"^[A-Za-zŽĐŠČĆžđšćč .,'/()-]+,\s*[A-Za-zŽĐŠČĆžđšćč .,'/()-]+\s-\s[A-Za-zŽĐŠČĆžđšćč .,'/()-]+$")
TIME_RE   = re.compile(r"^([01]?\d|2[0-3]):[0-5]\d$")
INT_RE    = re.compile(r"^\d+$")
PLUS_ID   = re.compile(r"^\+\d+$")

LABELS = {"1","X","2","0-2","2+","3+"}

SKIP_HARD = {
    # Navigacija/sekcije koje se pojavljuju u RAW-u – ignorisati
    "KLAĐENJE","UŽIVO KLAĐENJE","SLOT","IGRE UŽIVO","VIRTUELNO KLAĐENJE","AVIATOR","REZULTATI",
    "STATUS TIKETA","PROMO","TURNIRI","Prijava","Registruj se","Svi Top mečevi","Vremenska ponuda","Bonus tip",
    "Danas","Sutra","1:00","23:59","1:00 - 23:59","FUDBAL","KOŠARKA","STRELCI FUDBAL","TENIS","HOKEJ","RUKOMET",
    "ODBOJKA","STONI TENIS","AMERIČKI FUDBAL","SPECIJAL NFL","SNUKER","FUTSAL","PIKADO","BEJZBOL","ESPORT",
    "FUDBAL VIRTUAL","TENIS VIRTUAL","POBEDNICI","GOLOVI PO LIGAMA","KONAČAN ISHOD","UKUPNO GOLOVA",
    "SPORT","SPORTS","MATCHES","MEČEVI","ISTAKNUTA TAKMIČENJA","OMILJENE LIGE",
    "Odigrani tiketi","Klikni na kvotu i pokreni","Srpski","O nama","Odgovorno klađenje","Kontakt",
    "Pomoć","Pravila igre","Preuzmi","Uslovi uplate karticama","Opis igara"
}

def _to_float(s: str) -> Optional[float]:
    s = s.strip()
    if s == "-" or not s:
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def _is_time(s: str) -> bool:
    return bool(TIME_RE.fullmatch(s.strip()))

def _read_trimmed_text(path: Path, trim_last: int = 80) -> str:
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if len(lines) > trim_last:
        lines = lines[:-trim_last]
    return "\n".join(lines)

def parse_brazil_raw(text: str) -> List[Dict]:
    """
    Očekivani tok po meču (primer iz RAW-a):
        LIGA
        (opciono) int (kod)
        HH:MM
        Home
        Away
        (opciono) int (match_id)
        1 / <kvota>
        X / <kvota>
        2 / <kvota>
        0-2 / <kvota>
        2+ / <kvota>
        3+ / <kvota>
    Sledeća LIGA signalizira novu sekciju. Usput se pojavljuju i 'plus ID' (+1151 …) – ignorišemo ih.
    """
    lines = [ln.replace("\xa0", " ").strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln and ln not in SKIP_HARD]

    out: List[Dict] = []
    n = len(lines)
    i = 0
    current_league = ""

    def take_label_value(i: int, lab: str) -> Tuple[Optional[float], int]:
        """Ako je lines[i] == lab, vrati float sa lines[i+1], inače (None, i)."""
        if i < n and lines[i] == lab:
            if i+1 < n:
                return _to_float(lines[i+1]), i+2
            return None, i+1
        return None, i

    while i < n:
        ln = lines[i]

        # 1) Liga
        if LEAGUE_RE.match(ln):
            current_league = ln
            i += 1
            # preskoči eventualni int posle lige (liga kod)
            if i < n and INT_RE.fullmatch(lines[i]):
                i += 1
            continue

        # 2) Vreme
        if not _is_time(ln):
            i += 1
            continue
        time_s = ln
        i += 1
        if i >= n:
            break

        # 3) Timovi (sledeće dve linije)
        #   U nekim slučajevima postoji “datum” tipa 22.10 – u tvom fajlu ga nema između vremena i timova,
        #   ali ako naidje nešto tipa DD.MM, samo ga preskoči (nije label/kvota).
        if i < n and re.fullmatch(r"\d{1,2}\.\d{1,2}\.?", lines[i]):
            i += 1

        if i + 1 >= n:
            break
        home = lines[i]; away = lines[i+1]
        i += 2

        # 4) Opcioni int kao match_id
        match_id = ""
        if i < n and INT_RE.fullmatch(lines[i]):
            match_id = lines[i]
            i += 1

        # 5) Labelirane kvote
        odds = {"1": None, "X": None, "2": None, "0-2": None, "2+": None, "3+": None}

        # Moguća je pojava “+1234” – ignoriši
        while i < n and PLUS_ID.fullmatch(lines[i]):
            i += 1

        # Uzima redom 1, X, 2, 0-2, 2+, 3+ — ali tolerantno:
        for lab in ("1","X","2","0-2","2+","3+"):
            val, i = take_label_value(i, lab)
            odds[lab] = val
            # preskoči eventualne zalutale "+id" između labela
            while i < n and PLUS_ID.fullmatch(lines[i]):
                i += 1
            # stop ako smo stigli do sledeće sekcije (nova liga ili vreme)
            if i < n and (LEAGUE_RE.match(lines[i]) or _is_time(lines[i])):
                break

        out.append({
            "time": time_s,
            "day": "",
            "date": "",
            "league": current_league,
            "home": home,
            "away": away,
            "match_id": match_id,
            "odds": odds
        })

    return out

def write_pretty(blocks: List[Dict], out_path: Path):
    def fmt(x: Optional[float]) -> str:
        if x is None:
            return "-"
        return str(int(x)) if float(x).is_integer() else f"{x}"

    lines: List[str] = []
    for b in blocks:
        lines.append("=" * 70)
        header = b["time"]
        if b.get("day"):  header += f"  {b['day']}"
        if b.get("date"): header += f"  {b['date']}"
        if b.get("league"):
            header += f"  [{b['league']}]"
        lines.append(header)

        id_part = f"   (ID: {b['match_id']})" if b.get("match_id") else ""
        lines.append(f"{b['home']}  vs  {b['away']}{id_part}")

        od = b["odds"]
        lines.append(f"1={fmt(od.get('1'))}   X={fmt(od.get('X'))}   2={fmt(od.get('2'))}")
        lines.append(f"0-2={fmt(od.get('0-2'))}   2+={fmt(od.get('2+'))}   3+={fmt(od.get('3+'))}")
        # Brazil RAW nema GG/IGG/GG&3+ u ovom modu — ostavi crte zbog uniformnosti:
        lines.append("GG=-   IGG=-   GG&3+=-")
    out_path.write_text("\n".join(lines), encoding="utf-8")

# -----------------------------
# Glavni tok
# -----------------------------

def main(headless=True):
    # 1) (Opc.) Skrol i snimi RAW – možeš preskočiti ako već imaš fajl
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            locale="sr-RS",
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"),
            viewport={"width": 1440, "height": 1100},
        )
        page = context.new_page()
        try:
            page.goto(URL, wait_until="domcontentloaded", timeout=60000)
            accept_cookies(page)
            try:
                page.wait_for_load_state("networkidle", timeout=1500)
            except PWTimeoutError:
                pass
            inner = find_inner_scroll_container(page)
            do_down_scrolls(page, inner, steps=80, pause=0.3)

            try: page.evaluate("window.scrollTo(0,0)")
            except Exception: pass

            raw = page.locator("body").inner_text()
            OUT_TXT.write_text(raw, encoding="utf-8")
            print(f"[OK] RAW snimljen: {OUT_TXT.resolve()}")
        finally:
            browser.close()

    # 2) Parsiranje (odbaci poslednjih ~80 linija – footer/popup)
    raw_trim = _read_trimmed_text(OUT_TXT, trim_last=80)
    blocks = parse_brazil_raw(raw_trim)
    write_pretty(blocks, OUT_PRETTY)
    print(f"[OK] Pretty: {OUT_PRETTY.resolve()} (mečeva: {len(blocks)})")

if __name__ == "__main__":
    main(headless=True)
