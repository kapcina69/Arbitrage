# soccer_text_30scrolls_Vremenska_bounce.py
# -*- coding: utf-8 -*-

import re
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

URL = "https://www.betole.com/sport/S/calendar"
OUT_TXT_RAW = Path("betole_sledeci_mecevi.txt")
OUT_TXT_PRETTY = Path("betole_mecevi_pregled.txt")

# -----------------------------
# Playwright deo (skrolovanje)
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

# Pronađi najlogičniji skrol-panel (traži i kroz shadow DOM).
FIND_SCROLLABLE_JS = """
() => {
  const deepCollect = (root) => {
    const out = [];
    const walk = (node) => {
      if (!node) return;
      if (node.nodeType === 1) out.push(node);
      const kids = node.children || [];
      for (const k of kids) walk(k);
      if (node.shadowRoot) {
        const all = node.shadowRoot.querySelectorAll('*');
        for (const el of all) out.push(el);
      }
    };
    walk(document.documentElement);
    return out;
  };
  const canScroll = (el) => {
    try {
      const r = el.getBoundingClientRect();
      if (r.height < 160 || r.width < 240) return false;
      return (el.scrollHeight || 0) > (el.clientHeight || 0) + 4;
    } catch { return false; }
  };
  const looksLikeMatches = (el) => {
    const t = (el.innerText || "").toLowerCase();
    return t.includes(" 1 ") || t.includes(" x ") || t.includes(" 2 ")
        || t.includes("utakm") || t.includes("meč") || t.includes("gol")
        || /\\b\\d{1,2}:\\d{2}\\b/.test(t);
  };
  const all = deepCollect(document);
  let cands = [];
  for (const el of all) if (el instanceof Element && canScroll(el) && looksLikeMatches(el)) cands.push(el);
  if (!cands.length) for (const el of all) if (el instanceof Element && canScroll(el)) cands.push(el);
  if (!cands.length) return null;
  cands.sort((a,b)=>{
    const ra=a.getBoundingClientRect(), rb=b.getBoundingClientRect();
    const da=(a.scrollHeight-a.clientHeight)*(ra.width*ra.height);
    const db=(b.scrollHeight-b.clientHeight)*(rb.width*rb.height);
    return db-da;
  });
  return cands[0];
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

def click_Vremenska(page) -> None:
    """
    Klik na 'Vremenska' (dugme/link). Probamo više selektora.
    """
    attempts = [
        lambda: page.get_by_role("button", name=re.compile(r"^\s*Vremenska\s*$", re.I)).click(timeout=1500),
        lambda: page.get_by_role("link",   name=re.compile(r"^\s*Vremenska\s*$", re.I)).click(timeout=1500),
        lambda: page.locator("button:has-text('Vremenska')").first.click(timeout=1500),
        lambda: page.locator("a:has-text('Vremenska')").first.click(timeout=1500),
        lambda: page.locator(":text('Vremenska')").first.click(timeout=1500),
    ]
    for fn in attempts:
        try:
            fn()
            try:
                page.wait_for_load_state("networkidle", timeout=2000)
            except PWTimeoutError:
                pass
            time.sleep(0.4)
            return
        except Exception:
            continue
    # ako ne nađe, moguće da je već aktivno

def do_30_down_with_bounce(page, container_handle=None, pause=0.45,
                           delta_down=1500, delta_up=-1200, bounce_every=4):
    """
    TAČNO 30 skrolova NA DOLE; posle svakih 'bounce_every' down skrolova,
    uradi JEDAN skrol NA GORE (koji se ne broji u 30).
    """
    if container_handle:
        try:
            box = page.evaluate(
                "(e)=>{const r=e.getBoundingClientRect();return {x:r.left + r.width/2, y:r.top + Math.min(r.height/2, r.height-30)};}",
                container_handle
            )
            page.mouse.move(box["x"], box["y"])
            try:
                container_handle.click(force=True)
            except Exception:
                pass
        except Exception:
            container_handle = None

    down_done = 0
    while down_done < 30:
        if container_handle:
            page.mouse.wheel(0, delta_down)
        else:
            page.evaluate("window.scrollBy(0, Math.max(window.innerHeight, 600))")

        down_done += 1
        time.sleep(pause)
        try:
            page.wait_for_load_state("networkidle", timeout=int(pause*1000))
        except PWTimeoutError:
            pass

        if down_done % bounce_every == 0:
            if container_handle:
                page.mouse.wheel(0, delta_up)
            else:
                page.evaluate("window.scrollBy(0, -Math.max(window.innerHeight, 600))")
            time.sleep(max(0.25, pause - 0.1))
            try:
                page.wait_for_load_state("networkidle", timeout=int(pause*1000))
            except PWTimeoutError:
                pass

# -----------------------------
# Parser + formatiranje
# -----------------------------

DAY_NAMES_SR = ["Pon", "Uto", "Sre", "Čet", "Pet", "Sub", "Ned"]

LEAGUE_RE     = re.compile(r"^[A-ZŽĐŠČĆa-zžđšćč .,'/-]+,\s*[A-ZŽĐŠČĆa-zžđšćč ./'-]+$")
ID_RE         = re.compile(r"^\+\d+$")
DATE_TIME_RE  = re.compile(r"^(\d{1,2}\.\d{1,2}\.)\s+(\d{1,2}:\d{2})$")

def _day_from_date(date_str: str) -> str:
    s = date_str.replace("\xa0", " ").strip().rstrip(".")
    if not s:
        return ""
    parts = s.split(".")
    try:
        if len(parts) >= 2:
            d = int(parts[0]); m = int(parts[1])
            y = datetime.now().year
            dt = datetime(year=y, month=m, day=d)
            return DAY_NAMES_SR[dt.weekday()]
    except Exception:
        return ""
    return ""

def _to_float(s: str) -> Optional[float]:
    try:
        return float(s.replace(",", ".").replace("\xa0", " ").strip())
    except Exception:
        return None

def _fmt(x: Optional[float]) -> str:
    if x is None:
        return "-"
    return str(int(x)) if float(x).is_integer() else f"{x}"

def parse_betole_raw(text: str) -> List[Dict]:
    """
    Očekivani obrazac:
      <LEAGUE>                 npr. "Guatemala, Primera Division" (opciono; važi do promene)
      +ID
      Home
      Away
      DD.MM. HH:MM
      ki 1        <odd>
      ki X        <odd>
      ki 2        <odd>
      manje 2.5   <odd>   -> 0-2
      više 2.5    <odd>   -> 3+   (ISPRAVKA)
    """
    lines = [ln.strip().replace("\xa0", " ") for ln in text.splitlines()]
    lines = [ln for ln in lines if ln and ln.strip()]

    current_league = ""
    i, n = 0, len(lines)
    out: List[Dict] = []

    while i < n:
        ln = lines[i]

        # Liga (sekcijski naslov)
        if LEAGUE_RE.match(ln):
            current_league = ln
            i += 1
            continue

        # Početak meča -> +ID
        if not ID_RE.match(ln):
            i += 1
            continue

        match_id = ln[1:]
        i += 1
        if i + 2 >= n:
            break

        home = lines[i]; i += 1
        away = lines[i]; i += 1

        # Datum + vreme
        if i >= n or not DATE_TIME_RE.match(lines[i]):
            i += 1
            continue
        mdt = DATE_TIME_RE.match(lines[i])
        date_s = mdt.group(1) if mdt else ""
        time_s = mdt.group(2) if mdt else ""
        i += 1

        odds = {"1": None, "X": None, "2": None, "0-2": None, "2+": None, "3+": None,
                "GG": None, "IGG": None, "GG&3+": None}

        # Čitaj parove labela i vrednosti dok ne naiđe novi blok
        while i + 1 < n:
            label = lines[i].lower()
            val_line = lines[i + 1]

            # Kraj bloka ako nailazi nešto što liči na novi meč/ligu/datum
            if ID_RE.match(label) or LEAGUE_RE.match(label) or DATE_TIME_RE.match(label):
                break

            val = _to_float(val_line)

            if label == "ki 1":
                odds["1"] = val
            elif label == "ki x":
                odds["X"] = val
            elif label == "ki 2":
                odds["2"] = val
            elif label.startswith("manje") and "2.5" in label:
                odds["0-2"] = val
            elif label.startswith("više") and "2.5" in label:
                odds["3+"] = val  # *** ispravljeno: 3+ ***
            # dodatne mape lako proširljive ovde

            i += 2

        out.append({
            "time": time_s,
            "day": _day_from_date(date_s),
            "date": date_s,
            "league": current_league,
            "home": home,
            "away": away,
            "match_id": match_id,
            "odds": odds
        })

    return out

def write_pretty(blocks: List[Dict], out_path: Path):
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
        lines.append(f"0-2={_fmt(od.get('0-2'))}   2={_fmt(od.get('2+'))}   3+={_fmt(od.get('3+'))}".replace(" 2=", " 2+="))
        lines.append(f"GG={_fmt(od.get('GG'))}   IGG={_fmt(od.get('IGG'))}   GG&3+={_fmt(od.get('GG&3+'))}")
    out_path.write_text("\n".join(lines), encoding="utf-8")

# -----------------------------
# Glavni tok
# -----------------------------

def main(headless=True):
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

            time.sleep(0.8)
            try:
                page.wait_for_load_state("networkidle", timeout=1200)
            except PWTimeoutError:
                pass

            click_Vremenska(page)

            inner = find_inner_scroll_container(page)
            do_30_down_with_bounce(page, inner, pause=0.45,
                                   delta_down=1500, delta_up=-1200, bounce_every=4)

            try:
                page.evaluate("window.scrollTo(0,0)")
            except Exception:
                pass

            # snimi RAW tekst
            body_text = page.locator("body").inner_text()
            OUT_TXT_RAW.write_text(body_text, encoding="utf-8")
            print(f"[OK] Tekst sačuvan u: {OUT_TXT_RAW.resolve()}")

        finally:
            browser.close()

    # --- PARSIRANJE + LEPI IZLAZ ---
    raw = OUT_TXT_RAW.read_text(encoding="utf-8", errors="ignore")
    blocks = parse_betole_raw(raw)
    write_pretty(blocks, OUT_TXT_PRETTY)
    print(f"[OK] Formatirano: {OUT_TXT_PRETTY.resolve()}  (ukupno mečeva: {len(blocks)})")

if __name__ == "__main__":
    # za “uživo” praćenje, stavi headless=False
    main(headless=False)
