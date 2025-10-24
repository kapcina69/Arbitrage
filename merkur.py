# merkur_text_20scrolls_parse.py
# -*- coding: utf-8 -*-

import re
import time
import csv
from pathlib import Path
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

URL = "https://www.merkurxtip.rs/sr/sportsko-kladjenje/sledeci-mecevi"
RAW_TXT = Path("merkur_sledeci_mecevi.txt")
OUT_CSV = Path("merkur_mecevi_format.csv")
OUT_TXT = Path("merkur_mecevi_pregled.txt")

# --- Playwright deo: 20 skrolova + snimanje teksta ---

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

FIND_SCROLLABLE_JS = """
() => {
  // Pronađi najlogičniji skrolabilni kontejner (uklj. shadow DOM)
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
           || t.includes("utakm") || t.includes("meč") || t.includes(" 03:") || t.includes(" 02:");
  };
  const all = deepCollect(document);
  let cands = [];
  for (const el of all) if (el instanceof Element && canScroll(el) && looksLikeMatches(el)) cands.push(el);
  if (!cands.length) for (const el of all) if (canScroll(el)) cands.push(el);
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
        handle = page.evaluate_handle(FIND_SCROLLABLE_JS)
        if page.evaluate("(el)=>el!==null", handle):
            return handle
    except Exception:
        pass
    return None

def do_20_scrolls(page, container_handle=None, pause=0.45):
    # Ako nađemo panel, skrolujemo točkićem baš preko njega;
    # inače skrolujemo ceo page.
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

    # (ostavljam veći broj “koraka” jer stranica često lenjo učitava)
    for _ in range(100):  # prvobitno 20; praktično je više koraka
        if container_handle:
            page.mouse.wheel(0, 1500)
        else:
            page.evaluate("window.scrollBy(0, Math.max(window.innerHeight, 600))")
        time.sleep(pause)
        try:
            page.wait_for_load_state("networkidle", timeout=int(pause*1000))
        except PWTimeoutError:
            pass

def capture_text_after_20_scrolls(headless: bool = True):
    from playwright.sync_api import sync_playwright
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

            inner = find_inner_scroll_container(page)
            do_20_scrolls(page, inner, pause=0.45)

            try:
                page.evaluate("window.scrollTo(0,0)")
            except Exception:
                pass

            body_text = page.locator("body").inner_text()
            RAW_TXT.write_text(body_text, encoding="utf-8")
        finally:
            browser.close()

# --- Parser: iz RAW_TXT u CSV + TXT ---

ODD_KEYS = ["1", "X", "2", "UG_0_2", "UG_3_PLUS", "UG_4_PLUS", "GG", "I_GG", "GG_3_PLUS"]

def _is_time(s: str) -> bool:
    return bool(re.fullmatch(r"(?:[01]?\d|2[0-3]):[0-5]\d", s.strip()))

def _is_float_like(s: str) -> bool:
    return bool(re.fullmatch(r"\d+(?:[.,]\d+)?", s.strip()))

def _to_float(s: str) -> float:
    return float(s.replace(",", ".").strip())

def parse_matches_from_text(text: str) -> List[Dict]:
    lines = [ln.strip() for ln in text.splitlines()]
    # ukloni prazne i samotne “•”
    clean = [ln for ln in lines if ln and ln not in {"•", "·", "• "}]

    matches: List[Dict] = []
    i = 0
    n = len(clean)
    while i < n:
        # traži vreme
        if not _is_time(clean[i]):
            i += 1
            continue

        time_str = clean[i].strip()
        i += 1

        # očekujemo dva naredna ne-prazna reda = home, away
        if i + 1 >= n:
            break
        home = clean[i].strip(); i += 1
        away = clean[i].strip(); i += 1

        # sledi do 9 kvota
        odds: List[Optional[float]] = []
        while i < n and len(odds) < 9:
            tok = clean[i]
            if _is_float_like(tok):
                odds.append(_to_float(tok))
                i += 1
            else:
                break

        # moguće da postoji ID oblika "761»" – pokupi ga ako postoji
        match_id: Optional[str] = None
        if i < n:
            m = re.search(r"(\d+)\s*»", clean[i])
            if m:
                match_id = m.group(1)
                i += 1  # pojeli smo i taj red

        # Ako nismo dobili 9 kvota, dopuni None do 9
        while len(odds) < 9:
            odds.append(None)

        # validacija minimalna: vreme + 2 tima + 3 kvote (1,X,2)
        if home and away and odds[0] is not None and odds[1] is not None and odds[2] is not None:
            rec = {
                "time": time_str,
                "home": home,
                "away": away,
                "match_id": match_id or "",
                "odd_1": odds[0],
                "odd_x": odds[1],
                "odd_2": odds[2],
                "ug_0_2": odds[3],
                "ug_3_plus": odds[4],
                "ug_4_plus": odds[5],
                "gg": odds[6],
                "i_gg": odds[7],
                "gg_3_plus": odds[8],
            }
            matches.append(rec)

    return matches

def save_csv(matches: List[Dict], path: Path):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["TIME", "HOME", "AWAY", "MATCH_ID"] + ODD_KEYS)
        for m in matches:
            w.writerow([
                m["time"], m["home"], m["away"], m["match_id"],
                m["odd_1"], m["odd_x"], m["odd_2"],
                m["ug_0_2"], m["ug_3_plus"], m["ug_4_plus"],
                m["gg"], m["i_gg"], m["gg_3_plus"]
            ])

def _fmt(x: Optional[float]) -> str:
    if x is None:
        return "-"
    # Bez nepotrebnih .0 na celim brojevima
    return str(int(x)) if float(x).is_integer() else f"{x}"

def save_pretty(matches: List[Dict], path: Path):
    """
    Traženi format:
    ======================================================================
    18:45  Uto
    Barcelona  vs  Olympiakos   (ID: 523)
    1=1.18   X=7.25   2=15
    0-2=-   2+=-   3+=-
    GG=-   IGG=-   GG&3+=-
    """
    lines = []
    for m in matches:
        lines.append("=" * 70)

        # Nemamo “dan” u raw podacima — ostavi prazan string
        day_abbr = ""  # npr. "Uto" ako ga negde budeš izvlačio
        time_and_day = f"{m['time']}" if not day_abbr else f"{m['time']}  {day_abbr}"
        lines.append(time_and_day)

        id_part = f"   (ID: {m['match_id']})" if m['match_id'] else ""
        lines.append(f"{m['home']}  vs  {m['away']}{id_part}")

        # 1, X, 2
        lines.append(f"1={_fmt(m['odd_1'])}   X={_fmt(m['odd_x'])}   2={_fmt(m['odd_2'])}")

        # 0-2, 2+, 3+  (Merkur nema direktnu 2+, pa je postavljamo na '-')
        two_plus = None  # nema u ovoj šemi -> '-'
        lines.append(f"0-2={_fmt(m['ug_0_2'])}   2+={_fmt(two_plus)}   3+={_fmt(m['ug_3_plus'])}")

        # GG, IGG, GG&3+
        lines.append(f"GG={_fmt(m['gg'])}   IGG={_fmt(m['i_gg'])}   GG&3+={_fmt(m['gg_3_plus'])}")

    if not matches:
        lines.append("Nije pronađen nijedan meč u očekivanom formatu.")

    path.write_text("\n".join(lines), encoding="utf-8")

def run(headless=True):
    # 1) Skroluj 20 puta i sačuvaj sirovi tekst
    capture_text_after_20_scrolls(headless=headless)

    # 2) Parsiraj i upiši lepe izlaze
    text = RAW_TXT.read_text(encoding="utf-8")
    matches = parse_matches_from_text(text)
    save_csv(matches, OUT_CSV)
    save_pretty(matches, OUT_TXT)

    print(f"[OK] Sačuvano:\n - RAW: {RAW_TXT.resolve()}\n - CSV: {OUT_CSV.resolve()}\n - TXT: {OUT_TXT.resolve()}")

if __name__ == "__main__":
    #Ako želiš da GLEDAŠ skrol, stavi headless=False
    run(headless=True)
