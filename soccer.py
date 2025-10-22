# soccer_text_30scrolls_naredni_bounce_parse.py
# -*- coding: utf-8 -*-

import re
import time
import csv
from pathlib import Path
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

URL = "https://www.soccerbet.rs/sr/sportsko-kladjenje/fudbal/S"
RAW_TXT = Path("soccer_sledeci_mecevi.txt")
OUT_CSV = Path("soccer_mecevi.csv")
OUT_TXT = Path("soccer_mecevi_pregled.txt")

# -------------------- Playwright deo --------------------

def accept_cookies(page) -> None:
    labels = ["Prihvatam", "Prihvatam sve", "Prihvati sve", "Slažem se",
              "Accept", "Accept all", "I agree", "U redu", "Ok"]
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
        || t.includes("gol") || t.includes("meč") || t.includes("utakm")
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

def click_naredni(page) -> None:
    tries = [
        lambda: page.get_by_role("button", name=re.compile(r"^\\s*Naredni\\s*$", re.I)).click(timeout=1500),
        lambda: page.get_by_role("link",   name=re.compile(r"^\\s*Naredni\\s*$", re.I)).click(timeout=1500),
        lambda: page.locator("button:has-text('Naredni')").first.click(timeout=1500),
        lambda: page.locator("a:has-text('Naredni')").first.click(timeout=1500),
        lambda: page.locator(":text('Naredni')").first.click(timeout=1500),
    ]
    for fn in tries:
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

def do_30_down_with_bounce(page, container_handle=None, pause=0.45,
                           delta_down=1500, delta_up=-1200, bounce_every=4):
    # pripremi target za wheel
    if container_handle:
        try:
            box = page.evaluate(
                "(e)=>{const r=e.getBoundingClientRect();return {x:r.left + r.width/2, y:r.top + Math.min(r.height/2, r.height-30)};}",
                container_handle
            )
            page.mouse.move(box["x"], box["y"])
            try: container_handle.click(force=True)
            except Exception: pass
        except Exception:
            container_handle = None

    down_done = 0
    while down_done < 100:
        # jedan DOWN
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

        # bounce (ne broji se u 30)
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

def capture_text_and_close(headless: bool = True):
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

            click_naredni(page)
            inner = find_inner_scroll_container(page)
            do_30_down_with_bounce(page, inner, pause=0.45,
                                   delta_down=1500, delta_up=-1200, bounce_every=4)

            try: page.evaluate("window.scrollTo(0,0)")
            except Exception: pass

            text = page.locator("body").inner_text()
            RAW_TXT.write_text(text, encoding="utf-8")
        finally:
            browser.close()

# -------------------- Parser (novi format) --------------------

DAY_RE = r"(Pon|Uto|Sre|Čet|Cet|Pet|Sub|Ned)"
TIME_RE = r"(?:[01]?\d|2[0-3]):[0-5]\d"
DATE_RE = r"\d{1,2}\.\d{1,2}\."         # npr. 19.10.
LEAGUE_RE = r"[A-ZČĆŠĐŽ0-9]{2,6}"       # ARG2, KRK1, MEX1...

def _is_time(s: str) -> bool:
    return bool(re.fullmatch(TIME_RE, s.strip()))

def _is_day(s: str) -> bool:
    return bool(re.fullmatch(DAY_RE, s.strip(), flags=re.I))

def _is_date(s: str) -> bool:
    return bool(re.fullmatch(DATE_RE, s.strip()))

def _is_league(s: str) -> bool:
    return bool(re.fullmatch(LEAGUE_RE, s.strip()))

def _is_float_like(s: str) -> bool:
    return bool(re.fullmatch(r"\d+(?:[.,]\d+)?", s.strip()))

def _to_float(s: str) -> float:
    return float(s.replace(",", ".").strip())

def parse_soccerbet_text(text: str) -> List[Dict]:
    """
    Očekivani blok po meču:
      TIME
      DAY
      DATE
      LEAGUE
      HOME
      AWAY
      9 * ODD
      +ID   (opciono, npr. '+494')

    Kvote redom: 1, X, 2, 0-2, 2+, 3+, GG, IGG, GG&3+
    """
    lines = [ln.strip() for ln in text.splitlines()]
    # izbaci prazne linije
    lines = [ln for ln in lines if ln]

    matches: List[Dict] = []
    i, n = 0, len(lines)

    while i < n:
        # 1) vreme
        if not _is_time(lines[i]):
            i += 1
            continue
        time_s = lines[i]; i += 1
        if i >= n or not _is_day(lines[i]):     # 2) dan
            continue
        day_s = lines[i]; i += 1
        if i >= n or not _is_date(lines[i]):    # 3) datum
            continue
        date_s = lines[i]; i += 1
        if i >= n or not _is_league(lines[i]):  # 4) liga
            continue
        league_s = lines[i]; i += 1

        # 5) home, 6) away
        if i + 1 >= n:
            break
        home = lines[i]; i += 1
        away = lines[i]; i += 1

        # 7) 9 kvota
        odds: List[float] = []
        while i < n and len(odds) < 9 and _is_float_like(lines[i]):
            odds.append(_to_float(lines[i]))
            i += 1

        # 8) opcioni +ID
        match_id: Optional[str] = ""
        if i < n:
            m = re.match(r"^\+(\d+)$", lines[i])
            if m:
                match_id = m.group(1)
                i += 1

        # validacija: moramo imati makar 3 kvote; idealno 9
        if len(odds) < 3:
            continue
        while len(odds) < 9:
            odds.append(None)

        matches.append({
            "time": time_s,
            "day": day_s,
            "date": date_s,
            "league": league_s,
            "home": home,
            "away": away,
            "match_id": match_id,
            "odd_1": odds[0],
            "odd_x": odds[1],
            "odd_2": odds[2],
            "ug_0_2": odds[3],
            "ug_2_plus": odds[4],
            "ug_3_plus": odds[5],
            "gg": odds[6],
            "igg": odds[7],
            "gg_3_plus": odds[8],
        })

    return matches

def save_csv(matches: List[Dict], path: Path):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["TIME","DAY","DATE","LEAGUE","HOME","AWAY","MATCH_ID",
                    "1","X","2","0-2","2+","3+","GG","IGG","GG&3+"])
        for m in matches:
            w.writerow([
                m["time"], m["day"], m["date"], m["league"], m["home"], m["away"], m["match_id"],
                m["odd_1"], m["odd_x"], m["odd_2"],
                m["ug_0_2"], m["ug_2_plus"], m["ug_3_plus"],
                m["gg"], m["igg"], m["gg_3_plus"]
            ])

def save_pretty(matches: List[Dict], path: Path):
    out = []
    for m in matches:
        out.append("=" * 70)
        out.append(f"{m['time']}  {m['day']}  {m['date']}  [{m['league']}]")
        out.append(f"{m['home']}  vs  {m['away']}" + (f"   (ID: {m['match_id']})" if m['match_id'] else ""))
        out.append(f"1={m['odd_1']}   X={m['odd_x']}   2={m['odd_2']}")
        out.append(f"0-2={m['ug_0_2']}   2+={m['ug_2_plus']}   3+={m['ug_3_plus']}")
        out.append(f"GG={m['gg']}   IGG={m['igg']}   GG&3+={m['gg_3_plus']}")
        out.append("")
    if not matches:
        out.append("Nije pronađen nijedan meč u očekivanom formatu.")
    path.write_text("\n".join(out), encoding="utf-8")

def run(headless=True):
    # 1) skroluj + kopiraj tekst i zatvori prozor
    capture_text_and_close(headless=headless)
    # 2) parsiraj tekst i upiši CSV/TXT
    text = RAW_TXT.read_text(encoding="utf-8")
    matches = parse_soccerbet_text(text)
    save_csv(matches, OUT_CSV)
    save_pretty(matches, OUT_TXT)
    print(f"[OK] Sačuvano:\n - RAW: {RAW_TXT.resolve()}\n - CSV: {OUT_CSV.resolve()}\n - TXT: {OUT_TXT.resolve()}")

if __name__ == "__main__":
    # Ako želiš da posmatraš prozor, stavi headless=False.
    run(headless=True)
