# mozzart.py
# -*- coding: utf-8 -*-

import re
import time
from pathlib import Path
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

URL = "https://www.mozzartbet.com/sr/kladjenje/sport/1?date=all_days"
RAW_TXT = Path("mozzart_sledeci_mecevi.txt")
OUT_TXT = Path("mozzart_mecevi_pregled.txt")

# =========================
#  Playwright deo (scroll)
# =========================

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

def do_30_down_with_bounce(page, container_handle=None, pause=0.45,
                           delta_down=1500, delta_up=-1200, bounce_every=4):
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
    while down_done < 300:
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

            inner = find_inner_scroll_container(page)
            do_30_down_with_bounce(page, inner, pause=0.45,
                                   delta_down=1500, delta_up=-1200, bounce_every=4)

            try:
                page.evaluate("window.scrollTo(0,0)")
            except Exception:
                pass

            text = page.locator("body").inner_text()
            RAW_TXT.write_text(text, encoding="utf-8")
        finally:
            browser.close()

# =========================
#  Parser & format izlaza
# =========================

DAY_RE = r"(Pon|Uto|Sre|Čet|Cet|Pet|Sub|Ned)"
TIME_RE = r"(?:[01]?\d|2[0-3]):[0-5]\d"
DAY_TIME_RE = rf"^{DAY_RE}\s+{TIME_RE}$"
ID_RE = r"^\+\d+$"
NUM_RE = r"^\d+(?:[.,]\d+)?$"
DATE_RE = r"^\d{1,2}\.\d{1,2}\.$"   # ako se negde pojavi npr. "19.10."
LEAGUE_TAG_RE = r"^\[[A-Z0-9]{2,6}\]$"  # ako već dobijemo [ELS1] itd.

def _norm(s: str) -> str:
    return s.strip()

def _is_day_time(line: str) -> bool:
    return bool(re.match(DAY_TIME_RE, _norm(line)))

def _is_id(line: str) -> bool:
    return bool(re.match(ID_RE, _norm(line)))

def _is_num(line: str) -> bool:
    return bool(re.match(NUM_RE, _norm(line)))

def parse_mozzart_text_to_blocks(text: str) -> List[Dict]:
    """
    Očekivani blok (koliko dozvoljava Mozzartov tekst):
      Uto 18:45
      Home
      Away
      +ID (opciono)
      9 kvota: 1, X, 2, 1X, 12, X2, 2+, 0-2, 3+
    Pokuša da uhvati i DATUM (npr. "19.10.") i [LIGA] ako su blizu, ali su opcioni.
    """
    lines = [_norm(ln) for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]

    HEADER_SET = {"1","X","2","1X","12","X2","2+","0-2","3+",
                  "Liga evrope","Liga šampiona","Liga sampiona","Liga konferencija"}

    blocks: List[Dict] = []
    i, n = 0, len(lines)

    while i < n:
        if lines[i] in HEADER_SET:
            i += 1
            continue

        if not _is_day_time(lines[i]):
            i += 1
            continue

        day_time = lines[i]; i += 1

        # pokušaj da vidimo da li odmah sledi datum ili liga u uglastim zagradama
        date_s = ""
        league_s = ""
        if i < n and re.match(DATE_RE, lines[i]):
            date_s = lines[i]; i += 1
        if i < n and re.match(LEAGUE_TAG_RE, lines[i]):
            league_s = lines[i][1:-1]; i += 1  # skini zagrade

        if i + 1 >= n:
            break
        home = lines[i]; i += 1
        away = lines[i]; i += 1

        match_id = ""
        if i < n and _is_id(lines[i]):
            match_id = lines[i][1:]
            i += 1

        odds: List[Optional[float]] = []
        while i < n and len(odds) < 9:
            if _is_num(lines[i]):
                odds.append(float(lines[i].replace(",", ".")))
                i += 1
            else:
                break

        if len(odds) < 3:
            continue
        while len(odds) < 9:
            odds.append(None)

        # izvuci dan i vreme iz "Uto 18:45"
        day = ""
        time_s = ""
        mtime = re.search(TIME_RE, day_time)
        if mtime:
            time_s = mtime.group(0)
        mday = re.match(DAY_RE, day_time)
        if mday:
            day = mday.group(1)

        blocks.append({
            "day": day,
            "time": time_s,
            "date": date_s,        # npr. "19.10."
            "league": league_s,    # npr. "ELS1"
            "home": home,
            "away": away,
            "match_id": match_id,
            # redosled sa Mozzarta:
            # 1, X, 2, 1X, 12, X2, 2+, 0-2, 3+
            "odds": odds
        })

    return blocks

def write_pretty_like_soccer(blocks: List[Dict], out_path: Path):
    """
    Štampa po uzorku:
    ======================================================================
    HH:MM  Dan  DD.MM.  [LIGA]
    Home  vs  Away   (ID: N)
    1=a   X=b   2=c
    0-2=d   2+=e   3+=f
    GG=g   IGG=h   GG&3+=i
    """
    lines: List[str] = []
    for b in blocks:
        # mapiranje kvota iz Mozzarta na tražene linije
        # indices: 0:1, 1:X, 2:2, 3:1X, 4:12, 5:X2, 6:2+, 7:0-2, 8:3+
        v1, vx, v2 = b["odds"][0], b["odds"][1], b["odds"][2]
        v02, v2p, v3p = b["odds"][7], b["odds"][6], b["odds"][8]
        # Mozzart obično nema GG, IGG, GG&3+ u ovih 9 — ako ih nema, ostavi '-'
        # (Ako ih imaš iz dodatnih izvora, ovde ih možeš popuniti.)
        vgg = None
        vigg = None
        vgg3 = None

        lines.append("=" * 70)
        # zaglavlje: vreme, dan, datum, [LIGA]
        day = b["day"]
        date = b["date"]
        league = b["league"]
        header = f"{b['time']}  {day}  "
        header += (f"{date}  " if date else "")
        header += (f"[{league}]" if league else "")
        lines.append(header.rstrip())

        # timovi + ID
        id_part = f"   (ID: {b['match_id']})" if b["match_id"] else ""
        lines.append(f"{b['home']}  vs  {b['away']}{id_part}")

        # 1/X/2
        def fmt(x): 
            return "-" if x is None else (str(int(x)) if float(x).is_integer() else f"{x}")
        lines.append(f"1={fmt(v1)}   X={fmt(vx)}   2={fmt(v2)}")
        # 0-2 / 2+ / 3+
        lines.append(f"0-2={fmt(v02)}   2+={fmt(v2p)}   3+={fmt(v3p)}")
        # GG / IGG / GG&3+ (ako ih nema u ulazu, biće '-')
        lines.append(f"GG={fmt(vgg)}   IGG={fmt(vigg)}   GG&3+={fmt(vgg3)}")

    out_path.write_text("\n".join(lines), encoding="utf-8")

# =========================
#  Run sve
# =========================

def run(headless=True):
    # 1) Skroluj i snimi sirovi tekst
    capture_text_and_close(headless=headless)
    # 2) Parsiraj i ispiši "soccer-like" pregled
    text = RAW_TXT.read_text(encoding="utf-8", errors="ignore")
    blocks = parse_mozzart_text_to_blocks(text)
    write_pretty_like_soccer(blocks, OUT_TXT)
    print(f"[OK] Sačuvano: {OUT_TXT.resolve()}")

if __name__ == "__main__":
    # Za live pregled: run(headless=False)
    run(headless=True)
