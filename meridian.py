# meridian.py
# -*- coding: utf-8 -*-

import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# URL može biti sr ili en verzija; ostavi onaj koji ti bolje radi
URL = "https://meridianbet.rs/en/betting/football"
OUT_TXT = Path("meridian_sledeci_mecevi.txt")        # RAW
OUT_PRETTY = Path("meridian_mecevi_pregled.txt")     # Soccer-like

# -----------------------------
# Skrolovanje
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

def click_all(page) -> None:
    """Pokušaj da klikneš 'All' ili 'All matches' pre skrolovanja (ako postoji)."""
    import re as _re
    def _click_first(locator) -> bool:
        try:
            n = locator.count()
        except Exception:
            return False
        for i in range(min(n, 6)):
            try:
                el = locator.nth(i)
                if not el.is_visible(timeout=1200):
                    continue
                el.click(timeout=1500)
                time.sleep(0.2)
                return True
            except Exception:
                continue
        return False

    exact_all = _re.compile(r"^\s*All\s*$", _re.I)
    if _click_first(page.get_by_role("tab", name=exact_all)) \
       or _click_first(page.get_by_role("button", name=exact_all)) \
       or _click_first(page.get_by_role("link", name=exact_all)):
        return
    try:
        if _click_first(page.get_by_text("All", exact=True)):
            return
    except Exception:
        pass

    exact_all_matches = _re.compile(r"^\s*All matches\s*$", _re.I)
    if _click_first(page.get_by_role("tab", name=exact_all_matches)) \
       or _click_first(page.get_by_role("button", name=exact_all_matches)) \
       or _click_first(page.get_by_role("link", name=exact_all_matches)):
        return
    try:
        _click_first(page.get_by_text("All matches", exact=True))
    except Exception:
        pass

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
# PARSER Meridian RAW → Pretty
# -----------------------------

LEAGUE_RE = re.compile(r"^[A-Za-zŽĐŠČĆžđšćč .'/()-]+\s-\s[A-Za-zŽĐŠČĆžđšćč .'/()-]+$")  # npr. "Italian - Supercoppa Italiana"
DATE_RE   = re.compile(r"^\d{1,2}\.\d{1,2}\.?$")  # "18.12" ili "18.12."

def _to_float(s: str) -> Optional[float]:
    s = s.strip().replace(",", ".")
    if s == "-" or not s:
        return None
    try:
        return float(s)
    except Exception:
        return None

def _is_time(s: str) -> bool:
    return bool(re.fullmatch(r"([01]?\d|2[0-3]):[0-5]\d", s.strip()))

def _is_id(s: str) -> bool:
    return bool(re.fullmatch(r"\+\d+", s.strip()))

def _day_and_date(token: str) -> Tuple[str, str]:
    t = token.strip().lower()
    days = ["Pon","Uto","Sre","Čet","Pet","Sub","Ned"]
    now = datetime.now()
    if t in ("danas", "today"):
        return days[now.weekday()], now.strftime("%d.%m.")
    if t in ("sutra", "tomorrow"):
        d = now + timedelta(days=1)
        return days[d.weekday()], d.strftime("%d.%m.")
    if DATE_RE.fullmatch(token.strip()):
        try:
            dd, mm = token.replace(".", " ").split()[:2]
            dt = f"{int(dd):02d}.{int(mm):02d}."
            return "", dt
        except Exception:
            return "", token.strip()
    return "", ""

def _read_trimmed_text(path: Path, trim_last: int = 100) -> str:
    """Odbaci zadnjih `trim_last` linija (popup/footer); ako fajl ima manje — ne diraj."""
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if len(lines) > trim_last:
        lines = lines[:-trim_last]
    return "\n".join(lines)

def parse_meridian_raw(text: str) -> List[Dict]:
    """
    Format:
      LIGA (npr. 'Italian - Supercoppa Italiana')
      HH:MM
      Danas/Sutra/Today/Tomorrow ili datum DD.MM( . )
      Home
      Away
      <kvote ...> do '+ID' ili do sledeće sekcije
      +ID (opciono)

    Kvote:
      Minimalno: 1, X, 2
      Produženo: 1, X, 2, U2.5, [etiketa] 2.5 (IGNORISATI), O2.5, GG, GG&3+, GG&4+
    """
    SKIP = {
        # sr/eng sekcioni naslovi i filteri koje ponekad upiše u RAW
        "KONAČAN ISHOD","UKUPNO GOLOVA 2.5","OBA TIMA DAJU GOL",
        "FULL TIME RESULT","TOTAL GOALS 2.5","BOTH TEAMS TO SCORE",
        "SPORT","FUDBAL","MEČEVI","Sortiraj po","ISTAKNUTA TAKMIČENJA","OMILJENE LIGE",
        "SPORTS","FOOTBALL","MATCHES","FAVOURITES"
    }

    lines = [ln.replace("\xa0", " ").strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln and ln not in SKIP]

    out: List[Dict] = []
    current_league = ""
    i, n = 0, len(lines)

    while i < n:
        ln = lines[i]

        # Liga
        if LEAGUE_RE.match(ln):
            current_league = ln
            i += 1
            continue

        # Vreme
        if not _is_time(ln):
            i += 1
            continue
        time_s = ln.strip()
        i += 1
        if i >= n:
            break

        # Danas/Sutra/Today/Tomorrow/ili datum
        day_s, date_s = _day_and_date(lines[i])
        i += 1
        if i + 1 >= n:
            break

        # Timovi
        home = lines[i]; away = lines[i+1]
        i += 2

        # Kvote do +ID / sledeće sekcije
        nums: List[float] = []
        while i < n:
            tk = lines[i]
            if _is_id(tk) or _is_time(tk) or LEAGUE_RE.match(tk):
                break

            # IGNORIŠI SAMO literal granicu "2.5" / "2,5" (etiketa), NE i kvote tipa "2.50"
            norm = tk.strip().replace(",", ".")
            if norm == "2.5":
                i += 1
                continue

            val = _to_float(tk)
            if val is not None:
                nums.append(val)
            i += 1

        match_id = ""
        if i < n and _is_id(lines[i]):
            match_id = lines[i][1:]  # skini '+'
            i += 1

        # mapiranje: 1,X,2,U2.5,O2.5,GG,GG&3+,GG&4+ (popuni None ako nema)
        vals = (nums + [None]*8)[:8]
        q1, qx, q2, u25, o25, qgg, qgg3, qgg4 = vals

        odds = {
            "1": q1, "X": qx, "2": q2,
            "0-2": u25,     # Manje 2.5
            "2+": None,     # nema u ovom formatu
            "3+": o25,      # Više 2.5
            "GG": qgg,
            "IGG": None,
            "GG&3+": qgg3,
            "GG&4+": qgg4,
        }

        out.append({
            "time": time_s,
            "day": day_s,
            "date": date_s,
            "league": current_league,
            "home": home,
            "away": away,
            "match_id": match_id,
            "odds": odds
        })

    return out

def write_pretty_meridian(blocks: List[Dict], out_path: Path):
    def fmt(x: Optional[float]) -> str:
        if x is None:
            return "-"
        return str(int(x)) if float(x).is_integer() else f"{x}"

    lines: List[str] = []
    for b in blocks:
        lines.append("=" * 70)
        league_tag = f"[{b['league']}]" if b.get("league") else ""
        header = f"{b['time']}  {b['day']}  {b['date']}  {league_tag}".rstrip()
        lines.append(header)

        id_part = f"   (ID: {b['match_id']})" if b.get("match_id") else ""
        lines.append(f"{b['home']}  vs  {b['away']}{id_part}")

        od = b["odds"]
        lines.append(f"1={fmt(od.get('1'))}   X={fmt(od.get('X'))}   2={fmt(od.get('2'))}")
        lines.append(f"0-2={fmt(od.get('0-2'))}   2+={fmt(od.get('2+'))}   3+={fmt(od.get('3+'))}")
        lines.append(f"GG={fmt(od.get('GG'))}   IGG={fmt(od.get('IGG'))}   GG&3+={fmt(od.get('GG&3+'))}")
        if od.get("GG&4+") is not None:
            # popravljen f-string (navodnici)
            lines.append(f"GG&4+={fmt(od.get('GG&4+'))}")
    out_path.write_text("\n".join(lines), encoding="utf-8")

# -----------------------------
# Glavni tok
# -----------------------------

def main(headless=True):
    # 1) Skrol + RAW
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
            click_all(page)

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

            body_text = page.locator("body").inner_text()
            OUT_TXT.write_text(body_text, encoding="utf-8")
            print(f"[OK] RAW: {OUT_TXT.resolve()}")
        finally:
            browser.close()

    # 2) Parsiranje RAW → Pretty (uz odbacivanje zadnjih 100 linija)
    raw_trimmed = _read_trimmed_text(OUT_TXT, trim_last=100)
    blocks = parse_meridian_raw(raw_trimmed)
    write_pretty_meridian(blocks, OUT_PRETTY)
    print(f"[OK] Pretty: {OUT_PRETTY.resolve()}  (mečeva: {len(blocks)})")

if __name__ == "__main__":
    # Za praćenje prozora: headless=False
    main(headless=True)
