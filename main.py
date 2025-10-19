# main.py
# -*- coding: utf-8 -*-

import csv
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher
from collections import defaultdict, deque
from statistics import mode

# -----------------------------
# Ulazni fajlovi (pretty)
# -----------------------------
SOCCER_TXT   = Path("soccer_mecevi_pregled.txt")
MERKUR_TXT   = Path("merkur_mecevi_pregled.txt")
MOZZART_TXT  = Path("mozzart_mecevi_pregled.txt")
BETOLE_TXT   = Path("betole_mecevi_pregled.txt")
MERIDIAN_TXT = Path("meridian_mecevi_pregled.txt")
BALKAN_TXT   = Path("balkanbet_mecevi_pregled.txt")

# Izlazi
OUT_CSV      = Path("mecevi_spojeno.csv")
OUT_TXT      = Path("mecevi_spojeno.txt")
REPORT_MATCH = Path("izvestaj_o_mecevima.txt")
REPORT_ARB   = Path("izvestaj.txt")  # arbitražno klađenje

# -----------------------------
# Regex i pomoćne rutine
# -----------------------------
TIME_RE = r"(?:[01]?\d|2[0-3]):[0-5]\d"
DAY_RE = r"(Pon|Uto|Sre|Čet|Cet|Pet|Sub|Ned)"
DATE_RE = r"\d{1,2}\.\d{1,2}\."
LEAGUE_BRACKET_RE = r"\[([A-ZČĆŠĐŽA-Za-zčćšđž0-9 .'/()-]{2,40})\]"
FLOAT_VAL_RE = r"(\d+(?:[.,]\d+)?)"

def _to_float(s: str) -> Optional[float]:
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None

def _grab_val(label: str, line: str) -> Optional[float]:
    pat = rf"{re.escape(label)}\s*=\s*{FLOAT_VAL_RE}"
    m = re.search(pat, line, flags=re.I)
    return _to_float(m.group(1)) if m else None

def strip_accents(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))

def normalize_team(s: str) -> str:
    s0 = strip_accents(s.lower())
    s0 = re.sub(r"[’'`]", "", s0)
    s0 = re.sub(r"[.\-–—_/]", " ", s0)
    s0 = re.sub(r"[()]", " ", s0)
    s0 = re.sub(r"\s+", " ", s0).strip()
    return s0

# Stop-reči (nebitni tokeni u poređenju)
_STOPWORDS = {
    "fc","cf","sc","if","afc","bk","fk","kk","ac","as","cd","sd","ud","acf","sv","ss","sp","ca",
    "united","city","club","the","de","la","el","da","do","del","dep","ud","ac","atletico","atl",
    "calcio","cf","cp","st","saint","saints","om","psg","bc","bcf","w","wom","women","ladies"
}

def team_tokens(name: str) -> set:
    n = normalize_team(name)
    toks = [t for t in re.split(r"\s+", n) if t]
    toks = [t for t in toks if t not in _STOPWORDS]
    return set(toks)

def fuzzy(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def _time_to_minutes(t: str) -> Optional[int]:
    m = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", (t or "").strip())
    if not m:
        return None
    return int(m.group(1)) * 60 + int(m.group(2))

def _times_close(t1: str, t2: str, tolerance_min: int = 20) -> bool:
    m1, m2 = _time_to_minutes(t1), _time_to_minutes(t2)
    if m1 is None or m2 is None:
        return False
    return abs(m1 - m2) <= tolerance_min

# -----------------------------
# Blok parser: PRETTY → record
# -----------------------------
def parse_blocks_pretty(txt: str) -> List[List[str]]:
    lines = [ln.rstrip() for ln in txt.splitlines()]
    blocks: List[List[str]] = []
    cur: List[str] = []
    for ln in lines:
        if re.match(r"^=+\s*$", ln):
            if cur:
                blocks.append(cur)
            cur = []
        else:
            if ln.strip():
                cur.append(ln.strip())
    if cur:
        blocks.append(cur)
    return blocks

def parse_pretty_block(lines: List[str], src_tag: str) -> Optional[Dict]:
    if not lines:
        return None

    head = lines[0]
    m_time = re.search(TIME_RE, head)
    time_s = m_time.group(0) if m_time else ""
    m_day = re.search(DAY_RE, head, re.I)
    day_s = m_day.group(0) if m_day else ""
    m_date = re.search(DATE_RE, head)
    date_s = m_date.group(0) if m_date else ""
    m_league = re.search(LEAGUE_BRACKET_RE, head)
    league_s = (m_league.group(1).strip() if m_league else "")

    home = away = match_id = ""
    team_line = next((l for l in lines if " vs " in l.lower()), "")
    if team_line:
        mid = re.search(r"\(ID:\s*([^)]+)\)", team_line)
        if mid:
            match_id = mid.group(1).strip()
            team_line = re.sub(r"\(ID:[^)]+\)", "", team_line).strip()
        mt = re.search(r"(.+?)\s+vs\s+(.+)", team_line, re.I)
        if mt:
            home = mt.group(1).strip()
            away = mt.group(2).strip()

    odds_map: Dict[str, Optional[float]] = {
        "1": None, "X": None, "2": None,
        "0-2": None, "2+": None, "3+": None,
        "GG": None, "IGG": None, "GG&3+": None, "GG&4+": None
    }

    for ln in lines:
        odds_map["1"] = odds_map["1"] or _grab_val("1", ln)
        odds_map["X"] = odds_map["X"] or _grab_val("X", ln)
        odds_map["2"] = odds_map["2"] or _grab_val("2", ln)

        if odds_map["0-2"] is None:
            odds_map["0-2"] = _grab_val("0-2", ln) or _grab_val("UG 0-2", ln)
        if odds_map["2+"] is None:
            odds_map["2+"] = _grab_val("2+", ln)
        if odds_map["3+"] is None:
            odds_map["3+"] = _grab_val("3+", ln)

        odds_map["GG"]    = odds_map["GG"]    or _grab_val("GG", ln)
        odds_map["IGG"]   = odds_map["IGG"]   or _grab_val("IGG", ln) or _grab_val("I GG", ln)
        odds_map["GG&3+"] = odds_map["GG&3+"] or _grab_val("GG&3+", ln)
        odds_map["GG&4+"] = odds_map["GG&4+"] or _grab_val("GG&4+", ln)

    if not (time_s and home and away):
        return None

    return {
        "src": src_tag,
        "time": time_s,
        "day": day_s,
        "date": date_s,
        "league": league_s,
        "home": home,
        "away": away,
        "match_id": match_id,
        "odds": odds_map,
    }

def parse_file_pretty(path: Path, src_tag: str) -> List[Dict]:
    if not path.exists():
        print(f"(!) Upozorenje: nema fajla {path}")
        return []
    txt = path.read_text(encoding="utf-8", errors="ignore")
    blocks = parse_blocks_pretty(txt)
    out: List[Dict] = []
    for b in blocks:
        rec = parse_pretty_block(b, src_tag)
        if rec:
            out.append(rec)
    return out

# -----------------------------
# Napredna sličnost timova
# -----------------------------
def _ngrams(s: str, n: int = 3) -> set:
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) < n:
        return {s} if s else set()
    return {s[i:i+n] for i in range(len(s)-n+1)}

def token_jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0

def token_dice(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = 2 * len(a & b)
    denom = (len(a) + len(b))
    return inter / denom if denom else 0.0

def team_signature(name: str) -> str:
    toks = [t for t in team_tokens(name)]
    toks.sort()
    sig = "".join(t[:4] for t in toks)
    return sig[:24]

def prefix_or_contains(a_name: str, b_name: str) -> bool:
    a = normalize_team(a_name)
    b = normalize_team(b_name)
    if a and b:
        if a == b:
            return True
        if a in b or b in a:
            return True
        if b.startswith(a) or a.startswith(b):
            return True
    return False

def significant_tokens(tokset: set) -> set:
    return {t for t in tokset if len(t) >= 3 and re.search(r"[a-z0-9]", t)}

def is_token_subset(a: set, b: set) -> bool:
    A = significant_tokens(a)
    B = significant_tokens(b)
    return bool(A) and A.issubset(B)

def team_char_score(a_name: str, b_name: str) -> float:
    a = normalize_team(a_name)
    b = normalize_team(b_name)
    ng_a = _ngrams(a, 3)
    ng_b = _ngrams(b, 3)
    ng_score = (len(ng_a & ng_b) / len(ng_a | ng_b)) if (ng_a and ng_b) else 0.0
    fz = fuzzy(a, b)
    sig_a, sig_b = team_signature(a), team_signature(b)
    sig_sim = fuzzy(sig_a, sig_b)
    base = 0.5 * ng_score + 0.4 * fz + 0.1 * sig_sim
    if prefix_or_contains(a_name, b_name):
        base = min(1.0, base + 0.12)
    return base

# "Al" izuzetak
def has_al(name: str) -> bool:
    n = normalize_team(name)
    if re.match(r"^al($|[\s\-])", n): return True
    if re.search(r"\bal\b", n): return True
    return False

def al_enforced_equal(a_name: str, b_name: str) -> bool:
    if has_al(a_name) or has_al(b_name):
        return normalize_team(a_name) == normalize_team(b_name)
    return True

def league_tokens(league: str) -> set:
    if not league:
        return set()
    s = strip_accents(league.lower())
    s = re.sub(r"[-–—/()]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    toks = [t for t in s.split(" ") if len(t) >= 2]
    stop = {"liga","league","division","divison","apertura","clausura",
            "serija","serie","premier","prva","druga","championship",
            "cup","women","w","mx","la","de","el"}
    return {t for t in toks if t not in stop}

# -----------------------------
# ALL-vs-ALL: građenje klastera
# -----------------------------
def _time_factor(t1: str, t2: str) -> float:
    m1, m2 = _time_to_minutes(t1), _time_to_minutes(t2)
    if m1 is None or m2 is None:
        return 0.90
    dt = abs(m1 - m2)
    if dt <= 5:   return 1.00
    if dt <= 10:  return 0.97
    if dt <= 20:  return 0.92
    return 0.0

def _side_score(nameA, nameB):
    toksA, toksB = team_tokens(nameA), team_tokens(nameB)
    tj = token_jaccard(toksA, toksB)
    td = token_dice(toksA, toksB)
    tok = max(tj, td)

    subset_bonus = 0.0
    if is_token_subset(toksA, toksB) or is_token_subset(toksB, toksA):
        subset_bonus += 0.20
    if prefix_or_contains(nameA, nameB):
        subset_bonus += 0.10
    subset_bonus = min(subset_bonus, 0.25)

    ch = team_char_score(nameA, nameB)
    w_tok = 0.55 if (toksA and toksB and (toksA & toksB)) else 0.40
    base = w_tok * tok + (1.0 - w_tok) * ch
    return min(1.0, base + subset_bonus)

def _match_ok(a: Dict, b: Dict, team_threshold: float = 0.82) -> bool:
    if not _times_close(a.get("time",""), b.get("time",""), tolerance_min=20):
        return False
    tf = _time_factor(a.get("time",""), b.get("time",""))
    if tf == 0.0:
        return False

    lg1, lg2 = league_tokens(a.get("league","")), league_tokens(b.get("league",""))
    if lg1 and lg2:
        if not (lg1 & lg2):
            sig1 = "".join(sorted(lg1))[:24]
            sig2 = "".join(sorted(lg2))[:24]
            if fuzzy(sig1, sig2) < 0.50:
                return False

    straight_al = al_enforced_equal(a["home"], b["home"]) and al_enforced_equal(a["away"], b["away"])
    swap_al     = al_enforced_equal(a["home"], b["away"]) and al_enforced_equal(a["away"], b["home"])
    if not (straight_al or swap_al):
        return False

    straight = -1.0
    swap     = -1.0
    if straight_al:
        straight = (_side_score(a["home"], b["home"]) + _side_score(a["away"], b["away"])) / 2.0
    if swap_al:
        swap = (_side_score(a["home"], b["away"]) + _side_score(a["away"], b["home"])) / 2.0

    base = max(straight, swap)
    if base < 0:
        return False

    final_score = base * tf
    base_threshold = 0.53
    if final_score >= base_threshold:
        return True

    if straight >= swap and straight_al:
        fz = (fuzzy(normalize_team(a["home"]), normalize_team(b["home"])) +
              fuzzy(normalize_team(a["away"]), normalize_team(b["away"]))) / 2.0
    elif swap_al:
        fz = (fuzzy(normalize_team(a["home"]), normalize_team(b["away"])) +
              fuzzy(normalize_team(a["away"]), normalize_team(b["home"]))) / 2.0
    else:
        fz = 0.0
    return (fz >= team_threshold and tf >= 0.92)

def cluster_all_sources(records: List[Dict]) -> List[List[Dict]]:
    n = len(records)
    adj = [[] for _ in range(n)]

    buckets = defaultdict(list)
    def bucket_key(t: str):
        m = _time_to_minutes(t or "")
        if m is None: return "none"
        return m // 10

    for i, r in enumerate(records):
        buckets[bucket_key(r.get("time",""))].append(i)

    for _, idxs in buckets.items():
        L = len(idxs)
        for ii in range(L):
            a = records[idxs[ii]]
            for jj in range(ii+1, L):
                b = records[idxs[jj]]
                if _match_ok(a, b):
                    u, v = idxs[ii], idxs[jj]
                    adj[u].append(v)
                    adj[v].append(u)

    seen = [False]*n
    clusters: List[List[Dict]] = []
    for i in range(n):
        if seen[i]: continue
        q = deque([i]); seen[i] = True; comp = [i]
        while q:
            u = q.popleft()
            for v in adj[u]:
                if not seen[v]:
                    seen[v] = True
                    q.append(v)
                    comp.append(v)
        clusters.append([records[k] for k in comp])
    return clusters

# -----------------------------
# Izlazi (CSV, TXT, Report)
# -----------------------------
SRC_ORDER = ["soccer", "merkur", "mozzart", "betole", "meridian", "balkanbet"]
SRC_LABEL = {
    "soccer":"Soccer",
    "merkur":"Merkur",
    "mozzart":"Mozzart",
    "betole":"Betole",
    "meridian":"Meridian",
    "balkanbet":"BalkanBet",
}

def fmt_od(x: Optional[float]) -> str:
    if x is None: return "-"
    return str(int(x)) if float(x).is_integer() else f"{x}"

def canonical_time(cluster: List[Dict]) -> str:
    ts = [r.get("time","") for r in cluster if r.get("time")]
    return mode(ts) if ts else (cluster[0].get("time","") if cluster else "")

def canonical_pair(cluster: List[Dict]) -> Tuple[str,str]:
    pref = ["soccer","mozzart","merkur","meridian","betole","balkanbet"]
    by_src = {r["src"]: r for r in cluster}
    for p in pref:
        if p in by_src:
            return by_src[p]["home"], by_src[p]["away"]
    return cluster[0]["home"], cluster[0]["away"]

# -----------------------------
# >>> NOVO: Poravnanje i izbor najboljeg zapisa po izvoru
# -----------------------------
def is_plausible_odd(x: Optional[float]) -> bool:
    """Osnovni sanity check za kvotu."""
    if x is None:
        return False
    return 1.01 <= float(x) <= 25.0

def coherent_1x2_for_source(rec: Dict) -> Tuple[bool, Optional[float]]:
    """
    Proverava da izvor ima smislen kompletan 1X2 set i vraća (ok, S),
    gde je S suma recipročnih.
    """
    od = rec.get("odds", {})
    a, b, c = od.get("1"), od.get("X"), od.get("2")
    if not (is_plausible_odd(a) and is_plausible_odd(b) and is_plausible_odd(c)):
        return False, None
    S = (1.0/float(a)) + (1.0/float(b)) + (1.0/float(c))
    if 0.95 <= S <= 1.90:
        return True, S
    return False, S

def filter_sources_for_1x2_best(by_src: Dict[str, Dict]) -> Dict[str, Dict]:
    """Zadrži samo izvore sa koherentnim 1X2 setom."""
    out = {}
    for code, rec in by_src.items():
        ok, _ = coherent_1x2_for_source(rec)
        if ok:
            out[code] = rec
    return out

def _orientation_vs_canon(rec: Dict, canon_home: str, canon_away: str) -> str:
    """Vrati 'straight' ili 'swap' u odnosu na (canon_home, canon_away)."""
    straight_al = al_enforced_equal(rec["home"], canon_home) and al_enforced_equal(rec["away"], canon_away)
    swap_al     = al_enforced_equal(rec["home"], canon_away) and al_enforced_equal(rec["away"], canon_home)
    if straight_al and not swap_al:
        return "straight"
    if swap_al and not straight_al:
        return "swap"
    s_st = (_side_score(rec["home"], canon_home) + _side_score(rec["away"], canon_away)) / 2.0
    s_sw = (_side_score(rec["home"], canon_away) + _side_score(rec["away"], canon_home)) / 2.0
    return "straight" if s_st >= s_sw else "swap"

def _swap_1x2_if_needed(odds: Dict[str, Optional[float]], orientation: str) -> Dict[str, Optional[float]]:
    """Ako je 'swap', zameni 1<->2. Ostalo ostaje isto."""
    if orientation != "swap":
        return odds
    new_odds = dict(odds)
    new_odds["1"], new_odds["2"] = odds.get("2"), odds.get("1")
    return new_odds

def _rec_completeness_score(rec: Dict) -> Tuple[int, int]:
    """
    Rang za izbor najboljeg dvojnika istog izvora:
    1) koherentan 1X2 (1/0)
    2) broj popunjenih polja u rec['odds']
    """
    ok, _ = coherent_1x2_for_source(rec)
    filled = sum(1 for v in rec.get("odds", {}).values() if isinstance(v, (int, float)))
    return (1 if ok else 0, filled)

def aligned_by_src(cluster: List[Dict]) -> Dict[str, Dict]:
    """
    {src: rec_aligned} — poravnato na kanonski HOME/AWAY i 1↔2 po potrebi.
    Ako u klasteru postoji više zapisa istog izvora, bira najkompletniji.
    """
    canon_home, canon_away = canonical_pair(cluster)

    per_src: Dict[str, List[Dict]] = defaultdict(list)
    for r in cluster:
        per_src[r["src"]].append(r)

    chosen: Dict[str, Dict] = {}
    for code, items in per_src.items():
        items_sorted = sorted(items, key=_rec_completeness_score, reverse=True)
        chosen[code] = items_sorted[0]

    out: Dict[str, Dict] = {}
    for code, r in chosen.items():
        ori = _orientation_vs_canon(r, canon_home, canon_away)
        aligned_odds = _swap_1x2_if_needed(r.get("odds", {}), ori)
        rr = dict(r)
        rr["home"] = canon_home
        rr["away"] = canon_away
        rr["odds"] = aligned_odds
        out[code] = rr
    return out

# -----------------------------
# CSV/TXT/Report upis
# -----------------------------
def row_for_source(rec: Optional[Dict]) -> List[Optional[float]]:
    if not rec:
        return ["", "", "", "", "", "", "", "", ""]
    od = rec["odds"]
    return [
        od.get("1"), od.get("X"), od.get("2"),
        od.get("0-2"), od.get("2+"), od.get("3+"),
        od.get("GG"), od.get("IGG"), od.get("GG&3+"),
    ]

def write_csv(clusters: List[List[Dict]]):
    header = [
        "TIME","HOME","AWAY",
        # Soccer
        "S_1","S_X","S_2","S_0-2","S_2+","S_3+","S_GG","S_IGG","S_GG&3+",
        # Merkur
        "M_1","M_X","M_2","M_0-2","M_2+","M_3+","M_GG","M_IGG","M_GG&3+",
        # Mozzart
        "Z_1","Z_X","Z_2","Z_0-2","Z_2+","Z_3+","Z_GG","Z_IGG","Z_GG&3+",
        # Betole
        "B_1","B_X","B_2","B_0-2","B_2+","B_3+","B_GG","B_IGG","B_GG&3+",
        # Meridian
        "D_1","D_X","D_2","D_0-2","D_2+","D_3+","D_GG","D_IGG","D_GG&3+",
        # BalkanBet
        "Q_1","Q_X","Q_2","Q_0-2","Q_2+","Q_3+","Q_GG","Q_IGG","Q_GG&3+",
    ]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(header)
        for cl in clusters:
            t = canonical_time(cl)
            h, a = canonical_pair(cl)
            by_src = aligned_by_src(cl)   # <<< PORAVNATO
            row = [t, h, a]
            row += row_for_source(by_src.get("soccer"))
            row += row_for_source(by_src.get("merkur"))
            row += row_for_source(by_src.get("mozzart"))
            row += row_for_source(by_src.get("betole"))
            row += row_for_source(by_src.get("meridian"))
            row += row_for_source(by_src.get("balkanbet"))
            w.writerow(row)

def write_txt(clusters: List[List[Dict]]):
    lines: List[str] = []
    for cl in clusters:
        lines.append("="*86)
        t = canonical_time(cl)
        h, a = canonical_pair(cl)
        leagues = [r.get("league","") for r in cl if r.get("league")]
        league_tag = f"[{mode(leagues)}]" if leagues else ""
        lines.append(f"{t}  {league_tag}".strip())
        present = ", ".join(SRC_LABEL[r["src"]] for r in cl)
        lines.append(f"{h}  vs  {a}   (izvori: {present})")

        by_src = aligned_by_src(cl)  # <<< PORAVNATO
        for code in SRC_ORDER:
            r = by_src.get(code)
            if not r: continue
            od = r["odds"]
            tag = SRC_LABEL[code].upper()
            lines.append(f"{tag}:  1={fmt_od(od.get('1'))}  X={fmt_od(od.get('X'))}  2={fmt_od(od.get('2'))}")
            lines.append(f"       0-2={fmt_od(od.get('0-2'))}  2+={fmt_od(od.get('2+'))}  3+={fmt_od(od.get('3+'))}")
            lines.append(f"       GG={fmt_od(od.get('GG'))}  IGG={fmt_od(od.get('IGG'))}  GG&3+={fmt_od(od.get('GG&3+'))}")
        lines.append("")
    OUT_TXT.write_text("\n".join(lines), encoding="utf-8")

def write_report(clusters: List[List[Dict]], totals_by_src: Dict[str,int]):
    matched: Dict[str, List[Tuple[str,List[str]]]] = {s: [] for s in SRC_ORDER}
    leftovers: Dict[str, List[str]] = {s: [] for s in SRC_ORDER}

    def rec_key(r: Dict) -> str:
        return f"{r.get('time','')}  {r['home']} vs {r['away']}"

    for cl in clusters:
        if len(cl) == 1:
            r = cl[0]
            leftovers[r["src"]].append(rec_key(r))
        else:
            present_codes = [r["src"] for r in cl]
            for r in cl:
                others = [SRC_LABEL[c] for c in present_codes if c != r["src"]]
                matched[r["src"]].append((rec_key(r), others))

    lines: List[str] = []
    lines.append("="*90)
    lines.append("UKUPAN BROJ MEČEVA PO KLADIONICI")
    lines.append("-"*90)
    for code in SRC_ORDER:
        tot = totals_by_src.get(code, 0)
        lines.append(f"{SRC_LABEL[code]}: {tot}")
    lines.append("")

    lines.append("PROCENAT NEUPAREĐENIH PO KLADIONICI")
    lines.append("-"*90)
    for code in SRC_ORDER:
        tot = totals_by_src.get(code, 0)
        if tot <= 0:
            lines.append(f"{SRC_LABEL[code]}: -")
            continue
        unp = len(leftovers[code])
        pct = 100.0 * unp / tot
        lines.append(f"{SRC_LABEL[code]}: {unp}/{tot}  ({pct:.1f}%) nije upareno")
    lines.append("")

    def sec(title_code: str) -> List[str]:
        L: List[str] = []
        L.append("="*90)
        L.append(SRC_LABEL[title_code].upper())
        L.append("-"*90)
        L.append("POKLAPAJU SE:")
        if matched[title_code]:
            for k, others in matched[title_code]:
                L.append(f"  • {k}   (sa: {', '.join(others)})")
        else:
            L.append("  (nema)")
        L.append("")
        L.append("NE POKLAPAJU SE:")
        if leftovers[title_code]:
            for k in leftovers[title_code]:
                L.append(f"  • {k}")
        else:
            L.append("  (nema)")
        L.append("")
        return L

    for code in SRC_ORDER:
        lines += sec(code)

    REPORT_MATCH.write_text("\n".join(lines), encoding="utf-8")

# -----------------------------
# Arbitraža: izvestaj.txt
# -----------------------------
def best_odds_for_market(by_src: Dict[str, Dict], keys: List[str], *, strict_1x2: bool = False) -> Dict[str, Tuple[float, str]]:
    """
    Vrati najbolju kvotu (value, source_label) po svakoj stavci iz 'keys'.
    Ako je strict_1x2=True i keys su 1/X/2, koristi samo izvore koji imaju
    koherentan komplet 1X2 (sprečava mešanje pogrešnih marketa).
    """
    scan_src = by_src
    if strict_1x2 and set(keys) == {"1","X","2"}:
        scan_src = filter_sources_for_1x2_best(by_src)

    best: Dict[str, Tuple[float, str]] = {k: (0.0, "") for k in keys}
    for code, rec in scan_src.items():
        if not rec:
            continue
        od = rec.get("odds", {})
        for k in keys:
            val = od.get(k)
            if is_plausible_odd(val):
                v = float(val)
                if v > best[k][0]:
                    best[k] = (v, SRC_LABEL.get(code, code))

    return {k: v for k, v in best.items() if v[0] > 0.0 and v[1]}

def surebet_3way(best: Dict[str, Tuple[float,str]]) -> Optional[Dict]:
    need = ["1","X","2"]
    if not all(k in best for k in need):
        return None
    a, b, c = best["1"][0], best["X"][0], best["2"][0]
    S = (1.0/a) + (1.0/b) + (1.0/c)
    if S >= 1.0:
        return None
    profit_pct = (1.0 - S) * 100.0
    bankroll = 100.0
    s1 = (bankroll / a) / S
    sX = (bankroll / b) / S
    s2 = (bankroll / c) / S
    return {
        "S": S,
        "profit_pct": profit_pct,
        "bankroll": bankroll,
        "stakes": {
            "1": (s1, best["1"][1], a),
            "X": (sX, best["X"][1], b),
            "2": (s2, best["2"][1], c),
        }
    }

def surebet_2way(best: Dict[str, Tuple[float, str]], under_key: str, over_key: str) -> Optional[Dict]:
    if under_key not in best or over_key not in best:
        return None
    a = best[under_key][0]
    b = best[over_key][0]
    S = (1.0/a) + (1.0/b)
    if S >= 1.0:
        return None
    profit_pct = (1.0 - S) * 100.0
    bankroll = 100.0
    su = (bankroll / a) / S
    so = (bankroll / b) / S
    return {
        "S": S,
        "profit_pct": profit_pct,
        "bankroll": bankroll,
        "stakes": {
            under_key: (su, best[under_key][1], a),
            over_key:  (so, best[over_key][1], b),
        }
    }

def write_arbitrage_report(clusters: List[List[Dict]]):
    out: List[str] = []
    total_found = 0

    for cl in clusters:
        by_src_raw = aligned_by_src(cl)  # <<< PORAVNATO KROZ SVE
        t = canonical_time(cl)
        h, a = canonical_pair(cl)
        leagues = [r.get("league","") for r in cl if r.get("league")]
        league_tag = f"[{mode(leagues)}]" if leagues else ""

        # 1) 1X2 — samo koherentni izvori
        best_1x2 = best_odds_for_market(by_src_raw, ["1","X","2"], strict_1x2=True)
        sb3 = surebet_3way(best_1x2)

        # 2) OU 2.5 (0-2 vs 3+)
        best_ou = best_odds_for_market(by_src_raw, ["0-2","3+"], strict_1x2=False)
        sb2 = surebet_2way(best_ou, "0-2", "3+")

        if not sb3 and not sb2:
            continue

        total_found += 1
        out.append("="*86)
        out.append(f"{t}  {league_tag}".strip())
        present = ", ".join(SRC_LABEL[r["src"]] for r in cl)
        out.append(f"{h}  vs  {a}   (izvori: {present})")
        out.append("")

        if sb3:
            out.append("ARBITRAŽA — 1X2 (samo koherentni izvori)")
            out.append(f"  Najbolje kvote: 1={best_1x2['1'][0]} ({best_1x2['1'][1]}), "
                       f"X={best_1x2['X'][0]} ({best_1x2['X'][1]}), "
                       f"2={best_1x2['2'][0]} ({best_1x2['2'][1]})")
            out.append(f"  Suma reciprocala: S={sb3['S']:.4f}  → Profit ≈ {sb3['profit_pct']:.2f}%")
            out.append(f"  Raspodela uloga (bankroll=100):")
            st = sb3["stakes"]
            out.append(f"    1:  ulog={st['1'][0]:.2f}  | kvota={st['1'][2]}  | kladionica={st['1'][1]}")
            out.append(f"    X:  ulog={st['X'][0]:.2f}  | kvota={st['X'][2]}  | kladionica={st['X'][1]}")
            out.append(f"    2:  ulog={st['2'][0]:.2f}  | kvota={st['2'][2]}  | kladionica={st['2'][1]}")
            out.append("")

        if sb2:
            out.append("ARBITRAŽA — UKUPNO GOLOVA 2.5  (0-2 vs 3+)")
            out.append(f"  Najbolje kvote: 0-2={best_ou['0-2'][0]} ({best_ou['0-2'][1]}), "
                       f"3+={best_ou['3+'][0]} ({best_ou['3+'][1]})")
            out.append(f"  Suma reciprocala: S={sb2['S']:.4f}  → Profit ≈ {sb2['profit_pct']:.2f}%")
            out.append(f"  Raspodela uloga (bankroll=100):")
            st2 = sb2["stakes"]
            su = st2['0-2']; so = st2['3+']
            out.append(f"    0-2: ulog={su[0]:.2f}  | kvota={su[2]}  | kladionica={su[1]}")
            out.append(f"    3+:  ulog={so[0]:.2f}  | kvota={so[2]}  | kladionica={so[1]}")
            out.append("")

    if total_found == 0:
        out.append("Nisu pronađene arbitražne prilike na tržištima 1X2 i 0-2/3+ (na osnovu dostupnih kvota).")
    REPORT_ARB.write_text("\n".join(out), encoding="utf-8")

# -----------------------------
# Main
# -----------------------------
def main():
    # Parsiranje svih dostupnih izvora
    soccer   = parse_file_pretty(SOCCER_TXT,   "soccer")
    merkur   = parse_file_pretty(MERKUR_TXT,   "merkur")
    mozzart  = parse_file_pretty(MOZZART_TXT,  "mozzart")
    betole   = parse_file_pretty(BETOLE_TXT,   "betole")
    meridian = parse_file_pretty(MERIDIAN_TXT, "meridian")
    balkan   = parse_file_pretty(BALKAN_TXT,   "balkanbet")

    all_records: List[Dict] = []
    for rec in soccer:   all_records.append(rec)
    for rec in merkur:   all_records.append(rec)
    for rec in mozzart:  all_records.append(rec)
    for rec in betole:   all_records.append(rec)
    for rec in meridian: all_records.append(rec)
    for rec in balkan:   all_records.append(rec)

    if not all_records:
        raise SystemExit("Nema nijednog ulaznog fajla sa mečevima (pretty).")

    # ALL-vs-ALL klasterisanje
    clusters = cluster_all_sources(all_records)

    # Izlazi
    write_csv(clusters)
    write_txt(clusters)

    totals_by_src = {
        "soccer":   len(soccer),
        "merkur":   len(merkur),
        "mozzart":  len(mozzart),
        "betole":   len(betole),
        "meridian": len(meridian),
        "balkanbet":len(balkan),
    }
    write_report(clusters, totals_by_src)

    # izvestaj.txt — arbitraža
    write_arbitrage_report(clusters)

    print("[OK] Napravljeno:")
    print(" -", OUT_CSV.resolve())
    print(" -", OUT_TXT.resolve())
    print(" -", REPORT_MATCH.resolve())
    print(" -", REPORT_ARB.resolve())

if __name__ == "__main__":
    main()
