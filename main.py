# main.py
# -*- coding: utf-8 -*-

import csv
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher

# Ulazni fajlovi (pretty)
SOCCER_TXT    = Path("soccer_mecevi_pregled.txt")
MERKUR_TXT    = Path("merkur_mecevi_pregled.txt")
MOZZART_TXT   = Path("mozzart_mecevi_pregled.txt")      # opcioni
BETOLE_TXT    = Path("betole_mecevi_pregled.txt")        # opcioni
MERIDIAN_TXT  = Path("meridian_mecevi_pregled.txt")      # NOVO (pretty)

# Izlazi
OUT_CSV = Path("mecevi_spojeno.csv")
OUT_TXT = Path("mecevi_spojeno.txt")

# Regex paterni
TIME_RE = r"(?:[01]?\d|2[0-3]):[0-5]\d"
DAY_RE = r"(Pon|Uto|Sre|Čet|Cet|Pet|Sub|Ned)"
DATE_RE = r"\d{1,2}\.\d{1,2}\."
LEAGUE_BRACKET_RE = r"\[([A-ZČĆŠĐŽ0-9 ,.'/-]{2,40})\]"
FLOAT_VAL_RE = r"(\d+(?:[.,]\d+)?)"

# ----------------- util -----------------

def _to_float(s: str) -> Optional[float]:
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None

def _grab_val(label: str, line: str) -> Optional[float]:
    """Traži '<label>\s*=\s*<broj>'."""
    pat = rf"{re.escape(label)}\s*=\s*{FLOAT_VAL_RE}"
    m = re.search(pat, line, flags=re.I)
    return _to_float(m.group(1)) if m else None

def strip_accents(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))

def normalize_team(s: str) -> str:
    s0 = strip_accents(s.lower())
    s0 = re.sub(r"[.\-–—_/]", " ", s0)
    replacements = {
        r"\butd\b": "united",
        r"\bu\.?n\.?a\.?m\.?\b": "unam",
        r"\bpumas\b": "pumas",
        r"\bafc\b": "",
        r"\bfc\b": "",
        r"\bcf\b": "",
        r"\bsc\b": "",
        r"\bif\b": "",
        r"\bab\b": "",
        r"\bthe\b": "",
    }
    for pat, rep in replacements.items():
        s0 = re.sub(pat, rep, s0)
    s0 = re.sub(r"\s+", " ", s0).strip()
    return s0

def fuzzy(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def _time_to_minutes(t: str) -> Optional[int]:
    m = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", t.strip())
    if not m:
        return None
    return int(m.group(1)) * 60 + int(m.group(2))

def _times_close(t1: str, t2: str, tolerance_min: int = 10) -> bool:
    m1, m2 = _time_to_minutes(t1), _time_to_minutes(t2)
    if m1 is None or m2 is None:
        return False
    return abs(m1 - m2) <= tolerance_min

def parse_blocks_pretty(txt: str) -> List[List[str]]:
    """Razbije na blokove po linijama '====...'; vraća liste nepraznih linija po bloku."""
    lines = [ln.rstrip() for ln in txt.splitlines()]
    blocks: List[List[str]] = []
    cur: List[str] = []
    for ln in lines:
        if re.match(r"^=+", ln):
            if cur:
                blocks.append(cur)
            cur = []
        else:
            if ln.strip():
                cur.append(ln.strip())
    if cur:
        blocks.append(cur)
    return blocks

# ----------------- PARSERI -----------------

def parse_pretty_block(lines: List[str], src_name: str) -> Optional[Dict]:
    """
    Očekivani pretty format:
      0: "HH:MM  DAY  DD.MM.  [LEAGUE]"   (datum/league mogu izostati)
      1: "Home  vs  Away   (ID: ...?)"
      2: "1=...   X=...   2=..."
      3: "0-2=...  2+=...  3+=..."
      4: "GG=...   IGG=...   GG&3+=..."
    """
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
    league_s = m_league.group(1).strip() if m_league else ""

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
        "GG": None, "IGG": None, "GG&3+": None
    }

    for ln in lines:
        if odds_map["1"] is None: odds_map["1"] = _grab_val("1", ln)
        if odds_map["X"] is None: odds_map["X"] = _grab_val("X", ln)
        if odds_map["2"] is None: odds_map["2"] = _grab_val("2", ln)

        if odds_map["0-2"] is None: odds_map["0-2"] = _grab_val("0-2", ln)
        if odds_map["2+"]  is None: odds_map["2+"]  = _grab_val("2+", ln)
        if odds_map["3+"]  is None: odds_map["3+"]  = _grab_val("3+", ln)

        if odds_map["GG"]     is None: odds_map["GG"]     = _grab_val("GG", ln)
        if odds_map["IGG"]    is None: odds_map["IGG"]    = _grab_val("IGG", ln) or _grab_val("I GG", ln)
        if odds_map["GG&3+"]  is None: odds_map["GG&3+"]  = _grab_val("GG&3+", ln)

    if not (time_s and home and away):
        return None

    return {
        "src": src_name,
        "time": time_s,
        "day": day_s,
        "date": date_s,
        "league": league_s,
        "home": home,
        "away": away,
        "match_id": match_id,
        "odds": odds_map,
    }

def parse_soccer_block(lines: List[str]) -> Optional[Dict]:
    return parse_pretty_block(lines, "soccer")

def parse_mozzart_block(lines: List[str]) -> Optional[Dict]:
    return parse_pretty_block(lines, "mozzart")

def parse_betole_block(lines: List[str]) -> Optional[Dict]:
    return parse_pretty_block(lines, "betole")

def parse_meridian_block(lines: List[str]) -> Optional[Dict]:
    return parse_pretty_block(lines, "meridian")

def parse_merkur_block(lines: List[str]) -> Optional[Dict]:
    """
    Merkur pretty je malo drugačiji (može 'UG 0-2', '4+' itd.)
    """
    if not lines:
        return None

    head_line = next((l for l in lines if "vs" in l.lower()), "")
    if not head_line:
        return None

    m_time = re.search(TIME_RE, head_line)
    time_s = m_time.group(0) if m_time else ""

    seg = head_line
    if "|" in seg:
        seg = seg.split("|", 1)[1].strip()

    match_id = ""
    mid = re.search(r"\(ID:\s*([^)]+)\)", seg)
    if mid:
        match_id = mid.group(1).strip()
        seg = re.sub(r"\(ID:[^)]+\)", "", seg).strip()

    mt = re.search(r"(.+?)\s+vs\s+(.+)", seg, re.I)
    if not mt:
        return None
    home = mt.group(1).strip()
    away = mt.group(2).strip()

    odds_map: Dict[str, Optional[float]] = {
        "1": None, "X": None, "2": None,
        "0-2": None, "2+": None, "3+": None,
        "GG": None, "IGG": None, "GG&3+": None
    }

    for ln in lines:
        odds_map["1"] = odds_map["1"] or _grab_val("1", ln)
        odds_map["X"] = odds_map["X"] or _grab_val("X", ln)
        odds_map["2"] = odds_map["2"] or _grab_val("2", ln)

        if odds_map["0-2"] is None:
            odds_map["0-2"] = _grab_val("0-2", ln) or _grab_val("UG 0-2", ln)
        if odds_map["3+"] is None:
            odds_map["3+"] = _grab_val("3+", ln)
        if odds_map["3+"] is None:
            alt_4 = _grab_val("4+", ln)
            if alt_4 is not None:
                odds_map["3+"] = alt_4

        odds_map["GG"] = odds_map["GG"] or _grab_val("GG", ln)
        if odds_map["IGG"] is None:
            odds_map["IGG"] = _grab_val("IGG", ln) or _grab_val("I GG", ln)
        odds_map["GG&3+"] = odds_map["GG&3+"] or _grab_val("GG&3+", ln)

    if not (time_s and home and away):
        return None

    return {
        "src": "merkur",
        "time": time_s,
        "day": "",
        "date": "",
        "league": "",
        "home": home,
        "away": away,
        "match_id": match_id,
        "odds": odds_map,
    }

def parse_file_generic(path: Path, which: str) -> List[Dict]:
    txt = path.read_text(encoding="utf-8", errors="ignore")
    blocks = parse_blocks_pretty(txt)
    out: List[Dict] = []
    for b in blocks:
        if which == "soccer":
            rec = parse_soccer_block(b)
        elif which == "merkur":
            rec = parse_merkur_block(b)
        elif which == "mozzart":
            rec = parse_mozzart_block(b)
        elif which == "betole":
            rec = parse_betole_block(b)
        elif which == "meridian":
            rec = parse_meridian_block(b)
        else:
            rec = None
        if rec:
            out.append(rec)
    return out

# ----------------- UPARIVANJE (5 izvora) -----------------

def match_records_five(
    soc: List[Dict], mer: List[Dict], moz: List[Dict], bet: List[Dict], mdi: List[Dict],
    team_threshold: float = 0.82,
    time_tolerance_min: int = 10
) -> Tuple[
    List[Tuple[Dict, Optional[Dict], bool, Optional[Dict], bool, Optional[Dict], bool, Optional[Dict], bool]],
    List[Dict], List[Dict], List[Dict], List[Dict]
]:
    """
    Vraća:
      merged: (soccer, merkur?, mer_swap, mozzart?, moz_swap, betole?, bet_swap, meridian?, mdi_swap)
      leftovers_mer / leftovers_moz / leftovers_bet / leftovers_mdi
    """
    used_mer, used_moz, used_bet, used_mdi = set(), set(), set(), set()
    merged: List[Tuple[Dict, Optional[Dict], bool, Optional[Dict], bool, Optional[Dict], bool, Optional[Dict], bool]] = []

    for s in soc:
        h_s = normalize_team(s["home"])
        a_s = normalize_team(s["away"])
        t_s = s["time"]

        # MERKUR
        best_mer_j, best_mer_sw, best_mer_score = -1, False, 0.0
        for j, m in enumerate(mer):
            if j in used_mer:
                continue
            if not _times_close(m["time"], t_s, tolerance_min=time_tolerance_min):
                continue
            h_m = normalize_team(m["home"]); a_m = normalize_team(m["away"])
            score_straight = (fuzzy(h_s, h_m) + fuzzy(a_s, a_m)) / 2.0
            score_swap    = (fuzzy(h_s, a_m) + fuzzy(a_s, h_m)) / 2.0
            if score_straight >= best_mer_score and score_straight >= team_threshold:
                best_mer_score, best_mer_j, best_mer_sw = score_straight, j, False
            if score_swap >= best_mer_score and score_swap >= team_threshold:
                best_mer_score, best_mer_j, best_mer_sw = score_swap, j, True

        # MOZZART
        best_moz_k, best_moz_sw, best_moz_score = -1, False, 0.0
        for k, z in enumerate(moz):
            if k in used_moz:
                continue
            if not _times_close(z["time"], t_s, tolerance_min=time_tolerance_min):
                continue
            h_z = normalize_team(z["home"]); a_z = normalize_team(z["away"])
            score_straight = (fuzzy(h_s, h_z) + fuzzy(a_s, a_z)) / 2.0
            score_swap    = (fuzzy(h_s, a_z) + fuzzy(a_s, h_z)) / 2.0
            if score_straight >= best_moz_score and score_straight >= team_threshold:
                best_moz_score, best_moz_k, best_moz_sw = score_straight, k, False
            if score_swap >= best_moz_score and score_swap >= team_threshold:
                best_moz_score, best_moz_k, best_moz_sw = score_swap, k, True

        # BETOLE
        best_bet_l, best_bet_sw, best_bet_score = -1, False, 0.0
        for l, b in enumerate(bet):
            if l in used_bet:
                continue
            if not _times_close(b["time"], t_s, tolerance_min=time_tolerance_min):
                continue
            h_b = normalize_team(b["home"]); a_b = normalize_team(b["away"])
            score_straight = (fuzzy(h_s, h_b) + fuzzy(a_s, a_b)) / 2.0
            score_swap    = (fuzzy(h_s, a_b) + fuzzy(a_s, h_b)) / 2.0
            if score_straight >= best_bet_score and score_straight >= team_threshold:
                best_bet_score, best_bet_l, best_bet_sw = score_straight, l, False
            if score_swap >= best_bet_score and score_swap >= team_threshold:
                best_bet_score, best_bet_l, best_bet_sw = score_swap, l, True

        # MERIDIAN
        best_mdi_h, best_mdi_sw, best_mdi_score = -1, False, 0.0
        for h, d in enumerate(mdi):
            if h in used_mdi:
                continue
            if not _times_close(d["time"], t_s, tolerance_min=time_tolerance_min):
                continue
            h_d = normalize_team(d["home"]); a_d = normalize_team(d["away"])
            score_straight = (fuzzy(h_s, h_d) + fuzzy(a_s, a_d)) / 2.0
            score_swap    = (fuzzy(h_s, a_d) + fuzzy(a_s, h_d)) / 2.0
            if score_straight >= best_mdi_score and score_straight >= team_threshold:
                best_mdi_score, best_mdi_h, best_mdi_sw = score_straight, h, False
            if score_swap >= best_mdi_score and score_swap >= team_threshold:
                best_mdi_score, best_mdi_h, best_mdi_sw = score_swap, h, True

        mer_obj = mer[best_mer_j] if best_mer_j >= 0 else None
        moz_obj = moz[best_moz_k] if best_moz_k >= 0 else None
        bet_obj = bet[best_bet_l] if best_bet_l >= 0 else None
        mdi_obj = mdi[best_mdi_h] if best_mdi_h >= 0 else None

        if best_mer_j >= 0: used_mer.add(best_mer_j)
        if best_moz_k >= 0: used_moz.add(best_moz_k)
        if best_bet_l >= 0: used_bet.add(best_bet_l)
        if best_mdi_h >= 0: used_mdi.add(best_mdi_h)

        merged.append((s, mer_obj, best_mer_sw, moz_obj, best_moz_sw, bet_obj, best_bet_sw, mdi_obj, best_mdi_sw))

    leftovers_mer = [m for j, m in enumerate(mer) if j not in used_mer]
    leftovers_moz = [z for k, z in enumerate(moz) if k not in used_moz]
    leftovers_bet = [b for l, b in enumerate(bet) if l not in used_bet]
    leftovers_mdi = [d for h, d in enumerate(mdi) if h not in used_mdi]
    return merged, leftovers_mer, leftovers_moz, leftovers_bet, leftovers_mdi

# ----------------- IZLAZ -----------------

def write_csv_and_txt_five(
    merged: List[Tuple[Dict, Optional[Dict], bool, Optional[Dict], bool, Optional[Dict], bool, Optional[Dict], bool]],
    leftovers_mer: List[Dict],
    leftovers_moz: List[Dict],
    leftovers_bet: List[Dict],
    leftovers_mdi: List[Dict],
    summary: Dict[str, Tuple[int, int, float]]
):
    # CSV
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow([
            "TIME","DAY","DATE","LEAGUE","HOME","AWAY",
            "SOCCER_ID","MERKUR_ID","MOZ_ID","BET_ID","MDI_ID",
            "S_1","S_X","S_2","S_0-2","S_2+","S_3+","S_GG","S_IGG","S_GG&3+",
            "M_1","M_X","M_2","M_0-2","M_2+","M_3+","M_GG","M_IGG","M_GG&3+",
            "MOZ_1","MOZ_X","MOZ_2","MOZ_0-2","MOZ_2+","MOZ_3+","MOZ_GG","MOZ_IGG","MOZ_GG&3+",
            "BET_1","BET_X","BET_2","BET_0-2","BET_2+","BET_3+","BET_GG","BET_IGG","BET_GG&3+",
            "MDI_1","MDI_X","MDI_2","MDI_0-2","MDI_2+","MDI_3+","MDI_GG","MDI_IGG","MDI_GG&3+",
            "SWAP_M","SWAP_MOZ","SWAP_BET","SWAP_MDI"
        ])
        for s, m, m_sw, z, z_sw, b, b_sw, d, d_sw in merged:
            s_od = s["odds"]
            m_od = (m or {}).get("odds") if m else {}
            z_od = (z or {}).get("odds") if z else {}
            b_od = (b or {}).get("odds") if b else {}
            d_od = (d or {}).get("odds") if d else {}

            w.writerow([
                s["time"], s["day"], s["date"], s["league"], s["home"], s["away"],
                s.get("match_id",""), (m or {}).get("match_id",""), (z or {}).get("match_id",""),
                (b or {}).get("match_id",""), (d or {}).get("match_id",""),
                s_od.get("1"), s_od.get("X"), s_od.get("2"),
                s_od.get("0-2"), s_od.get("2+"), s_od.get("3+"),
                s_od.get("GG"), s_od.get("IGG"), s_od.get("GG&3+"),
                (m_od or {}).get("1"), (m_od or {}).get("X"), (m_od or {}).get("2"),
                (m_od or {}).get("0-2"), (m_od or {}).get("2+"), (m_od or {}).get("3+"),
                (m_od or {}).get("GG"), (m_od or {}).get("IGG"), (m_od or {}).get("GG&3+"),
                (z_od or {}).get("1"), (z_od or {}).get("X"), (z_od or {}).get("2"),
                (z_od or {}).get("0-2"), (z_od or {}).get("2+"), (z_od or {}).get("3+"),
                (z_od or {}).get("GG"), (z_od or {}).get("IGG"), (z_od or {}).get("GG&3+"),
                (b_od or {}).get("1"), (b_od or {}).get("X"), (b_od or {}).get("2"),
                (b_od or {}).get("0-2"), (b_od or {}).get("2+"), (b_od or {}).get("3+"),
                (b_od or {}).get("GG"), (b_od or {}).get("IGG"), (b_od or {}).get("GG&3+"),
                (d_od or {}).get("1"), (d_od or {}).get("X"), (d_od or {}).get("2"),
                (d_od or {}).get("0-2"), (d_od or {}).get("2+"), (d_od or {}).get("3+"),
                (d_od or {}).get("GG"), (d_od or {}).get("IGG"), (d_od or {}).get("GG&3+"),
                "swap" if m_sw else "", "swap" if z_sw else "", "swap" if b_sw else "", "swap" if d_sw else ""
            ])

    # TXT
    def fmt(x):
        return "-" if x is None else (str(int(x)) if isinstance(x,(int,float)) and float(x).is_integer() else f"{x}")

    soc_u, soc_t, soc_p = summary.get("soccer", (0,0,0.0))
    mer_u, mer_t, mer_p = summary.get("merkur", (0,0,0.0))
    moz_u, moz_t, moz_p = summary.get("mozzart", (0,0,0.0))
    bet_u, bet_t, bet_p = summary.get("betole",  (0,0,0.0))
    mdi_u, mdi_t, mdi_p = summary.get("meridian",(0,0,0.0))

    lines: List[str] = []
    lines.append("SAŽETAK NEUPAREĐENIH (u odnosu na broj mečeva u kladionici)")
    lines.append(f"- Soccer:   {soc_u}/{soc_t}  ({soc_p:.1f}%) nije upareno")
    lines.append(f"- Merkur:   {mer_u}/{mer_t}  ({mer_p:.1f}%) nije upareno")
    lines.append(f"- Mozzart:  {moz_u}/{moz_t}  ({moz_p:.1f}%) nije upareno")
    lines.append(f"- Betole:   {bet_u}/{bet_t}  ({bet_p:.1f}%) nije upareno")
    lines.append(f"- Meridian: {mdi_u}/{mdi_t}  ({mdi_p:.1f}%) nije upareno")
    lines.append("")

    for s, m, m_sw, z, z_sw, b, b_sw, d, d_sw in merged:
        lines.append("=" * 84)
        hdr = f"{s['time']}  {s['day']} {s['date']}  [{s['league']}]".strip()
        lines.append(hdr)
        pair = f"{s['home']}  vs  {s['away']}"
        flags = []
        if m: flags.append("upareno sa Merkur" + (" [swap]" if m_sw else ""))
        if z: flags.append("upareno sa Mozzart" + (" [swap]" if z_sw else ""))
        if b: flags.append("upareno sa Betole"  + (" [swap]" if b_sw else ""))
        if d: flags.append("upareno sa Meridian" + (" [swap]" if d_sw else ""))
        pair += "    (" + " ; ".join(flags) + ")" if flags else "    (NEMA parova u drugim kladionicama)"
        lines.append(pair)

        s_od = s["odds"]
        lines.append(f"SOCCER:    1={fmt(s_od.get('1'))}   X={fmt(s_od.get('X'))}   2={fmt(s_od.get('2'))}")
        lines.append(f"           0-2={fmt(s_od.get('0-2'))}   2+={fmt(s_od.get('2+'))}   3+={fmt(s_od.get('3+'))}")
        lines.append(f"           GG={fmt(s_od.get('GG'))}   IGG={fmt(s_od.get('IGG'))}   GG&3+={fmt(s_od.get('GG&3+'))}")

        if m:
            m_od = m["odds"]
            lines.append(f"MERKUR:    1={fmt(m_od.get('1'))}   X={fmt(m_od.get('X'))}   2={fmt(m_od.get('2'))}")
            lines.append(f"           0-2={fmt(m_od.get('0-2'))}   2+={fmt(m_od.get('2+'))}   3+={fmt(m_od.get('3+'))}")
            lines.append(f"           GG={fmt(m_od.get('GG'))}   IGG={fmt(m_od.get('IGG'))}   GG&3+={fmt(m_od.get('GG&3+'))}")

        if z:
            z_od = z["odds"]
            lines.append(f"MOZZART:   1={fmt(z_od.get('1'))}   X={fmt(z_od.get('X'))}   2={fmt(z_od.get('2'))}")
            lines.append(f"           0-2={fmt(z_od.get('0-2'))}   2+={fmt(z_od.get('2+'))}   3+={fmt(z_od.get('3+'))}")
            lines.append(f"           GG={fmt(z_od.get('GG'))}   IGG={fmt(z_od.get('IGG'))}   GG&3+={fmt(z_od.get('GG&3+'))}")

        if b:
            b_od = b["odds"]
            lines.append(f"BETOLE:    1={fmt(b_od.get('1'))}   X={fmt(b_od.get('X'))}   2={fmt(b_od.get('2'))}")
            lines.append(f"           0-2={fmt(b_od.get('0-2'))}   2+={fmt(b_od.get('2+'))}   3+={fmt(b_od.get('3+'))}")
            lines.append(f"           GG={fmt(b_od.get('GG'))}   IGG={fmt(b_od.get('IGG'))}   GG&3+={fmt(b_od.get('GG&3+'))}")

        if d:
            d_od = d["odds"]
            lines.append(f"MERIDIAN:  1={fmt(d_od.get('1'))}   X={fmt(d_od.get('X'))}   2={fmt(d_od.get('2'))}")
            lines.append(f"           0-2={fmt(d_od.get('0-2'))}   2+={fmt(d_od.get('2+'))}   3+={fmt(d_od.get('3+'))}")
            lines.append(f"           GG={fmt(d_od.get('GG'))}   IGG={fmt(d_od.get('IGG'))}   GG&3+={fmt(d_od.get('GG&3+'))}")

        lines.append("")

    if leftovers_mer:
        lines.append("\nNEUPAREĐENI MERKUR:")
        for m in leftovers_mer:
            m_od = m["odds"]
            lines.append("-" * 84)
            lines.append(f"{m['time']}  {m['home']} vs {m['away']}")
            lines.append(f"  1={fmt(m_od.get('1'))}  X={fmt(m_od.get('X'))}  2={fmt(m_od.get('2'))}")
            lines.append(f"  0-2={fmt(m_od.get('0-2'))}  2+={fmt(m_od.get('2+'))}  3+={fmt(m_od.get('3+'))}")
            lines.append(f"  GG={fmt(m_od.get('GG'))}  IGG={fmt(m_od.get('IGG'))}  GG&3+={fmt(m_od.get('GG&3+'))}")

    if leftovers_moz:
        lines.append("\nNEUPAREĐENI MOZZART:")
        for z in leftovers_moz:
            z_od = z["odds"]
            lines.append("-" * 84)
            lines.append(f"{z['time']}  {z['home']} vs {z['away']}")
            lines.append(f"  1={fmt(z_od.get('1'))}  X={fmt(z_od.get('X'))}  2={fmt(z_od.get('2'))}")
            lines.append(f"  0-2={fmt(z_od.get('0-2'))}  2+={fmt(z_od.get('2+'))}  3+={fmt(z_od.get('3+'))}")
            lines.append(f"  GG={fmt(z_od.get('GG'))}  IGG={fmt(z_od.get('IGG'))}  GG&3+={fmt(z_od.get('GG&3+'))}")

    if leftovers_bet:
        lines.append("\nNEUPAREĐENI BETOLE:")
        for b in leftovers_bet:
            b_od = b["odds"]
            lines.append("-" * 84)
            lines.append(f"{b['time']}  {b['home']} vs {b['away']}")
            lines.append(f"  1={fmt(b_od.get('1'))}  X={fmt(b_od.get('X'))}  2={fmt(b_od.get('2'))}")
            lines.append(f"  0-2={fmt(b_od.get('0-2'))}  2+={fmt(b_od.get('2+'))}  3+={fmt(b_od.get('3+'))}")
            lines.append(f"  GG={fmt(b_od.get('GG'))}  IGG={fmt(b_od.get('IGG'))}  GG&3+={fmt(b_od.get('GG&3+'))}")

    if leftovers_mdi:
        lines.append("\nNEUPAREĐENI MERIDIAN:")
        for d in leftovers_mdi:
            d_od = d["odds"]
            lines.append("-" * 84)
            lines.append(f"{d['time']}  {d['home']} vs {d['away']}")
            lines.append(f"  1={fmt(d_od.get('1'))}  X={fmt(d_od.get('X'))}  2={fmt(d_od.get('2'))}")
            lines.append(f"  0-2={fmt(d_od.get('0-2'))}  2+={fmt(d_od.get('2+'))}  3+={fmt(d_od.get('3+'))}")
            lines.append(f"  GG={fmt(d_od.get('GG'))}  IGG={fmt(d_od.get('IGG'))}  GG&3+={fmt(d_od.get('GG&3+'))}")

    OUT_TXT.write_text("\n".join(lines), encoding="utf-8")

# ----------------- MAIN -----------------

def main():
    if not SOCCER_TXT.exists():
        raise SystemExit(f"Nema fajla: {SOCCER_TXT}")
    if not MERKUR_TXT.exists():
        raise SystemExit(f"Nema fajla: {MERKUR_TXT}")

    moz_exists = MOZZART_TXT.exists()
    bet_exists = BETOLE_TXT.exists()
    mdi_exists = MERIDIAN_TXT.exists()

    soccer   = parse_file_generic(SOCCER_TXT, "soccer")
    merkur   = parse_file_generic(MERKUR_TXT, "merkur")
    mozzart  = parse_file_generic(MOZZART_TXT, "mozzart")  if moz_exists else []
    betole   = parse_file_generic(BETOLE_TXT, "betole")    if bet_exists else []
    meridian = parse_file_generic(MERIDIAN_TXT, "meridian") if mdi_exists else []

    merged, leftovers_mer, leftovers_moz, leftovers_bet, leftovers_mdi = match_records_five(
        soccer, merkur, mozzart, betole, meridian,
        team_threshold=0.82,
        time_tolerance_min=10
    )

    # procenti neupaređenih
    total_soc, total_mer, total_moz, total_bet, total_mdi = len(soccer), len(merkur), len(mozzart), len(betole), len(meridian)
    soc_unp = sum(1 for s, m, _, z, _, b, _, d, _ in merged if m is None and z is None and b is None and d is None)
    mer_unp = len(leftovers_mer)
    moz_unp = len(leftovers_moz)
    bet_unp = len(leftovers_bet)
    mdi_unp = len(leftovers_mdi)

    pct_soc = (100.0 * soc_unp / total_soc) if total_soc else 0.0
    pct_mer = (100.0 * mer_unp / total_mer) if total_mer else 0.0
    pct_moz = (100.0 * moz_unp / total_moz) if total_moz else 0.0
    pct_bet = (100.0 * bet_unp / total_bet) if total_bet else 0.0
    pct_mdi = (100.0 * mdi_unp / total_mdi) if total_mdi else 0.0

    summary = {
        "soccer":   (soc_unp, total_soc, pct_soc),
        "merkur":   (mer_unp, total_mer, pct_mer),
        "mozzart":  (moz_unp, total_moz, pct_moz),
        "betole":   (bet_unp, total_bet, pct_bet),
        "meridian": (mdi_unp, total_mdi, pct_mdi),
    }

    write_csv_and_txt_five(merged, leftovers_mer, leftovers_moz, leftovers_bet, leftovers_mdi, summary)

    print("[OK] Napravljeno:")
    print(" -", OUT_CSV.resolve())
    print(" -", OUT_TXT.resolve())
    if not moz_exists:
        print("(!) Upozorenje: nema fajla", MOZZART_TXT)
    if not bet_exists:
        print("(!) Upozorenje: nema fajla", BETOLE_TXT)
    if not mdi_exists:
        print("(!) Upozorenje: nema fajla", MERIDIAN_TXT)

if __name__ == "__main__":
    main()
