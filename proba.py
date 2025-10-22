# -*- coding: utf-8 -*-
"""
Izveštaj po meču:
1) Kompletan ispis kvota po kladionici (svi dostupni marketi).
2) Ispod: najveća kvota za 1, X, 2, 0-2, 3+ (sa imenima kladionica).
3) Provera arbitraže: posebno za 1-X-2 i za 0-2 / 3+.
4) Poseban TXT sa samo mečevima koji imaju bilo kakvu arbitražu.
5) Sažetak: broj zapisa po kladionici, broj "spojenih", broj grupa sa veličinom ≥3, ≥4, ≥5, ≥6.
6) POPRAVKE: čuvamo datum i ligu iz prve linije, i deduplikuje se po bookmaker-u.

Ulaz:
    soccer_mecevi_pregled.txt, merkur_mecevi_pregled.txt, mozzart_mecevi_pregled.txt,
    balkanbet_mecevi_pregled.txt, meridian_mecevi_pregled.txt, betole_mecevi_pregled.txt

Izlaz:
    kvote_arbitraza_FULL.txt           # samo mečevi (bez sažetka)
    kvote_arbitraza_ONLY_arbs.txt      # samo mečevi sa arbitražom + SAŽETAK
"""

from pathlib import Path
import re
import unicodedata
from typing import Dict, List, Optional, Tuple
import pandas as pd

INPUT_FILES = {
    "Soccer": "soccer_mecevi_pregled.txt",
    "Merkur": "merkur_mecevi_pregled.txt",
    "Mozzart": "mozzart_mecevi_pregled.txt",
    "BalkanBet": "balkanbet_mecevi_pregled.txt",
    "Meridian": "meridian_mecevi_pregled.txt",
    "Brazil_d": "brazil_mecevi_pregled.txt",
    "Brazil_s": "brazil_sutra_mecevi_pregled.txt",
    "Brazil_p": "brazil_prekosutra_mecevi_pregled.txt",
    "BetOle": "betole_mecevi_pregled.txt",
    "Topbet": "topbet_mecevi_pregled.txt"
}

ALL_MARKETS = ["1", "X", "2", "0-2", "2+", "3+", "GG", "IGG", "GG&3+", "GG&4+", "4+"]
FOCUS_MARKETS = ["1", "X", "2", "0-2", "3+"]

SEP_RE = re.compile(r"^=+\s*$", re.MULTILINE)
TEAM_STOPWORDS = {
    "fc","fk","al","cf","sc","ac","bc","ud","cd","sd","ad","ca",
    "the","club","de","of","sv","ss","ks","ik","if","sk",
    "u19","u20","u21","b","c","a","u23","u17","u16","u15","u14","u13"
}

# ============================================================
# 1) SINONIMI TIMOVA
# ============================================================
TEAM_SYNONYMS: Dict[str, List[str]] = {
    "Villarreal": ["Villareal", "Vila Real", "Villarreal CF"],
    "Dinamo Moscow": ["Dynamo Moscow", "Dinamo Moskva", "Dinamo M.", "FC Dynamo Moscow"],
    "CSKA Moscow": ["CSKA Moskva", "CSKA M.", "PFC CSKA Moscow"],
    "Spartak Moscow": ["Spartak Moskva", "Spartak M.", "FC Spartak Moscow"],
    "Lokomotiv Moscow": ["Lokomotiv Moskva", "Lokomotiv M.", "FC Lokomotiv Moscow"],
    "Brondby": ["Brøndby", "Brondby IF","Brndby IF"],
    "Nordsjaelland": ["Nordsjælland", "Nordsjaelland FC"],
    "Hajduk Split": ["Hajduk", "HNK Hajduk Split"],
    "Dinamo Zagreb": ["Dinamo", "GNK Dinamo Zagreb"],
    "Soenderjyske": ["Sonderjyske", "Soenderjyske FK"],
    "Crvena Zvezda": ["Red Star", "Red Star Belgrade", "Crvena Zvezda Beograd", "FK Crvena Zvezda"],
    "Partizan": ["FK Partizan", "Partizan Beograd"],
    "Atletico Madrid": ["Atl Madrid", "Atletico de Madrid", "Atlético Madrid"],
    "Athletic Bilbao": ["Athletic Club", "Ath Bilbao", "Athl. Bilbao"],
    "Inter": ["Inter Milan", "Inter Milano", "Internazionale", "FC Internazionale"],
    "AC Milan": ["Milan", "A.C. Milan"],
    "Manchester United": ["Man Utd", "Manchester Utd", "Man United", "Man. United"],
    "Manchester City": ["Man City", "Manchester C", "Man. City"],
    "Newcastle United": ["Newcastle Utd"],
    "Sporting CP": ["Sporting Lisbon", "Sporting Clube de Portugal"],
    "Marseille": ["Olympique Marseille", "OM", "O. Marseille"],
    "Real Betis": ["Betis", "Real Betis Balompie"],
    "Sevilla": ["Sevilla FC"],
    "Bayern Munich": ["Bayern Munchen", "Bayern München", "FC Bayern"],
    "Koln": ["Cologne", "1. FC Koln", "1. FC Köln", "FC Koln", "FC Köln"],
    "Fenerbahce": ["Fenerbahçe", "Fener"],
    "Besiktas": ["Beşiktaş", "Besiktas JK"],
    "Galatasaray": ["Gala", "Galata", "Galatasaray SK"],
    "AIK": ["AIK Stockholm"],
    "Rangers": ["Glasgow Rangers"],
    "Celtic": ["Celtic Glasgow"],
}

def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def norm_word(w: str) -> str:
    w = strip_accents(w).lower()
    w = re.sub(r"[^a-z0-9]+", "", w)
    return w

def split_team_words(name: str) -> List[str]:
    parts = re.split(r"[\s\-\._/]+", name)
    clean = [norm_word(p) for p in parts if norm_word(p)]
    return [w for w in clean if (w not in TEAM_STOPWORDS and len(w) >= 2)]

# ---------------------------------------------
# 2) Mapiranje aliasa na kanonski naziv
# ---------------------------------------------
def make_key(name: str) -> str:
    s = strip_accents(name).lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

ALIAS_TO_CANON: Dict[str, str] = {}
for canon, aliases in TEAM_SYNONYMS.items():
    all_forms = [canon] + list(aliases)
    for form in all_forms:
        ALIAS_TO_CANON[make_key(form)] = canon

def alias_normalize(name: str) -> str:
    key = make_key(name)
    return ALIAS_TO_CANON.get(key, name)

def share_meaningful_word(a: str, b: str) -> bool:
    a_n = alias_normalize(a)
    b_n = alias_normalize(b)
    A = set(split_team_words(a_n))
    B = set(split_team_words(b_n))
    return len(A.intersection(B)) > 0

def parse_block(block: str) -> Optional[Dict]:
    lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
    if not lines:
        return None

    m_hdr = re.match(
        r"^\s*(?P<time>\d{1,2}:\d{2})"
        r"(?:\s+(?:Pon|Uto|Sre|Čet|Pet|Sub|Ned))?"
        r"(?:\s+(?P<date>\d{1,2}\.\d{1,2}\.))?"
        r"(?:\s+\[(?P<league>[^\]]+)\])?\s*$",
        lines[0]
    )
    if not m_hdr:
        m_time = re.match(r"^\s*(\d{1,2}:\d{2})", lines[0])
        if not m_time:
            return None
        match_time = m_time.group(1)
        match_date = None
        match_league = None
    else:
        match_time = m_hdr.group("time")
        match_date = m_hdr.group("date")
        match_league = m_hdr.group("league")

    teams_line = None
    for ln in lines[1:3]:
        if re.search(r"\bvs\b", ln, flags=re.IGNORECASE):
            teams_line = ln
            break
    if not teams_line:
        for ln in lines:
            if re.search(r"\bvs\b", ln, flags=re.IGNORECASE):
                teams_line = ln
                break
    if not teams_line:
        return None

    clean_teams = re.sub(r"\(ID:\s*\d+\)\s*", "", teams_line).strip()
    m_vs = re.split(r"\bvs\b", clean_teams, flags=re.IGNORECASE)
    if len(m_vs) != 2:
        return None
    home = m_vs[0].strip(" -\t")
    away = m_vs[1].strip(" -\t")

    odds: Dict[str, str] = {}
    key_val_pat = re.compile(r"((?:IGG|GG)(?:&[0-9]+\+)?|X|[0-9]+(?:-[0-9]+)?\+?)=([^\s]+)")
    for ln in lines[1:]:
        for key, val in key_val_pat.findall(ln):
            odds[key] = val
    normalized = {m: odds.get(m, "-") for m in ALL_MARKETS}

    return {
        "time": match_time,
        "date": match_date,
        "league": match_league,
        "home": home,
        "away": away,
        **normalized
    }

def parse_file(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8", errors="replace")
    blocks = [b.strip() for b in SEP_RE.split(text) if b.strip()]
    out = []
    for b in blocks:
        rec = parse_block(b)
        if rec:
            out.append(rec)
    return out

def to_float(x: str) -> Optional[float]:
    try:
        v = float(str(x).replace(",", "."))
        return v if v > 1.0 else None
    except:
        return None

def best_odds_for_market(df_subset: pd.DataFrame, market: str) -> Tuple[Optional[float], List[str]]:
    vals = []
    for _, r in df_subset.iterrows():
        v = to_float(r.get(market, "-"))
        if v is not None:
            vals.append((v, r["bookmaker"]))
    if not vals:
        return None, []
    max_val = max(v for v, _ in vals)
    books = sorted({b for v, b in vals if abs(v - max_val) < 1e-9})
    return max_val, books

def arbitrage_1x2(best1: Optional[float], bestX: Optional[float], best2: Optional[float]):
    if not all([best1, bestX, best2]):
        return False, None, None
    inv = 1.0/best1 + 1.0/bestX + 1.0/best2
    return (inv < 1.0), inv, (1.0 - inv) * 100.0

def arbitrage_two_way(a: Optional[float], b: Optional[float]):
    if not all([a, b]):
        return False, None, None
    inv = 1.0/a + 1.0/b
    return (inv < 1.0), inv, (1.0 - inv) * 100.0

def compose_line_all_markets(r: pd.Series) -> str:
    parts = []
    for k in ALL_MARKETS:
        v = r.get(k, "-")
        if v and v != "-":
            parts.append(f"{k}={v}")
    return "  |  ".join(parts) if parts else "-"

def nonempty_markets_count(r: pd.Series) -> int:
    return sum(1 for k in ALL_MARKETS if (k in r and r.get(k, "-") not in ("-", None, "")))

def main():
    rows = []
    for bk_name, fname in INPUT_FILES.items():
        p = Path(fname)
        recs = parse_file(p)
        for r in recs:
            row = {"bookmaker": bk_name}
            row.update(r)
            rows.append(row)

    if not rows:
        Path("kvote_arbitraza_FULL.txt").write_text("Nije nađen nijedan meč u ulaznim fajlovima.\n", encoding="utf-8")
        Path("kvote_arbitraza_ONLY_arbs.txt").write_text("", encoding="utf-8")
        return

    df_all = pd.DataFrame(rows)

    # Grupisanje po: isto vreme + smisleno poklapanje timova
    match_groups: List[List[int]] = []
    used = set()
    for i, row in df_all.iterrows():
        if i in used:
            continue
        group = [i]
        used.add(i)
        t0, h0, a0 = row["time"], row["home"], row["away"]
        for j, row2 in df_all.iloc[i+1:].iterrows():
            if j in used:
                continue
            if row2["time"] != t0:
                continue
            if share_meaningful_word(h0, row2["home"]) and share_meaningful_word(a0, row2["away"]):
                group.append(j)
                used.add(j)
        match_groups.append(group)

    all_lines: List[str] = []      # FULL: samo mečevi
    arb_only_lines: List[str] = [] # ONLY_ARBS: mečevi sa arbitražom

    # podaci za SAŽETAK (samo za ONLY_ARBS)
    total_per_book = df_all.groupby("bookmaker").size().to_dict()
    paired_indices = set(idx for g in match_groups if len(g) > 1 for idx in g)
    paired_per_book = (
        df_all.loc[list(paired_indices)].groupby("bookmaker").size().to_dict()
        if paired_indices else {}
    )
    total_groups = len(match_groups)
    paired_groups = sum(1 for g in match_groups if len(g) > 1)
    ge3 = sum(1 for g in match_groups if len(g) >= 3)
    ge4 = sum(1 for g in match_groups if len(g) >= 4)
    ge5 = sum(1 for g in match_groups if len(g) >= 5)
    ge6 = sum(1 for g in match_groups if len(g) >= 6)
    ge7 = sum(1 for g in match_groups if len(g) >= 7)

    arb_1x2_groups = 0
    arb_uo_groups = 0
    arb_any_groups = 0

    for g in match_groups:
        subset = df_all.loc[g].copy()

        # DEDUPE po bookmaker-u
        subset["__filled"] = subset.apply(nonempty_markets_count, axis=1)
        subset = subset.sort_values(["bookmaker","__filled"], ascending=[True, False])
        subset = subset.drop_duplicates("bookmaker", keep="first")
        subset = subset.drop(columns="__filled")

        base = subset.iloc[0]
        time_str = base["time"]
        date_str = base.get("date") or ""
        league_str = base.get("league") or ""
        home_str = base["home"]
        away_str = base["away"]

        block: List[str] = []
        hdr_parts = [time_str]
        if date_str:
            hdr_parts.append(date_str)
        if league_str:
            hdr_parts.append(f"[{league_str}]")
        block.append("   ".join(hdr_parts).rstrip())
        block.append(f"{home_str}  vs  {away_str}")

        # === FORMAT FULL: umesto unutrašnje linije crtica, stavi praznu liniju ===
        block.append("")

        # 1) Kompletan listing kvota po kladionici
        for _, r in subset.sort_values("bookmaker").iterrows():
            block.append(f"- {r['bookmaker']:<10} {compose_line_all_markets(r)}")

        # 2) Najveće kvote po fokus tržištima
        block.append("")
        best_map = {}
        for m in FOCUS_MARKETS:
            best_val, best_books = best_odds_for_market(subset, m)
            best_map[m] = (best_val, best_books)
            block.append(
                f"Najveća {m:<3}: {best_val:.2f}  [{', '.join(best_books)}]" if best_val
                else f"Najveća {m:<3}: -"
            )

        # 3) Arbitraže
        block.append("")
        best1, _ = best_map.get("1", (None, []))
        bestX, _ = best_map.get("X", (None, []))
        best2, _ = best_map.get("2", (None, []))
        ok3, inv3, prof3 = arbitrage_1x2(best1, bestX, best2)
        if inv3 is not None:
            block.append(f"Arbitraža (1-X-2): {'DA' if ok3 else 'NE'}   inv_sum={inv3:.4f}   profit≈{(prof3 if prof3 is not None else 0):.2f}%")
        else:
            block.append("Arbitraža (1-X-2): nedovoljno podataka")

        best_u, _ = best_map.get("0-2", (None, []))
        best_o, _ = best_map.get("3+", (None, []))
        ok2, inv2, prof2 = arbitrage_two_way(best_u, best_o)
        if inv2 is not None:
            block.append(f"Arbitraža (0-2 / 3+): {'DA' if ok2 else 'NE'}   inv_sum={inv2:.4f}   profit≈{(prof2 if prof2 is not None else 0):.2f}%")
        else:
            block.append("Arbitraža (0-2 / 3+): nedovoljno podataka")
        block.append("")

        # === FORMAT FULL: spoljne crte pre prvog i posle svakog bloka ===
        if not all_lines:
            all_lines.append("-"*86)     # top crta pre prvog bloka
        all_lines.extend(block)          # sam blok
        all_lines.append("-"*86)         # završna crta bloka (i separator ka sledećem)

        # ONLY_ARBS (ne menjamo stil, ostaje raniji)
        any_arb = False
        if inv3 is not None and ok3:
            arb_1x2_groups += 1
            any_arb = True
        if inv2 is not None and ok2:
            arb_uo_groups += 1
            any_arb = True
        if any_arb:
            arb_any_groups += 1
            arb_only_lines.extend(block)

    # ======= SAŽETAK — samo u ONLY_ARBS =======
    summary_lines: List[str] = []
    summary_lines.append("="*86)
    summary_lines.append("SAŽETAK".center(86))
    summary_lines.append("="*86)
    summary_lines.append(f"Ukupno mečeva (grupa): {total_groups}")
    summary_lines.append(f"Mečeva spojenih sa ≥2 kladionice (grupa size>1): {paired_groups}")
    summary_lines.append(f"Grupa sa veličinom ≥3: {ge3}")
    summary_lines.append(f"Grupa sa veličinom ≥4: {ge4}")
    summary_lines.append(f"Grupa sa veličinom ≥5: {ge5}")
    summary_lines.append(f"Grupa sa veličinom ≥6: {ge6}")
    summary_lines.append(f"Grupa sa veličinom ≥7: {ge7}")
    summary_lines.append("")
    summary_lines.append("Po kladionici:")
    all_books = sorted(set(df_all["bookmaker"].tolist()))
    total_per_book = total_per_book
    paired_per_book = paired_per_book
    for bk in all_books:
        total_bk = total_per_book.get(bk, 0)
        paired_bk = paired_per_book.get(bk, 0)
        summary_lines.append(f"  - {bk:<10} ukupno zapisa: {total_bk:>4}   spojeno (u grupama >1): {paired_bk:>4}")
    summary_lines.append("")
    summary_lines.append("Arbitraže (broj mečeva/grupa):")
    summary_lines.append(f"  - 1-X-2: {arb_1x2_groups}")
    summary_lines.append(f"  - 0-2 / 3+: {arb_uo_groups}")
    summary_lines.append(f"  - Barem jedna arbitraža: {arb_any_groups}")
    summary_lines.append("")

    # Pišemo fajlove:
    Path("kvote_arbitraza_FULL.txt").write_text(
        ("\n".join(all_lines).rstrip() + "\n") if all_lines else "",
        encoding="utf-8"
    )

    only_arbs_out = []
    if arb_only_lines:
        only_arbs_out.extend(arb_only_lines)
        only_arbs_out.append("")
    only_arbs_out.extend(summary_lines)
    Path("kvote_arbitraza_ONLY_arbs.txt").write_text(
        ("\n".join(only_arbs_out).rstrip() + "\n") if only_arbs_out else "\n".join(summary_lines),
        encoding="utf-8"
    )

if __name__ == "__main__":
    main()
