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
    kvote_arbitraza_FULL.txt
    kvote_arbitraza_ONLY_arbs.txt
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

def share_meaningful_word(a: str, b: str) -> bool:
    A = set(split_team_words(a))
    B = set(split_team_words(b))
    return len(A.intersection(B)) > 0

def parse_block(block: str) -> Optional[Dict]:
    """
    1. linija (primeri):
        "20:45    21.10.  [English - Southern Central]"
        "22:00"
        "18:30  Ned  21.10.  [ITA1]"  (dan ignorisemo)
    2. linija:
        "Banbury United  vs  Kettering Town   (ID: 715)"
    Sledece linije: KEY=VAL ...
    """
    lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
    if not lines:
        return None

    # 1) vreme + (opciono) datum + (opciono) liga iz PRVE linije
    m_hdr = re.match(
        r"^\s*(?P<time>\d{1,2}:\d{2})"
        r"(?:\s+(?:Pon|Uto|Sre|Čet|Pet|Sub|Ned))?"        # opcioni dan
        r"(?:\s+(?P<date>\d{1,2}\.\d{1,2}\.))?"           # opcioni datum
        r"(?:\s+\[(?P<league>[^\]]+)\])?\s*$",            # opcioni [liga]
        lines[0]
    )
    if not m_hdr:
        # fallback: bar vreme
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

    # 2) linija sa timovima
    teams_line = None
    for ln in lines[1:3]:  # najcesce je odmah na 2. liniji
        if re.search(r"\bvs\b", ln, flags=re.IGNORECASE):
            teams_line = ln
            break
    if not teams_line:
        # ako ipak nije na 2., pretrazimo sve
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

    # 3) kvote KEY=VAL
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

    # Grupisanje po: isto vreme + smisleno poklapanje timova (domacin/gost)
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

    all_lines: List[str] = []
    arb_only_lines: List[str] = []

    # Sažetak po kladionici i veličine grupa
    total_per_book = df_all.groupby("bookmaker").size().to_dict()
    paired_indices = set(idx for g in match_groups if len(g) > 1 for idx in g)
    paired_per_book = df_all.loc[list(paired_indices)].groupby("bookmaker").size().to_dict() if paired_indices else {}
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

        # DEDUPE po bookmaker-u: zadrži red sa najviše popunjenih tržišta
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
        # Prva linija: vreme + (datum) + [liga] — samo ako postoje
        hdr_parts = [time_str]
        if date_str:
            hdr_parts.append(date_str)
        if league_str:
            hdr_parts.append(f"[{league_str}]")
        block.append("   ".join(hdr_parts).rstrip())
        # Druga linija: timovi
        block.append(f"{home_str}  vs  {away_str}")
        block.append("-"*86)

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
                f"Najveća {m:<3}: {best_val:.2f}  [{', '.join(best_books)}]"
                if best_val else
                f"Najveća {m:<3}: -"
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

        all_lines.extend(block)

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

    # SAŽETAK
    all_lines.append("="*86)
    all_lines.append("SAŽETAK".center(86))
    all_lines.append("="*86)
    all_lines.append(f"Ukupno mečeva (grupa): {total_groups}")
    all_lines.append(f"Mečeva spojenih sa ≥2 kladionice (grupa size>1): {paired_groups}")
    all_lines.append(f"Grupa sa veličinom ≥3: {ge3}")
    all_lines.append(f"Grupa sa veličinom ≥4: {ge4}")
    all_lines.append(f"Grupa sa veličinom ≥5: {ge5}")
    all_lines.append(f"Grupa sa veličinom ≥6: {ge6}")
    all_lines.append(f"Grupa sa veličinom ≥7: {ge7}")
    all_lines.append("")
    all_lines.append("Po kladionici:")
    all_books = sorted(set(df_all["bookmaker"].tolist()))
    for bk in all_books:
        total_bk = total_per_book.get(bk, 0)
        paired_bk = paired_per_book.get(bk, 0)
        all_lines.append(f"  - {bk:<10} ukupno zapisa: {total_bk:>4}   spojeno (u grupama >1): {paired_bk:>4}")
    all_lines.append("")
    all_lines.append("Arbitraže (broj mečeva/grupa):")
    all_lines.append(f"  - 1-X-2: {arb_1x2_groups}")
    all_lines.append(f"  - 0-2 / 3+: {arb_uo_groups}")
    all_lines.append(f"  - Barem jedna arbitraža: {arb_any_groups}")
    all_lines.append("")

    Path("kvote_arbitraza_FULL.txt").write_text("\n".join(all_lines).rstrip() + "\n", encoding="utf-8")
    Path("kvote_arbitraza_ONLY_arbs.txt").write_text("\n".join(arb_only_lines).rstrip() + ("\n" if arb_only_lines else ""), encoding="utf-8")

if __name__ == "__main__":
    main()
