# run_all.py
# -*- coding: utf-8 -*-

import subprocess
import time
import os
from pathlib import Path
from typing import List, Tuple, Optional

# === Podesivo ===
PY = "python3"            # ili "python" ako tako pokrećeš
STAGGER_SEC = 8           # vremenski razmak između startova skripti (sekunde)
TIMEOUT_EACH = 12 * 60    # timeout za svaku scraper skriptu (sekunde)
MIN_BYTES = 50            # minimalna veličina očekivanih izlaznih fajlova
STABILITY_CHECKS = 3      # koliko puta proveriti da fajl ostaje iste veličine
STABILITY_SLEEP = 1.5     # pauza između provera (sek)

# Popis tvojih scraper skripti i očekivanih izlaza (po ranijem dogovoru)
SCRAPERS: List[Tuple[str, List[Path]]] = [
    ("soccer.py",   [Path("soccer_sledeci_mecevi.txt"),   Path("soccer_mecevi_pregled.txt")]),
    ("merkur.py",   [Path("merkur_sledeci_mecevi.txt"),   Path("merkur_mecevi_pregled.txt")]),
    ("meridian.py", [Path("meridian_sledeci_mecevi.txt"), Path("meridian_mecevi_pregled.txt")]),
    ("mozzart.py",  [Path("mozzart_sledeci_mecevi.txt"),  Path("mozzart_mecevi_pregled.txt")]),
    # ("betole.py",   [Path("betole_sledeci_mecevi.txt"),   Path("betole_mecevi_pregled.txt")]),
    ("balkanbet.py",[Path("balkanbet_sledeci_mecevi.txt"),Path("balkanbet_mecevi_pregled.txt")]),
]

# Na kraju, pokrećemo glavni spajanje/izveštaj
MAIN_SCRIPT = "main.py"


def wait_for_file_stable(path: Path, min_bytes: int = MIN_BYTES,
                         checks: int = STABILITY_CHECKS, sleep_s: float = STABILITY_SLEEP) -> bool:
    """
    Čeka da fajl postoji, da pređe min_bytes, i da mu veličina ostane stabilna kroz 'checks' provera.
    """
    deadline = time.time() + TIMEOUT_EACH
    # 1) čekaj da fajl nastane i pređe minimalnu veličinu
    while time.time() < deadline:
        if path.exists():
            try:
                size = path.stat().st_size
            except OSError:
                size = 0
            if size >= min_bytes:
                break
        time.sleep(0.8)
    else:
        print(f"[!] Fajl {path} nije napravljen ili je premali u zadatom roku.")
        return False

    # 2) stabilnost veličine
    last = None
    stable_count = 0
    while stable_count < checks and time.time() < deadline:
        try:
            size = path.stat().st_size
        except OSError:
            size = 0
        if last is None or size != last:
            last = size
            stable_count = 0
        else:
            stable_count += 1
        time.sleep(sleep_s)

    ok = stable_count >= checks
    if not ok:
        print(f"[!] Fajl {path} nije stabilan (menja se veličina).")
    return ok


def run_scrapers_parallel(scrapers: List[Tuple[str, List[Path]]]) -> None:
    """
    Paralelno pokreće sve scrapere sa STAGGER_SEC razmakom između startova.
    Čeka završetak svakog procesa (sa TIMEOUT_EACH), i nakon toga čeka da očekivani
    izlazi budu stabilni.
    """
    procs = []
    for i, (script, _) in enumerate(scrapers):
        if not Path(script).exists():
            print(f"[!] Preskačem {script} — ne postoji u folderu.")
            continue
        # Stagger pre starta (osim za prvi)
        if i > 0 and STAGGER_SEC > 0:
            time.sleep(STAGGER_SEC)
        print(f"[*] Startujem: {script}")
        p = subprocess.Popen([PY, script], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        procs.append((script, p))

    # Sačekaj završetak svake skripte
    for script, p in procs:
        try:
            stdout, stderr = p.communicate(timeout=TIMEOUT_EACH)
        except subprocess.TimeoutExpired:
            p.kill()
            stdout, stderr = p.communicate()
            print(f"[!] TIMEOUT: {script} (ubijen proces)")
        if stdout:
            print(f"[STDOUT:{script}]\n{stdout.strip()}\n")
        if stderr:
            print(f"[STDERR:{script}]\n{stderr.strip()}\n")
        if p.returncode != 0:
            print(f"[!] {script} završio sa kodom {p.returncode}")

    # Proveri stabilnost očekivanih fajlova
    for script, outputs in scrapers:
        if not Path(script).exists():
            continue
        for outp in outputs:
            print(f"[*] Čekam stabilan izlaz: {outp}")
            wait_for_file_stable(outp)


def run_main() -> int:
    if not Path(MAIN_SCRIPT).exists():
        print(f"[!] Nema {MAIN_SCRIPT} — preskačem spajanje/izveštaj.")
        return 1
    print(f"[*] Pokrećem {MAIN_SCRIPT}...")
    p = subprocess.Popen([PY, MAIN_SCRIPT], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        stdout, stderr = p.communicate(timeout=20 * 60)  # do 20 min za main
    except subprocess.TimeoutExpired:
        p.kill()
        stdout, stderr = p.communicate()
        print("[!] TIMEOUT: main.py (ubijen proces)")
    if stdout:
        print(f"[STDOUT:{MAIN_SCRIPT}]\n{stdout.strip()}\n")
    if stderr:
        print(f"[STDERR:{MAIN_SCRIPT}]\n{stderr.strip()}\n")
    if p.returncode == 0:
        print("[OK] main.py završio uspešno.")
    else:
        print(f"[!] main.py exit code: {p.returncode}")
    return p.returncode


def main():
    # (Opcija) Ako želiš da lako uključiš/isključiš scrapere, filtriraj ovde:
    scrapers_to_run = []
    for script, outs in SCRAPERS:
        # Primer: isključi jednog bukija:
        # if script == "balkanbet.py":
        #     continue
        scrapers_to_run.append((script, outs))

    # 1) pokreni scrapere paralelno
    run_scrapers_parallel(scrapers_to_run)

    # 2) zatim spoji i napravi izveštaje
    run_main()


if __name__ == "__main__":
    main()
