# run_all.py
# -*- coding: utf-8 -*-

import subprocess
import time
import os
from pathlib import Path
from typing import List, Tuple
from datetime import datetime

# === Podesivo ===
PY = "python3"            # ili "python" ako tako pokrećeš
STAGGER_SEC = 8           # vremenski razmak između startova skripti (sekunde)
TIMEOUT_EACH = 12 * 60    # timeout za svaku scraper skriptu (sekunde)
MIN_BYTES = 50            # minimalna veličina očekivanih izlaznih fajlova
STABILITY_CHECKS = 3      # koliko puta proveriti da fajl ostaje iste veličine
STABILITY_SLEEP = 1.5     # pauza između provera (sek)
RUN_EVERY_MIN = 60        # na koliko minuta da se ponavlja ceo ciklus

# Popis tvojih scraper skripti i očekivanih izlaza (po ranijem dogovoru)
# Drugi fajl u listi se tretira kao "pregled" i ulazi u finalni izveštaj
SCRAPERS: List[Tuple[str, List[Path]]] = [
    ("soccer.py",   [Path("soccer_sledeci_mecevi.txt"),   Path("soccer_mecevi_pregled.txt")]),
    ("merkur.py",   [Path("merkur_sledeci_mecevi.txt"),   Path("merkur_mecevi_pregled.txt")]),
    ("meridian.py", [Path("meridian_sledeci_mecevi.txt"), Path("meridian_mecevi_pregled.txt")]),
    ("mozzart.py",  [Path("mozzart_sledeci_mecevi.txt"),  Path("mozzart_mecevi_pregled.txt")]),
    ("betole.py",   [Path("betole_sledeci_mecevi.txt"),   Path("betole_mecevi_pregled.txt")]),
    ("balkanbet.py",[Path("balkanbet_sledeci_mecevi.txt"),Path("balkanbet_mecevi_pregled.txt")]),
    ("brazil.py",   [Path("brazil_sledeci_mecevi.txt"),   Path("brazil_mecevi_pregled.txt")]),
    # ("brazil_sutra.py", [Path("brazil_sutra_sledeci_mecevi.txt"),   Path("brazil_sutra_mecevi_pregled.txt")]),
    ("brazil_prekosutra.py", [Path("brazil_prekosutra_sledeci_mecevi.txt"),   Path("brazil_prekosutra_mecevi_pregled.txt")]),
    ("topbet.py",   [Path("topbet_sledeci_mecevi.txt"),   Path("topbet_mecevi_pregled.txt")]),
]

# Glavni spajanje/izveštaj
MAIN_SCRIPT = "proba.py"

# Fajlovi koje proba.py generiše i koje želiš u zbirnom izveštaju:
MAIN_OUTPUTS = [
    Path("kvote_arbitraza_FULL.txt"),
    Path("kvote_arbitraza_ONLY_arbs.txt"),
]


# Folder za istorijske izveštaje (jedan fajl po satu)
REPORT_DIR = Path("izvestaji")


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
        print("[!] TIMEOUT: proba.py (ubijen proces)")
    if stdout:
        print(f"[STDOUT:{MAIN_SCRIPT}]\n{stdout.strip()}\n")
    if stderr:
        print(f"[STDERR:{MAIN_SCRIPT}]\n{stderr.strip()}\n")
    if p.returncode == 0:
        print("[OK] proba.py završio uspešno.")
    else:
        print(f"[!] proba.py exit code: {p.returncode}")
    return p.returncode


def gather_report(scrapers: List[Tuple[str, List[Path]]]) -> str:
    """
    Skuplja sadržaje 'pregled' fajlova iz SCRAPERS (+ dodatne MAIN_OUTPUTS ako postoje)
    i vraća kao jedan veliki string.
    """
    lines = []
    now = datetime.now()
    header = f"Izveštaj od {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
    lines.append(header)
    lines.append("=" * max(40, len(header) - 1))
    lines.append("")

    # Dodaj pojedinačne 'pregled' fajlove
    for script, outs in scrapers:
        pregled = outs[1] if len(outs) > 1 else None
        if pregled and pregled.exists():
            try:
                content = pregled.read_text(encoding="utf-8", errors="replace")
            except Exception as e:
                content = f"[!] Greška pri čitanju {pregled}: {e}"
            lines.append(f"\n--- {script} :: {pregled.name} ---\n")
            lines.append(content.strip())
            lines.append("\n")

    # Dodaj glavne izlaze (ako postoje)
    for pth in MAIN_OUTPUTS:
        if pth.exists():
            try:
                content = pth.read_text(encoding="utf-8", errors="replace")
            except Exception as e:
                content = f"[!] Greška pri čitanju {pth}: {e}"
            lines.append(f"\n--- MAIN :: {pth.name} ---\n")
            lines.append(content.strip())
            lines.append("\n")

    return "\n".join(lines).rstrip() + "\n"


def write_timestamped_report(report_text: str) -> Path:
    """
    Upisuje izveštaj u 'izvestaji/izvestaj_YYYY-mm-dd_HH-MM.txt'
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    out_path = REPORT_DIR / f"izvestaj_{ts}.txt"
    try:
        out_path.write_text(report_text, encoding="utf-8")
        print(f"[OK] Sačuvan izveštaj: {out_path}")
    except Exception as e:
        print(f"[!] Ne mogu da sačuvam izveštaj {out_path}: {e}")
    return out_path


def one_cycle():
    """
    Jedan kompletan ciklus: scrapers -> main -> izveštaj.
    """
    # (Opcija) lako isključivanje/uključivanje buki skripti:
    scrapers_to_run = []
    for script, outs in SCRAPERS:
        # primer isključenja:
        # if script == "balkanbet.py":
        #     continue
        scrapers_to_run.append((script, outs))

    # 1) pokreni scrapere paralelno i sačekaj stabilne izlaze
    run_scrapers_parallel(scrapers_to_run)

    # 2) pokreni glavni sklopnik/izveštaj (ako postoji)
    run_main()

    # 3) pokupi pregled fajlove (+ MAIN_OUTPUTS ako postoje) i snimi timestampovani izveštaj
    report_text = gather_report(scrapers_to_run)
    write_timestamped_report(report_text)


def main_loop():
    """
    Beskonačna petlja koja vrti one_cycle() na svakih RUN_EVERY_MIN minuta.
    """
    print(f"[*] Startujem run_all u petlji. Ciklus: {RUN_EVERY_MIN} min. Prekid: Ctrl+C")
    while True:
        start = time.time()
        try:
            one_cycle()
        except KeyboardInterrupt:
            print("\n[!] Prekid od korisnika. Izlazim.")
            break
        except Exception as e:
            print(f"[!] Neočekivana greška u ciklusu: {e}")

        # izračunaj koliko da spava do sledećeg ciklusa
        elapsed = time.time() - start
        sleep_sec = max(0, RUN_EVERY_MIN * 60 - elapsed)
        # mala poruka koliko spavamo
        mins = int(sleep_sec // 60)
        secs = int(sleep_sec % 60)
        print(f"[*] Sledeći ciklus za ~{mins} min {secs} sek.\n")
        time.sleep(sleep_sec)


if __name__ == "__main__":
    main_loop()
