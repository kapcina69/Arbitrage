# -*- coding: utf-8 -*-

import subprocess
import time
import os
from pathlib import Path
from typing import List, Tuple, Optional
from datetime import datetime
import concurrent.futures

# === Podesivo ===
PY = "python3"
STAGGER_SEC = 3           # smanjeno sa 8 na 3 sekunde
TIMEOUT_EACH = 10 * 60    # smanjeno sa 12 na 10 minuta
MIN_BYTES = 100           # povećano sa 50 na 100 (prazan fajl često ima <50 bytes)
STABILITY_CHECKS = 2      # smanjeno sa 3 na 2 (dovoljno za stabilnost)
STABILITY_SLEEP = 1.0     # smanjeno sa 1.5 na 1.0 sekundi
MAX_WAIT_STABLE = 120     # max 2 minuta čekanja na stabilizaciju (NOVO)
RUN_EVERY_MIN = 60

# === REŽIM RADA ===
# False = normalan režim (ciklus na svakih RUN_EVERY_MIN minuta)
# True  = neprestano pokretanje (čim se završi jedan ciklus, počinje sledeći)
CONTINUOUS_MODE = False

# Popis skripti i izlaza
SCRAPERS: List[Tuple[str, List[Path]]] = [
    ("soccer.py",   [Path("soccer_sledeci_mecevi.txt"),   Path("soccer_mecevi_pregled.txt")]),
    ("merkur.py",   [Path("merkur_sledeci_mecevi.txt"),   Path("merkur_mecevi_pregled.txt")]),
    ("meridian.py", [Path("meridian_sledeci_mecevi.txt"), Path("meridian_mecevi_pregled.txt")]),
    ("mozzart.py",  [Path("mozzart_sledeci_mecevi.txt"),  Path("mozzart_mecevi_pregled.txt")]),
    ("betole.py",   [Path("betole_sledeci_mecevi.txt"),   Path("betole_mecevi_pregled.txt")]),
    ("balkanbet.py",[Path("balkanbet_sledeci_mecevi.txt"),Path("balkanbet_mecevi_pregled.txt")]),
    ("brazil.py",   [Path("brazil_sledeci_mecevi.txt"),   Path("brazil_mecevi_pregled.txt")]),
    ("brazil_sutra.py", [Path("brazil_sutra_sledeci_mecevi.txt"), Path("brazil_sutra_mecevi_pregled.txt")]),
    ("brazil_prekosutra.py", [Path("brazil_prekosutra_sledeci_mecevi.txt"), Path("brazil_prekosutra_mecevi_pregled.txt")]),
    ("topbet.py",   [Path("topbet_sledeci_mecevi.txt"),   Path("topbet_mecevi_pregled.txt")]),
    ("oktagon.py",  [Path("oktagonbet_sledeci_mecevi.txt"),  Path("oktagonbet_mecevi_pregled.txt")]),
]

MAIN_SCRIPT = "proba.py"
MAIN_OUTPUTS = [
    Path("kvote_arbitraza_FULL.txt"),
    Path("kvote_arbitraza_ONLY_arbs.txt"),
]
TARGET_PUSH = Path("kvote_arbitraza_FULL.txt")
REPORT_DIR = Path("izvestaji")


# =================== GIT pomoćne funkcije ===================

def _run(cmd: list, check: bool = True) -> subprocess.CompletedProcess:
    """Wrapper oko subprocess.run."""
    return subprocess.run(cmd, text=True, capture_output=True, check=check)


def git_in_repo() -> bool:
    """Provera da li smo u Git repozitorijumu."""
    try:
        cp = _run(["git", "rev-parse", "--is-inside-work-tree"], check=False)
        return cp.returncode == 0 and cp.stdout.strip() == "true"
    except Exception:
        return False


def git_has_remote() -> bool:
    """Provera da li postoji remote."""
    try:
        cp = _run(["git", "remote"], check=False)
        return bool(cp.stdout.strip())
    except Exception:
        return False


def git_push_file(path: Path) -> None:
    """Dodaj/commit/push samo zadati fajl."""
    if not path.exists():
        print(f"[git] Preskačem push — ne postoji {path}")
        return
    if not git_in_repo():
        print("[git] Nisi u Git repou. Preskačem push.")
        return
    if not git_has_remote():
        print("[git] Nema remote-a. Preskačem push.")
        return

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        _run(["git", "add", str(path)], check=False)
        
        msg = f"auto: update {path.name} @ {ts}"
        cp_commit = _run(["git", "commit", "-m", msg], check=False)
        if cp_commit.returncode != 0:
            print(f"[git] Nema promena za commit ({path.name}).")
            return
        
        print(f"[git] Commit ok: {msg}")
        cp_push = _run(["git", "push"], check=False)
        if cp_push.returncode != 0:
            print(f"[git] PUSH FAIL:\n{cp_push.stderr.strip()}")
        else:
            print("[git] Push uspešan.")
    except Exception as e:
        print(f"[git] Greška: {e}")


# =================== Optimizovana logika čekanja ===================

def wait_for_file_stable(
    path: Path, 
    min_bytes: int = MIN_BYTES,
    checks: int = STABILITY_CHECKS, 
    sleep_s: float = STABILITY_SLEEP,
    max_wait: int = MAX_WAIT_STABLE
) -> bool:
    """
    Čeka da fajl postoji, da ima min_bytes, i da bude stabilan.
    OPTIMIZACIJE:
    - Dodato max_wait za brže odustajanje od praznih fajlova
    - Pamti broj uzastopnih čitanja sa istom veličinom
    - Rano odustaje ako fajl ne raste dovoljno dugo
    """
    start_time = time.time()
    deadline = start_time + TIMEOUT_EACH
    max_stable_wait = start_time + max_wait
    
    # 1) Čekaj da fajl nastane
    while time.time() < deadline:
        if path.exists():
            break
        time.sleep(0.5)
    else:
        print(f"[!] Fajl {path} nije nastao u roku.")
        return False
    
    # 2) Čekaj minimalnu veličinu sa timeout-om
    size_deadline = time.time() + max_wait
    while time.time() < size_deadline and time.time() < deadline:
        try:
            size = path.stat().st_size
            if size >= min_bytes:
                break
        except OSError:
            pass
        time.sleep(0.5)
    else:
        try:
            final_size = path.stat().st_size
            print(f"[!] Fajl {path} je premali ({final_size} bytes < {min_bytes}). Preskačem.")
        except OSError:
            print(f"[!] Fajl {path} nije dostupan.")
        return False
    
    # 3) Provera stabilnosti veličine
    last_size = None
    stable_count = 0
    
    while stable_count < checks and time.time() < max_stable_wait and time.time() < deadline:
        try:
            current_size = path.stat().st_size
        except OSError:
            print(f"[!] Greška pri čitanju {path}.")
            return False
        
        if last_size is None:
            last_size = current_size
        elif current_size == last_size:
            stable_count += 1
        else:
            # Veličina se promenila, restartuj brojač
            last_size = current_size
            stable_count = 0
        
        if stable_count < checks:
            time.sleep(sleep_s)
    
    if stable_count >= checks:
        print(f"[OK] Fajl {path} je stabilan ({last_size} bytes).")
        return True
    else:
        print(f"[!] Fajl {path} nije postao stabilan u roku.")
        return False


# =================== Paralelno pokretanje ===================

def run_single_scraper(script: str, outputs: List[Path]) -> Tuple[str, int, str, str]:
    """
    Pokreće jednu scraper skriptu i vraća rezultat.
    Koristi se u ThreadPoolExecutor za istovremeno pokretanje.
    """
    if not Path(script).exists():
        return (script, -1, "", f"Skripta {script} ne postoji")
    
    print(f"[*] Startujem: {script}")
    try:
        p = subprocess.Popen(
            [PY, script], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True
        )
        stdout, stderr = p.communicate(timeout=TIMEOUT_EACH)
        return (script, p.returncode, stdout, stderr)
    except subprocess.TimeoutExpired:
        p.kill()
        stdout, stderr = p.communicate()
        return (script, -999, stdout, f"TIMEOUT nakon {TIMEOUT_EACH}s\n{stderr}")
    except Exception as e:
        return (script, -1, "", str(e))


def run_scrapers_parallel(scrapers: List[Tuple[str, List[Path]]]) -> None:
    """
    OPTIMIZOVANO: Koristi ThreadPoolExecutor za pravu paralelizaciju.
    Svi scraperi se pokreću skoro istovremeno (sa malim stagger-om).
    """
    results = []
    
    # Pokreni sve scrapere paralelno sa ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(scrapers)) as executor:
        futures = []
        for i, (script, outputs) in enumerate(scrapers):
            # Mali stagger između pokretanja
            if i > 0 and STAGGER_SEC > 0:
                time.sleep(STAGGER_SEC)
            future = executor.submit(run_single_scraper, script, outputs)
            futures.append((future, script, outputs))
        
        # Prikupi rezultate kako se završavaju
        for future, script, outputs in futures:
            try:
                result = future.result()
                results.append(result)
                
                script_name, returncode, stdout, stderr = result
                
                if stdout:
                    print(f"[STDOUT:{script_name}]\n{stdout.strip()}\n")
                if stderr:
                    print(f"[STDERR:{script_name}]\n{stderr.strip()}\n")
                if returncode != 0:
                    print(f"[!] {script_name} završio sa kodom {returncode}")
                else:
                    print(f"[OK] {script_name} završen uspešno.")
                    
            except Exception as e:
                print(f"[!] Greška pri prikupljanju rezultata za {script}: {e}")
    
    # Proveri stabilnost izlaznih fajlova (paralelno)
    print("\n[*] Provera stabilnosti izlaznih fajlova...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(scrapers) * 2) as executor:
        stability_futures = []
        for script, outputs in scrapers:
            if not Path(script).exists():
                continue
            for output in outputs:
                future = executor.submit(wait_for_file_stable, output)
                stability_futures.append((future, output))
        
        # Sačekaj sve provere stabilnosti
        for future, output in stability_futures:
            try:
                future.result()
            except Exception as e:
                print(f"[!] Greška pri proveri {output}: {e}")


# =================== Main skripta ===================

def run_main() -> int:
    """Pokreće glavni skript za spajanje podataka."""
    if not Path(MAIN_SCRIPT).exists():
        print(f"[!] Nema {MAIN_SCRIPT} — preskačem.")
        return 1
    
    print(f"\n[*] Pokrećem {MAIN_SCRIPT}...")
    try:
        p = subprocess.Popen(
            [PY, MAIN_SCRIPT], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True
        )
        stdout, stderr = p.communicate(timeout=20 * 60)
        
        if stdout:
            print(f"[STDOUT:{MAIN_SCRIPT}]\n{stdout.strip()}\n")
        if stderr:
            print(f"[STDERR:{MAIN_SCRIPT}]\n{stderr.strip()}\n")
        
        if p.returncode == 0:
            print(f"[OK] {MAIN_SCRIPT} završen uspešno.")
        else:
            print(f"[!] {MAIN_SCRIPT} exit code: {p.returncode}")
        
        return p.returncode
        
    except subprocess.TimeoutExpired:
        p.kill()
        stdout, stderr = p.communicate()
        print(f"[!] TIMEOUT: {MAIN_SCRIPT}")
        if stdout:
            print(f"[STDOUT:{MAIN_SCRIPT}]\n{stdout.strip()}\n")
        if stderr:
            print(f"[STDERR:{MAIN_SCRIPT}]\n{stderr.strip()}\n")
        return -999
    except Exception as e:
        print(f"[!] Greška pri pokretanju {MAIN_SCRIPT}: {e}")
        return -1


# =================== Izveštaji ===================

def gather_report(scrapers: List[Tuple[str, List[Path]]]) -> str:
    """Prikuplja sadržaje pregled fajlova i vraća kao string."""
    lines = []
    now = datetime.now()
    header = f"Izveštaj od {now.strftime('%Y-%m-%d %H:%M:%S')}"
    lines.append(header)
    lines.append("=" * len(header))
    lines.append("")

    # Dodaj pojedinačne 'pregled' fajlove
    for script, outs in scrapers:
        pregled = outs[1] if len(outs) > 1 else None
        if pregled and pregled.exists():
            try:
                content = pregled.read_text(encoding="utf-8", errors="replace")
                if content.strip():  # samo ako nije prazan
                    lines.append(f"\n--- {script} :: {pregled.name} ---\n")
                    lines.append(content.strip())
                    lines.append("")
            except Exception as e:
                lines.append(f"\n[!] Greška pri čitanju {pregled}: {e}\n")

    # Dodaj glavne izlaze
    for pth in MAIN_OUTPUTS:
        if pth.exists():
            try:
                content = pth.read_text(encoding="utf-8", errors="replace")
                if content.strip():  # samo ako nije prazan
                    lines.append(f"\n--- MAIN :: {pth.name} ---\n")
                    lines.append(content.strip())
                    lines.append("")
            except Exception as e:
                lines.append(f"\n[!] Greška pri čitanju {pth}: {e}\n")

    return "\n".join(lines).rstrip() + "\n"


def write_timestamped_report(report_text: str) -> Path:
    """Upisuje izveštaj u folder 'izvestaji' sa timestamp-om."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    out_path = REPORT_DIR / f"izvestaj_{ts}.txt"
    try:
        out_path.write_text(report_text, encoding="utf-8")
        print(f"[OK] Sačuvan izveštaj: {out_path}")
    except Exception as e:
        print(f"[!] Greška pri čuvanju izveštaja {out_path}: {e}")
    return out_path


# =================== Glavni ciklus ===================

def one_cycle():
    """Jedan kompletan ciklus: scrapers -> main -> git push -> izveštaj."""
    cycle_start = time.time()
    
    # Filtriraj scrapere ako želiš neke isključiti
    scrapers_to_run = [
        (script, outs) for script, outs in SCRAPERS
        if Path(script).exists()
    ]
    
    if not scrapers_to_run:
        print("[!] Nema dostupnih scraper skripti!")
        return
    
    print(f"\n{'='*60}")
    print(f"NOVI CIKLUS: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # 1) Pokreni scrapere paralelno
    print(f"[*] Pokretanje {len(scrapers_to_run)} scraper skripti...")
    run_scrapers_parallel(scrapers_to_run)
    
    # 2) Pokreni glavni skript
    ret = run_main()
    
    # 3) Git push ako je proba.py uspela
    if ret == 0 and TARGET_PUSH.exists():
        print(f"\n[*] Provera stabilnosti {TARGET_PUSH} pre push-a...")
        if wait_for_file_stable(TARGET_PUSH, min_bytes=MIN_BYTES):
            git_push_file(TARGET_PUSH)
    elif ret == 0:
        print(f"[git] {TARGET_PUSH} ne postoji — nema šta da se pushuje.")
    
    # 4) Prikupi i snimi izveštaj
    print("\n[*] Generisanje izveštaja...")
    report_text = gather_report(scrapers_to_run)
    write_timestamped_report(report_text)
    
    cycle_duration = time.time() - cycle_start
    print(f"\n[OK] Ciklus završen za {cycle_duration/60:.1f} min.")


def main_loop():
    """Beskonačna petlja koja pokreće cikluse."""
    if CONTINUOUS_MODE:
        print(f"[*] Pokretanje run_all.py u NEPREKIDNOM REŽIMU")
        print(f"[*] Scraperi će se pokretati NEPRESTANO bez pauza")
        print(f"[*] Prekid: Ctrl+C\n")
    else:
        print(f"[*] Pokretanje run_all.py u NORMALNOM REŽIMU")
        print(f"[*] Ciklus: {RUN_EVERY_MIN} min | Prekid: Ctrl+C\n")
    
    cycle_count = 0
    
    while True:
        cycle_count += 1
        start = time.time()
        
        try:
            one_cycle()
        except KeyboardInterrupt:
            print("\n\n[!] Prekid od korisnika. Izlazim.")
            break
        except Exception as e:
            print(f"\n[!] NEOČEKIVANA GREŠKA: {e}")
            import traceback
            traceback.print_exc()
        
        elapsed = time.time() - start
        
        if CONTINUOUS_MODE:
            # Neprestani režim - kratka pauza samo da se sistem odmori
            cooldown = 10  # 10 sekundi pauze između ciklusa
            print(f"\n[*] Ciklus #{cycle_count} završen za {elapsed/60:.1f} min.")
            print(f"[*] Kratka pauza ({cooldown}s) pa kreće sledeći ciklus...")
            print(f"{'='*60}\n")
            time.sleep(cooldown)
        else:
            # Normalan režim - čeka RUN_EVERY_MIN minuta
            sleep_sec = max(0, RUN_EVERY_MIN * 60 - elapsed)
            
            if sleep_sec > 0:
                mins = int(sleep_sec // 60)
                secs = int(sleep_sec % 60)
                print(f"\n[*] Ciklus #{cycle_count} završen za {elapsed/60:.1f} min.")
                print(f"[*] Sledeći ciklus za {mins} min {secs} sek.")
                print(f"{'='*60}\n")
                time.sleep(sleep_sec)
            else:
                print(f"\n[*] Ciklus #{cycle_count} trajao duže od {RUN_EVERY_MIN} min.")
                print(f"[*] Pokrećem sledeći ciklus odmah...")
                print(f"{'='*60}\n")


if __name__ == "__main__":
    main_loop()