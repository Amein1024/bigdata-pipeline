import posixpath
import subprocess
from urllib.parse import urlparse

import requests


def extract_to_hdfs(url: str, hdfs_input_dir: str) -> str:
    """
    Downloader en fil fra en HTTPS-datakilde direkte til HDFS.

    Filen gemmes ikke på det lokale filsystem.
    Filnavnet udledes automatisk fra URL'en.

    Returnerer HDFS-stien til den hentede fil.
    """

    # Udleder filnavnet fra URL'en i stedet for at hardcode f.eks. "iris.csv".
    # Eksempel:
    # https://.../iris.csv -> iris.csv
    # XXX09
    filename = posixpath.basename(urlparse(url).path)

    if not filename:
        raise ValueError("URL'en indeholder ikke et gyldigt filnavn.")

    # XXX03
    # Opgaven kræver HTTPS under transport af data.
    if urlparse(url).scheme != "https":
        raise ValueError("Datakilden skal anvende HTTPS.")

    # Bygger den endelige HDFS-sti.
    hdfs_path = f"{hdfs_input_dir.rstrip('/')}/{filename}"

    # Vi bruger først en midlertidig fil PÅ HDFS.
    # Hvis download bliver afbrudt, erstatter en halvfærdig fil derfor
    # ikke den eksisterende korrekte fil.
    # XXX08
    temp_hdfs_path = f"{hdfs_path}.tmp"

    # Opretter Input_dir i HDFS, hvis den ikke allerede findes.
    #
    # SIKKERHED:
    # subprocess køres med en liste af argumenter og IKKE med shell=True.
    # Dermed bliver værdierne ikke fortolket som shell-kommandoer,
    # hvilket reducerer risikoen for command-line injection.
    # XXX01
    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", hdfs_input_dir],
        check=True
    )

    # Fjerner en eventuel gammel midlertidig fil fra en tidligere
    # afbrudt download.
    subprocess.run(
        ["hdfs", "dfs", "-rm", "-f", temp_hdfs_path],
        check=True
    )

    try:
        # stream=True betyder, at hele filen ikke indlæses i hukommelsen
        # på én gang. Data behandles i mindre blokke (chunks).
        #
        # timeout beskytter programmet mod at hænge uendeligt,
        # hvis serveren eller netværket holder op med at svare.
        # XXX04
        with requests.get(
            url,
            stream=True,
        # XXX06
            timeout=(10, 30)
        ) as response:

            # Stopper med en exception ved HTTP-fejl som 404 eller 500.
            ### XXX07
            response.raise_for_status()

            # "hdfs dfs -put - <sti>" læser data fra standard input.
            # Derfor kan HTTP-data sendes DIREKTE til HDFS uden først
            # at oprette iris.csv på det lokale filsystem.
            # XXX02
            hdfs_process = subprocess.Popen(
                ["hdfs", "dfs", "-put", "-", temp_hdfs_path],
                stdin=subprocess.PIPE
            )

            # Tilføjer kolonnenavne som første linje i CSV-filen.
            # Datasættet fra kilden har ingen header, så vi skriver den direkte
            # til HDFS-streamen før de downloadede data.
            # XXX10
            header = (
                "sepal_length,sepal_width,petal_length,"
                "petal_width,species\n"
            )
            # XXX11
            hdfs_process.stdin.write(header.encode("utf-8"))

            try:
                # Downloader og videresender data i mindre blokke.
                # XXX05
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        hdfs_process.stdin.write(chunk)

                # Lukker input-strømmen til HDFS-processen, når alle data er sendt.
                hdfs_process.stdin.close()

                # Venter på, at HDFS-processen afslutter.
                return_code = hdfs_process.wait()

                # Hvis HDFS-processen fejler, stopper programmet med en fejl.
                if return_code != 0:
                    raise RuntimeError(
                        "HDFS kunne ikke gemme den downloadede fil."
                    )

            except Exception:
                # Sørger for at HDFS-processen bliver lukket,
                # hvis download eller skrivning fejler.
                if hdfs_process.stdin:
                    hdfs_process.stdin.close()

                hdfs_process.wait()
                raise

        # Downloaden er nu gennemført korrekt.
        # Den tidligere slutfil fjernes, hvorefter den komplette
        # midlertidige HDFS-fil får det endelige filnavn.
        subprocess.run(
            ["hdfs", "dfs", "-rm", "-f", hdfs_path],
            check=True
        )

        subprocess.run(
            ["hdfs", "dfs", "-mv", temp_hdfs_path, hdfs_path],
            check=True
        )

        return hdfs_path

    except Exception:
        # ROBUSTHED:
        # Hvis netværket afbrydes eller en anden fejl opstår,
        # fjernes den ufuldstændige .tmp-fil fra HDFS.
        subprocess.run(
            ["hdfs", "dfs", "-rm", "-f", temp_hdfs_path],
            check=False
        )

        raise