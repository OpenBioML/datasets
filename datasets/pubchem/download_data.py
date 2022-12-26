import concurrent.futures
import multiprocessing as mp
import os
import subprocess

JSONL_COMMAND = "modal volume get pubchem-selfies data/{COMPOUND}/{COMPOUND}_SELFIES.jsonl.gz data/{COMPOUND}"
GZIP_COMMAND = (
    "modal volume get pubchem-selfies data/{COMPOUND}/{COMPOUND}.sdf.gz data/{COMPOUND}"
)

output = subprocess.run(
    "modal volume ls pubchem-selfies data/",
    shell=True,
    check=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)

compounds = output.stdout.decode("utf-8").split("\n")
compounds = [c for c in compounds if c != ""]


def download_and_save(compound):
    os.makedirs(f"data/{compound}", exist_ok=True)

    if os.path.exists(f"data/{compound}/{compound}_SELFIES.jsonl") and os.path.exists(
        f"data/{compound}/{compound}.sdf.gz"
    ):
        return True

    jsonl_command = JSONL_COMMAND.format(COMPOUND=compound)

    print(f"| Downloading {compound}")

    if not os.path.exists(f"data/{compound}/{compound}_SELFIES.jsonl.gz"):
        count = 0
        while True:
            try:
                output = subprocess.run(
                    jsonl_command,
                    shell=True,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            except subprocess.CalledProcessError as e:
                print(e)
                print(f"Failed to download {compound} jsonl")
                return False

            if output.returncode == 0 or count > 2:
                break
            count += 1

    print(f"| Finished {compound}")

    return True


if __name__ == "__main__":
    print(len(compounds))
    with concurrent.futures.ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        for compound, downloaded in zip(
            compounds, executor.map(download_and_save, compounds)
        ):
            if not downloaded:
                print(f"Failed to download {compound}")
