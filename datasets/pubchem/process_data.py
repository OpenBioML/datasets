import gzip
import os

import datamol as dm
import modal

from datasets.pubchem.dm_utils import (compress_jsonl, filter_jsonl,
                                       process_mol, write_jsonl)

URL = "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF"
BATCH_SIZE = 100_000


stub = modal.Stub(
    image=modal.Image.debian_slim()
    .run_commands(
        ["apt-get install -y libxrender1 libsm-dev libxext-dev"],
    )
    .pip_install(["tqdm", "datamol", "selfies", "rdkit", "requests", "aiohttp"])
)

volume = modal.SharedVolume().persist("pubchem-selfies")
CACHE_DIR = "/cache"


@stub.function(
    mounts=modal.create_package_mounts(["datasets"]),
    memory=4051,
    shared_volumes={CACHE_DIR: volume},
    concurrency_limit=100,
    cpu=4,
)
def run_process_mol(path):
    from pathlib import Path

    import datamol as dm
    from rdkit import Chem, RDLogger

    RDLogger.DisableLog("rdApp.*")
    Chem.SetDefaultPickleProperties(Chem.PropertyPickleOptions.AllProps)

    folder = Path(os.path.join(CACHE_DIR, "data"))
    basename = dm.fs.get_basename(path)
    filename = basename.replace(".sdf.gz", "_SELFIES.jsonl")
    compressed_filename = filename + ".gz"
    subfolder = folder / basename.split(".")[0]
    if dm.fs.exists(subfolder / compressed_filename):
        return True

    dm.utils.fs.mkdir(subfolder, exist_ok=True)

    destination = subfolder / basename

    if dm.fs.exists(destination):
        print(f"File already downloaded: {destination}")
    else:
        print(f"Downloading {path}")
        try:
            dm.fs.copy_file(
                source=path, destination=destination, force=True, progress=False
            )
        except Exception:
            print(f"Failed to download {path}")
            return False

    suppl = Chem.ForwardSDMolSupplier(gzip.open(destination))
    batch = []
    mols_json_str = []
    for mol in suppl:
        if mol is None:
            continue

        batch.append(mol)
        try:
            if len(batch) == BATCH_SIZE:
                mols_json_str.extend(
                    dm.parallelized(process_mol, batch, n_jobs=-1, progress=False)
                )
                batch = []
        except Exception:
            print(f"Failed to process {path}")
            return False

    # case where len(suppl) % BATCH_SIZE != 0
    try:
        if batch:
            mols_json_str.extend(
                dm.parallelized(process_mol, batch, n_jobs=-1, progress=False)
            )
    except Exception:
        print(f"Failed to process {path}")
        return False

    filtered_json_str = filter_jsonl(mols_json_str)
    write_jsonl(mols_json_str, subfolder / filename)
    compress_jsonl(
        filtered_json_str, subfolder / filename.replace(".jsonl", ".jsonl.gz")
    )
    return True


if __name__ == "__main__":
    paths = sorted(dm.fs.glob(f"{URL}/**.gz"))
    missing = []
    with stub.run():
        times = run_process_mol.map(paths)

        for t, path in zip(times, paths):
            if t:
                print(f"Processed {path}")
            else:
                missing.append(path)

    print(f"Missing: {missing}")
