import gzip
import time
from pathlib import Path

import datamol as dm
import selfies as sf
from rdkit import Chem, RDLogger
from tqdm import tqdm

URL = "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF"
FILE = "Compound_048500001_049000000.sdf.gz"  # Only 10MB
OUTPUT_DIR = "data"
BATCH_SIZE = 100_000

RDLogger.DisableLog("rdApp.*")
Chem.SetDefaultPickleProperties(Chem.PropertyPickleOptions.AllProps)


def process_mol(mol):
    try:
        mol.SetProp("CAN_SELFIES", dm.to_selfies(mol))
    except sf.exceptions.EncoderError as e:
        mol.SetProp("ERROR", str(e))
    except Exception:
        mol.SetProp("ERROR", "Unknown error")

    try:
        json_str = Chem.rdMolInterchange.MolToJSON(mol)
    except Exception:
        json_str = None

    return json_str


def write_jsonl(mols, path):
    with open(path, "w") as f:
        for mol in mols:
            f.write(mol)
            f.write("\n")


folder = Path(OUTPUT_DIR)
dm.utils.fs.mkdir(folder, exist_ok=True)

paths = dm.fs.glob(f"{URL}/**.gz")
# paths = dm.fs.glob(f"{URL}/{FILE}") # single file
paths = [
    "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/Compound_160000001_160500000.sdf.gz"
]


for path in tqdm(paths):
    start = time.time()
    basename = dm.fs.get_basename(path)
    subfolder = folder / basename.split(".")[0]
    dm.utils.fs.mkdir(subfolder, exist_ok=True)
    print(f"Created {subfolder}")

    destination = subfolder / basename

    if dm.fs.exists(destination):
        print(f"File already downloaded: {destination}")
    else:
        print(f"Downloading {path}")
        dm.fs.copy_file(source=path, destination=destination, force=True, progress=True)
        print(f"Downloaded {destination}")

    # datamol's fn loads everything in memory, so we use rdkit's
    suppl = Chem.ForwardSDMolSupplier(gzip.open(destination))
    batch = []
    mols_json_str = []
    for mol in suppl:
        if mol is None:
            continue

        batch.append(mol)
        if len(batch) == BATCH_SIZE:
            mols_json_str.extend(
                dm.parallelized(process_mol, batch, n_jobs=-1, progress=True)
            )
            batch = []

    # case where len(suppl) % BATCH_SIZE != 0
    if batch:
        mols_json_str.extend(
            dm.parallelized(process_mol, batch, n_jobs=-1, progress=True)
        )

    filename = basename.replace(".sdf.gz", "_SELFIES.jsonl")
    write_jsonl(mols_json_str, subfolder / filename)
    print(f"Saved to {subfolder / filename}")
    end = time.time()
    print(f"Elapsed time for {path}: {end - start:.2f}s")
