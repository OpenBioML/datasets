import gzip
import time
from pathlib import Path

import datamol as dm
import selfies as sf
from rdkit import Chem, RDLogger

URL = "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF"
FILE = "Compound_048500001_049000000.sdf.gz"  # Only 10MB
OUTPUT_DIR = "data"

folder = Path(OUTPUT_DIR)
dm.utils.fs.mkdir(folder, exist_ok=True)


RDLogger.DisableLog("rdApp.*")


def process_mol(mol):
    try:
        mol.SetProp("CAN_SELFIES", dm.to_selfies(mol))
    except sf.exceptions.EncoderError as e:
        mol.SetProp("ERROR", str(e))
    return Chem.rdMolInterchange.MolToJSON(mol)


def write_jsonl(mols, path):
    with open(path, "w") as f:
        for mol in mols:
            f.write(mol)
            f.write("\n")


# dm.fs.glob(f"{RESOURCE_URL}/**.gz") to query all files (would prob freeze)
start = time.time()
for path in dm.fs.glob(f"{URL}/{FILE}"):
    basename = dm.fs.get_basename(path)
    subfolder = folder / basename.split(".")[0]
    dm.utils.fs.mkdir(subfolder, exist_ok=True)
    print(f"Created {subfolder}")
    filename = basename.replace(".sdf.gz", "_SELFIES.jsonl")

    destination = subfolder / basename
    dm.fs.copy_file(
        source=path,
        destination=destination,
        force=True,  # TODO: check if file already exists
    )
    print(f"Downloaded {destination}")

    mols = []

    # datamol's fn loads everything in memory, so we use rdkit's
    suppl = Chem.ForwardSDMolSupplier(gzip.open(destination))
    batch = []
    mols_json_str = []
    for mol in suppl:
        if mol is None:
            continue
        batch.append(mol)
        if len(batch) == 100000:
            mols_json_str.extend(
                dm.parallelized(process_mol, batch, n_jobs=-1, progress=True)
            )
            batch = []

    # case where len(suppl) % 100000 != 0
    if batch:
        mols_json_str.extend(
            dm.parallelized(process_mol, batch, n_jobs=-1, progress=True)
        )

    filename = basename.replace(".sdf.gz", "_SELFIES.jsonl")
    write_jsonl(mols_json_str, subfolder / filename)
    print(f"Saved to {subfolder / filename}")
    end = time.time()
    print(f"Elapsed time for {path}: {end - start:.2f}s")
