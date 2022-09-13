import gzip
from pathlib import Path

import datamol as dm
from rdkit import Chem

URL = "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF"
FILE = "Compound_048500001_049000000.sdf.gz"  # Only 10MB
OUTPUT_DIR = "data"

folder = Path(OUTPUT_DIR)
dm.utils.fs.mkdir(folder, exist_ok=True)


def process_mol(mol):
    mol.SetProp("CAN_SELFIES", dm.to_selfies(mol))
    return Chem.rdMolInterchange.MolToJSON(mol)


def write_jsonl(mols, path):
    with open(path, "w") as f:
        for mol in mols:
            f.write(mol)
            f.write("\n")


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
    for mol in suppl:
        if mol is None:
            continue
        json_str = dm.parallelized(process_mol, [mol], n_jobs=-1)
        mols.append(json_str[0])

    filename = basename.replace(".sdf.gz", "_SELFIES.jsonl")
    write_jsonl(mols, subfolder / filename)
    print(f"Saved to {subfolder / filename}")
