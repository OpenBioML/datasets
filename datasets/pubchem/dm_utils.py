import gzip
import json

import datamol as dm
import selfies as sf
from rdkit import Chem, RDLogger


def process_mol(mol):
    RDLogger.DisableLog("rdApp.*")
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


def filter_jsonl(jsonlines):
    filtered = []
    for json_str in jsonlines:
        obj = json.loads(json_str)
        properties = obj["molecules"][0]["properties"]
        if "CAN_SELFIES" in properties:
            filtered.append(json_str)
    print(f"Filtered {len(jsonlines) - len(filtered)} molecules")
    return filtered


def compress_jsonl(jsonlines, path):
    with gzip.open(path, "wt") as f:
        for json_str in jsonlines:
            f.write(json_str)
            f.write("\n")
