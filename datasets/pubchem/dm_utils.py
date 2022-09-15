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
