import json

import selfies as sf
from rdkit import Chem

from datasets.pubchem.download import download_sdf
from datasets.pubchem.sdf import read_sdf


def to_jsonl(objects, filename):
    with open(filename, "w") as f:
        for obj in objects:
            f.write(json.dumps(obj))
            f.write("\n")


def process_file(filename):
    print("| Downloading")
    downloaded = download_sdf(filename)

    objs = []
    failed = []
    print("| Reading SDF")
    num_processed = 0
    suppl = read_sdf(downloaded)
    for mol in suppl:
        if mol is None:
            continue

        json_obj = json.loads(Chem.rdMolInterchange.MolToJSON(mol))

        try:
            json_obj["CAN_SELFIE"] = sf.encoder(
                json_obj["molecules"][0]["properties"]["PUBCHEM_OPENEYE_CAN_SMILES"]
            )

            objs.append(json_obj)
            num_processed += 1
        except sf.EncoderError as e:
            print(f"Failed to encode {json_obj['molecules'][0]['name']} with error {e}")
            json_obj["ERROR"] = str(e)
            failed.append(json_obj)

        if num_processed % 1000 == 0:
            print(f"| Processed {num_processed} molecules")

    print(f"| Processed {num_processed} molecules out of {len(objs) + len(failed)}")
    print("| Writing JSONL")
    jsonl_name = downloaded.replace(".sdf", ".jsonl")
    to_jsonl(objs, jsonl_name)

    # keeping for now for debugging
    to_jsonl(failed, jsonl_name.replace(".jsonl", "_failed.jsonl"))


if __name__ == "__main__":
    process_file(
        "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/Compound_000000001_000500000.sdf.gz"
    )
