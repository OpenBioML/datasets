from rdkit import Chem
import selfies as sf
import pandas as pd


def make_dataframe(dataset, dataset_type, tasks, tasks_wanted):
    # adapted from: https://github.dev/seyonechithrananda/bert-loves-chemistry/blob/b0b29205b4db002a043152e0dd3c0d23fbd351fe/chemberta/utils/molnet_dataloader.py#L165
    df = dataset.to_dataframe()
    if len(tasks) == 1:
        mapper = {"y": tasks[0]}
    else:
        mapper = {f"y{y_i+1}": task for y_i, task in enumerate(tasks)}
    df.rename(mapper, axis="columns", inplace=True)

    # Canonicalize SMILES
    smiles_list = [Chem.MolToSmiles(s, isomericSmiles=True) for s in df["X"]]
    valid = []
    selfies_list = []
    for smile in smiles_list:
        try:
            selfie = sf.encoder(smile)
            selfies_list.append(selfie)
            valid.append(True)
        except sf.exceptions.EncoderError:
            valid.append(False)
    
    print(f"Valid SELFIES: {sum(valid)}/{len(valid)}")
    smiles_list = [smile for i, smile in enumerate(smiles_list) if valid[i]]

    # Convert labels to integer for classification
    assert tasks_wanted in df.columns, f"{tasks_wanted} not in dataset"

    targets = df[tasks_wanted]
    if dataset_type == "classification":
        targets = targets.astype(int)

    elif dataset_type == "regression":
        targets = targets.astype(float)

    targets = [target for i, target in enumerate(targets) if valid[i]]
    print(len(smiles_list), len(selfies_list), len(targets))

    return pd.DataFrame(
        {"smiles": smiles_list, "selfies": selfies_list, "target": targets}
    )
