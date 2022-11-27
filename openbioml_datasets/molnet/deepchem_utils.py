from rdkit import Chem
import selfies as sf
import pandas as pd


def make_dataframe(
    dataset, dataset_type, tasks, tasks_wanted
):
    # adapted from: https://github.dev/seyonechithrananda/bert-loves-chemistry/blob/b0b29205b4db002a043152e0dd3c0d23fbd351fe/chemberta/utils/molnet_dataloader.py#L165
    df = dataset.to_dataframe()
    if len(tasks) == 1:
        mapper = {"y": tasks[0]}
    else:
        mapper = {f"y{y_i+1}": task for y_i, task in enumerate(tasks_wanted)}
    df.rename(mapper, axis="columns", inplace=True)

    # Canonicalize SMILES
    smiles_list = [Chem.MolToSmiles(s, isomericSmiles=True) for s in df["X"]]
    selfies_list = [sf.encoder(s) for s in smiles_list]

    # Convert labels to integer for classification
    targets = df[tasks_wanted]
    if dataset_type == "classification":
        targets = targets.astype(int)

    elif dataset_type == "regression":
        targets = targets.astype(float)

    if len(tasks_wanted) == 1:
        targets = targets.values.flatten()
    else:
        # Convert labels to list for simpletransformers multi-label
        targets = targets.values.tolist()
    return pd.DataFrame({"smiles": smiles_list, "selfies": selfies_list, "target": targets})