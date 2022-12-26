import argparse
from deepchem.molnet.load_function.bace_datasets import (
    load_bace_regression,
    load_bace_classification,
)
from deepchem.molnet.load_function.tox21_datasets import load_tox21
from deepchem.molnet.load_function.pcba_datasets import load_pcba
from deepchem.molnet.load_function.clintox_datasets import load_clintox
from deepchem.molnet.load_function.delaney_datasets import load_delaney
from deepchem.molnet.load_function.clearance_datasets import load_clearance
from deepchem.molnet.load_function.lipo_datasets import load_lipo
from deepchem.molnet.load_function.bbbp_datasets import load_bbbp
import os

from datasets.molnet.deepchem_utils import make_dataframe


DATASETS = {
    "bace_regression": {
        "load_fn": load_bace_regression,
        "task_type": "regression",
        "target": "pIC50",
        "split": "scaffold",
    },
    "bace_classification": {
        "load_fn": load_bace_classification,
        "task_type": "classification",
        "target": "Class",
        "split": "scaffold",
    },
    "pcba": {
        "load_fn": load_pcba,
        "task_type": "classification",
        "target": "PCBA-686978",
        "splitter": "random",
    },
    "tox21": {
        "load_fn": load_tox21,
        "task_type": "classification",
        "target": "SR-p53",
        "splitter": "scaffold",
    },
    "clintox": {
        "load_fn": load_clintox,
        "task_type": "classification",
        "target": "CT_TOX",
        "splitter": "scaffold",
    },
    "delaney": {
        "load_fn": load_delaney,
        "task_type": "reression",
        "target": "measured log solubility in mols per litre",
        "splitter": "scaffold",
    },
    "clearance": {
        "load_fn": load_clearance,
        "task_type": "reression",
        "target": "target",
        "splitter": "scaffold",
    },
    "lipo": {
        "load_fn": load_lipo,
        "task_type": "reression",
        "target": "exp",
        "splitter": "scaffold",
    },
    "bbbp": {
        "load_fn": load_bbbp,
        "task_type": "classification",
        "target": "p_np",
        "splitter": "scaffold",
    },
}


def process_dataset(data_dir, dataset_name):
    load_fn = DATASETS[dataset_name]["load_fn"]
    task_type = DATASETS[dataset_name]["task_type"]
    target = DATASETS[dataset_name]["target"]
    splitter = DATASETS[dataset_name]["splitter"]

    save_dir = os.path.join(data_dir, dataset_name)
    print(f"Processing {dataset_name}")
    tasks, splits, _ = load_fn(featurizer="Raw", splitter=splitter)

    assert target in tasks, f"Target {target} not in tasks {tasks}"

    splits = [
        make_dataframe(
            s,
            task_type,
            tasks,
            target,
        )
        for s in splits
    ]

    (train, valid, test) = splits

    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    print(f"Saving {dataset_name}")

    train.to_csv(os.path.join(save_dir, f"{dataset_name}_train.csv"), index=False)
    valid.to_csv(os.path.join(save_dir, f"{dataset_name}_valid.csv"), index=False)
    test.to_csv(os.path.join(save_dir, f"{dataset_name}_test.csv"), index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=str, default="data")
    parser.add_argument("--dataset_name", type=str, required=True)
    args = parser.parse_args()

    process_dataset(args.data_dir, args.dataset_name)
