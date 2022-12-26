# bio-datasets

bio-datasets



## PubChem Compound Dataset

Processing and convering [PubChem Compoud Dataset](https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/) can be found in `datasets/pubchem`. The `process_data.py` script downloads the `SDF`
file, converts the canonical SMILES representation to SELFIES, and saves it in a `jsonl` file. 
