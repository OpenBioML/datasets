from rdkit import Chem


def read_sdf(filename, multithread=False):
    if multithread:
        suppl = Chem.MultithreadedSDMolSupplier(filename)
    else:
        suppl = Chem.ForwardSDMolSupplier(filename)
    return suppl
