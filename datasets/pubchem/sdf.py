from rdkit import Chem


def read_sdf(filename):
    suppl = Chem.MultithreadedSDMolSupplier(filename)
    return suppl
