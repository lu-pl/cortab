"""corpusTable row partitions for isolated testing."""

import pathlib
import pandas as pd

_corpus_table_path = pathlib.Path(__file__).parent / "corpusTable_prep.xlsx"
corpus_table = pd.read_excel(_corpus_table_path, engine="openpyxl")

# partitions
rem_partition = corpus_table.loc[
    corpus_table["corpusAcronym"] == "ReM"
]

greekdracor_partition = corpus_table.loc[
    corpus_table["corpusAcronym"] == "GreekDraCor"
]

fredracor_partition = corpus_table.loc[
    corpus_table["corpusAcronym"] == "FreDraCor"
]

dramawebben_partition = corpus_table.loc[
    corpus_table["corpusAcronym"] == "DramaWebben"
]
