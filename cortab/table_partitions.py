"""corpusTable row partitions for isolated testing."""

import pathlib
import pandas as pd

# _corpus_table_path = pathlib.Path(__file__).parent / "corpusTable_prep.xlsx"
# _corpus_table_path = pathlib.Path(__file__).parent / "corpusTable.csv"
_corpus_table_path = pathlib.Path(__file__).parent / "sheets/corporaList.csv"
corpus_table = pd.read_csv(_corpus_table_path)

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

disco_partition = corpus_table.loc[
    corpus_table["corpusAcronym"] == "DISCO"
]


_additional_link_table_path = pathlib.Path(__file__).parent / "sheets/additionalLinks.csv"
additional_link_table = pd.read_csv(_additional_link_table_path)

disco_additional_link_table = additional_link_table.loc[
    additional_link_table["corpusAcronym"] == "DISCO"
]
