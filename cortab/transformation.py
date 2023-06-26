"""Script for corpusTable to RDF transformations."""

import pandas as pd
# from rdfdf import DFGraphConverter
from rdfdf.rdfdf import DFGraphConverter

from rules import rules
from table_partitions import (
    corpus_table,
    rem_partition,
    greekdracor_partition,
    fredracor_partition
)

dfgraph = DFGraphConverter(
    dataframe=corpus_table,
    subject_column="corpusAcronym",
    column_rules=rules
)

if __name__ == "__main__":
    # run transformation + serialize
    print(dfgraph.serialize())
