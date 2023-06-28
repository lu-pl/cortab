"""Script for corpusTable to RDF transformations."""

import pandas as pd
from rdfdf.rdfdf import DFGraphConverter
from rdflib import Graph

from rules import rules

from table_partitions import (
    corpus_table,
    rem_partition,
    greekdracor_partition,
    fredracor_partition,
    dramawebben_partition,
)

test_partition = corpus_table[corpus_table["id"].isin([14, 16])]

dfconversion = DFGraphConverter(
    dataframe=test_partition,
    subject_column="corpusAcronym",
    column_rules=rules,
)

graph = dfconversion.to_graph()
print(graph.serialize())

if __name__ == "__main__":
    graph = dfconversion.to_graph()
    print(graph.serialize())
