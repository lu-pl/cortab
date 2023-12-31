"""Script for corpusTable to RDF transformations."""

from clisn import CLSInfraNamespaceManager
from tabulardf import FieldGraphConverter
from rdflib import Graph

from rules import rules
from table_partitions import (
    corpus_table,
    rem_partition,
    greekdracor_partition,
    fredracor_partition,
    dramawebben_partition,
)

nsgraph = Graph()
clsnm = CLSInfraNamespaceManager(nsgraph)

dfconversion = FieldGraphConverter(
    dataframe=rem_partition,
    subject_column="corpusAcronym",
    column_rules=rules,
    graph=nsgraph
)

graph = dfconversion.to_graph()
# print(graph.serialize())

if __name__ == "__main__":
    graph = dfconversion.to_graph()
    print(graph.serialize())
