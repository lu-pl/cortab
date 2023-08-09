"""Small command line interface for testing corpusTable conversions."""

from collections.abc import Iterable
from typing import Any

import click

from clisn import CLSInfraNamespaceManager
from rdflib import Graph
from tabulardf import FieldGraphConverter

from cortab.click_custom import RequiredIf, RequiredMultiOptions
from cortab.rules import rules
from cortab.table_partitions import corpus_table



@click.command()
@click.option("-c", "--column",
              type=str,
              cls=RequiredIf,
              required_if="rows")
@click.option("-r", "--rows",
              type=tuple,
              cls=RequiredMultiOptions,
              required_if="column")
@click.option("-f", "--format",
              default="ttl",
              show_default=True)
def cortacl(column: str, rows: tuple[Any], format="ttl"):
    """Generate a corpusTable partition and run rdfdf rules on that partition.

    This is a convenience CLI that allows to swiftly test corpusTable conversions;
    Table partitions are created by specifying a column and one or more rows.

    E.g. "python dfconvert-cli.py --column 'id' --rows 14 16"
    creates a partition comprised of row 14 and 16 of the 'id' column
    and runs rules according to rules.py on that partition.
    """

    def _rows():
        """String/integer handling kludge."""
        for value in rows:
            try:
                value = int(value)
            except ValueError:
                pass

            yield value

    if column:  # column and rows are mutually dependent in the CLI
        dataframe = corpus_table[
            corpus_table[column].isin(list(_rows()))
        ]
    else:
        dataframe = corpus_table

    nsgraph = Graph()
    CLSInfraNamespaceManager(nsgraph)

    dfconversion = FieldGraphConverter(
        dataframe=dataframe,
        subject_column="corpusAcronym",
        column_rules=rules,
        graph=nsgraph
    )

    graph = dfconversion.to_graph()
    click.echo(graph.serialize(format=format))


if __name__ == "__main__":
    cortacl()
