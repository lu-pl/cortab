"""Small command line interface for testing corpusTable conversions."""

from collections.abc import Iterable
from typing import Any

import click

from table_partitions import corpus_table
from rdfdf.rdfdf import DFGraphConverter

from rules import rules


# https://stackoverflow.com/a/48394004/6455731
class OptionEatAll(click.Option):

    def __init__(self, *args, **kwargs):
        self.save_other_options = kwargs.pop('save_other_options', True)
        nargs = kwargs.pop('nargs', -1)
        assert nargs == -1, 'nargs, if set, must be -1 not {}'.format(nargs)
        super(OptionEatAll, self).__init__(*args, **kwargs)
        self._previous_parser_process = None
        self._eat_all_parser = None

    def add_to_parser(self, parser, ctx):

        def parser_process(value, state):
            # method to hook to the parser.process
            done = False
            value = [value]
            if self.save_other_options:
                # grab everything up to the next option
                while state.rargs and not done:
                    for prefix in self._eat_all_parser.prefixes:
                        if state.rargs[0].startswith(prefix):
                            done = True
                    if not done:
                        value.append(state.rargs.pop(0))
            else:
                # grab everything remaining
                value += state.rargs
                state.rargs[:] = []
            value = tuple(value)

            # call the actual process
            self._previous_parser_process(value, state)

        retval = super(OptionEatAll, self).add_to_parser(parser, ctx)
        for name in self.opts:
            our_parser = parser._long_opt.get(name) or parser._short_opt.get(name)
            if our_parser:
                self._eat_all_parser = our_parser
                self._previous_parser_process = our_parser.process
                our_parser.process = parser_process
                break
        return retval


@click.command()
@click.option("-c", "--column",
              required=True,
              type=str)
@click.option("-r", "--rows",
              required=True,
              type=tuple,
              cls=OptionEatAll)
@click.option("-f", "--format",
              default="ttl",
              show_default=True)
def convert_cli(column: str, rows: tuple[Any], format="ttl"):
    """Generate a corpusTable partition to run rdfdf rules on.

    This is a convenience CLI that allows to swiftly test corpusTable conversions;
    Table partitions are create by specifying a column and one or more rows.

    E.g. "python dfconvert-cli.py --column 'id' --rows 14 16" creates a partition
    comprised of row 14 and 16 of the 'id' column.
    """

    # kludgy handling of integers/strings
    rows = map(lambda x: int(x) if x.isdigit else x, rows)

    table_partition = corpus_table[
        corpus_table[column].isin(list(rows))
    ]

    dfconversion = DFGraphConverter(
        dataframe=table_partition,
        subject_column="corpusAcronym",
        column_rules=rules
    )

    graph = dfconversion.to_graph()
    click.echo(graph.serialize(format=format))


if __name__ == "__main__":
    convert_cli()
