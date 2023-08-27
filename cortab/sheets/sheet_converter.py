"""Read an excel file and create CSV files for every sheet."""

import pathlib

from os import PathLike
from typing import Optional

import click
import pandas as pd


def sheets_to_csv(excel_file: PathLike | str,
                  output_path: Optional[PathLike | str] = None):
    """Generate CSVs for every sheet in an Excel file."""
    excel_file_path = pathlib.Path(excel_file)
    output_path = (
        pathlib.Path(output_path)
        if output_path is not None
        else excel_file_path.parent)

    # xls = pd.ExcelFile(excel_path, engine="xlrd")
    xls = pd.ExcelFile(excel_file_path)

    for sheet in xls.sheet_names:
        dataframe = pd.read_excel(xls, sheet_name=sheet)

        # clean whitespace
        dataframe = dataframe.applymap(
            lambda x: x.strip() if isinstance(x, str) else x
        )

        # write CSVs
        csv_path = output_path / f"{sheet}.csv"

        with open(csv_path, "w") as f:
            csv = dataframe.to_csv()
            f.write(csv)



@click.command()
@click.argument("input", type=click.Path())
@click.argument("output", type=click.Path(), default=None)
def sheet_converter(input, output):
    """Click command for running sheets_to_csv."""
    sheets_to_csv(input, output)


if __name__ == "__main__":
    sheet_converter()
