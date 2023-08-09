# CorTab

CorTab - Script for corpusTable to RDF conversions.

## Setup
Either use [poetry](https://python-poetry.org/) or activate a virtual environment (Python >=3.10) and run the following commands:
```shell
git clone git@github.com:lu-pl/cortab.git
cd cortab/
pip install .
```

## Usage

Either run `cortab.py` directly or use the CLI `cortacl`.

The CLI either transforms the entire corpusTable (if invoked without options) or creates a corpusTable partition by specifying a column and one or multiple rows and runs [tabulardf](https://github.com/lu-pl/tabulardf) rules defined in `rules.py`.

E.g.
```shell
cortacl --column 'id' --rows 14 16
```

creates a partition comprised of row 14 and 16 of the 'id' column (i.e. "SweDraCor" and "ReM") and runs the rdfdf conversion on that partition.

This is equivalent to: 
```shell
cortacl --column 'corpusAcronym' --rows 'ReM' 'SweDraCor'
```

Also a `--format` flag is supported, default value is "ttl".

## Example output

See [example_output.ttl](https://github.com/lu-pl/cortab/blob/main/cortab/example_output.ttl).

