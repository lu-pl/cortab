# CorTab

CorTab - Script for corpusTable to RDF transformation.

## Setup

Either use [poetry](https://python-poetry.org/) or activate a virtual environment (Python >=3.10) and run the following commands:

```shell
git clone git@github.com:lu-pl/cortab.git
cd cortab/
pip install .
```

## Usage

Either run `dfconvert.py` directly or use the CLI `dfconvert-cli.py`.

The CLI creates a corpusTable partition by specifying a column and one or multiple rows and runs [rdfdf](https://github.com/lu-pl/rdfdf) rules defined in `rules.py`.

E.g.
```shell
python dfconvert-cli.py --column 'id' --rows 14 16
```

creates a partition comprised of row 14 and 16 of the 'id' column (i.e. "SweDraCor" and "ReM") and runs the rdfdf conversion on that partition.

Also a `--format` flag is supported, default value is "ttl".
