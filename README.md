# Database

Database is a database creator for quantum researchers to construct their own database on the personal computer.

## Installation

```shell
git clone https://github.com/ppoak/bearalpha
git submodule update database
```

## Usage

The project is depended on selenium crawler, so it is neccessary to download a chrome driver in prior. About how to find and download a version correspond to your chrom, please google it yourself.

You need firstly upload the file `ricequant_fetcher.ipynb` to your ricequant research environment (the jupyter notebook). Then on your personal computer, or your personal server. simple set a daily task run schedully. The command for running update is:

```
python ./update.py --user <username> --password <password> --driver <your chrome driver path> --backup <your data backup directory>
```

## Documentation

Not finished yet!😅

## Change Log

version 0.1.1: (current version) reorganize the structure, move `fetch_data` to `collector.py`

version 0.1.0: support for daily update data from ricequant.
