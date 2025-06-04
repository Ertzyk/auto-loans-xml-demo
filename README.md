# auto-loans-xml-demo

This project extracts, cleans, and organizes loan-level and lease-level data from auto loan prospectuses (424B5 filings), using SEC EDGAR data. This is a simplified version of an ETL pipeline developed as part of a collaborative project. The original dataset is confidential and not included here.

---

## What It Does

When you run `main.py`, it optionally:
- Downloads EDGAR index data and 424B5 prospectuses
- Converts filings from HTML/XML to plain text
- Extracts financial summaries from each filing
- Normalizes raw loan-level/lease-level CSVs
- Loads cleaned data into a SQLite database

Control which steps run by editing flags in `settings.py`.

---

This project includes code adapted from the MIT-licensed
[python-edgar](https://github.com/greedo/python-edgar). The original licence
text is included in `licenses/python-edgar-LICENSE.txt`.