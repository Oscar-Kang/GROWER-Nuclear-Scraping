# NRC 1999 Reactor Status Scraper

Scrapes NRC “Power Reactor Status Report” pages for every day in 1999 and outputs one line per unit:

`M/D/YYYY|UNIT|POWER|REASON_OR_COMMENT`

## Setup

```bash
python3 --version
```

## Run

```bash
python 1999scraper.py
```

Output:

- `output/nrc_reactor_status_1999.psv`

## Notes

- HTML is cached under `.cache/nrc_1999_html/` by default (safe to delete).
