dumpfreeze
==========

Create MySQL dumps and backup to Amazon Glacier

Installation
------------
Make sure your AWS credentials are located in ~/.aws/credentials

## Pip

` pip install --user .`

Use:

`dumpfreeze --help`

## Virtualenv

Create virtualenv:

```
virtualenv .venv
. .venv/bin/activate
```

Install Dependencies:

`pip install -r requirements.txt`

Use:

`python -m dumpfreze.main --help`
