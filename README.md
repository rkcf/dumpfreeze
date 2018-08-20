dumpfreeze
==========

Create MySQL dumps and backup to Amazon Glacier

Installation
------------
Create virtualenv:

```
virtualenv .venv
. .venv/bin/activate
```

Install Dependencies:

`pip install -r requirements.txt`

Make sure your AWS credentials are located in ~/.aws/credentials

Use
---
python backup.py --database DATABASE --vault VAULT
