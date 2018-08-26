dumpfreeze
==========

Create and manage MySQL dumps locally and on AWS Glacier

**Note: dumpfreeze is under heavy development, do not use for vital services**

Installation
------------
Depends on mysqldump being in your system path.  This is typically provided by your distro in a mysql|mariadb-client package.

### Pip
```
git clone https://github.com/rkcf/dumpfreeze
cd dumpfreeze
pip install --user .
```

Usage
-----
Make sure your AWS credentials are located in ~/.aws/credentials

dumpfreeze uses a local sqlite database to keep track of the inventory.  By default this is located at ~/.dumpfreeze/inventory.db

In general commands follow the format: `dumpfreeze noun verb --options UUID`

#### Getting Help

For any subcommand, append --help to view usage and options.

#### Backup Commands

Create a backup:

`dumpfreeze backup create DATABASE`

Upload a backup to AWS Glacier:

`dumpfreeze backup upload UUID`

Delete a backup:

`dumpfreeze backup delete UUID`

List backups in local inventory:

`dumpfreeze backup list`

#### Archive Commands

Delete an archive:

`dumpfreeze archive delete UUID`

List archives in local inventory:

`dumpfreeze archive list`

Contributing
------------

### Virtualenv Installation

Create virtualenv:

```
virtualenv .venv
. .venv/bin/activate
```

Install Dependencies:

`pip install -r requirements.txt`

Use:

`python -m dumpfreze.main --help`

License
-------
dumpfreeze is licensed under the MIT License.  See LICENSE for full text.
