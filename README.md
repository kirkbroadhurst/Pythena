# Pythena
Python library for Athena, extending functionality.

The main feature of this library right now is that it retrieves queries results via S3 call vs via Athena's JDBC connection. This can be significantly faster for large data sets.

## Requirements

- AWS profile/role configured (i.e. either `awscli` or an IAM role
- Default region either configured in awscli, or via `Client` constructor
- Output location either defined in `PYTHENA_OUTPUTLOCATION` environment variable, or via `Client` constructor

## Usage

```
from pythena import Client

df = Client().athena_query('select * from db.my_table limit 100')
```



