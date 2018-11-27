# promethize

Insert seed data into Prometheus database.

## Setup/build

```bash
$ dep ensure
$ go build
$ go install
```

## Import data

Prepare data in Prometheus scrape format (`datafile.txt`):

```
# TYPE xyz counter
xyz{label1="one",label2="aaa"} 100 1543252259148
xyz{label1="one",label2="bbb"} 200 1543252259148
xyz{label1="one",label2="aaa"} 150 1543252319148
xyz{label1="one",label2="bbb"} 250 1543252319148
...
```

```bash
# Initialize a database
$ promethize init /path/to/prometheus/database

# Load data to the database
$ promethize load /path/to/prometheus/database datafile.txt
```

## Notes

1. Only counter metrics were tested.
2. Running multiple successive `load` commands was not tested.
