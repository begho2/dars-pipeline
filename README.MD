## Summary

# Manual Testing
## Ingest
open spark shell with suitable memory, then copy paste code from dars-ingest repo
spark-shell --driver-memory 3g --conf spark.memory.storageFraction=0.1|tee -a ~/spark1.out
3g is enough for ae. still need to try for apc

## Setting up Github actions
The .github folder contains config to force run python tests using pytest
pytest is relying on the pytest.ini file to define where the source code is

## Set up tests

