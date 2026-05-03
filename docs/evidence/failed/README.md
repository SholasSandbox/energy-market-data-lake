# Failed Evidence Samples

Files in this folder are intentionally malformed payloads used to prove schema
validation rejects bad data.

Naming convention:

```text
bad_<contract_name>.sample.json
```

Run the failure check from the repository root:

```bash
python scripts/validate_contracts.py --include-evidence --check-failures
```

Expected behavior:

- valid examples and generated evidence pass
- files in this folder are rejected as expected
