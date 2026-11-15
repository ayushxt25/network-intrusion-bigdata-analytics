# UNSW-NB15 provenance

- Provider: UNSW Canberra at ADFA / UNSW Research.
- Landing page: <https://research.unsw.edu.au/projects/unsw-nb15-dataset>
- Official download folder: <https://unsw-my.sharepoint.com/:f:/g/personal/z5025758_ad_unsw_edu_au/EnuQZZn3XuNBjgfcUu4DIVMBLCHyoLHqOswirpOQifr1ag?e=gKWkLS>
- Preferred files: headerless `UNSW-NB15_1.csv` … `UNSW-NB15_4.csv` (2,540,044 records total). They retain endpoint identifiers and `Stime`/`Ltime`.
- Do not substitute the training (175,341 rows) and testing (82,332 rows) subsets; they are smaller prepared partitions.
- Raw capture archive: 100 GB PCAP traffic, documented by UNSW; it is not required for this CSV-flow phase.
- Per-CSV byte sizes and checksums are not published on the UNSW landing page. After manual download, the validator writes each local byte size and SHA-256 to ignored `artifacts/unsw_nb15_validation/unsw_nb15_metadata.json`.

## Terms

UNSW grants academic research use in perpetuity. Commercial use requires agreement with the authors; provider-requested citations apply. Do not redistribute raw files from this repository.

## Validation contract

The four full CSVs are read as headerless rows in the official 49-column order. Raw values remain strings during inspection; numeric-designated fields are deterministically tested for `double` castability. No malformed row, null, duplicate, label/category mismatch, timestamp ordering issue, or invalid numeric value is repaired or silently dropped.

`Stime` and `Ltime` are available as raw temporal fields and `srcip`, `sport`, `dstip`, and `dsport` as endpoint fields. Phase 2A does not assert a timezone/unit beyond the source schema and therefore does not manufacture timestamps or validate geographic identity.
