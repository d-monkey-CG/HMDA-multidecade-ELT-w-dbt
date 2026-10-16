This project analyzes national HMDA mortgage application records from 2007 to 2025, harmonizing schema shifts following the 2018 reporting revisions.

Due to the dataset's scale, extraction, staging, and pre-processing are handled via PySpark, Google Cloud Storage, and BigQuery. Transformations are managed through dbt within a dedicated data engineering directory. Because HMDA data is released on an annual cycle, the EL pipeline operates as an on-demand batch job executed as new data becomes available.

