This project looks at the mortgage application data from the national HMDA data sets for the years 2007 through 2025. Taking into consideration the schema change (reposting requirements) as of 2018.

The data set is quite large, it was extracted, loaded, and pre-processed using PySpark, Google Cloud Storage and BigQuery data pipelines. The transformation layer was completely done by using dbt studio. The data engineering portion has its own folder structure. The EL part of the pipeline is a simple batch job, which can be run on demand; this is largely due to the nature of the data, which for my purposes only becomes available and needs to be updated once a year. 


