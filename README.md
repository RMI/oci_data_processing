# oci_data_processing

This repo contains the scripts to batch process upstream/midstream/downstream modeled data for the OCI+ project.
Save the repo in a local directory and change sp_dir path variable to the sharepoint folder directory to access the data. 

What do the scripts in the repo do?

- upstream_data_processing.py: automatically loads all the csv files from the processor to our Sharepoint folder and extract all the information we need from the OPGEE results (i.e. results, flow sheet, energy summary, vff, ghg summary)

- midstream_data_processing.py: processed Liam's batch run results and built an assay library for all Haverly, OCI and PRELIM assays. The assay library includes product slate (needed for downstream OPEM modeling) and midstream- emission data;

- downstream_data_processing,py: merges upstream and midstream results and prepare inputs for OPEM python package: opem_input.csv, which takes the upstream results and all_product_slates.csv, which stores assay slate inventory. After the data are prepared, it will run the OPEM package and produce the dowstream emission results in opem_output.csv.

- data_for_webtool.py: prepares data for the OCI+ webtool and produces infobase.csv, info20.csv and info100.csv files.

Next steps:
- Working on a script to automatically produce Trace output;
- Refactoring


