#!/bin/bash
#
# DATA CLEANING SCRIPT
# ====================
#
# This Bash script prepares the data in order for it to be uploaded on a DuckDB database.
# Important variables should be defined in the .env file, located in the same folder.
#
# # .env
# RAW_DATA_FOLDER="[absolute path to the raw data folder]"
# TMP_DATA_FOLDER="[absolute path to the temporary data folder]"
# DB_FILE="[absolute path to the DuckDB database instance]"
#
# Last modified: 2022-10-10

# ENV VARIABLES
# =============
#
# Load environment variables
 
set -a;
source .env;
set +a;

# DATASET NAME AND TYPES
# ======================
#
# Define the name of the dataset to be loaded in the DB and its subfolders, i.e. dataset types.

dataset_name="Italy Coronavirus Disease Prevention Map Feb 24 2020 Id"

declare -A dataset_types=(
	["[Discontinued] Facebook Population (Administrative Region) v1"]="population_adm"
	["[Discontinued] Facebook Population (Tile Level) v1"]="population_tile"
	["[Discontinued] Movement Between Administrative Regions v1"]="movement_adm"
	["[Discontinued] Movement Between Tiles v1"]="movement_tile"
	["[Discontinued] Colocation"]="colocation"
)

declare -A dataset_columns=(
	["population_adm"]="date_time,polygon_id,n_baseline,n_crisis,density_baseline,density_crisis"
	["population_tile"]="date_time,quadkey,n_baseline,n_crisis,density_baseline,density_crisis"
	["movement_adm"]="date_time,start_polygon_id,end_polygon_id,n_baseline,n_crisis"
	["movement_tile"]="date_time,start_quadkey,end_quadkey,n_crisis,n_baseline"
	["colocation"]="date_time,polygon1_id,polygon2_id,link_value"
)

# DATABASE INITIALIZATION
# =======================
#
# We initialize the database and make sure the tables are clean

for table_name in "${dataset_types[@]}"; do
	duckdb "${DB_FILE}" -c "DROP TABLE IF EXISTS ${table_name};"
done

# DATA INGESTION
# ==============
#
# Scan through the subfolders of the main raw data folder.
#
# Generally the criterion for subfolder hierarchy is the following:
# 	[PROJECT_FOLDER] / data / raw / [DATASET_NAME] / [DATASET_TYPE] / [CSV_FILENAME].csv
#
# In our case, a plausible set of paths might be, i.e.:
# 	aiace_dp / data / raw / Italy Coronavirus Disease Prevention Map Feb 24 2020 Id / [Discontinued] Colocation / [...].csv


for dataset_type in "${!dataset_types[@]}"; do

	# Define the path of the folder the script will look into to find the zipfiles
	dataset_type_folder="${RAW_DATA_FOLDER}/${dataset_name}/${dataset_type}"

	if [ ! -d "$dataset_type_folder" ]; then
		echo "[WARNING] $dataset_type folder is not available in the filesystem"
		continue
	else
		echo "[LOG] Going to perform the data ingestion process for $dataset_type data"
	fi

	# For each subfolder (i.e. corresponding to a dataset type), scan throught the
	# datafiles, unpack and load each of the unpacked csv files in a temporary folder

	for datafile in "${dataset_type_folder}"/*; do

		# Unzip the archives
		# ------------------
		#
		# Check whether the temporary data folder exists / is empty
		# and proceed to creation or cleaning

		if [ -d "${TMP_DATA_FOLDER}" ]; then
			echo "[LOG] Temporary data folder exists and is located in ${TMP_DATA_FOLDER}"
			echo "[LOG] The content of the folder will be cleaned up"
			rm -Rf "${TMP_DATA_FOLDER}"/*
		else
			echo "[LOG] Temporary data folder does not exist and will be created in ${TMP_DATA_FOLDER}"
			mkdir -p "${TMP_DATA_FOLDER}"
		fi
	
		# Unzip the files quietly in the temporary folder, or copy the csv file
		if [[ "${datafile}" == *.zip ]]; then
			unzip -q "${datafile}" -d "${TMP_DATA_FOLDER}"
		elif [[ "${datafile}" == *.csv ]]; then
			cp "${datafile}" "${TMP_DATA_FOLDER}"
		else
			echo "[WARNING] A file that is not a zip nor a csv was detected."
			echo "	${datafile}"
			continue
		fi

		# Duplicates check (INACTIVE)
		# ---------------------------
		#
		# Check for redundant data files
		# from https://unix.stackexchange.com/questions/277697/whats-the-quickest-way-to-find-duplicated-files
		# find ${TMP_DATA_FOLDER} ! -empty -type f -exec md5sum {} + | sort | uniq -w32 -dD
		# Should not be critical since files with the same filenames will be mutually overwritten

		# Let us now proceed to data cleaning and loading
		for csvfile in "${TMP_DATA_FOLDER}"/*.csv; do

			# Import the data in the database
			# -------------------------------
			#
			# For each datafile unzipped in the temporary folder, scan throught all the unzipped
			# csv files, filter only entries for Italy (ITA), select only columns of interest to
			# us and feed the resulting csv stdout to the DuckDB `.import` tool.

			# We should make an exception here for the `colocation` dataset, because there is a
			# field missing (`date_time`, that we are extracting from the filename) and also the
			# main value of interest is not in the column `n_crisis` but in `link_value`.

			if [[ "${dataset_types[$dataset_type]}" == "colocation" ]]; then
				mlr --csv filter '$country == "IT" && !is_empty($link_value)' then \
					put '$date_time = splita(splita(FILENAME, "/")[-1], "_")[-1][:-5] . " 00:00:00"' then \
					cut -o -f "${dataset_columns[${dataset_types[$dataset_type]}]}" "${csvfile}" | \
				duckdb "${DB_FILE}" -c ".import --csv /dev/stdin ${dataset_types[$dataset_type]}"
			else
				mlr --csv filter '$country == "IT" && !is_empty($n_crisis)' then \
					cut -o -f "${dataset_columns[${dataset_types[$dataset_type]}]}" "${csvfile}" | \
				duckdb "${DB_FILE}" -c ".import --csv /dev/stdin ${dataset_types[$dataset_type]}"
			fi
		done
	done
done

# CLEAN UP
# ========
# Clean up the workspace

finish() {
  result=$?
  rm -Rf $TMP_DATA_FOLDER
  exit ${result}
}

trap finish EXIT ERR