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

# this should work only with docker
# otherwise set environment variables before running
if [ -f /.dockerenv ]; then
	echo "[LOG] Currently working in a Docker container"
	RAW_DATA_FOLDER=/app/data/raw
	TMP_DATA_FOLDER=/app/data/tmp
	DB_FILE=/app/data/aiace.db
else
	set -a;
	source .env;
	set +a;
fi

#
# Last modified: 2022-10-18

# ENV VARIABLES
# =============
#
# Load environment variables
 
# set -a;
# source .env;
# set +a;

# DATASET NAME AND TYPES
# ======================
#
# Define the name of the dataset to be loaded in the DB and its subfolders, i.e. dataset types.

dataset_name="Italy Coronavirus Disease Prevention Map Feb 24 2020 Id"

declare -A dataset_types=(
	["[Discontinued] Facebook Population (Administrative Regions) v1"]="population_adm"
	["[Discontinued] Facebook Population (Tile Level) v1"]="population_tile"
	["[Discontinued] Movement Between Administrative Regions v1"]="movement_adm"
	["[Discontinued] Movement Between Tiles v1"]="movement_tile"
	["[Discontinued] Colocation"]="colocation"
)

declare -A dataset_columns=(
	["population_adm"]="date_time,polygon_name,n_baseline,n_crisis,density_baseline,density_crisis"
	["population_tile"]="date_time,quadkey,n_baseline,n_crisis,density_baseline,density_crisis"
	["movement_adm"]="date_time,start_polygon_id,end_polygon_id,n_baseline,n_crisis"
	["movement_tile"]="date_time,start_quadkey,end_quadkey,n_crisis,n_baseline"
	["colocation"]="date_time,polygon1_id,polygon2_id,link_value"
)

declare -A createtable_strings=(
	["population_adm"]="date_time DATETIME,polygon_name VARCHAR,n_baseline REAL,n_crisis REAL,density_baseline REAL,density_crisis REAL"
	["population_tile"]="date_time DATETIME,quadkey VARCHAR,n_baseline REAL,n_crisis REAL,density_baseline REAL,density_crisis REAL"
	["movement_adm"]="date_time DATETIME,start_polygon_id VARCHAR,end_polygon_id VARCHAR,n_baseline REAL,n_crisis REAL"
	["movement_tile"]="date_time DATETIME,start_quadkey VARCHAR,end_quadkey VARCHAR,n_crisis REAL,n_baseline REAL"
	["colocation"]="date_time DATETIME,polygon1_id VARCHAR,polygon2_id VARCHAR,link_value REAL"
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

# Data Ingestion for Facebook Data
# --------------------------------

for dataset_type in "${!dataset_types[@]}"; do
	echo "[LOG] DATASET TYPE: ${dataset_type}"

	# Define the path of the folder the script will look into to find the zipfiles
	dataset_type_folder="${RAW_DATA_FOLDER}/${dataset_name}/${dataset_type}"

	if [ ! -d "$dataset_type_folder" ]; then
		echo "[WARNING] $dataset_type folder is not available in the filesystem"
		continue
	else
		echo "[LOG] Going to perform the data ingestion process for $dataset_type data"
	fi

	# Create table
	# ------------

	echo "[LOG] Creating database table..."
	duckdb "${DB_FILE}" -c "DROP TABLE IF EXISTS ${dataset_types[$dataset_type]}; CREATE TABLE ${dataset_types[$dataset_type]} (${createtable_strings[${dataset_types[$dataset_type]}]});"

	# For each subfolder (i.e. corresponding to a dataset type), scan throught the
	# datafiles, unpack and load each of the unpacked csv files in a temporary folder

	COUNTER=0

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
			continue
		fi

		if [[ $COUNTER -eq 0 ]]; then

			echo "[LOG] Creating reference table..."

			# Create reference tables
			# -----------------------

			first_file=$(ls -AU ${TMP_DATA_FOLDER} | head -1)

			if [[ "${dataset_types[$dataset_type]}" == "colocation" ]]; then
				duckdb "${DB_FILE}" -c "DROP TABLE IF EXISTS ref_adm; CREATE TABLE ref_adm (polygon_id VARCHAR, polygon_name VARCHAR, latitude REAL, longitude REAL);"
				mlr --csv filter '$country=="IT"' \
					then cut -o -f "polygon1_id,polygon1_name,latitude_1,longitude_1" \
					then head -n 1 -g polygon1_id ${TMP_DATA_FOLDER}/$first_file | \
					duckdb "${DB_FILE}" -c "COPY ref_adm FROM '/dev/stdin' (AUTO_DETECT TRUE);"
			elif [[ "${dataset_types[$dataset_type]}" == "population_tile" ]]; then
				duckdb "${DB_FILE}" -c "DROP TABLE IF EXISTS ref_tile; CREATE TABLE ref_tile (quadkey VARCHAR, latitude REAL, longitude REAL)"
				mlr --csv filter '$country=="IT"' \
					then cut -o -f "quadkey,lat,lon" \
					then head -n 1 -g quadkey ${TMP_DATA_FOLDER}/$first_file | \
					duckdb "${DB_FILE}" -c "COPY ref_tile FROM '/dev/stdin' (AUTO_DETECT TRUE);"
			else
				continue
			fi
		else
			continue
		fi

		let COUNTER=COUNTER+1

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
					duckdb "${DB_FILE}" -c "COPY ${dataset_types[$dataset_type]} FROM '/dev/stdin' (AUTO_DETECT TRUE);"
			else
				mlr --csv filter '$country == "IT" && !is_empty($n_crisis)' then \
					put '$date_time = $date_time . ":00"' then \
					cut -o -f "${dataset_columns[${dataset_types[$dataset_type]}]}" "${csvfile}" | \
					duckdb "${DB_FILE}" -c "COPY ${dataset_types[$dataset_type]} FROM '/dev/stdin' (AUTO_DETECT TRUE);"
			fi
		done
	done
done

# Data Ingestion for ISS Data
# ---------------------------

iss_data_folder=${RAW_DATA_FOLDER}/iss_data
iss_datatypes=("deceduti" "ricoveri" "positivi" "terapia_intensiva")

IFS="," read -r -a iss_provinces <<< $(cat ./iss_provinces.txt)
for datatype in ${iss_datatypes[@]};
do
	duckdb "${DB_FILE}" -c "DROP TABLE IF EXISTS iss_${datatype}; CREATE TABLE iss_${datatype} (province VARCHAR, date_time DATETIME, cases REAL);"
	for province in ${iss_provinces[@]};
	do
		mlr --csv filter '!is_empty($casi_media7gg)' \
			then put '$data = $data . " 00:00:00"' \
			then put '$province = "'"${province}"'"' \
			then cut -o -f "province,data,casi_media7gg" ${iss_data_folder}/iss_bydate_${province}_${datatype}.csv | \
				duckdb "${DB_FILE}" -c "COPY iss_${datatype} FROM '/dev/stdin' (AUTO_DETECT TRUE);"
	done
done

# Create province lookup table
duckdb "${DB_FILE}" -c "CREATE TABLE province_lookup AS SELECT FROM read_csv_auto(province_lookup.csv, header=True)"

# CLEAN UP
# ========
# Clean up the workspace

finish() {
  result=$?
  rm -Rf $TMP_DATA_FOLDER
  exit ${result}
}

trap finish EXIT ERR
