# AIACE Project - Data Integrator Toolkit

This repository contains a series of tools that are being used by the **AIACE project**
in the *data integration* process.

## Requirements

This toolkit makes mainly use of the following requirements:

- `bash` (shell for scripting)
- `unzip` (deal with `zip` compressed archives)
- Miller (CSV manipulation CLI toolkit)
- DuckDB (embedded SQL OLAP DBMS)

For example, on Ubuntu the first three requirements can be installed by using
the package manager:

```console
$ sudo apt install bash unzip miller
```

DuckDB has many APIs, in our case we are using the CLI interface. In order to
install the binary version visit the
[releases page](https://github.com/duckdb/duckdb/releases/) and download the
most appropriate version for your use case.

Our workflow was tested on Ubuntu 22.04 LTS, with version 0.5.1 of DuckDB.

## Pipeline

Let us begin by saying that the pipeline is tailored to the research needs of the
AIACE project and thus scripts and programs are specific to the file hierarchy
and the configuration of the machine that was used for the data analysis process.

Nonetheless, there is a well defined thought scheme behind the way the process
unfolds and here we will point out its main features.

TODO: expand Pipeline section

## Using Docker

TODO: docker image yet to be uploaded

### Pulling the image and running

The image can be pulled from the Github Containers Repository.

```console
$ docker pull ghcr.io/shoyip/aiace_data_integrator
```

Then it can be run as follows.

```console
$ docker run -v [host data folder]:/app/data --env-file .env -i -t ghcr.io/shoyip/aiace_data_integrator
```

where the *host data folder* has to be an absolute path pointing to the directory
of the host machine where we would like the data to be downloaded.

### Building and running the image

The image can be built by issuing the following command.

```console
$ docker build -t data_integrator
```

Once the image is built, the Bulk Downloader tool can be run as follows.

```console
$ docker run -v [host data folder]:/app/data --env-file .env -i -t data_integrator
```

where the *host data folder* has to be an absolute path pointing to the directory
of the host machine where we would like the data to be downloaded.

The `.env` file should contain the informations as pointed out previously.
`DOWNLOAD_FOLDER` should be set to `/app/data`.

## Impressum

Shoichi Yip // 2022

This tool contributes to the **AIACE project** (UniTrento).

- Principal Investigator: [prof. Luca Tubiana](https://sbp.physics.unitn.it/luca-tubiana/)
- Postdoctoral Student: [dr. Jules Morand](https://sbp.physics.unitn.it/jules-morand/)
- Research Assistant: Shoichi Yip
