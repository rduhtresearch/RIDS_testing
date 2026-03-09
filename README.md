# Research Finance Tool

Standalone Shiny app for:

- processing an ICT workbook
- generating posting lines for a selected costing scenario
- building EDGE template sheets for export

This repo is intended to be shared with colleagues as a self-contained local run
package.

## What is included

- `app_test.R` - Shiny app entry point
- `config.R` - local path configuration
- `pipeline_fixed.r` - workbook cleaning and transformation pipeline
- `posting_test.r` - posting plan generation logic
- `template_build_main.r` - EDGE template generation
- `utils/add_study_arm.r` - helper used by the pipeline
- `data/finance_rules_AH.duckdb` - finance rules database
- `data/ict_local.duckdb` - local ICT cost database updated by the app

## Onboarding

### 1. Prerequisites

Each user needs:

- R installed locally
- permission to install R packages into their user library
- internet access on first run so missing CRAN packages can be installed
- a writable copy of this repo, because the app updates `data/ict_local.duckdb`

### 2. Download the project

Clone or download this repo to your machine and keep the folder structure intact.

Expected structure:

```text
app_test_repo/
  app_test.R
  config.R
  pipeline_fixed.r
  posting_test.r
  template_build_main.r
  utils/
  data/
    finance_rules_AH.duckdb
    ict_local.duckdb
```

### 3. Check configuration

Default behavior:

- if you keep the repo structure unchanged, no config edits should be needed
- the app resolves its own folder automatically
- the DuckDB files are expected in `data/`

If you need to point to a different data folder, edit `config.R`.

Current config behavior:

- `DATA_DIR` is the folder containing the DuckDB files
- `RULES_DB_PATH` points to `finance_rules_AH.duckdb`
- `ICT_DB_PATH` points to `ict_local.duckdb`

### 4. Start the app

From R, run:

```r
shiny::runApp("app_test.R")
```

If you are not already in the repo directory, use the full path instead:

```r
shiny::runApp("/path/to/app_test_repo/app_test.R")
```

### 5. First run behavior

On first run, `app_test.R` checks for required R packages and installs any that
are missing.

Packages currently bootstrapped automatically:

- `shiny`
- `DBI`
- `duckdb`
- `DT`
- `dplyr`
- `stringr`
- `openxlsx`
- `tidyr`
- `rlang`
- `purrr`
- `readr`

The first startup may take a few minutes, especially on a new machine.

## Simple User Guide

### Normal workflow

1. Open the app.
2. In the `Input` panel, upload an ICT workbook (`.xlsx`).
3. Choose the required `Costing Scenario`.
4. Click `Run Pipeline`.
5. Wait for the pipeline overlay to finish all 5 stages.
6. Review the `Pipeline Log` for any warnings or errors.
7. Review results in the two main tabs:
   - `Posting Lines`
   - `EDGE Templates`
8. Export results using the `Export .xlsx` button on the relevant tab.

### What the app produces

#### Posting Lines

This tab shows the generated posting rows for the selected scenario after:

- workbook processing
- finance rule application
- amount adjustment
- destination resolution
- EDGE key assignment

Use this tab when checking the detailed line-level output.

#### EDGE Templates

This tab shows the generated EDGE-ready template sheets.

Use this tab when preparing the final workbook for EDGE import or review.

### Pipeline stages shown in the app

When you click `Run Pipeline`, the app runs:

1. Processing workbook
2. Generating posting plan
3. Adjusting amounts
4. Assigning EDGE keys
5. Building EDGE templates

## Notes for Colleagues

- The app does not include authentication or audit controls. Outputs should be reviewed before operational use.
- The uploaded ICT workbook is processed in-memory, and the local ICT DuckDB is refreshed as part of the run.
- `data/ict_local.duckdb` is a working file and may change after each run.
- If you are using git, expect `data/ict_local.duckdb` to show as modified after processing a workbook.

## Troubleshooting

### The app does not start

Check:

- R is installed correctly
- you launched the app from the correct file
- you have internet access for first-run package installation

### Package install fails on first run

Common causes:

- no internet connection
- corporate proxy / firewall restrictions
- no permission to write to your user R library

If this happens, ask your local IT-supporting colleague to help with CRAN access
or package installation permissions.

### The scenario list appears but the run fails later

Check that these files exist in the configured data folder:

- `data/finance_rules_AH.duckdb`
- `data/ict_local.duckdb`

### The app runs but returns errors for a workbook

Likely causes:

- the uploaded file is not a valid ICT workbook
- required worksheet columns are missing
- the workbook format differs from the expected source template

In that case, review the `Pipeline Log` first.

## Quick Start

For most users:

1. Download the repo.
2. Open R in the repo folder.
3. Run `shiny::runApp("app_test.R")`.
4. Upload an ICT workbook.
5. Select a scenario.
6. Click `Run Pipeline`.
7. Export the results.
