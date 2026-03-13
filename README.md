# Research Finance Tool

## Before you start

You will need:

- `R` installed on your laptop
- internet access the first time you run the app
- the full project folder, including the `data` folder

If `R` is not already installed, ask your IT team.

## Download the project from GitHub

1. Open the project page in GitHub.
2. Click the green `Code` button.
3. Click `Download ZIP`.
4. Save the ZIP file to your laptop.

## Extract the ZIP file

1. Find the ZIP file in `Downloads`.
2. Right-click it.
3. Click `Extract All`.
4. Choose a simple folder location, for example `Documents`.
5. Open the extracted folder.

Important:

- keep all files together
- do not move `app_test.R` out of the folder
- do not remove the `data` folder

## Start the app

1. Open `R` or `RStudio`.
2. Open the extracted project folder.
3. Run this command:

```r
shiny::runApp("app_test.R")
```

If you are not already inside the project folder, use the full path instead:

```r
shiny::runApp("C:/Users/your-name/Documents/app_test_repo/app_test.R")
```

## First run

The first time the app starts:

- it may take a few minutes
- it may install missing R packages automatically
- you must be connected to the internet

After startup, the app will open in your web browser.

## How to use the app

1. Upload your ICT workbook (`.xlsx`).
2. Choose the costing scenario.
3. Click `Run Pipeline`.
4. Wait for the processing steps to finish.
5. Review the results.
6. Export the output Excel files if needed.

## If it does not open

Check:

- `R` is installed
- you extracted the ZIP file fully
- the `data` folder is still inside the project folder
- you are running the command from the correct folder
- you are connected to the internet on first run

If it still does not work, send the error message to the project owner or IT support.
