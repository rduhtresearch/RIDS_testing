# ============================================================
# config.R  —  edit this file to match your machine
# ============================================================

# Folder containing finance_rules_AH.duckdb and ict_local.duckdb
if (!exists("APP_DIR", inherits = TRUE)) {
  APP_DIR <- normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

DATA_DIR <- file.path(APP_DIR, "data")

# Derived paths — no need to change these
RULES_DB_PATH <- file.path(DATA_DIR, "finance_rules_AH.duckdb")
ICT_DB_PATH   <- file.path(DATA_DIR, "ict_local.duckdb")
