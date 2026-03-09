
# template_build_main.r
#
# Cost adjustment, EDGE key assignment, and EDGE template generation.
# Sourced by app.R — do not modify pipeline_fixed.r or posting_test.r.
#
# Entry points (called in order by app.R):
#   1. adjust_posting_lines(out)             -> posting_lines_adjusted
#   2. assign_edge_keys(posting_lines_adjusted) -> posting_lines_adjusted (with edge_key)
#   3. build_all_edge_templates(posting_lines_adjusted) -> named list of dataframes

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
})

# ── Constants ──────────────────────────────────────────────────────────────────

# Sheets adjusted per-activity (not per-visit)
ADJUSTMENT_SPECIAL <- c("Unscheduled Activities", "Setup & Closedown")

# All special sheets (excluded from main arm processing)
SPECIAL_SHEETS <- c("Unscheduled Activities", "Setup & Closedown", "Pharmacy")

# Canonical EDGE import column order
edge_cols <- c(
  "EDGE Project ID",
  "Template Name",
  "Template Level (Project | Participant | ProjectSite)",
  "Project Arm (Participant only)",
  "Project Site Name (ProjectSite only)",
  "Cost Item Description",
  "Analysis Code",
  "Cost Category",
  "Default Cost",
  "Currency",
  "Department",
  "Overhead Cost",
  "Time"
)

# ── Cost adjustment ────────────────────────────────────────────────────────────

# Core adjustment logic: scales posting lines so they sum to contract_price,
# with the rounding residual absorbed by the DIRECT line.
adjust_postings <- function(df, group_vars) {
  df %>%
    mutate(contract_price = round(contract_cost, 0)) %>%
    group_by(across(all_of(group_vars))) %>%
    mutate(
      base_sum        = sum(posting_amount, na.rm = TRUE),
      contract_price  = first(contract_price),
      multiplier      = if_else(base_sum == 0, NA_real_, contract_price / base_sum),
      adjusted_amount = if_else(base_sum == 0, 0, round(posting_amount * multiplier, 2))
    ) %>%
    mutate(
      residual        = round(contract_price - sum(adjusted_amount, na.rm = TRUE), 2),
      has_direct      = any(posting_line_type_id == "DIRECT"),
      is_residual_row = if_else(
        has_direct,
        posting_line_type_id == "DIRECT" & row_number() == min(which(posting_line_type_id == "DIRECT")),
        row_number() == 1L
      ),
      adjusted_amount = if_else(
        is_residual_row,
        round(adjusted_amount + residual, 2),
        adjusted_amount
      )
    ) %>%
    mutate(
      adjusted_sum_check = round(sum(adjusted_amount, na.rm = TRUE), 2),
      diff_check         = round(contract_price - adjusted_sum_check, 2)
    ) %>%
    select(-has_direct) %>%
    ungroup()
}

#' Adjust posting amounts to match contract price.
#'
#' Special sheets (UA, SC) are grouped per-activity.
#' All other sheets (including Pharmacy) are grouped per-visit.
adjust_posting_lines <- function(out) {
  bind_rows(
    # UA and SC: adjust per activity
    out %>%
      filter(sheet_name %in% ADJUSTMENT_SPECIAL) %>%
      adjust_postings(c("row_id", "Activity", "staff_group", "scenario_id")),
    
    # Everything else: adjust per visit
    out %>%
      filter(!sheet_name %in% ADJUSTMENT_SPECIAL) %>%
      mutate(adj_group = trimws(
        if_else(sheet_name == "Pharmacy", Study_Arm, sheet_name)
      )) %>%
      adjust_postings(c("adj_group", "Visit", "scenario_id")) %>%
      select(-adj_group)
  )
}

# ── EDGE key assignment ────────────────────────────────────────────────────────

#' Stamp each posting line with a unique EDGE-XXXX key.
#'
#' Special sheets: keyed by sheet_name + Activity + row_id + staff_group.
#' Main arms:      keyed by Study_Arm + Visit.
assign_edge_keys <- function(data) {
  
  special_keys <- data |>
    filter(sheet_name %in% SPECIAL_SHEETS) |>
    distinct(sheet_name, Activity, row_id, staff_group, Study_Arm) |>
    mutate(edge_key = paste0("EDGE-", str_pad(row_number(), width = 4, pad = "0")))
  
  main_keys <- data |>
    filter(!sheet_name %in% SPECIAL_SHEETS) |>
    distinct(Study_Arm, Visit) |>
    mutate(edge_key = paste0("EDGE-", str_pad(
      row_number() + nrow(special_keys), width = 4, pad = "0"
    )))
  
  data |>
    left_join(special_keys, by = c("sheet_name", "Activity", "row_id",
                                   "staff_group", "Study_Arm")) |>
    left_join(main_keys,    by = c("Study_Arm", "Visit")) |>
    mutate(edge_key = coalesce(edge_key.x, edge_key.y)) |>
    select(-edge_key.x, -edge_key.y)
}

# ── Template builders ──────────────────────────────────────────────────────────

#' Build an EDGE template for one special sheet (UA / SC / Pharmacy).
#' One row per activity, summed across posting line types.
build_edge_template <- function(data) {
  
  data |>
    summarise(
      total = sum(adjusted_amount, na.rm = TRUE),
      .by   = c(Study_Arm, sheet_name, Activity, row_id, staff_group, edge_key)
    ) |>
    mutate(
      Full_Visit_Name                                        = paste0("VISIT - ", str_replace_all(Activity, "\\.", " ")),
      `EDGE Project ID`                                      = NA,
      `Template Name`                                        = Study_Arm,
      `Template Level (Project | Participant | ProjectSite)` = "Participant",
      `Project Arm (Participant only)`                       = NA,
      `Project Site Name (ProjectSite only)`                 = NA,
      `Cost Item Description`                                = Full_Visit_Name,
      `Analysis Code`                                        = edge_key,
      `Cost Category`                                        = "Research Cost",
      `Default Cost`                                         = total,
      `Currency`                                             = "GBP",
      `Department`                                           = NA,
      `Overhead Cost`                                        = NA,
      `Time`                                                 = NA
    ) |>
    select(all_of(edge_cols))
}

#' Build an EDGE template for one main study arm.
#' One row per visit, summed across all activities (including Pharmacy).
#' Visit_Label comes directly from the posting data — no ict_table join needed.
build_edge_template_main <- function(data) {
  
  # Edge keys live on main arm rows (not Pharmacy rows)
  visit_keys <- data |>
    filter(sheet_name != "Pharmacy") |>
    distinct(Study_Arm, Visit, edge_key)
  
  data |>
    summarise(
      total = sum(adjusted_amount, na.rm = TRUE),
      .by   = c(study_name, Visit, Study_Arm, Visit_Label)
    ) |>
    left_join(visit_keys, by = c("Study_Arm", "Visit")) |>
    mutate(
      Full_Visit_Name                                        = paste0("VISIT - ", str_replace_all(Visit_Label, "\\.", " ")),
      `EDGE Project ID`                                      = NA,
      `Template Name`                                        = Study_Arm,
      `Template Level (Project | Participant | ProjectSite)` = "Participant",
      `Project Arm (Participant only)`                       = NA,
      `Project Site Name (ProjectSite only)`                 = NA,
      `Cost Item Description`                                = Full_Visit_Name,
      `Analysis Code`                                        = edge_key,
      `Cost Category`                                        = "Research Cost",
      `Default Cost`                                         = total,
      `Currency`                                             = "GBP",
      `Department`                                           = NA,
      `Overhead Cost`                                        = NA,
      `Time`                                                 = NA
    ) |>
    select(all_of(edge_cols))
}

# ── Entry point ────────────────────────────────────────────────────────────────

#' Build all EDGE templates from an adjusted + keyed posting plan.
#'
#' @param data  Output of adjust_posting_lines() |> assign_edge_keys().
#' @return Named list of dataframes — one per special sheet, one per study arm.
build_all_edge_templates <- function(data) {
  
  special_data  <- data |> filter(sheet_name %in% SPECIAL_SHEETS)
  main_data     <- data |> filter(!sheet_name %in% SPECIAL_SHEETS | sheet_name == "Pharmacy")
  
  special_names <- sort(unique(special_data$sheet_name))
  main_arms     <- sort(unique(main_data$Study_Arm))
  
  special_list <- special_data |>
    group_by(sheet_name) |>
    group_map(~ build_edge_template(.x), .keep = TRUE) |>
    setNames(special_names)
  
  main_list <- main_data |>
    group_by(Study_Arm) |>
    group_map(~ build_edge_template_main(.x), .keep = TRUE) |>
    setNames(main_arms)
  
  c(special_list, main_list)
}


#' 
#' # template_build_main.r
#' #
#' # EDGE template generation functions.
#' # Sourced by app.R — do not modify pipeline_fixed.r or posting_test.r.
#' #
#' # Entry point: build_all_edge_templates(posting_lines_adjusted)
#' # Returns:     named list of dataframes, one per EDGE template sheet.
#' 
#' suppressPackageStartupMessages({
#'   library(dplyr)
#'   library(stringr)
#'   library(DBI)
#'   library(duckdb)
#' })
#' 
#' # ── Constants ──────────────────────────────────────────────────────────────────
#' 
#' SPECIAL_SHEETS <- c("Unscheduled Activities", "Setup & Closedown", "Pharmacy")
#' 
#' # Canonical EDGE import column order
#' edge_cols <- c(
#'   "EDGE Project ID",
#'   "Template Name",
#'   "Template Level (Project | Participant | ProjectSite)",
#'   "Project Arm (Participant only)",
#'   "Project Site Name (ProjectSite only)",
#'   "Cost Item Description",
#'   "Analysis Code",
#'   "Cost Category",
#'   "Default Cost",
#'   "Currency",
#'   "Department",
#'   "Overhead Cost",
#'   "Time"
#' )
#' 
#' # ── ICT cost lookup (loaded once from DuckDB) ──────────────────────────────────
#' # Populated by load_ict_table(); used inside build_edge_template_main().
#' 
#' ict_table <- NULL
#' 
#' load_ict_table <- function(ict_db_path) {
#'   if (is.null(ict_db_path) || !file.exists(ict_db_path)) {
#'     message("load_ict_table(): DB not found or not supplied — Visit_Label join skipped.")
#'     ict_table <<- data.frame(
#'       Study_Arm    = character(),
#'       Visit_Number = character(),
#'       Visit_Label  = character(),
#'       stringsAsFactors = FALSE
#'     )
#'     return(invisible(NULL))
#'   }
#'   
#'   con <- dbConnect(duckdb::duckdb(), dbdir = ict_db_path, read_only = TRUE)
#'   on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
#'   
#'   if (!dbExistsTable(con, "ict_costing_tbl")) {
#'     message("load_ict_table(): ict_costing_tbl not found — Visit_Label join skipped.")
#'     ict_table <<- data.frame(
#'       Study_Arm    = character(),
#'       Visit_Number = character(),
#'       Visit_Label  = character(),
#'       stringsAsFactors = FALSE
#'     )
#'     return(invisible(NULL))
#'   }
#'   
#'   ict_table <<- dbGetQuery(con, "
#'     SELECT DISTINCT Study_Arm, Visit_Number, Visit_Label
#'     FROM ict_costing_tbl
#'     WHERE Visit_Label IS NOT NULL
#'   ")
#'   
#'   message("load_ict_table(): loaded ", nrow(ict_table), " visit-label rows.")
#'   invisible(ict_table)
#' }
#' 
#' # ── Amount adjustment ──────────────────────────────────────────────────────────
#' 
#' #' Scale posting amounts so they sum exactly to contract_cost per row_id.
#' #' Applies a penny-correction residual to the DIRECT line.
#' adjust_posting_lines <- function(out) {
#'   out %>%
#'     mutate(contract_price = round(contract_cost, 0)) %>%
#'     group_by(row_id, scenario_id) %>%
#'     mutate(
#'       base_sum        = sum(posting_amount, na.rm = TRUE),
#'       contract_price  = first(contract_price),
#'       multiplier      = if_else(base_sum == 0, NA_real_, contract_price / base_sum),
#'       adjusted_amount = if_else(base_sum == 0, 0, round(posting_amount * multiplier, 2))
#'     ) %>%
#'     mutate(
#'       residual        = round(contract_price - sum(adjusted_amount, na.rm = TRUE), 2),
#'       has_direct      = any(posting_line_type_id == "DIRECT"),
#'       is_residual_row = if_else(
#'         has_direct,
#'         posting_line_type_id == "DIRECT" & row_number() == min(which(posting_line_type_id == "DIRECT")),
#'         row_number() == 1L
#'       ),
#'       adjusted_amount = if_else(
#'         is_residual_row,
#'         round(adjusted_amount + residual, 2),
#'         adjusted_amount
#'       )
#'     ) %>%
#'     mutate(
#'       adjusted_sum_check = round(sum(adjusted_amount, na.rm = TRUE), 2),
#'       diff_check         = round(contract_price - adjusted_sum_check, 2)
#'     ) %>%
#'     select(-has_direct) %>%
#'     ungroup()
#' }
#' 
#' # ── Key assignment ─────────────────────────────────────────────────────────────
#' 
#' assign_edge_keys <- function(data) {
#'   
#'   special_keys <- data |>
#'     filter(sheet_name %in% SPECIAL_SHEETS) |>
#'     distinct(sheet_name, Activity, row_id, staff_group, Study_Arm) |>
#'     mutate(edge_key = paste0("EDGE-", str_pad(row_number(), width = 4, pad = "0")))
#'   
#'   main_keys <- data |>
#'     filter(!sheet_name %in% SPECIAL_SHEETS) |>
#'     distinct(Study_Arm, Visit) |>
#'     mutate(edge_key = paste0("EDGE-", str_pad(
#'       row_number() + nrow(special_keys), width = 4, pad = "0"
#'     )))
#'   
#'   data |>
#'     left_join(special_keys, by = c("sheet_name", "Activity", "row_id",
#'                                    "staff_group", "Study_Arm")) |>
#'     left_join(main_keys,    by = c("Study_Arm", "Visit")) |>
#'     mutate(edge_key = coalesce(edge_key.x, edge_key.y)) |>
#'     select(-edge_key.x, -edge_key.y)
#' }
#' 
#' # ── Template builders ──────────────────────────────────────────────────────────
#' 
#' #' Build an EDGE template for one special sheet (UA / SC / Pharmacy).
#' build_edge_template <- function(data) {
#'   
#'   data |>
#'     select(Study_Arm, sheet_name, Activity, row_id,
#'            staff_group, adjusted_amount, edge_key) |>
#'     summarise(
#'       total = sum(adjusted_amount, na.rm = TRUE),
#'       .by   = c(Study_Arm, sheet_name, Activity, row_id, staff_group, edge_key)
#'     ) |>
#'     mutate(
#'       Full_Visit_Name              = paste0("VISIT - ", str_replace_all(Activity, "\\.", " ")),
#'       `EDGE Project ID`            = NA_character_,
#'       `Template Name`              = Study_Arm,
#'       `Template Level (Project | Participant | ProjectSite)` = "Participant",
#'       `Project Arm (Participant only)`         = NA_character_,
#'       `Project Site Name (ProjectSite only)`   = NA_character_,
#'       `Cost Item Description`      = Full_Visit_Name,
#'       `Analysis Code`              = edge_key,
#'       `Cost Category`              = "Research Cost",
#'       `Default Cost`               = total,
#'       `Currency`                   = "GBP",
#'       `Department`                 = NA_character_,
#'       `Overhead Cost`              = NA_real_,
#'       `Time`                       = NA_character_
#'     ) |>
#'     select(all_of(edge_cols))
#' }
#' 
#' #' Build an EDGE template for one main study arm.
#' build_edge_template_main <- function(data) {
#'   
#'   summarised <- data |>
#'     select(Study_Arm, Visit, adjusted_amount, sheet_name,
#'            study_name, Visit_Label, edge_key) |>
#'     summarise(
#'       total      = sum(adjusted_amount, na.rm = TRUE),
#'       study_name = first(study_name),
#'       .by        = c(study_name, Visit, Study_Arm, edge_key)
#'     )
#'   
#'   # Join Visit_Label from ict_table if available
#'   if (!is.null(ict_table) && nrow(ict_table) > 0) {
#'     summarised <- summarised |>
#'       left_join(ict_table, by = c("Study_Arm" = "Study_Arm",
#'                                   "Visit"     = "Visit_Number"))
#'   } else {
#'     summarised <- summarised |>
#'       mutate(Visit_Label = Visit)
#'   }
#'   
#'   summarised |>
#'     mutate(
#'       Visit_Label   = if_else(is.na(Visit_Label), Visit, Visit_Label),
#'       Full_Visit_Name = paste0("VISIT - ", str_replace_all(Visit_Label, "\\.", " ")),
#'       `EDGE Project ID`            = NA_character_,
#'       `Template Name`              = Study_Arm,
#'       `Template Level (Project | Participant | ProjectSite)` = "Participant",
#'       `Project Arm (Participant only)`         = NA_character_,
#'       `Project Site Name (ProjectSite only)`   = NA_character_,
#'       `Cost Item Description`      = Full_Visit_Name,
#'       `Analysis Code`              = edge_key,
#'       `Cost Category`              = "Research Cost",
#'       `Default Cost`               = total,
#'       `Currency`                   = "GBP",
#'       `Department`                 = NA_character_,
#'       `Overhead Cost`              = NA_real_,
#'       `Time`                       = NA_character_
#'     ) |>
#'     select(all_of(edge_cols))
#' }
#' 
#' # ── Entry point ────────────────────────────────────────────────────────────────
#' 
#' #' Build all EDGE templates from an adjusted posting plan.
#' #'
#' #' @param data  Output of adjust_posting_lines() + assign_edge_keys().
#' #' @return Named list of dataframes — one per special sheet and one per study arm.
#' build_all_edge_templates <- function(data) {
#'   
#'   # Special sheets
#'   special_list <- data |>
#'     filter(sheet_name %in% SPECIAL_SHEETS) |>
#'     group_by(sheet_name) |>
#'     group_map(~ build_edge_template(.x), .keep = TRUE) |>
#'     setNames(
#'       data |>
#'         filter(sheet_name %in% SPECIAL_SHEETS) |>
#'         pull(sheet_name) |>
#'         unique()
#'     )
#'   
#'   # Main arms — exclude pseudo-arms that aren't real visit arms
#'   exclude_arms <- c("Unscheduled Activities", "SSP", "SC")
#'   
#'   main_arms <- data |>
#'     filter(!Study_Arm %in% exclude_arms) |>
#'     pull(Study_Arm) |>
#'     unique()
#'   
#'   main_list <- data |>
#'     filter(!Study_Arm %in% exclude_arms) |>
#'     group_by(Study_Arm) |>
#'     group_map(~ build_edge_template_main(.x), .keep = TRUE) |>
#'     setNames(main_arms)
#'   
#'   c(special_list, main_list)
#' }

# build_all_edge_templates <- function(data) {
#   
#   special_sheets <- c('Unscheduled Activities', 'Setup & Closedown', 'Pharmacy')
#   
#   special_list <- data |>
#     filter(sheet_name %in% special_sheets) |>
#     group_by(sheet_name) |>
#     group_map(~ build_edge_template(.x), .keep = TRUE) |>
#     setNames(special_sheets)
#   
#   main_arms <- data |>
#     filter(!Study_Arm %in% c('Unscheduled Activities', 'SSP', 'SC')) |>
#     pull(Study_Arm) |>
#     unique()
#   
#   main_list <- data |>
#     filter(!Study_Arm %in% c('Unscheduled Activities', 'SSP', 'SC')) |>
#     group_by(Study_Arm) |>
#     group_map(~ build_edge_template_main(.x), .keep = TRUE) |>
#     setNames(main_arms)
#   
#   c(special_list, main_list)
# }
# 
# build_edge_template <- function(data) {
#   
#   data |>
#     select(Study_Arm, sheet_name, Activity, row_id, staff_group, adjusted_amount, edge_key) |>
#     summarise(
#       total = sum(adjusted_amount, na.rm = TRUE),
#       .by = c(Study_Arm, sheet_name, Activity, row_id, staff_group, edge_key)
#     ) |>
#     mutate(
#       Full_Visit_Name = paste0("VISIT - ", str_replace_all(Activity, "\\.", " "))
#     ) |>
#     mutate(
#       `EDGE Project ID` = NA,
#       `Template Name` = Study_Arm,
#       `Template Level (Project | Participant | ProjectSite)` = "Participant",
#       `Project Arm (Participant only)` = NA,
#       `Project Site Name (ProjectSite only)` = NA,
#       `Cost Item Description` = Full_Visit_Name,
#       `Analysis Code` = edge_key,
#       `Cost Category` = 'Research Cost',
#       `Default Cost` = total,
#       `Currency` = "GBP",
#       `Department` = NA,
#       `Overhead Cost` = NA,
#       `Time` = NA
#     ) |>
#     select(all_of(edge_cols))
# }
# 
# build_edge_template_main <- function(data) {
#   
#   data |>
#     select(Study_Arm, Visit, adjusted_amount, sheet_name, study_name, Visit_Label, edge_key) |>
#     summarise(
#       total = sum(adjusted_amount, na.rm = TRUE),
#       .by = c(study_name, Visit, Study_Arm, edge_key)
#     ) |>
#     left_join(ict_table, by = c('Study_Arm' = 'Study_Arm', 'Visit' = 'Visit_Number')) |>
#     mutate(
#       Full_Visit_Name = paste0("VISIT - ", str_replace_all(Visit_Label, "\\.", " "))
#     ) |>
#     mutate(
#       `EDGE Project ID` = NA,
#       `Template Name` = Study_Arm,
#       `Template Level (Project | Participant | ProjectSite)` = "Participant",
#       `Project Arm (Participant only)` = NA,
#       `Project Site Name (ProjectSite only)` = NA,
#       `Cost Item Description` = Full_Visit_Name,
#       `Analysis Code` = edge_key,
#       `Cost Category` = 'Research Cost',
#       `Default Cost` = total,
#       `Currency` = "GBP",
#       `Department` = NA,
#       `Overhead Cost` = NA,
#       `Time` = NA
#     ) |>
#     select(all_of(edge_cols))
# }
