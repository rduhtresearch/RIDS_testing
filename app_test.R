library(shiny)
library(DBI)
library(duckdb)
library(DT)
library(dplyr)
library(stringr)
library(openxlsx)

get_app_dir <- function() {
  frame_files <- vapply(sys.frames(), function(x) {
    if (!is.null(x$ofile)) normalizePath(x$ofile, winslash = "/", mustWork = FALSE) else NA_character_
  }, character(1))
  frame_files <- frame_files[!is.na(frame_files)]

  if (length(frame_files) > 0) {
    dirname(frame_files[[length(frame_files)]])
  } else {
    normalizePath(getwd(), winslash = "/", mustWork = FALSE)
  }
}

APP_DIR <- get_app_dir()

source(file.path(APP_DIR, "config.R"))
source(file.path(APP_DIR, "pipeline_fixed.r"))
source(file.path(APP_DIR, "posting_test.r"))
source(file.path(APP_DIR, "template_build_main.r"))

# ── NHS colour palette ─────────────────────────────────────────────────────────
C_BLUE        <- "#005EB8"
C_DARK_BLUE   <- "#003087"
C_PALE_BLUE   <- "#d9e8f4"
C_GREY        <- "#425563"
C_MID_GREY    <- "#768692"
C_LIGHT_GREY  <- "#f0f4f5"
C_WHITE       <- "#ffffff"
C_GREEN       <- "#009639"
C_DARK_GREEN  <- "#007a3d"
C_NOTICE_BG   <- "#e8f0f8"

# ── Helpers ────────────────────────────────────────────────────────────────────

get_scenarios <- function() {
  tryCatch({
    con <- dbConnect(duckdb::duckdb(), dbdir = RULES_DB_PATH, read_only = TRUE)
    on.exit(dbDisconnect(con, shutdown = TRUE))
    dbGetQuery(con, "SELECT DISTINCT scenario_id FROM dist_rules ORDER BY scenario_id;")$scenario_id
  }, error = function(e) LETTERS[1:8])
}

# adjust_posting_lines() and assign_edge_keys() live in template_build_main.r

# ── CSS (built with paste0 to avoid sprintf %% issues) ────────────────────────
nhs_css <- paste0("

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: 'Source Sans Pro', Arial, sans-serif;
    font-size: 14px;
    background: ", C_LIGHT_GREY, ";
    color: ", C_GREY, ";
    line-height: 1.5;
  }

  /* ── Header ── */
  .nhs-header {
    background: ", C_BLUE, ";
    border-bottom: 4px solid ", C_DARK_BLUE, ";
  }
  .nhs-header-inner {
    max-width: 1400px;
    margin: 0 auto;
    padding: 14px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .nhs-logo-block { display: flex; align-items: center; gap: 16px; }
  .nhs-lozenge {
    background: ", C_WHITE, ";
    color: ", C_BLUE, ";
    font-size: 22px;
    font-weight: 700;
    font-family: Arial, sans-serif;
    padding: 4px 10px;
    border-radius: 2px;
    line-height: 1;
    letter-spacing: -0.5px;
  }
  .nhs-app-title {
    color: ", C_WHITE, ";
    font-size: 17px;
    font-weight: 600;
  }
  .nhs-app-subtitle {
    color: rgba(255,255,255,0.6);
    font-size: 12px;
    margin-top: 2px;
  }
  .nhs-mvp-tag {
    display: inline-block;
    background: rgba(255,255,255,0.12);
    border: 1px solid rgba(255,255,255,0.25);
    color: rgba(255,255,255,0.85);
    font-size: 11px;
    font-weight: 700;
    padding: 3px 10px;
    border-radius: 2px;
    letter-spacing: 0.5px;
    text-transform: uppercase;
  }
  .nhs-version { color: rgba(255,255,255,0.4); font-size: 11px; margin-top: 4px; }

  /* ── Notice banner ── */
  .nhs-notice {
    background: ", C_NOTICE_BG, ";
    border-left: 4px solid ", C_BLUE, ";
    padding: 10px 24px;
  }
  .nhs-notice-inner {
    max-width: 1400px;
    margin: 0 auto;
    font-size: 13px;
    color: ", C_GREY, ";
  }
  .nhs-notice-inner strong { color: ", C_DARK_BLUE, "; font-weight: 700; }

  /* ── Page layout ── */
  .nhs-page {
    max-width: 1400px;
    margin: 0 auto;
    padding: 24px;
    display: flex;
    gap: 24px;
    align-items: flex-start;
  }

  /* ── Sidebar panels ── */
  .nhs-sidebar { width: 260px; flex-shrink: 0; }
  .nhs-panel {
    background: ", C_WHITE, ";
    border: 1px solid #d8dde0;
    margin-bottom: 16px;
  }
  .nhs-panel-header {
    background: ", C_LIGHT_GREY, ";
    padding: 9px 14px;
    font-size: 11px;
    font-weight: 700;
    color: ", C_GREY, ";
    text-transform: uppercase;
    letter-spacing: 0.6px;
    border-bottom: 1px solid #d8dde0;
  }
  .nhs-panel-body { padding: 16px 14px; }

  /* ── Form controls ── */
  .form-group { margin-bottom: 14px; }
  .form-group:last-child { margin-bottom: 0; }
  .control-label, label {
    font-size: 13px;
    font-weight: 600;
    color: ", C_GREY, ";
    display: block;
    margin-bottom: 4px;
  }
  .form-control, .selectize-input {
    border: 2px solid #adb5bd !important;
    border-radius: 0 !important;
    font-size: 13px !important;
    color: ", C_GREY, " !important;
    padding: 6px 10px !important;
    box-shadow: none !important;
    background: ", C_WHITE, " !important;
  }
  .form-control:focus, .selectize-input.focus { border-color: ", C_BLUE, " !important; }
  .btn-file {
    background: ", C_LIGHT_GREY, ";
    border: 2px solid #adb5bd;
    border-radius: 0;
    color: ", C_GREY, ";
    font-size: 13px;
    font-weight: 600;
    padding: 5px 12px;
  }
  .btn-file:hover { background: #e3edf7; border-color: ", C_BLUE, "; }

  /* ── Run button ── */
  #run {
    width: 100%;
    background: ", C_BLUE, ";
    border: none;
    border-bottom: 4px solid ", C_DARK_BLUE, ";
    color: ", C_WHITE, ";
    font-size: 14px;
    font-weight: 700;
    border-radius: 0;
    padding: 10px 0;
    cursor: pointer;
    transition: background 0.12s;
  }
  #run:hover  { background: ", C_DARK_BLUE, "; }
  #run:active { border-bottom-width: 2px; margin-top: 2px; }

  /* ── Log ── */
  .nhs-log {
    font-size: 11px;
    font-family: 'Courier New', monospace;
    background: #1d2d3e;
    color: #9ee7b5;
    padding: 10px 12px;
    min-height: 90px;
    max-height: 200px;
    overflow-y: auto;
    white-space: pre-wrap;
    line-height: 1.55;
    border: 1px solid #0a1929;
  }
  .nhs-log::-webkit-scrollbar       { width: 5px; }
  .nhs-log::-webkit-scrollbar-track { background: transparent; }
  .nhs-log::-webkit-scrollbar-thumb { background: #2e4a62; }

  /* ── Main area ── */
  .nhs-main { flex: 1; min-width: 0; }

  /* ── Tabs ── */
  .nhs-tabs {
    display: flex;
    border-bottom: 3px solid ", C_BLUE, ";
    margin-bottom: 20px;
  }
  .nhs-tab {
    padding: 9px 20px;
    font-size: 14px;
    font-weight: 600;
    color: ", C_GREY, ";
    background: ", C_LIGHT_GREY, ";
    border: 1px solid #d8dde0;
    border-bottom: none;
    cursor: pointer;
    margin-right: 4px;
    transition: background 0.1s, color 0.1s;
    user-select: none;
  }
  .nhs-tab:hover { background: #e3edf7; color: ", C_BLUE, "; }
  .nhs-tab.active {
    background: ", C_WHITE, ";
    color: ", C_BLUE, ";
    border-color: ", C_BLUE, ";
    border-bottom: 3px solid ", C_WHITE, ";
    margin-bottom: -3px;
    font-weight: 700;
  }

  /* ── Content header ── */
  .nhs-content-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 14px;
    padding-bottom: 12px;
    border-bottom: 1px solid #d8dde0;
  }
  .nhs-content-left { display: flex; align-items: center; gap: 10px; }
  .nhs-header-actions { display: flex; align-items: center; gap: 10px; }
  .nhs-content-title { font-size: 16px; font-weight: 700; color: ", C_DARK_BLUE, "; }
  .nhs-badge {
    background: ", C_PALE_BLUE, ";
    color: ", C_DARK_BLUE, ";
    font-size: 11px;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 2px;
  }
  .nhs-toggle-link {
    font-size: 12px;
    font-weight: 600;
    color: ", C_BLUE, ";
    cursor: pointer;
    text-decoration: underline;
    background: none;
    border: none;
    padding: 0;
  }
  .nhs-toggle-link:hover { color: ", C_DARK_BLUE, "; }

  /* ── Sheet selector ── */
  .sheet-row { margin-bottom: 14px; max-width: 320px; }
  .sheet-row .form-group { margin-bottom: 0; }

  /* ── Export button ── */
  #dl_edge, #dl_posting {
    background: ", C_GREEN, ";
    border: none;
    border-bottom: 3px solid ", C_DARK_GREEN, ";
    color: ", C_WHITE, ";
    font-size: 13px;
    font-weight: 700;
    padding: 7px 16px;
    border-radius: 0;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 7px;
    text-decoration: none;
  }
  #dl_edge:hover, #dl_posting:hover { background: ", C_DARK_GREEN, "; }

  /* ── Spinner overlay ── */
  #pipeline-overlay {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,48,135,0.5);
    z-index: 9999;
    align-items: center;
    justify-content: center;
  }
  #pipeline-overlay.active { display: flex; }
  .nhs-spinner-card {
    background: ", C_WHITE, ";
    border-top: 4px solid ", C_BLUE, ";
    padding: 28px 36px;
    width: 340px;
    box-shadow: 0 8px 32px rgba(0,0,0,0.2);
  }
  .nhs-spinner-title { font-size: 15px; font-weight: 700; color: ", C_DARK_BLUE, "; margin-bottom: 2px; }
  .nhs-spinner-sub   { font-size: 12px; color: ", C_MID_GREY, "; margin-bottom: 18px; }
  .nhs-spinner-ring {
    width: 36px; height: 36px;
    border: 3px solid ", C_PALE_BLUE, ";
    border-top-color: ", C_BLUE, ";
    border-radius: 50%;
    animation: nhs-spin 0.7s linear infinite;
    margin-bottom: 18px;
  }
  @keyframes nhs-spin { to { transform: rotate(360deg); } }
  .nhs-step {
    font-size: 13px;
    color: #adb5bd;
    padding: 6px 0;
    display: flex;
    align-items: center;
    gap: 10px;
    border-bottom: 1px solid ", C_LIGHT_GREY, ";
    transition: color 0.2s;
  }
  .nhs-step:last-child { border-bottom: none; }
  .nhs-step-num {
    width: 20px; height: 20px;
    border-radius: 50%;
    background: #d8dde0;
    color: ", C_GREY, ";
    font-size: 10px;
    font-weight: 700;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    transition: background 0.2s;
  }
  .nhs-step.active            { color: ", C_BLUE, "; font-weight: 600; }
  .nhs-step.active .nhs-step-num { background: ", C_BLUE, "; color: ", C_WHITE, "; }
  .nhs-step.done              { color: ", C_GREEN, "; }
  .nhs-step.done .nhs-step-num   { background: ", C_GREEN, "; color: ", C_WHITE, "; }

  /* ── DataTables ── */
  .dataTables_wrapper { font-size: 13px; }
  .dataTables_wrapper .dataTables_filter input,
  .dataTables_wrapper .dataTables_length select {
    border: 2px solid #adb5bd;
    border-radius: 0;
    padding: 4px 8px;
    font-size: 12px;
    color: ", C_GREY, ";
    outline: none;
  }
  .dataTables_wrapper .dataTables_filter input:focus,
  .dataTables_wrapper .dataTables_length select:focus { border-color: ", C_BLUE, "; }
  .dataTables_wrapper .dataTables_info { font-size: 12px; color: ", C_MID_GREY, "; }

  table.dataTable {
    border-collapse: collapse !important;
    width: 100% !important;
    border: 1px solid #d8dde0;
    font-size: 13px;
  }
  table.dataTable thead th {
    background: ", C_LIGHT_GREY, " !important;
    color: ", C_GREY, " !important;
    font-weight: 700 !important;
    font-size: 11px !important;
    text-transform: uppercase !important;
    letter-spacing: 0.5px !important;
    border-bottom: 2px solid #adb5bd !important;
    border-right: 1px solid #d8dde0 !important;
    padding: 9px 12px !important;
    white-space: nowrap;
  }
  table.dataTable thead th:last-child { border-right: none !important; }
  table.dataTable tbody td {
    padding: 8px 12px !important;
    border-bottom: 1px solid #eaecee !important;
    border-right: 1px solid #eaecee !important;
    color: ", C_GREY, ";
    vertical-align: middle;
  }
  table.dataTable tbody td:last-child { border-right: none !important; }
  table.dataTable tbody tr.odd  td { background: ", C_WHITE, " !important; }
  table.dataTable tbody tr.even td { background: #f7f9fa !important; }
  table.dataTable tbody tr:hover td {
    background: ", C_PALE_BLUE, " !important;
    color: ", C_DARK_BLUE, " !important;
  }
  .dataTables_paginate .paginate_button {
    border-radius: 0 !important;
    font-size: 12px !important;
    padding: 4px 10px !important;
    border: 1px solid #d8dde0 !important;
    color: ", C_GREY, " !important;
    background: ", C_WHITE, " !important;
    margin-left: 2px;
  }
  .dataTables_paginate .paginate_button:hover {
    background: ", C_PALE_BLUE, " !important;
    border-color: ", C_BLUE, " !important;
    color: ", C_BLUE, " !important;
  }
  .dataTables_paginate .paginate_button.current,
  .dataTables_paginate .paginate_button.current:hover {
    background: ", C_BLUE, " !important;
    border-color: ", C_BLUE, " !important;
    color: ", C_WHITE, " !important;
  }
  .dataTables_paginate .paginate_button.disabled { color: #adb5bd !important; }

  /* ── Page scroll layout ── */
  html, body {
    min-height: 100%;
    overflow-x: hidden;
    overflow-y: auto;
  }

  .nhs-page {
    max-width: 1360px;
    margin: 0 auto;
    padding: 16px 24px;
    display: flex;
    gap: 24px;
    align-items: flex-start;
    min-height: calc(100vh - 120px);
    overflow: visible;
    box-sizing: border-box;
  }

  .nhs-sidebar {
    width: 280px;
    flex-shrink: 0;
    height: auto;
    overflow: visible;
    scrollbar-width: thin;
  }
  .nhs-sidebar::-webkit-scrollbar       { width: 5px; }
  .nhs-sidebar::-webkit-scrollbar-thumb { background: #c8d0d6; border-radius: 4px; }

  .nhs-main {
    flex: 1;
    min-width: 0;
    height: auto;
    display: flex;
    flex-direction: column;
    overflow: visible;
  }

  /* Tabs + content header + sheet row never shrink */
  .nhs-tabs            { flex-shrink: 0; }
  .nhs-content-header  { flex-shrink: 0; }
  .sheet-row           { flex-shrink: 0; }

  /* Posting toggle show/hide wrapper needs flex context too */
  #posting-body {
    display: flex;
    flex-direction: column;
    min-height: auto;
    overflow: visible;
  }

  /* The table container scrolls — fills all remaining space */
  .table-scroll {
    flex: 1;
    overflow: auto;
    min-height: 0;
    border: 1px solid #d8dde0;
    border-radius: 6px;
    background: ", C_WHITE, ";
  }
  .table-scroll::-webkit-scrollbar       { width: 7px; height: 7px; }
  .table-scroll::-webkit-scrollbar-thumb { background: #c8d0d6; border-radius: 4px; }
  .table-scroll::-webkit-scrollbar-track { background: #f0f4f5; }

  /* Stop DT adding its own scroll wrapper */
  .table-scroll .dataTables_wrapper {
    overflow: visible !important;
    padding: 0 !important;
  }
  .table-scroll .dataTables_scroll      { overflow: visible !important; }
  .table-scroll .dataTables_scrollBody  { overflow: visible !important; }

  /* Controls bar above table (length + search) */
  .table-controls {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 12px;
    border-bottom: 1px solid #eef2f4;
    background: #fafbfc;
    flex-shrink: 0;
    border-radius: 6px 6px 0 0;
  }
  .table-controls .dataTables_length,
  .table-controls .dataTables_filter { margin: 0; }

  /* Footer */
  .nhs-footer {
    max-width: 1360px;
    margin: 0 auto;
    padding: 8px 24px;
    font-size: 11px;
    color: ", C_MID_GREY, ";
    border-top: 1px solid #d8dde0;
    background: ", C_WHITE, ";
  }

  /* ── Shiny overrides ── */
  .container-fluid { padding: 0 !important; }
  .shiny-title-panel { display: none; }
  .row { margin: 0 !important; }
")

# ── UI ─────────────────────────────────────────────────────────────────────────

ui <- fluidPage(
  title = "Research Finance",

  tags$head(
    tags$link(
      href = "https://fonts.googleapis.com/css2?family=Source+Sans+Pro:wght@400;600;700&display=swap",
      rel  = "stylesheet"
    ),
    tags$style(HTML(nhs_css))
  ),

  # Header
  div(class = "nhs-header",
    div(class = "nhs-header-inner",
      div(class = "nhs-logo-block",
        div(
          div(class = "nhs-app-title",    "Research Finance Tool"),
          div(class = "nhs-app-subtitle", "Commercial Activity Costing & EDGE Template Generation")
        )
      ),
      div(style = "text-align:right;",
        div(class = "nhs-mvp-tag", "Internal Preview — MVP"),
        div(class = "nhs-version", paste0("v0.1-alpha \u00b7 ", format(Sys.Date(), "%B %Y")))
      )
    )
  ),

  # Notice banner
  div(class = "nhs-notice",
    div(class = "nhs-notice-inner",
      tags$strong("Early access \u2014 for internal review only. "),
      paste0(
        "Core pipeline logic is validated. The full production system \u2014 including ",
        "authentication, audit logging, and access controls \u2014 is in development. ",
        "All outputs should be reviewed before operational use."
      )
    )
  ),

  # Spinner overlay
  div(id = "pipeline-overlay",
    div(class = "nhs-spinner-card",
      div(class = "nhs-spinner-title", "Running pipeline"),
      div(class = "nhs-spinner-sub",   "This may take a few moments\u2026"),
      div(class = "nhs-spinner-ring"),
      div(
        div(id = "step-1", class = "nhs-step", div(class = "nhs-step-num", "1"), span("Processing workbook")),
        div(id = "step-2", class = "nhs-step", div(class = "nhs-step-num", "2"), span("Generating posting plan")),
        div(id = "step-3", class = "nhs-step", div(class = "nhs-step-num", "3"), span("Adjusting amounts")),
        div(id = "step-4", class = "nhs-step", div(class = "nhs-step-num", "4"), span("Assigning EDGE keys")),
        div(id = "step-5", class = "nhs-step", div(class = "nhs-step-num", "5"), span("Building EDGE templates"))
      )
    )
  ),

  # JS
  tags$script(HTML("
    Shiny.addCustomMessageHandler('pipeline_step', function(data) {
      var overlay = document.getElementById('pipeline-overlay');
      if (data.step === 0) {
        overlay.classList.add('active');
        document.querySelectorAll('.nhs-step').forEach(function(el) {
          el.classList.remove('active', 'done');
        });
      } else if (data.step === -1) {
        overlay.classList.remove('active');
      } else {
        for (var i = 1; i < data.step; i++) {
          var p = document.getElementById('step-' + i);
          if (p) { p.classList.remove('active'); p.classList.add('done'); }
        }
        var c = document.getElementById('step-' + data.step);
        if (c) { c.classList.add('active'); c.classList.remove('done'); }
      }
    });

    window.toggleView = function(v) {
      Shiny.setInputValue('view_mode', v, { priority: 'event' });
      ['posting', 'edge'].forEach(function(b) {
        var el = document.getElementById('tab_' + b);
        if (el) el.classList.toggle('active', b === v);
      });
      setTimeout(function() {
        if (window.jQuery && $.fn.dataTable) {
          $.fn.dataTable.tables({ visible: true, api: true }).columns.adjust();
        }
        window.dispatchEvent(new Event('resize'));
      }, 50);
    };

    document.addEventListener('shiny:connected', function() {
      window.toggleView('posting');
    }, { once: true });
  ")),

  # Page
  div(class = "nhs-page",

    # Sidebar
    div(class = "nhs-sidebar",
      div(class = "nhs-panel",
        div(class = "nhs-panel-header", "Input"),
        div(class = "nhs-panel-body",
          fileInput("file", "ICT Workbook (.xlsx)", accept = ".xlsx",
                    buttonLabel = "Choose file"),
          selectInput("scenario", "Costing Scenario", choices = get_scenarios()),
          actionButton("run", label = tagList(icon("play"), " Run Pipeline"))
        )
      ),
      div(class = "nhs-panel",
        div(class = "nhs-panel-header", "Pipeline Log"),
        div(style = "padding:0;",
          div(class = "nhs-log", verbatimTextOutput("log"))
        )
      )
    ),

    # Main
    div(class = "nhs-main",

      div(class = "nhs-tabs",
        div(id = "tab_posting", class = "nhs-tab active", onclick = "toggleView('posting')", "Posting Lines"),
        div(id = "tab_edge",    class = "nhs-tab",        onclick = "toggleView('edge')",    "EDGE Templates")
      ),

      # Posting lines
      conditionalPanel("input.view_mode === 'posting' || !input.view_mode",
        div(class = "nhs-content-header",
          div(class = "nhs-content-left",
            span(class = "nhs-content-title", "Posting Lines"),
            uiOutput("posting_badge")
          ),
          div(class = "nhs-header-actions",
            downloadButton("dl_posting", label = "Export .xlsx"),
            tags$button(class = "nhs-toggle-link", id = "posting-toggle",
              onclick = "
                var b = document.getElementById('posting-body');
                var l = document.getElementById('posting-toggle');
                var hidden = b.style.display === 'none';
                b.style.display = hidden ? '' : 'none';
                l.textContent   = hidden ? 'Hide' : 'Show';
              ", "Hide")
          )
        ),
        div(id = "posting-body",
          div(class = "sheet-row", uiOutput("posting_sheet_select")),
          div(class = "table-scroll", DTOutput("posting_table"))
        )
      ),

      # EDGE templates
      conditionalPanel("input.view_mode === 'edge'",
        div(class = "nhs-content-header",
          div(class = "nhs-content-left",
            span(class = "nhs-content-title", "EDGE Templates"),
            uiOutput("edge_badge")
          ),
          div(class = "nhs-header-actions",
            downloadButton("dl_edge", label = "Export .xlsx")
          )
        ),
        div(class = "sheet-row", uiOutput("edge_sheet_select")),
        div(class = "table-scroll", DTOutput("edge_table"))
      )
    )
  ),

  # Footer
  div(class = "nhs-footer",
    paste0(
      "NHS Research Finance Tool \u00b7 Internal MVP \u00b7 Not for operational use \u00b7 ",
      "Production system in development \u00b7 ", format(Sys.Date(), "%Y")
    )
  )
)

# ── Server ─────────────────────────────────────────────────────────────────────

server <- function(input, output, session) {

  log_lines      <- reactiveVal(character(0))
  posting_adj    <- reactiveVal(NULL)
  edge_templates <- reactiveVal(NULL)

  step <- function(n) session$sendCustomMessage("pipeline_step", list(step = n))

  log <- function(...) {
    msg <- paste0("[", format(Sys.time(), "%H:%M:%S"), "] ", ...)
    log_lines(c(log_lines(), msg))
  }

  output$log <- renderText(paste(log_lines(), collapse = "\n"))

  # Run pipeline
  observeEvent(input$run, {
    req(input$file)
    log_lines(character(0))
    posting_adj(NULL)
    edge_templates(NULL)
    step(0)

    withCallingHandlers(
      tryCatch({
        log("Starting pipeline...")

        step(1)
        log("Stage A/B: processing workbook...")
        processed <- process_workbook(
          input_path = input$file$datapath,
          db_dir     = DATA_DIR
        )

        step(2)
        log("Generating posting plan (scenario ", input$scenario, ")...")
        out <- generate_posting_plan(
          ict           = processed,
          rules_db_path = RULES_DB_PATH,
          scenario_id   = input$scenario,
          ict_db_path   = ICT_DB_PATH
        )

        step(3)
        log("Adjusting posting amounts...")
        adj <- adjust_posting_lines(out)

        step(4)
        log("Assigning EDGE keys...")
        adj <- assign_edge_keys(adj)
        posting_adj(adj)

        step(5)
        log("Building EDGE templates...")
        templates <- build_all_edge_templates(adj)
        if (length(templates) == 0) stop("build_all_edge_templates() returned an empty list — check Study_Arm and sheet_name values.")
        edge_templates(templates)

        step(-1)
        log("Done - ", nrow(adj), " posting lines, ", length(templates), " EDGE templates.")

      }, error = function(e) {
        step(-1)
        log("ERROR: ", conditionMessage(e))
      }),
      message = function(m) {
        log(trimws(conditionMessage(m)))
        invokeRestart("muffleMessage")
      }
    )
  })

  # Posting lines
  posting_sheets <- reactive({
    req(posting_adj())
    sort(unique(posting_adj()$sheet_name))
  })

  output$posting_badge <- renderUI({
    req(posting_adj())
    span(class = "nhs-badge", format(nrow(posting_adj()), big.mark = ","), " rows")
  })

  output$posting_sheet_select <- renderUI({
    req(posting_sheets())
    selectInput("posting_sheet", "Sheet", choices = posting_sheets())
  })

  output$posting_table <- renderDT({
    req(posting_adj(), input$posting_sheet)
    df <- posting_adj() %>% filter(sheet_name == input$posting_sheet)

    money_cols <- intersect(
      c("posting_amount", "adjusted_amount", "contract_price",
        "contract_cost", "base_sum", "activity_cost_num"),
      names(df)
    )

    dt <- datatable(df,
      options  = list(
        pageLength = -1,
        dom        = "t",
        scrollX    = FALSE,
        autoWidth  = FALSE,
        ordering   = TRUE
      ),
      rownames = FALSE,
      class    = "stripe hover"
    )

    if (length(money_cols) > 0)
      dt <- formatCurrency(dt, columns = money_cols, currency = "\u00a3", digits = 2)

    num_cols <- setdiff(names(df)[sapply(df, is.numeric)], money_cols)
    if (length(num_cols) > 0)
      dt <- formatRound(dt, columns = num_cols, digits = 4)

    dt
  })

  # EDGE templates
  output$edge_badge <- renderUI({
    req(edge_templates())
    span(class = "nhs-badge", length(edge_templates()), " templates")
  })

  output$edge_sheet_select <- renderUI({
    req(edge_templates())
    template_names <- names(edge_templates())
    req(length(template_names) > 0)

    selected_template <- isolate(input$edge_sheet)
    if (is.null(selected_template) || !selected_template %in% template_names) {
      selected_template <- template_names[[1]]
    }

    selectInput("edge_sheet", "Template",
      choices  = template_names,
      selected = selected_template
    )
  })

  output$edge_table <- renderDT({
    req(edge_templates())

    template_names <- names(edge_templates())
    req(length(template_names) > 0)

    selected_template <- input$edge_sheet
    if (is.null(selected_template) || !selected_template %in% template_names) {
      selected_template <- template_names[[1]]
    }

    df <- edge_templates()[[selected_template]]
    req(!is.null(df))

    money_cols <- intersect(c("Default Cost", "Overhead Cost", "total"), names(df))

    dt <- datatable(df,
      options  = list(
        pageLength = -1,
        dom        = "t",
        scrollX    = FALSE,
        autoWidth  = FALSE,
        ordering   = TRUE
      ),
      rownames = FALSE,
      class    = "stripe hover"
    )

    if (length(money_cols) > 0)
      dt <- formatCurrency(dt, columns = money_cols, currency = "\u00a3", digits = 2)

    dt
  })

  outputOptions(output, "edge_badge", suspendWhenHidden = FALSE)
  outputOptions(output, "edge_sheet_select", suspendWhenHidden = FALSE)
  outputOptions(output, "edge_table", suspendWhenHidden = FALSE)

  # Export
  output$dl_posting <- downloadHandler(
    filename = function() {
      paste0("Posting_lines_scenario_", input$scenario, "_",
             format(Sys.Date(), "%Y%m%d"), ".xlsx")
    },
    content = function(file) {
      req(posting_adj())
      wb <- createWorkbook()

      for (nm in posting_sheets()) {
        safe_nm <- substr(gsub("[\\[\\]\\*\\?:/\\\\]", "_", nm), 1, 31)
        addWorksheet(wb, safe_nm)
        writeData(wb, safe_nm, posting_adj() %>% filter(sheet_name == nm))
      }

      saveWorkbook(wb, file, overwrite = TRUE)
    }
  )

  output$dl_edge <- downloadHandler(
    filename = function() {
      paste0("EDGE_templates_scenario_", input$scenario, "_",
             format(Sys.Date(), "%Y%m%d"), ".xlsx")
    },
    content = function(file) {
      req(edge_templates())
      wb <- createWorkbook()
      for (nm in names(edge_templates())) {
        safe_nm <- substr(gsub("[\\[\\]\\*\\?:/\\\\]", "_", nm), 1, 31)
        addWorksheet(wb, safe_nm)
        writeData(wb, safe_nm, edge_templates()[[nm]])
      }
      saveWorkbook(wb, file, overwrite = TRUE)
    }
  )
}

shinyApp(ui, server)



#' library(shiny)
#' library(DBI)
#' library(duckdb)
#' library(DT)
#' library(dplyr)
#' library(stringr)
#' library(openxlsx)
#' 
#' source("config.R")
#' setwd("/Users/tategraham/Documents/NHS/research_finance_tool/R/test_files")
#' source("pipeline_fixed.r")
#' source("posting_test.r")
#' source("template_build_main.r")
#' 
#' # ── NHS colour palette ─────────────────────────────────────────────────────────
#' C_BLUE        <- "#005EB8"
#' C_DARK_BLUE   <- "#003087"
#' C_PALE_BLUE   <- "#d9e8f4"
#' C_GREY        <- "#425563"
#' C_MID_GREY    <- "#768692"
#' C_LIGHT_GREY  <- "#f0f4f5"
#' C_WHITE       <- "#ffffff"
#' C_GREEN       <- "#009639"
#' C_DARK_GREEN  <- "#007a3d"
#' C_NOTICE_BG   <- "#e8f0f8"
#' 
#' # ── Helpers ────────────────────────────────────────────────────────────────────
#' 
#' get_scenarios <- function() {
#'   tryCatch({
#'     con <- dbConnect(duckdb::duckdb(), dbdir = RULES_DB_PATH, read_only = TRUE)
#'     on.exit(dbDisconnect(con, shutdown = TRUE))
#'     dbGetQuery(con, "SELECT DISTINCT scenario_id FROM dist_rules ORDER BY scenario_id;")$scenario_id
#'   }, error = function(e) LETTERS[1:8])
#' }
#' 
#' # adjust_posting_lines() and assign_edge_keys() live in template_build_main.r
#' 
#' # ── CSS (built with paste0 to avoid sprintf %% issues) ────────────────────────
#' nhs_css <- paste0("
#' 
#'   *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
#' 
#'   body {
#'     font-family: 'Source Sans Pro', Arial, sans-serif;
#'     font-size: 14px;
#'     background: ", C_LIGHT_GREY, ";
#'     color: ", C_GREY, ";
#'     line-height: 1.5;
#'   }
#' 
#'   /* ── Header ── */
#'   .nhs-header {
#'     background: ", C_BLUE, ";
#'     border-bottom: 4px solid ", C_DARK_BLUE, ";
#'   }
#'   .nhs-header-inner {
#'     max-width: 1400px;
#'     margin: 0 auto;
#'     padding: 14px 24px;
#'     display: flex;
#'     align-items: center;
#'     justify-content: space-between;
#'   }
#'   .nhs-logo-block { display: flex; align-items: center; gap: 16px; }
#'   .nhs-lozenge {
#'     background: ", C_WHITE, ";
#'     color: ", C_BLUE, ";
#'     font-size: 22px;
#'     font-weight: 700;
#'     font-family: Arial, sans-serif;
#'     padding: 4px 10px;
#'     border-radius: 2px;
#'     line-height: 1;
#'     letter-spacing: -0.5px;
#'   }
#'   .nhs-app-title {
#'     color: ", C_WHITE, ";
#'     font-size: 17px;
#'     font-weight: 600;
#'     border-left: 1px solid rgba(255,255,255,0.3);
#'     padding-left: 16px;
#'   }
#'   .nhs-app-subtitle {
#'     color: rgba(255,255,255,0.6);
#'     font-size: 12px;
#'     margin-top: 2px;
#'   }
#'   .nhs-mvp-tag {
#'     display: inline-block;
#'     background: rgba(255,255,255,0.12);
#'     border: 1px solid rgba(255,255,255,0.25);
#'     color: rgba(255,255,255,0.85);
#'     font-size: 11px;
#'     font-weight: 700;
#'     padding: 3px 10px;
#'     border-radius: 2px;
#'     letter-spacing: 0.5px;
#'     text-transform: uppercase;
#'   }
#'   .nhs-version { color: rgba(255,255,255,0.4); font-size: 11px; margin-top: 4px; }
#' 
#'   /* ── Notice banner ── */
#'   .nhs-notice {
#'     background: ", C_NOTICE_BG, ";
#'     border-left: 4px solid ", C_BLUE, ";
#'     padding: 10px 24px;
#'   }
#'   .nhs-notice-inner {
#'     max-width: 1400px;
#'     margin: 0 auto;
#'     font-size: 13px;
#'     color: ", C_GREY, ";
#'   }
#'   .nhs-notice-inner strong { color: ", C_DARK_BLUE, "; font-weight: 700; }
#' 
#'   /* ── Page layout ── */
#'   .nhs-page {
#'     max-width: 1400px;
#'     margin: 0 auto;
#'     padding: 24px;
#'     display: flex;
#'     gap: 24px;
#'     align-items: flex-start;
#'   }
#' 
#'   /* ── Sidebar panels ── */
#'   .nhs-sidebar { width: 260px; flex-shrink: 0; }
#'   .nhs-panel {
#'     background: ", C_WHITE, ";
#'     border: 1px solid #d8dde0;
#'     margin-bottom: 16px;
#'   }
#'   .nhs-panel-header {
#'     background: ", C_LIGHT_GREY, ";
#'     padding: 9px 14px;
#'     font-size: 11px;
#'     font-weight: 700;
#'     color: ", C_GREY, ";
#'     text-transform: uppercase;
#'     letter-spacing: 0.6px;
#'     border-bottom: 1px solid #d8dde0;
#'   }
#'   .nhs-panel-body { padding: 16px 14px; }
#' 
#'   /* ── Form controls ── */
#'   .form-group { margin-bottom: 14px; }
#'   .form-group:last-child { margin-bottom: 0; }
#'   .control-label, label {
#'     font-size: 13px;
#'     font-weight: 600;
#'     color: ", C_GREY, ";
#'     display: block;
#'     margin-bottom: 4px;
#'   }
#'   .form-control, .selectize-input {
#'     border: 2px solid #adb5bd !important;
#'     border-radius: 0 !important;
#'     font-size: 13px !important;
#'     color: ", C_GREY, " !important;
#'     padding: 6px 10px !important;
#'     box-shadow: none !important;
#'     background: ", C_WHITE, " !important;
#'   }
#'   .form-control:focus, .selectize-input.focus { border-color: ", C_BLUE, " !important; }
#'   .btn-file {
#'     background: ", C_LIGHT_GREY, ";
#'     border: 2px solid #adb5bd;
#'     border-radius: 0;
#'     color: ", C_GREY, ";
#'     font-size: 13px;
#'     font-weight: 600;
#'     padding: 5px 12px;
#'   }
#'   .btn-file:hover { background: #e3edf7; border-color: ", C_BLUE, "; }
#' 
#'   /* ── Run button ── */
#'   #run {
#'     width: 100%;
#'     background: ", C_BLUE, ";
#'     border: none;
#'     border-bottom: 4px solid ", C_DARK_BLUE, ";
#'     color: ", C_WHITE, ";
#'     font-size: 14px;
#'     font-weight: 700;
#'     border-radius: 0;
#'     padding: 10px 0;
#'     cursor: pointer;
#'     transition: background 0.12s;
#'   }
#'   #run:hover  { background: ", C_DARK_BLUE, "; }
#'   #run:active { border-bottom-width: 2px; margin-top: 2px; }
#' 
#'   /* ── Log ── */
#'   .nhs-log {
#'     font-size: 11px;
#'     font-family: 'Courier New', monospace;
#'     background: #1d2d3e;
#'     color: #9ee7b5;
#'     padding: 10px 12px;
#'     min-height: 90px;
#'     max-height: 200px;
#'     overflow-y: auto;
#'     white-space: pre-wrap;
#'     line-height: 1.55;
#'     border: 1px solid #0a1929;
#'   }
#'   .nhs-log::-webkit-scrollbar       { width: 5px; }
#'   .nhs-log::-webkit-scrollbar-track { background: transparent; }
#'   .nhs-log::-webkit-scrollbar-thumb { background: #2e4a62; }
#' 
#'   /* ── Main area ── */
#'   .nhs-main { flex: 1; min-width: 0; }
#' 
#'   /* ── Tabs ── */
#'   .nhs-tabs {
#'     display: flex;
#'     border-bottom: 3px solid ", C_BLUE, ";
#'     margin-bottom: 20px;
#'   }
#'   .nhs-tab {
#'     padding: 9px 20px;
#'     font-size: 14px;
#'     font-weight: 600;
#'     color: ", C_GREY, ";
#'     background: ", C_LIGHT_GREY, ";
#'     border: 1px solid #d8dde0;
#'     border-bottom: none;
#'     cursor: pointer;
#'     margin-right: 4px;
#'     transition: background 0.1s, color 0.1s;
#'     user-select: none;
#'   }
#'   .nhs-tab:hover { background: #e3edf7; color: ", C_BLUE, "; }
#'   .nhs-tab.active {
#'     background: ", C_WHITE, ";
#'     color: ", C_BLUE, ";
#'     border-color: ", C_BLUE, ";
#'     border-bottom: 3px solid ", C_WHITE, ";
#'     margin-bottom: -3px;
#'     font-weight: 700;
#'   }
#' 
#'   /* ── Content header ── */
#'   .nhs-content-header {
#'     display: flex;
#'     align-items: center;
#'     justify-content: space-between;
#'     margin-bottom: 14px;
#'     padding-bottom: 12px;
#'     border-bottom: 1px solid #d8dde0;
#'   }
#'   .nhs-content-left { display: flex; align-items: center; gap: 10px; }
#'   .nhs-content-title { font-size: 16px; font-weight: 700; color: ", C_DARK_BLUE, "; }
#'   .nhs-badge {
#'     background: ", C_PALE_BLUE, ";
#'     color: ", C_DARK_BLUE, ";
#'     font-size: 11px;
#'     font-weight: 700;
#'     padding: 2px 8px;
#'     border-radius: 2px;
#'   }
#'   .nhs-toggle-link {
#'     font-size: 12px;
#'     font-weight: 600;
#'     color: ", C_BLUE, ";
#'     cursor: pointer;
#'     text-decoration: underline;
#'     background: none;
#'     border: none;
#'     padding: 0;
#'   }
#'   .nhs-toggle-link:hover { color: ", C_DARK_BLUE, "; }
#' 
#'   /* ── Sheet selector ── */
#'   .sheet-row { margin-bottom: 14px; max-width: 320px; }
#'   .sheet-row .form-group { margin-bottom: 0; }
#' 
#'   /* ── Export button ── */
#'   #dl_edge {
#'     background: ", C_GREEN, ";
#'     border: none;
#'     border-bottom: 3px solid ", C_DARK_GREEN, ";
#'     color: ", C_WHITE, ";
#'     font-size: 13px;
#'     font-weight: 700;
#'     padding: 7px 16px;
#'     border-radius: 0;
#'     cursor: pointer;
#'     display: inline-flex;
#'     align-items: center;
#'     gap: 7px;
#'     text-decoration: none;
#'   }
#'   #dl_edge:hover { background: ", C_DARK_GREEN, "; }
#' 
#'   /* ── Spinner overlay ── */
#'   #pipeline-overlay {
#'     display: none;
#'     position: fixed;
#'     inset: 0;
#'     background: rgba(0,48,135,0.5);
#'     z-index: 9999;
#'     align-items: center;
#'     justify-content: center;
#'   }
#'   #pipeline-overlay.active { display: flex; }
#'   .nhs-spinner-card {
#'     background: ", C_WHITE, ";
#'     border-top: 4px solid ", C_BLUE, ";
#'     padding: 28px 36px;
#'     width: 340px;
#'     box-shadow: 0 8px 32px rgba(0,0,0,0.2);
#'   }
#'   .nhs-spinner-title { font-size: 15px; font-weight: 700; color: ", C_DARK_BLUE, "; margin-bottom: 2px; }
#'   .nhs-spinner-sub   { font-size: 12px; color: ", C_MID_GREY, "; margin-bottom: 18px; }
#'   .nhs-spinner-ring {
#'     width: 36px; height: 36px;
#'     border: 3px solid ", C_PALE_BLUE, ";
#'     border-top-color: ", C_BLUE, ";
#'     border-radius: 50%;
#'     animation: nhs-spin 0.7s linear infinite;
#'     margin-bottom: 18px;
#'   }
#'   @keyframes nhs-spin { to { transform: rotate(360deg); } }
#'   .nhs-step {
#'     font-size: 13px;
#'     color: #adb5bd;
#'     padding: 6px 0;
#'     display: flex;
#'     align-items: center;
#'     gap: 10px;
#'     border-bottom: 1px solid ", C_LIGHT_GREY, ";
#'     transition: color 0.2s;
#'   }
#'   .nhs-step:last-child { border-bottom: none; }
#'   .nhs-step-num {
#'     width: 20px; height: 20px;
#'     border-radius: 50%;
#'     background: #d8dde0;
#'     color: ", C_GREY, ";
#'     font-size: 10px;
#'     font-weight: 700;
#'     display: flex;
#'     align-items: center;
#'     justify-content: center;
#'     flex-shrink: 0;
#'     transition: background 0.2s;
#'   }
#'   .nhs-step.active            { color: ", C_BLUE, "; font-weight: 600; }
#'   .nhs-step.active .nhs-step-num { background: ", C_BLUE, "; color: ", C_WHITE, "; }
#'   .nhs-step.done              { color: ", C_GREEN, "; }
#'   .nhs-step.done .nhs-step-num   { background: ", C_GREEN, "; color: ", C_WHITE, "; }
#' 
#'   /* ── DataTables ── */
#'   .dataTables_wrapper { font-size: 13px; }
#'   .dataTables_wrapper .dataTables_filter input,
#'   .dataTables_wrapper .dataTables_length select {
#'     border: 2px solid #adb5bd;
#'     border-radius: 0;
#'     padding: 4px 8px;
#'     font-size: 12px;
#'     color: ", C_GREY, ";
#'     outline: none;
#'   }
#'   .dataTables_wrapper .dataTables_filter input:focus,
#'   .dataTables_wrapper .dataTables_length select:focus { border-color: ", C_BLUE, "; }
#'   .dataTables_wrapper .dataTables_info { font-size: 12px; color: ", C_MID_GREY, "; }
#' 
#'   table.dataTable {
#'     border-collapse: collapse !important;
#'     width: 100% !important;
#'     border: 1px solid #d8dde0;
#'     font-size: 13px;
#'   }
#'   table.dataTable thead th {
#'     background: ", C_LIGHT_GREY, " !important;
#'     color: ", C_GREY, " !important;
#'     font-weight: 700 !important;
#'     font-size: 11px !important;
#'     text-transform: uppercase !important;
#'     letter-spacing: 0.5px !important;
#'     border-bottom: 2px solid #adb5bd !important;
#'     border-right: 1px solid #d8dde0 !important;
#'     padding: 9px 12px !important;
#'     white-space: nowrap;
#'   }
#'   table.dataTable thead th:last-child { border-right: none !important; }
#'   table.dataTable tbody td {
#'     padding: 8px 12px !important;
#'     border-bottom: 1px solid #eaecee !important;
#'     border-right: 1px solid #eaecee !important;
#'     color: ", C_GREY, ";
#'     vertical-align: middle;
#'   }
#'   table.dataTable tbody td:last-child { border-right: none !important; }
#'   table.dataTable tbody tr.odd  td { background: ", C_WHITE, " !important; }
#'   table.dataTable tbody tr.even td { background: #f7f9fa !important; }
#'   table.dataTable tbody tr:hover td {
#'     background: ", C_PALE_BLUE, " !important;
#'     color: ", C_DARK_BLUE, " !important;
#'   }
#'   .dataTables_paginate .paginate_button {
#'     border-radius: 0 !important;
#'     font-size: 12px !important;
#'     padding: 4px 10px !important;
#'     border: 1px solid #d8dde0 !important;
#'     color: ", C_GREY, " !important;
#'     background: ", C_WHITE, " !important;
#'     margin-left: 2px;
#'   }
#'   .dataTables_paginate .paginate_button:hover {
#'     background: ", C_PALE_BLUE, " !important;
#'     border-color: ", C_BLUE, " !important;
#'     color: ", C_BLUE, " !important;
#'   }
#'   .dataTables_paginate .paginate_button.current,
#'   .dataTables_paginate .paginate_button.current:hover {
#'     background: ", C_BLUE, " !important;
#'     border-color: ", C_BLUE, " !important;
#'     color: ", C_WHITE, " !important;
#'   }
#'   .dataTables_paginate .paginate_button.disabled { color: #adb5bd !important; }
#' 
#'   /* ── Footer ── */
#'   .nhs-footer {
#'     max-width: 1400px;
#'     margin: 8px auto 0;
#'     padding: 14px 24px;
#'     border-top: 1px solid #d8dde0;
#'     font-size: 11px;
#'     color: ", C_MID_GREY, ";
#'   }
#' 
#'   /* ── Shiny overrides ── */
#'   .container-fluid { padding: 0 !important; }
#'   .shiny-title-panel { display: none; }
#'   .row { margin: 0 !important; }
#' ")
#' 
#' # ── UI ─────────────────────────────────────────────────────────────────────────
#' 
#' ui <- fluidPage(
#'   title = "Research Finance — NHS",
#'   
#'   tags$head(
#'     tags$link(
#'       href = "https://fonts.googleapis.com/css2?family=Source+Sans+Pro:wght@400;600;700&display=swap",
#'       rel  = "stylesheet"
#'     ),
#'     tags$style(HTML(nhs_css))
#'   ),
#'   
#'   # Header
#'   div(class = "nhs-header",
#'       div(class = "nhs-header-inner",
#'           div(class = "nhs-logo-block",
#'               div(class = "nhs-lozenge", "NHS"),
#'               div(
#'                 div(class = "nhs-app-title",    "Research Finance Tool"),
#'                 div(class = "nhs-app-subtitle", "Commercial Activity Costing & EDGE Template Generation")
#'               )
#'           ),
#'           div(style = "text-align:right;",
#'               div(class = "nhs-mvp-tag", "Internal Preview — MVP"),
#'               div(class = "nhs-version", paste0("v0.1-alpha \u00b7 ", format(Sys.Date(), "%B %Y")))
#'           )
#'       )
#'   ),
#'   
#'   # Notice banner
#'   div(class = "nhs-notice",
#'       div(class = "nhs-notice-inner",
#'           tags$strong("Early access \u2014 for internal review only. "),
#'           paste0(
#'             "Core pipeline logic is validated. The full production system \u2014 including ",
#'             "authentication, audit logging, and access controls \u2014 is in development. ",
#'             "All outputs should be reviewed before operational use."
#'           )
#'       )
#'   ),
#'   
#'   # Spinner overlay
#'   div(id = "pipeline-overlay",
#'       div(class = "nhs-spinner-card",
#'           div(class = "nhs-spinner-title", "Running pipeline"),
#'           div(class = "nhs-spinner-sub",   "This may take a few moments\u2026"),
#'           div(class = "nhs-spinner-ring"),
#'           div(
#'             div(id = "step-1", class = "nhs-step", div(class = "nhs-step-num", "1"), span("Processing workbook")),
#'             div(id = "step-2", class = "nhs-step", div(class = "nhs-step-num", "2"), span("Generating posting plan")),
#'             div(id = "step-3", class = "nhs-step", div(class = "nhs-step-num", "3"), span("Adjusting amounts")),
#'             div(id = "step-4", class = "nhs-step", div(class = "nhs-step-num", "4"), span("Assigning EDGE keys")),
#'             div(id = "step-5", class = "nhs-step", div(class = "nhs-step-num", "5"), span("Building EDGE templates"))
#'           )
#'       )
#'   ),
#'   
#'   # JS
#'   tags$script(HTML("
#'     Shiny.addCustomMessageHandler('pipeline_step', function(data) {
#'       var overlay = document.getElementById('pipeline-overlay');
#'       if (data.step === 0) {
#'         overlay.classList.add('active');
#'         document.querySelectorAll('.nhs-step').forEach(function(el) {
#'           el.classList.remove('active', 'done');
#'         });
#'       } else if (data.step === -1) {
#'         overlay.classList.remove('active');
#'       } else {
#'         for (var i = 1; i < data.step; i++) {
#'           var p = document.getElementById('step-' + i);
#'           if (p) { p.classList.remove('active'); p.classList.add('done'); }
#'         }
#'         var c = document.getElementById('step-' + data.step);
#'         if (c) { c.classList.add('active'); c.classList.remove('done'); }
#'       }
#'     });
#' 
#'     function toggleView(v) {
#'       Shiny.setInputValue('view_mode', v, { priority: 'event' });
#'       ['posting', 'edge'].forEach(function(b) {
#'         var el = document.getElementById('tab_' + b);
#'         if (el) el.classList.toggle('active', b === v);
#'       });
#'     }
#'   ")),
#'   
#'   # Page
#'   div(class = "nhs-page",
#'       
#'       # Sidebar
#'       div(class = "nhs-sidebar",
#'           div(class = "nhs-panel",
#'               div(class = "nhs-panel-header", "Input"),
#'               div(class = "nhs-panel-body",
#'                   fileInput("file", "ICT Workbook (.xlsx)", accept = ".xlsx",
#'                             buttonLabel = "Choose file"),
#'                   selectInput("scenario", "Costing Scenario", choices = get_scenarios()),
#'                   actionButton("run", label = tagList(icon("play"), " Run Pipeline"))
#'               )
#'           ),
#'           div(class = "nhs-panel",
#'               div(class = "nhs-panel-header", "Pipeline Log"),
#'               div(style = "padding:0;",
#'                   div(class = "nhs-log", verbatimTextOutput("log"))
#'               )
#'           )
#'       ),
#'       
#'       # Main
#'       div(class = "nhs-main",
#'           
#'           div(class = "nhs-tabs",
#'               div(id = "tab_posting", class = "nhs-tab active", onclick = "toggleView('posting')", "Posting Lines"),
#'               div(id = "tab_edge",    class = "nhs-tab",        onclick = "toggleView('edge')",    "EDGE Templates")
#'           ),
#'           
#'           # Posting lines
#'           conditionalPanel("input.view_mode === 'posting' || !input.view_mode",
#'                            div(class = "nhs-content-header",
#'                                div(class = "nhs-content-left",
#'                                    span(class = "nhs-content-title", "Posting Lines"),
#'                                    uiOutput("posting_badge")
#'                                ),
#'                                tags$button(class = "nhs-toggle-link", id = "posting-toggle",
#'                                            onclick = "
#'               var b = document.getElementById('posting-body');
#'               var l = document.getElementById('posting-toggle');
#'               var hidden = b.style.display === 'none';
#'               b.style.display = hidden ? '' : 'none';
#'               l.textContent   = hidden ? 'Hide' : 'Show';
#'             ", "Hide")
#'                            ),
#'                            div(id = "posting-body",
#'                                div(class = "sheet-row", uiOutput("posting_sheet_select")),
#'                                DTOutput("posting_table")
#'                            )
#'           ),
#'           
#'           # EDGE templates
#'           conditionalPanel("input.view_mode === 'edge'",
#'                            div(class = "nhs-content-header",
#'                                div(class = "nhs-content-left",
#'                                    span(class = "nhs-content-title", "EDGE Templates"),
#'                                    uiOutput("edge_badge")
#'                                ),
#'                                downloadButton("dl_edge", label = tagList(icon("download"), " Export .xlsx"))
#'                            ),
#'                            div(class = "sheet-row", uiOutput("edge_sheet_select")),
#'                            DTOutput("edge_table")
#'           )
#'       )
#'   ),
#'   
#'   # Footer
#'   div(class = "nhs-footer",
#'       paste0(
#'         "NHS Research Finance Tool \u00b7 Internal MVP \u00b7 Not for operational use \u00b7 ",
#'         "Production system in development \u00b7 ", format(Sys.Date(), "%Y")
#'       )
#'   )
#' )
#' 
#' # ── Server ─────────────────────────────────────────────────────────────────────
#' 
#' server <- function(input, output, session) {
#'   
#'   log_lines      <- reactiveVal(character(0))
#'   posting_adj    <- reactiveVal(NULL)
#'   edge_templates <- reactiveVal(NULL)
#'   
#'   step <- function(n) session$sendCustomMessage("pipeline_step", list(step = n))
#'   
#'   log <- function(...) {
#'     msg <- paste0("[", format(Sys.time(), "%H:%M:%S"), "] ", ...)
#'     log_lines(c(log_lines(), msg))
#'   }
#'   
#'   output$log <- renderText(paste(log_lines(), collapse = "\n"))
#'   
#'   # Run pipeline
#'   observeEvent(input$run, {
#'     req(input$file)
#'     log_lines(character(0))
#'     posting_adj(NULL)
#'     edge_templates(NULL)
#'     step(0)
#'     
#'     withCallingHandlers(
#'       tryCatch({
#'         log("Starting pipeline...")
#'         
#'         step(1)
#'         log("Stage A/B: processing workbook...")
#'         processed <- process_workbook(
#'           input_path = input$file$datapath,
#'           db_dir     = DATA_DIR
#'         )
#'         
#'         step(2)
#'         log("Generating posting plan (scenario ", input$scenario, ")...")
#'         out <- generate_posting_plan(
#'           ict           = processed,
#'           rules_db_path = RULES_DB_PATH,
#'           scenario_id   = input$scenario,
#'           ict_db_path   = ICT_DB_PATH
#'         )
#'         
#'         step(3)
#'         log("Adjusting posting amounts...")
#'         adj <- adjust_posting_lines(out)
#'         
#'         step(4)
#'         log("Assigning EDGE keys...")
#'         adj <- assign_edge_keys(adj)
#'         posting_adj(adj)
#'         
#'         step(5)
#'         log("Building EDGE templates...")
#'         load_ict_table(ICT_DB_PATH)
#'         templates <- build_all_edge_templates(adj)
#'         edge_templates(templates)
#'         
#'         step(-1)
#'         log("Done - ", nrow(adj), " posting lines, ", length(templates), " EDGE templates.")
#'         
#'       }, error = function(e) {
#'         step(-1)
#'         log("ERROR: ", conditionMessage(e))
#'       }),
#'       message = function(m) {
#'         log(trimws(conditionMessage(m)))
#'         invokeRestart("muffleMessage")
#'       }
#'     )
#'   })
#'   
#'   # Posting lines
#'   posting_sheets <- reactive({
#'     req(posting_adj())
#'     sort(unique(posting_adj()$sheet_name))
#'   })
#'   
#'   output$posting_badge <- renderUI({
#'     req(posting_adj())
#'     span(class = "nhs-badge", format(nrow(posting_adj()), big.mark = ","), " rows")
#'   })
#'   
#'   output$posting_sheet_select <- renderUI({
#'     req(posting_sheets())
#'     selectInput("posting_sheet", "Sheet", choices = posting_sheets())
#'   })
#'   
#'   output$posting_table <- renderDT({
#'     req(posting_adj(), input$posting_sheet)
#'     df <- posting_adj() %>% filter(sheet_name == input$posting_sheet)
#'     
#'     money_cols <- intersect(
#'       c("posting_amount", "adjusted_amount", "contract_price",
#'         "contract_cost", "base_sum", "activity_cost_num"),
#'       names(df)
#'     )
#'     
#'     dt <- datatable(df,
#'                     options  = list(pageLength = 20, scrollX = TRUE,
#'                                     dom = "<'row'<'col-sm-6'l><'col-sm-6'f>>tip",
#'                                     autoWidth = FALSE),
#'                     rownames = FALSE,
#'                     class    = "stripe hover"
#'     )
#'     
#'     if (length(money_cols) > 0)
#'       dt <- formatCurrency(dt, columns = money_cols, currency = "\u00a3", digits = 2)
#'     
#'     num_cols <- setdiff(names(df)[sapply(df, is.numeric)], money_cols)
#'     if (length(num_cols) > 0)
#'       dt <- formatRound(dt, columns = num_cols, digits = 4)
#'     
#'     dt
#'   })
#'   
#'   # EDGE templates
#'   output$edge_badge <- renderUI({
#'     req(edge_templates())
#'     span(class = "nhs-badge", length(edge_templates()), " templates")
#'   })
#'   
#'   output$edge_sheet_select <- renderUI({
#'     req(edge_templates())
#'     selectInput("edge_sheet", "Template", choices = names(edge_templates()))
#'   })
#'   
#'   output$edge_table <- renderDT({
#'     req(edge_templates(), input$edge_sheet)
#'     df <- edge_templates()[[input$edge_sheet]]
#'     
#'     money_cols <- intersect(c("Default Cost", "Overhead Cost", "total"), names(df))
#'     
#'     dt <- datatable(df,
#'                     options  = list(pageLength = 20, scrollX = TRUE,
#'                                     dom = "<'row'<'col-sm-6'l><'col-sm-6'f>>tip",
#'                                     autoWidth = FALSE),
#'                     rownames = FALSE,
#'                     class    = "stripe hover"
#'     )
#'     
#'     if (length(money_cols) > 0)
#'       dt <- formatCurrency(dt, columns = money_cols, currency = "\u00a3", digits = 2)
#'     
#'     dt
#'   })
#'   
#'   # Export
#'   output$dl_edge <- downloadHandler(
#'     filename = function() {
#'       paste0("EDGE_templates_scenario_", input$scenario, "_",
#'              format(Sys.Date(), "%Y%m%d"), ".xlsx")
#'     },
#'     content = function(file) {
#'       req(edge_templates())
#'       wb <- createWorkbook()
#'       for (nm in names(edge_templates())) {
#'         safe_nm <- substr(gsub("[\\[\\]\\*\\?:/\\\\]", "_", nm), 1, 31)
#'         addWorksheet(wb, safe_nm)
#'         writeData(wb, safe_nm, edge_templates()[[nm]])
#'       }
#'       saveWorkbook(wb, file, overwrite = TRUE)
#'     }
#'   )
#' }
#' 
#' shinyApp(ui, server)

#' library(shiny)
#' library(DBI)
#' library(duckdb)
#' library(DT)
#' library(dplyr)
#' library(stringr)
#' library(openxlsx)
#' 
#' source("config.R")
#' setwd("/Users/tategraham/Documents/NHS/research_finance_tool/R/test_files")
#' source('pipeline_fixed.r')
#' source('posting_test.r')
#' source("template_build_main.r")
#' 
#' # ── NHS colour palette ─────────────────────────────────────────────────────────
#' C_BLUE        <- "#005EB8"
#' C_DARK_BLUE   <- "#003087"
#' C_PALE_BLUE   <- "#d9e8f4"
#' C_GREY        <- "#425563"
#' C_MID_GREY    <- "#768692"
#' C_LIGHT_GREY  <- "#f0f4f5"
#' C_WHITE       <- "#ffffff"
#' C_GREEN       <- "#009639"
#' C_DARK_GREEN  <- "#007a3d"
#' C_NOTICE_BG   <- "#e8f0f8"
#' 
#' # ── Helpers ────────────────────────────────────────────────────────────────────
#' 
#' get_scenarios <- function() {
#'   tryCatch({
#'     con <- dbConnect(duckdb::duckdb(), dbdir = RULES_DB_PATH, read_only = TRUE)
#'     on.exit(dbDisconnect(con, shutdown = TRUE))
#'     dbGetQuery(con, "SELECT DISTINCT scenario_id FROM dist_rules ORDER BY scenario_id;")$scenario_id
#'   }, error = function(e) LETTERS[1:8])
#' }
#' 
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
#' assign_edge_keys <- function(data) {
#'   special_sheets <- c("Unscheduled Activities", "Setup & Closedown", "Pharmacy")
#'   special_keys <- data %>%
#'     filter(sheet_name %in% special_sheets) %>%
#'     distinct(sheet_name, Activity, row_id, staff_group, Study_Arm) %>%
#'     mutate(edge_key = paste0("EDGE-", str_pad(row_number(), width = 4, pad = "0")))
#'   main_keys <- data %>%
#'     filter(!sheet_name %in% special_sheets) %>%
#'     distinct(Study_Arm, Visit) %>%
#'     mutate(edge_key = paste0("EDGE-", str_pad(row_number() + nrow(special_keys), width = 4, pad = "0")))
#'   data %>%
#'     left_join(special_keys, by = c("sheet_name", "Activity", "row_id", "staff_group", "Study_Arm")) %>%
#'     left_join(main_keys,    by = c("Study_Arm", "Visit")) %>%
#'     mutate(edge_key = coalesce(edge_key.x, edge_key.y)) %>%
#'     select(-edge_key.x, -edge_key.y)
#' }
#' 
#' nhs_css <- paste0("
#' 
#'   *, *::before, *::after { box-sizing: border-box; }
#'   html, body { height: 100%; }
#' 
#'   body {
#'     font-family: 'Source Sans Pro', Arial, sans-serif;
#'     font-size: 14px;
#'     background: #f6f8f9;
#'     color: ", C_GREY, ";
#'     line-height: 1.45;
#'   }
#' 
#'   /* ── Global typography ── */
#'   h1, h2, h3, h4, h5, h6 {
#'     margin: 0;
#'     font-weight: 700;
#'     color: ", C_DARK_BLUE, ";
#'   }
#' 
#'   .container-fluid { padding: 0 !important; }
#'   .shiny-title-panel { display: none; }
#'   .row { margin: 0 !important; }
#' 
#'   /* ── Header ── */
#'   .nhs-header {
#'     background: ", C_BLUE, ";
#'     border-bottom: 1px solid rgba(0,0,0,0.08);
#'   }
#' 
#'   .nhs-header-inner {
#'     max-width: 1360px;
#'     margin: 0 auto;
#'     padding: 14px 24px;
#'     display: flex;
#'     align-items: center;
#'     justify-content: space-between;
#'     min-height: 68px;
#'   }
#' 
#'   .nhs-logo-block {
#'     display: flex;
#'     align-items: center;
#'     gap: 16px;
#'   }
#' 
#'   .nhs-lozenge {
#'     background: ", C_WHITE, ";
#'     color: ", C_BLUE, ";
#'     font-size: 22px;
#'     font-weight: 700;
#'     font-family: Arial, sans-serif;
#'     padding: 4px 10px;
#'     border-radius: 2px;
#'     line-height: 1;
#'     letter-spacing: -0.3px;
#'   }
#' 
#'   .nhs-app-title {
#'     color: ", C_WHITE, ";
#'     font-size: 18px;
#'     font-weight: 700;
#'     line-height: 1.1;
#'   }
#' 
#'   .nhs-app-subtitle {
#'     color: rgba(255,255,255,0.82);
#'     font-size: 12px;
#'     margin-top: 3px;
#'     line-height: 1.3;
#'   }
#' 
#'   .nhs-mvp-tag {
#'     display: inline-block;
#'     background: rgba(255,255,255,0.14);
#'     border: 1px solid rgba(255,255,255,0.18);
#'     color: rgba(255,255,255,0.92);
#'     font-size: 10px;
#'     font-weight: 700;
#'     padding: 4px 8px;
#'     border-radius: 3px;
#'     letter-spacing: 0.3px;
#'     text-transform: uppercase;
#'   }
#' 
#'   .nhs-version {
#'     color: rgba(255,255,255,0.65);
#'     font-size: 11px;
#'     margin-top: 5px;
#'   }
#' 
#'   /* ── Notice banner ── */
#'   .nhs-notice {
#'     background: #edf4fb;
#'     border-bottom: 1px solid #d9e4ef;
#'     padding: 10px 24px;
#'   }
#' 
#'   .nhs-notice-inner {
#'     max-width: 1360px;
#'     margin: 0 auto;
#'     font-size: 13px;
#'     color: ", C_GREY, ";
#'     line-height: 1.45;
#'   }
#' 
#'   .nhs-notice-inner strong {
#'     color: ", C_DARK_BLUE, ";
#'     font-weight: 700;
#'   }
#' 
#'   /* ── Page layout ── */
#'   .nhs-page {
#'     max-width: 1360px;
#'     margin: 0 auto;
#'     padding: 24px;
#'     display: flex;
#'     gap: 24px;
#'     align-items: flex-start;
#'   }
#' 
#'   .nhs-sidebar {
#'     width: 280px;
#'     flex-shrink: 0;
#'   }
#' 
#'   .nhs-main {
#'     flex: 1;
#'     min-width: 0;
#'   }
#' 
#'   /* ── Panels / cards ── */
#'   .nhs-panel {
#'     background: ", C_WHITE, ";
#'     border: 1px solid #d8dde0;
#'     border-radius: 6px;
#'     margin-bottom: 16px;
#'     overflow: hidden;
#'     box-shadow: 0 1px 2px rgba(0,0,0,0.03);
#'   }
#' 
#'   .nhs-panel-header {
#'     background: ", C_WHITE, ";
#'     padding: 14px 16px 10px 16px;
#'     font-size: 12px;
#'     font-weight: 700;
#'     color: ", C_DARK_BLUE, ";
#'     letter-spacing: 0.2px;
#'     border-bottom: 1px solid #eef2f4;
#'     text-transform: none;
#'   }
#' 
#'   .nhs-panel-body {
#'     padding: 16px;
#'   }
#' 
#'   /* ── Form controls ── */
#'   .form-group {
#'     margin-bottom: 16px;
#'   }
#' 
#'   .form-group:last-child {
#'     margin-bottom: 0;
#'   }
#' 
#'   .control-label, label {
#'     font-size: 13px;
#'     font-weight: 600;
#'     color: ", C_GREY, ";
#'     display: block;
#'     margin-bottom: 6px;
#'   }
#' 
#'   .form-control,
#'   .selectize-input {
#'     border: 1px solid #bfc8ce !important;
#'     border-radius: 4px !important;
#'     font-size: 13px !important;
#'     color: ", C_GREY, " !important;
#'     padding: 8px 10px !important;
#'     box-shadow: none !important;
#'     background: ", C_WHITE, " !important;
#'     min-height: 38px !important;
#'   }
#' 
#'   .form-control:focus,
#'   .selectize-input.focus {
#'     border-color: ", C_BLUE, " !important;
#'     box-shadow: 0 0 0 3px rgba(0,94,184,0.12) !important;
#'   }
#' 
#'   .selectize-dropdown {
#'     border-radius: 4px !important;
#'     border: 1px solid #bfc8ce !important;
#'   }
#' 
#'   .btn-file {
#'     background: ", C_WHITE, ";
#'     border: 1px solid #bfc8ce;
#'     border-radius: 4px;
#'     color: ", C_GREY, ";
#'     font-size: 13px;
#'     font-weight: 600;
#'     padding: 8px 12px;
#'   }
#' 
#'   .btn-file:hover {
#'     background: #f7f9fb;
#'     border-color: ", C_BLUE, ";
#'   }
#' 
#'   /* ── Primary action ── */
#'   #run {
#'     width: 100%;
#'     background: ", C_BLUE, ";
#'     border: 1px solid ", C_BLUE, ";
#'     color: ", C_WHITE, ";
#'     font-size: 14px;
#'     font-weight: 700;
#'     border-radius: 4px;
#'     padding: 10px 14px;
#'     cursor: pointer;
#'     transition: background 0.12s ease, border-color 0.12s ease;
#'     box-shadow: none;
#'   }
#' 
#'   #run:hover {
#'     background: ", C_DARK_BLUE, ";
#'     border-color: ", C_DARK_BLUE, ";
#'   }
#' 
#'   #run:active {
#'     transform: translateY(1px);
#'   }
#' 
#'   /* ── Log ── */
#'   .nhs-log {
#'     font-size: 11px;
#'     font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
#'     background: #1f2933;
#'     color: #d7f7df;
#'     padding: 12px;
#'     min-height: 120px;
#'     max-height: 220px;
#'     overflow-y: auto;
#'     white-space: pre-wrap;
#'     line-height: 1.5;
#'     border-top: 1px solid #eef2f4;
#'   }
#' 
#'   .nhs-log::-webkit-scrollbar { width: 8px; }
#'   .nhs-log::-webkit-scrollbar-thumb { background: #51606e; border-radius: 10px; }
#' 
#'   /* ── Tabs ── */
#'   .nhs-tabs {
#'     display: inline-flex;
#'     gap: 6px;
#'     margin-bottom: 18px;
#'     padding: 4px;
#'     background: #e9eef2;
#'     border-radius: 8px;
#'     border: 1px solid #d8dde0;
#'   }
#' 
#'   .nhs-tab {
#'     padding: 9px 14px;
#'     font-size: 13px;
#'     font-weight: 600;
#'     color: ", C_GREY, ";
#'     background: transparent;
#'     border: none;
#'     border-radius: 6px;
#'     cursor: pointer;
#'     margin-right: 0;
#'     transition: background 0.12s ease, color 0.12s ease;
#'     user-select: none;
#'   }
#' 
#'   .nhs-tab:hover {
#'     background: rgba(255,255,255,0.65);
#'     color: ", C_DARK_BLUE, ";
#'   }
#' 
#'   .nhs-tab.active {
#'     background: ", C_WHITE, ";
#'     color: ", C_DARK_BLUE, ";
#'     box-shadow: 0 1px 2px rgba(0,0,0,0.06);
#'   }
#' 
#'   /* ── Content section ── */
#'   .nhs-section {
#'     background: ", C_WHITE, ";
#'     border: 1px solid #d8dde0;
#'     border-radius: 6px;
#'     padding: 18px;
#'     box-shadow: 0 1px 2px rgba(0,0,0,0.03);
#'   }
#' 
#'   .nhs-content-header {
#'     display: flex;
#'     align-items: center;
#'     justify-content: space-between;
#'     margin-bottom: 14px;
#'     padding-bottom: 12px;
#'     border-bottom: 1px solid #eef2f4;
#'   }
#' 
#'   .nhs-content-left {
#'     display: flex;
#'     align-items: center;
#'     gap: 10px;
#'   }
#' 
#'   .nhs-content-title {
#'     font-size: 18px;
#'     font-weight: 700;
#'     color: ", C_DARK_BLUE, ";
#'     line-height: 1.2;
#'   }
#' 
#'   .nhs-badge {
#'     display: inline-flex;
#'     align-items: center;
#'     background: #eef4f8;
#'     color: ", C_GREY, ";
#'     font-size: 11px;
#'     font-weight: 700;
#'     padding: 3px 8px;
#'     border-radius: 999px;
#'     border: 1px solid #d8e2e9;
#'   }
#' 
#'   .nhs-toggle-link {
#'     font-size: 12px;
#'     font-weight: 600;
#'     color: ", C_BLUE, ";
#'     cursor: pointer;
#'     text-decoration: none;
#'     background: none;
#'     border: none;
#'     padding: 0;
#'   }
#' 
#'   .nhs-toggle-link:hover {
#'     text-decoration: underline;
#'     color: ", C_DARK_BLUE, ";
#'   }
#' 
#'   .sheet-row {
#'     margin-bottom: 14px;
#'     max-width: 340px;
#'   }
#' 
#'   /* ── Export button ── */
#'   #dl_edge {
#'     background: ", C_GREEN, ";
#'     border: 1px solid ", C_GREEN, ";
#'     color: ", C_WHITE, ";
#'     font-size: 13px;
#'     font-weight: 700;
#'     padding: 8px 14px;
#'     border-radius: 4px;
#'     cursor: pointer;
#'     display: inline-flex;
#'     align-items: center;
#'     gap: 6px;
#'     text-decoration: none;
#'   }
#' 
#'   #dl_edge:hover {
#'     background: ", C_DARK_GREEN, ";
#'     border-color: ", C_DARK_GREEN, ";
#'   }
#' 
#'   /* ── Spinner overlay ── */
#'   #pipeline-overlay {
#'     display: none;
#'     position: fixed;
#'     inset: 0;
#'     background: rgba(22, 29, 37, 0.34);
#'     z-index: 9999;
#'     align-items: center;
#'     justify-content: center;
#'     padding: 24px;
#'   }
#' 
#'   #pipeline-overlay.active {
#'     display: flex;
#'   }
#' 
#'   .nhs-spinner-card {
#'     background: ", C_WHITE, ";
#'     border: 1px solid #d8dde0;
#'     border-radius: 8px;
#'     padding: 24px;
#'     width: 360px;
#'     box-shadow: 0 8px 28px rgba(0,0,0,0.14);
#'   }
#' 
#'   .nhs-spinner-title {
#'     font-size: 16px;
#'     font-weight: 700;
#'     color: ", C_DARK_BLUE, ";
#'     margin-bottom: 4px;
#'   }
#' 
#'   .nhs-spinner-sub {
#'     font-size: 12px;
#'     color: ", C_MID_GREY, ";
#'     margin-bottom: 18px;
#'   }
#' 
#'   .nhs-spinner-ring {
#'     width: 34px;
#'     height: 34px;
#'     border: 3px solid ", C_PALE_BLUE, ";
#'     border-top-color: ", C_BLUE, ";
#'     border-radius: 50%;
#'     animation: nhs-spin 0.7s linear infinite;
#'     margin-bottom: 16px;
#'   }
#' 
#'   @keyframes nhs-spin {
#'     to { transform: rotate(360deg); }
#'   }
#' 
#'   .nhs-step {
#'     font-size: 13px;
#'     color: #7b8791;
#'     padding: 8px 0;
#'     display: flex;
#'     align-items: center;
#'     gap: 10px;
#'     border-bottom: 1px solid #f1f4f6;
#'     transition: color 0.2s;
#'   }
#' 
#'   .nhs-step:last-child {
#'     border-bottom: none;
#'   }
#' 
#'   .nhs-step-num {
#'     width: 20px;
#'     height: 20px;
#'     border-radius: 50%;
#'     background: #dde3e7;
#'     color: ", C_GREY, ";
#'     font-size: 10px;
#'     font-weight: 700;
#'     display: flex;
#'     align-items: center;
#'     justify-content: center;
#'     flex-shrink: 0;
#'   }
#' 
#'   .nhs-step.active {
#'     color: ", C_DARK_BLUE, ";
#'     font-weight: 600;
#'   }
#' 
#'   .nhs-step.active .nhs-step-num {
#'     background: ", C_BLUE, ";
#'     color: ", C_WHITE, ";
#'   }
#' 
#'   .nhs-step.done {
#'     color: ", C_GREEN, ";
#'   }
#' 
#'   .nhs-step.done .nhs-step-num {
#'     background: ", C_GREEN, ";
#'     color: ", C_WHITE, ";
#'   }
#' 
#'   /* ── Tables ── */
#'   .dataTables_wrapper {
#'     font-size: 13px;
#'   }
#' 
#'   .dataTables_wrapper .dataTables_length,
#'   .dataTables_wrapper .dataTables_filter {
#'     margin-bottom: 10px;
#'   }
#' 
#'   .dataTables_wrapper .dataTables_filter input,
#'   .dataTables_wrapper .dataTables_length select {
#'     border: 1px solid #bfc8ce;
#'     border-radius: 4px;
#'     padding: 6px 8px;
#'     font-size: 12px;
#'     color: ", C_GREY, ";
#'     background: ", C_WHITE, ";
#'     outline: none;
#'   }
#' 
#'   .dataTables_wrapper .dataTables_filter input:focus,
#'   .dataTables_wrapper .dataTables_length select:focus {
#'     border-color: ", C_BLUE, ";
#'     box-shadow: 0 0 0 3px rgba(0,94,184,0.12);
#'   }
#' 
#'   .dataTables_wrapper .dataTables_info {
#'     font-size: 12px;
#'     color: ", C_MID_GREY, ";
#'     padding-top: 12px;
#'   }
#' 
#'   table.dataTable {
#'     border-collapse: separate !important;
#'     border-spacing: 0;
#'     width: 100% !important;
#'     border: 1px solid #d8dde0;
#'     border-radius: 6px;
#'     overflow: hidden;
#'     font-size: 13px;
#'     background: ", C_WHITE, ";
#'   }
#' 
#'   table.dataTable thead th {
#'     background: #f7f9fa !important;
#'     color: ", C_GREY, " !important;
#'     font-weight: 700 !important;
#'     font-size: 11px !important;
#'     text-transform: uppercase !important;
#'     letter-spacing: 0.35px !important;
#'     border-bottom: 1px solid #d8dde0 !important;
#'     border-right: none !important;
#'     padding: 10px 12px !important;
#'     white-space: nowrap;
#'   }
#' 
#'   table.dataTable tbody td {
#'     padding: 9px 12px !important;
#'     border-bottom: 1px solid #eef2f4 !important;
#'     border-right: none !important;
#'     color: ", C_GREY, ";
#'     vertical-align: middle;
#'     background: ", C_WHITE, " !important;
#'   }
#' 
#'   table.dataTable tbody tr:last-child td {
#'     border-bottom: none !important;
#'   }
#' 
#'   table.dataTable tbody tr:hover td {
#'     background: #f4f8fb !important;
#'     color: ", C_DARK_BLUE, " !important;
#'   }
#' 
#'   .dataTables_paginate {
#'     padding-top: 10px !important;
#'   }
#' 
#'   .dataTables_paginate .paginate_button {
#'     border-radius: 4px !important;
#'     font-size: 12px !important;
#'     padding: 4px 10px !important;
#'     border: 1px solid #d8dde0 !important;
#'     color: ", C_GREY, " !important;
#'     background: ", C_WHITE, " !important;
#'     margin-left: 4px;
#'   }
#' 
#'   .dataTables_paginate .paginate_button:hover {
#'     background: #f4f8fb !important;
#'     border-color: ", C_BLUE, " !important;
#'     color: ", C_BLUE, " !important;
#'   }
#' 
#'   .dataTables_paginate .paginate_button.current,
#'   .dataTables_paginate .paginate_button.current:hover {
#'     background: ", C_BLUE, " !important;
#'     border-color: ", C_BLUE, " !important;
#'     color: ", C_WHITE, " !important;
#'   }
#' 
#'   .dataTables_paginate .paginate_button.disabled {
#'     color: #adb5bd !important;
#'   }
#' 
#'   /* ── Footer ── */
#'   .nhs-footer {
#'     max-width: 1360px;
#'     margin: 8px auto 0;
#'     padding: 14px 24px 24px 24px;
#'     font-size: 11px;
#'     color: ", C_MID_GREY, ";
#'   }
#' 
#'   /* ── Utility spacing on mobile-ish widths ── */
#'   @media (max-width: 980px) {
#'     .nhs-page {
#'       flex-direction: column;
#'     }
#' 
#'     .nhs-sidebar {
#'       width: 100%;
#'     }
#' 
#'     .nhs-content-header {
#'       flex-direction: column;
#'       align-items: flex-start;
#'       gap: 10px;
#'     }
#' 
#'     .sheet-row {
#'       max-width: 100%;
#'     }
#'   }
#' ")
#' # ── UI ─────────────────────────────────────────────────────────────────────────
#' 
#' ui <- fluidPage(
#'   title = "Research Finance — NHS",
#'   
#'   tags$head(
#'     tags$link(
#'       href = "https://fonts.googleapis.com/css2?family=Source+Sans+Pro:wght@400;600;700&display=swap",
#'       rel  = "stylesheet"
#'     ),
#'     tags$style(HTML(nhs_css))
#'   ),
#'   
#'   # Header
#'   div(class = "nhs-header",
#'       div(class = "nhs-header-inner",
#'           div(class = "nhs-logo-block",
#'               div(class = "nhs-lozenge", "NHS"),
#'               div(
#'                 div(class = "nhs-app-title",    "Research Finance Tool"),
#'                 div(class = "nhs-app-subtitle", "Commercial Activity Costing & EDGE Template Generation")
#'               )
#'           ),
#'           div(style = "text-align:right;",
#'               div(class = "nhs-mvp-tag", "Internal Preview — MVP"),
#'               div(class = "nhs-version", paste0("v0.1-alpha \u00b7 ", format(Sys.Date(), "%B %Y")))
#'           )
#'       )
#'   ),
#'   
#'   # Notice banner
#'   div(class = "nhs-notice",
#'       div(class = "nhs-notice-inner",
#'           tags$strong("Early access \u2014 for internal review only. "),
#'           paste0(
#'             "Core pipeline logic is validated. The full production system \u2014 including ",
#'             "authentication, audit logging, and access controls \u2014 is in development. ",
#'             "All outputs should be reviewed before operational use."
#'           )
#'       )
#'   ),
#'   
#'   # Spinner overlay
#'   div(id = "pipeline-overlay",
#'       div(class = "nhs-spinner-card",
#'           div(class = "nhs-spinner-title", "Running pipeline"),
#'           div(class = "nhs-spinner-sub",   "This may take a few moments\u2026"),
#'           div(class = "nhs-spinner-ring"),
#'           div(
#'             div(id = "step-1", class = "nhs-step", div(class = "nhs-step-num", "1"), span("Processing workbook")),
#'             div(id = "step-2", class = "nhs-step", div(class = "nhs-step-num", "2"), span("Generating posting plan")),
#'             div(id = "step-3", class = "nhs-step", div(class = "nhs-step-num", "3"), span("Adjusting amounts")),
#'             div(id = "step-4", class = "nhs-step", div(class = "nhs-step-num", "4"), span("Assigning EDGE keys")),
#'             div(id = "step-5", class = "nhs-step", div(class = "nhs-step-num", "5"), span("Building EDGE templates"))
#'           )
#'       )
#'   ),
#'   
#'   # JS
#'   tags$script(HTML("
#'     Shiny.addCustomMessageHandler('pipeline_step', function(data) {
#'       var overlay = document.getElementById('pipeline-overlay');
#'       if (data.step === 0) {
#'         overlay.classList.add('active');
#'         document.querySelectorAll('.nhs-step').forEach(function(el) {
#'           el.classList.remove('active', 'done');
#'         });
#'       } else if (data.step === -1) {
#'         overlay.classList.remove('active');
#'       } else {
#'         for (var i = 1; i < data.step; i++) {
#'           var p = document.getElementById('step-' + i);
#'           if (p) { p.classList.remove('active'); p.classList.add('done'); }
#'         }
#'         var c = document.getElementById('step-' + data.step);
#'         if (c) { c.classList.add('active'); c.classList.remove('done'); }
#'       }
#'     });
#' 
#'     function toggleView(v) {
#'       Shiny.setInputValue('view_mode', v, { priority: 'event' });
#'       ['posting', 'edge'].forEach(function(b) {
#'         var el = document.getElementById('tab_' + b);
#'         if (el) el.classList.toggle('active', b === v);
#'       });
#'     }
#'   ")),
#'   
#'   # Page
#'   div(class = "nhs-page",
#'       
#'       # Sidebar
#'       div(class = "nhs-sidebar",
#'           div(class = "nhs-panel",
#'               div(class = "nhs-panel-header", "Input"),
#'               div(class = "nhs-panel-body",
#'                   fileInput("file", "ICT Workbook (.xlsx)", accept = ".xlsx",
#'                             buttonLabel = "Choose file"),
#'                   selectInput("scenario", "Costing Scenario", choices = get_scenarios()),
#'                   actionButton("run", label = tagList(icon("play"), " Run Pipeline"))
#'               )
#'           ),
#'           div(class = "nhs-panel",
#'               div(class = "nhs-panel-header", "Pipeline Log"),
#'               div(style = "padding:0;",
#'                   div(class = "nhs-log", verbatimTextOutput("log"))
#'               )
#'           )
#'       ),
#'       
#'       # Main
#'       div(class = "nhs-main",
#'           
#'           div(class = "nhs-tabs",
#'               div(id = "tab_posting", class = "nhs-tab active", onclick = "toggleView('posting')", "Posting Lines"),
#'               div(id = "tab_edge",    class = "nhs-tab",        onclick = "toggleView('edge')",    "EDGE Templates")
#'           ),
#'           
#'           # Posting lines
#'           conditionalPanel("input.view_mode === 'posting' || !input.view_mode",
#'                            div(class = "nhs-content-header",
#'                                div(class = "nhs-content-left",
#'                                    span(class = "nhs-content-title", "Posting Lines"),
#'                                    uiOutput("posting_badge")
#'                                ),
#'                                tags$button(class = "nhs-toggle-link", id = "posting-toggle",
#'                                            onclick = "
#'               var b = document.getElementById('posting-body');
#'               var l = document.getElementById('posting-toggle');
#'               var hidden = b.style.display === 'none';
#'               b.style.display = hidden ? '' : 'none';
#'               l.textContent   = hidden ? 'Hide' : 'Show';
#'             ", "Hide")
#'                            ),
#'                            div(id = "posting-body",
#'                                div(class = "sheet-row", uiOutput("posting_sheet_select")),
#'                                DTOutput("posting_table")
#'                            )
#'           ),
#'           
#'           # EDGE templates
#'           conditionalPanel("input.view_mode === 'edge'",
#'                            div(class = "nhs-content-header",
#'                                div(class = "nhs-content-left",
#'                                    span(class = "nhs-content-title", "EDGE Templates"),
#'                                    uiOutput("edge_badge")
#'                                ),
#'                                downloadButton("dl_edge", label = tagList(icon("download"), " Export .xlsx"))
#'                            ),
#'                            div(class = "sheet-row", uiOutput("edge_sheet_select")),
#'                            DTOutput("edge_table")
#'           )
#'       )
#'   ),
#'   
#'   # Footer
#'   div(class = "nhs-footer",
#'       paste0(
#'         "NHS Research Finance Tool \u00b7 Internal MVP \u00b7 Not for operational use \u00b7 ",
#'         "Production system in development \u00b7 ", format(Sys.Date(), "%Y")
#'       )
#'   )
#' )
#' 
#' # ── Server ─────────────────────────────────────────────────────────────────────
#' 
#' server <- function(input, output, session) {
#'   
#'   log_lines      <- reactiveVal(character(0))
#'   posting_adj    <- reactiveVal(NULL)
#'   edge_templates <- reactiveVal(NULL)
#'   
#'   step <- function(n) session$sendCustomMessage("pipeline_step", list(step = n))
#'   
#'   log <- function(...) {
#'     msg <- paste0("[", format(Sys.time(), "%H:%M:%S"), "] ", ...)
#'     log_lines(c(log_lines(), msg))
#'   }
#'   
#'   output$log <- renderText(paste(log_lines(), collapse = "\n"))
#'   
#'   # Run pipeline
#'   observeEvent(input$run, {
#'     req(input$file)
#'     log_lines(character(0))
#'     posting_adj(NULL)
#'     edge_templates(NULL)
#'     step(0)
#'     
#'     withCallingHandlers(
#'       tryCatch({
#'         log("Starting pipeline...")
#'         
#'         step(1)
#'         log("Stage A/B: processing workbook...")
#'         processed <- process_workbook(
#'           input_path = input$file$datapath,
#'           db_dir     = DATA_DIR
#'         )
#'         
#'         step(2)
#'         log("Generating posting plan (scenario ", input$scenario, ")...")
#'         out <- generate_posting_plan(
#'           ict           = processed,
#'           rules_db_path = RULES_DB_PATH,
#'           scenario_id   = input$scenario,
#'           ict_db_path   = ICT_DB_PATH
#'         )
#'         
#'         step(3)
#'         log("Adjusting posting amounts...")
#'         adj <- adjust_posting_lines(out)
#'         
#'         step(4)
#'         log("Assigning EDGE keys...")
#'         adj <- assign_edge_keys(adj)
#'         posting_adj(adj)
#'         
#'         step(5)
#'         log("Building EDGE templates...")
#'         templates <- build_all_edge_templates(adj)
#'         edge_templates(templates)
#'         
#'         step(-1)
#'         log("Done - ", nrow(adj), " posting lines, ", length(templates), " EDGE templates.")
#'         
#'       }, error = function(e) {
#'         step(-1)
#'         log("ERROR: ", conditionMessage(e))
#'       }),
#'       message = function(m) {
#'         log(trimws(conditionMessage(m)))
#'         invokeRestart("muffleMessage")
#'       }
#'     )
#'   })
#'   
#'   # Posting lines
#'   posting_sheets <- reactive({
#'     req(posting_adj())
#'     sort(unique(posting_adj()$sheet_name))
#'   })
#'   
#'   output$posting_badge <- renderUI({
#'     req(posting_adj())
#'     span(class = "nhs-badge", format(nrow(posting_adj()), big.mark = ","), " rows")
#'   })
#'   
#'   output$posting_sheet_select <- renderUI({
#'     req(posting_sheets())
#'     selectInput("posting_sheet", "Sheet", choices = posting_sheets())
#'   })
#'   
#'   output$posting_table <- renderDT({
#'     req(posting_adj(), input$posting_sheet)
#'     df <- posting_adj() %>% filter(sheet_name == input$posting_sheet)
#'     
#'     money_cols <- intersect(
#'       c("posting_amount", "adjusted_amount", "contract_price",
#'         "contract_cost", "base_sum", "activity_cost_num"),
#'       names(df)
#'     )
#'     
#'     dt <- datatable(df,
#'                     options  = list(pageLength = 20, scrollX = TRUE,
#'                                     dom = "<'row'<'col-sm-6'l><'col-sm-6'f>>tip",
#'                                     autoWidth = FALSE),
#'                     rownames = FALSE,
#'                     class    = "stripe hover"
#'     )
#'     
#'     if (length(money_cols) > 0)
#'       dt <- formatCurrency(dt, columns = money_cols, currency = "\u00a3", digits = 2)
#'     
#'     num_cols <- setdiff(names(df)[sapply(df, is.numeric)], money_cols)
#'     if (length(num_cols) > 0)
#'       dt <- formatRound(dt, columns = num_cols, digits = 4)
#'     
#'     dt
#'   })
#'   
#'   # EDGE templates
#'   output$edge_badge <- renderUI({
#'     req(edge_templates())
#'     span(class = "nhs-badge", length(edge_templates()), " templates")
#'   })
#'   
#'   output$edge_sheet_select <- renderUI({
#'     req(edge_templates())
#'     selectInput("edge_sheet", "Template", choices = names(edge_templates()))
#'   })
#'   
#'   output$edge_table <- renderDT({
#'     req(edge_templates(), input$edge_sheet)
#'     df <- edge_templates()[[input$edge_sheet]]
#'     
#'     money_cols <- intersect(c("Default Cost", "Overhead Cost", "total"), names(df))
#'     
#'     dt <- datatable(df,
#'                     options  = list(pageLength = 20, scrollX = TRUE,
#'                                     dom = "<'row'<'col-sm-6'l><'col-sm-6'f>>tip",
#'                                     autoWidth = FALSE),
#'                     rownames = FALSE,
#'                     class    = "stripe hover"
#'     )
#'     
#'     if (length(money_cols) > 0)
#'       dt <- formatCurrency(dt, columns = money_cols, currency = "\u00a3", digits = 2)
#'     
#'     dt
#'   })
#'   
#'   # Export
#'   output$dl_edge <- downloadHandler(
#'     filename = function() {
#'       paste0("EDGE_templates_scenario_", input$scenario, "_",
#'              format(Sys.Date(), "%Y%m%d"), ".xlsx")
#'     },
#'     content = function(file) {
#'       req(edge_templates())
#'       wb <- createWorkbook()
#'       for (nm in names(edge_templates())) {
#'         safe_nm <- substr(gsub("[\\[\\]\\*\\?:/\\\\]", "_", nm), 1, 31)
#'         addWorksheet(wb, safe_nm)
#'         writeData(wb, safe_nm, edge_templates()[[nm]])
#'       }
#'       saveWorkbook(wb, file, overwrite = TRUE)
#'     }
#'   )
#' }
#' 
#' shinyApp(ui, server)
#' 
#' #' # ── Helpers ────────────────────────────────────────────────────────────────────
#' #' 
#' #' get_scenarios <- function() {
#' #'   tryCatch({
#' #'     con <- dbConnect(duckdb::duckdb(), dbdir = RULES_DB_PATH, read_only = TRUE)
#' #'     on.exit(dbDisconnect(con, shutdown = TRUE))
#' #'     dbGetQuery(con, "SELECT DISTINCT scenario_id FROM dist_rules ORDER BY scenario_id;")$scenario_id
#' #'   }, error = function(e) LETTERS[1:8])
#' #' }
#' #' 
#' #' adjust_posting_lines <- function(out) {
#' #'   out %>%
#' #'     mutate(contract_price = round(contract_cost, 0)) %>%
#' #'     group_by(row_id, scenario_id) %>%
#' #'     mutate(
#' #'       base_sum        = sum(posting_amount, na.rm = TRUE),
#' #'       contract_price  = first(contract_price),
#' #'       multiplier      = if_else(base_sum == 0, NA_real_, contract_price / base_sum),
#' #'       adjusted_amount = if_else(base_sum == 0, 0, round(posting_amount * multiplier, 2))
#' #'     ) %>%
#' #'     mutate(
#' #'       residual        = round(contract_price - sum(adjusted_amount, na.rm = TRUE), 2),
#' #'       has_direct      = any(posting_line_type_id == "DIRECT"),
#' #'       is_residual_row = if_else(
#' #'         has_direct,
#' #'         posting_line_type_id == "DIRECT" & row_number() == min(which(posting_line_type_id == "DIRECT")),
#' #'         row_number() == 1L
#' #'       ),
#' #'       adjusted_amount = if_else(
#' #'         is_residual_row,
#' #'         round(adjusted_amount + residual, 2),
#' #'         adjusted_amount
#' #'       )
#' #'     ) %>%
#' #'     mutate(
#' #'       adjusted_sum_check = round(sum(adjusted_amount, na.rm = TRUE), 2),
#' #'       diff_check         = round(contract_price - adjusted_sum_check, 2)
#' #'     ) %>%
#' #'     select(-has_direct) %>%
#' #'     ungroup()
#' #' }
#' #' 
#' #' assign_edge_keys <- function(data) {
#' #'   special_sheets <- c("Unscheduled Activities", "Setup & Closedown", "Pharmacy")
#' #'   special_keys <- data %>%
#' #'     filter(sheet_name %in% special_sheets) %>%
#' #'     distinct(sheet_name, Activity, row_id, staff_group, Study_Arm) %>%
#' #'     mutate(edge_key = paste0("EDGE-", str_pad(row_number(), width = 4, pad = "0")))
#' #'   main_keys <- data %>%
#' #'     filter(!sheet_name %in% special_sheets) %>%
#' #'     distinct(Study_Arm, Visit) %>%
#' #'     mutate(edge_key = paste0("EDGE-", str_pad(row_number() + nrow(special_keys), width = 4, pad = "0")))
#' #'   data %>%
#' #'     left_join(special_keys, by = c("sheet_name", "Activity", "row_id", "staff_group", "Study_Arm")) %>%
#' #'     left_join(main_keys,    by = c("Study_Arm", "Visit")) %>%
#' #'     mutate(edge_key = coalesce(edge_key.x, edge_key.y)) %>%
#' #'     select(-edge_key.x, -edge_key.y)
#' #' }
#' #' 
#' #' # ── UI ─────────────────────────────────────────────────────────────────────────
#' #' 
#' #' ui <- fluidPage(
#' #'   title = "Research Finance Tool",
#' #'   
#' #'   tags$head(
#' #'     tags$link(
#' #'       href = "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap",
#' #'       rel  = "stylesheet"
#' #'     ),
#' #'     tags$style(HTML("
#' #' 
#' #'       /* ── Reset & base ─────────────────────────────────────────────────────── */
#' #'       *, *::before, *::after { box-sizing: border-box; }
#' #' 
#' #'       body {
#' #'         font-family: 'Inter', 'Helvetica Neue', Arial, sans-serif;
#' #'         font-size: 13px;
#' #'         background: #f0f2f5;
#' #'         color: #1e293b;
#' #'         margin: 0;
#' #'       }
#' #' 
#' #'       /* ── Top nav bar ──────────────────────────────────────────────────────── */
#' #'       .top-bar {
#' #'         background: #0f172a;
#' #'         padding: 0 28px;
#' #'         height: 52px;
#' #'         display: flex;
#' #'         align-items: center;
#' #'         justify-content: space-between;
#' #'         position: sticky;
#' #'         top: 0;
#' #'         z-index: 100;
#' #'         box-shadow: 0 1px 0 rgba(255,255,255,.06);
#' #'       }
#' #'       .top-bar-left {
#' #'         display: flex;
#' #'         align-items: center;
#' #'         gap: 12px;
#' #'       }
#' #'       .top-bar-logo {
#' #'         font-size: 15px;
#' #'         font-weight: 700;
#' #'         color: #f8fafc;
#' #'         letter-spacing: -.2px;
#' #'       }
#' #'       .top-bar-logo span {
#' #'         color: #60a5fa;
#' #'       }
#' #'       .top-bar-divider {
#' #'         width: 1px;
#' #'         height: 18px;
#' #'         background: rgba(255,255,255,.12);
#' #'       }
#' #'       .top-bar-badge {
#' #'         display: flex;
#' #'         align-items: center;
#' #'         gap: 6px;
#' #'         background: rgba(234,179,8,.12);
#' #'         border: 1px solid rgba(234,179,8,.3);
#' #'         border-radius: 20px;
#' #'         padding: 3px 10px 3px 7px;
#' #'         font-size: 11px;
#' #'         font-weight: 600;
#' #'         color: #fbbf24;
#' #'         letter-spacing: .2px;
#' #'       }
#' #'       .badge-dot {
#' #'         width: 6px; height: 6px;
#' #'         border-radius: 50%;
#' #'         background: #fbbf24;
#' #'         animation: pulse-dot 2s ease-in-out infinite;
#' #'       }
#' #'       @keyframes pulse-dot {
#' #'         0%, 100% { opacity: 1; transform: scale(1); }
#' #'         50%       { opacity: .4; transform: scale(.7); }
#' #'       }
#' #'       .top-bar-right {
#' #'         font-size: 11px;
#' #'         color: rgba(255,255,255,.35);
#' #'         letter-spacing: .2px;
#' #'       }
#' #' 
#' #'       /* ── MVP notice banner ────────────────────────────────────────────────── */
#' #'       .mvp-banner {
#' #'         background: linear-gradient(90deg, #1e3a5f 0%, #1e3554 100%);
#' #'         border-bottom: 1px solid rgba(96,165,250,.2);
#' #'         padding: 9px 28px;
#' #'         display: flex;
#' #'         align-items: center;
#' #'         justify-content: space-between;
#' #'         gap: 12px;
#' #'       }
#' #'       .mvp-banner-text {
#' #'         font-size: 12px;
#' #'         color: #94a3b8;
#' #'         line-height: 1.5;
#' #'       }
#' #'       .mvp-banner-text strong {
#' #'         color: #e2e8f0;
#' #'         font-weight: 600;
#' #'       }
#' #'       .mvp-banner-pill {
#' #'         flex-shrink: 0;
#' #'         background: rgba(96,165,250,.1);
#' #'         border: 1px solid rgba(96,165,250,.25);
#' #'         border-radius: 20px;
#' #'         padding: 4px 12px;
#' #'         font-size: 11px;
#' #'         font-weight: 600;
#' #'         color: #93c5fd;
#' #'         white-space: nowrap;
#' #'       }
#' #' 
#' #'       /* ── Layout wrapper ───────────────────────────────────────────────────── */
#' #'       .page-wrap {
#' #'         display: flex;
#' #'         gap: 18px;
#' #'         padding: 20px 24px;
#' #'         max-width: 1600px;
#' #'         margin: 0 auto;
#' #'       }
#' #' 
#' #'       /* ── Sidebar card ─────────────────────────────────────────────────────── */
#' #'       .sidebar-card {
#' #'         width: 270px;
#' #'         flex-shrink: 0;
#' #'       }
#' #'       .card {
#' #'         background: #ffffff;
#' #'         border: 1px solid #e2e8f0;
#' #'         border-radius: 10px;
#' #'         box-shadow: 0 1px 3px rgba(0,0,0,.05), 0 1px 2px rgba(0,0,0,.03);
#' #'         padding: 20px;
#' #'       }
#' #'       .card + .card { margin-top: 14px; }
#' #' 
#' #'       .card-label {
#' #'         font-size: 10px;
#' #'         font-weight: 700;
#' #'         text-transform: uppercase;
#' #'         letter-spacing: .7px;
#' #'         color: #94a3b8;
#' #'         margin-bottom: 14px;
#' #'       }
#' #' 
#' #'       /* ── Form controls ────────────────────────────────────────────────────── */
#' #'       .form-group label {
#' #'         font-size: 11px;
#' #'         font-weight: 600;
#' #'         color: #475569;
#' #'         margin-bottom: 4px;
#' #'       }
#' #'       .form-control, .selectize-input {
#' #'         border: 1px solid #e2e8f0 !important;
#' #'         border-radius: 6px !important;
#' #'         font-size: 12px !important;
#' #'         color: #1e293b !important;
#' #'         box-shadow: none !important;
#' #'       }
#' #'       .selectize-input.focus { border-color: #93c5fd !important; }
#' #' 
#' #'       /* file input */
#' #'       .btn-file {
#' #'         background: #f8fafc;
#' #'         border: 1px solid #e2e8f0;
#' #'         border-radius: 6px;
#' #'         color: #475569;
#' #'         font-size: 12px;
#' #'         font-weight: 500;
#' #'       }
#' #'       .btn-file:hover { background: #f1f5f9; }
#' #' 
#' #'       /* ── Run button ───────────────────────────────────────────────────────── */
#' #'       #run {
#' #'         width: 100%;
#' #'         background: #2563eb;
#' #'         border: none;
#' #'         color: #fff;
#' #'         font-size: 13px;
#' #'         font-weight: 600;
#' #'         letter-spacing: .2px;
#' #'         border-radius: 7px;
#' #'         padding: 9px 0;
#' #'         margin-top: 4px;
#' #'         transition: background .15s, transform .1s;
#' #'         display: flex;
#' #'         align-items: center;
#' #'         justify-content: center;
#' #'         gap: 7px;
#' #'       }
#' #'       #run:hover  { background: #1d4ed8; transform: translateY(-1px); }
#' #'       #run:active { transform: translateY(0); }
#' #' 
#' #'       /* ── Log box ──────────────────────────────────────────────────────────── */
#' #'       .log-box {
#' #'         font-size: 10.5px;
#' #'         font-family: 'Courier New', monospace;
#' #'         background: #0f172a;
#' #'         color: #86efac;
#' #'         padding: 10px 12px;
#' #'         border-radius: 7px;
#' #'         min-height: 100px;
#' #'         max-height: 220px;
#' #'         overflow-y: auto;
#' #'         white-space: pre-wrap;
#' #'         line-height: 1.6;
#' #'         border: 1px solid #1e293b;
#' #'       }
#' #'       .log-box::-webkit-scrollbar { width: 4px; }
#' #'       .log-box::-webkit-scrollbar-track { background: transparent; }
#' #'       .log-box::-webkit-scrollbar-thumb { background: #334155; border-radius: 4px; }
#' #' 
#' #'       /* ── Main content area ────────────────────────────────────────────────── */
#' #'       .main-area { flex: 1; min-width: 0; }
#' #' 
#' #'       /* ── View toggle pills ────────────────────────────────────────────────── */
#' #'       .view-toggle {
#' #'         display: flex;
#' #'         gap: 6px;
#' #'         margin-bottom: 16px;
#' #'       }
#' #'       .view-btn {
#' #'         padding: 6px 18px;
#' #'         border-radius: 20px;
#' #'         border: 1.5px solid #e2e8f0;
#' #'         background: #ffffff;
#' #'         color: #64748b;
#' #'         font-size: 12px;
#' #'         font-weight: 500;
#' #'         cursor: pointer;
#' #'         transition: all .15s;
#' #'         display: flex;
#' #'         align-items: center;
#' #'         gap: 6px;
#' #'       }
#' #'       .view-btn:hover { border-color: #93c5fd; color: #2563eb; }
#' #'       .view-btn.active {
#' #'         background: #2563eb;
#' #'         border-color: #2563eb;
#' #'         color: #fff;
#' #'         box-shadow: 0 2px 8px rgba(37,99,235,.25);
#' #'       }
#' #'       .view-btn .btn-icon { font-size: 13px; }
#' #' 
#' #'       /* ── Section header ───────────────────────────────────────────────────── */
#' #'       .section-row {
#' #'         display: flex;
#' #'         align-items: center;
#' #'         justify-content: space-between;
#' #'         margin-bottom: 12px;
#' #'         padding-bottom: 10px;
#' #'         border-bottom: 1px solid #f1f5f9;
#' #'       }
#' #'       .section-left { display: flex; align-items: center; gap: 10px; }
#' #'       .section-title {
#' #'         font-size: 14px;
#' #'         font-weight: 700;
#' #'         color: #0f172a;
#' #'       }
#' #'       .section-count {
#' #'         font-size: 11px;
#' #'         font-weight: 600;
#' #'         background: #eff6ff;
#' #'         color: #2563eb;
#' #'         border-radius: 20px;
#' #'         padding: 2px 9px;
#' #'       }
#' #'       .toggle-link {
#' #'         font-size: 11px;
#' #'         font-weight: 500;
#' #'         color: #64748b;
#' #'         cursor: pointer;
#' #'         border: 1px solid #e2e8f0;
#' #'         border-radius: 5px;
#' #'         padding: 3px 9px;
#' #'         background: #f8fafc;
#' #'         transition: all .15s;
#' #'         text-decoration: none;
#' #'       }
#' #'       .toggle-link:hover { border-color: #93c5fd; color: #2563eb; background: #eff6ff; }
#' #' 
#' #'       /* ── Sheet selector ───────────────────────────────────────────────────── */
#' #'       .sheet-select-wrap {
#' #'         margin-bottom: 12px;
#' #'         max-width: 360px;
#' #'       }
#' #'       .sheet-select-wrap .form-group { margin-bottom: 0; }
#' #'       .sheet-select-wrap label { display: none; }   /* label redundant at this level */
#' #' 
#' #'       /* ── Export button ────────────────────────────────────────────────────── */
#' #'       #dl_edge {
#' #'         background: #059669;
#' #'         border: none;
#' #'         color: #fff;
#' #'         font-size: 12px;
#' #'         font-weight: 600;
#' #'         border-radius: 6px;
#' #'         padding: 6px 14px;
#' #'         display: flex;
#' #'         align-items: center;
#' #'         gap: 6px;
#' #'         transition: background .15s;
#' #'       }
#' #'       #dl_edge:hover { background: #047857; }
#' #' 
#' #'       /* ── Spinner overlay ──────────────────────────────────────────────────── */
#' #'       #pipeline-overlay {
#' #'         display: none;
#' #'         position: fixed;
#' #'         inset: 0;
#' #'         background: rgba(15,23,42,.65);
#' #'         backdrop-filter: blur(3px);
#' #'         z-index: 9999;
#' #'         align-items: center;
#' #'         justify-content: center;
#' #'         flex-direction: column;
#' #'         gap: 20px;
#' #'       }
#' #'       #pipeline-overlay.active { display: flex; }
#' #' 
#' #'       .spinner-card {
#' #'         background: #1e293b;
#' #'         border: 1px solid rgba(255,255,255,.08);
#' #'         border-radius: 14px;
#' #'         padding: 28px 36px;
#' #'         text-align: center;
#' #'         min-width: 300px;
#' #'         box-shadow: 0 24px 48px rgba(0,0,0,.4);
#' #'       }
#' #'       .spinner-title {
#' #'         font-size: 13px;
#' #'         font-weight: 600;
#' #'         color: #f1f5f9;
#' #'         margin-bottom: 20px;
#' #'         letter-spacing: .2px;
#' #'       }
#' #'       .spinner-ring {
#' #'         width: 44px; height: 44px;
#' #'         border: 4px solid rgba(255,255,255,.08);
#' #'         border-top-color: #60a5fa;
#' #'         border-radius: 50%;
#' #'         animation: spin .75s linear infinite;
#' #'         margin: 0 auto 20px;
#' #'       }
#' #'       @keyframes spin { to { transform: rotate(360deg); } }
#' #' 
#' #'       .spinner-steps { text-align: left; }
#' #'       .spinner-step {
#' #'         font-size: 12px;
#' #'         font-family: 'Inter', sans-serif;
#' #'         color: rgba(255,255,255,.3);
#' #'         padding: 4px 0;
#' #'         display: flex;
#' #'         align-items: center;
#' #'         gap: 8px;
#' #'         transition: color .25s;
#' #'       }
#' #'       .step-dot {
#' #'         width: 7px; height: 7px;
#' #'         border-radius: 50%;
#' #'         background: rgba(255,255,255,.15);
#' #'         flex-shrink: 0;
#' #'         transition: background .25s;
#' #'       }
#' #'       .spinner-step.active { color: #93c5fd; font-weight: 600; }
#' #'       .spinner-step.active .step-dot { background: #60a5fa; box-shadow: 0 0 6px #60a5fa; }
#' #'       .spinner-step.done   { color: rgba(255,255,255,.55); }
#' #'       .spinner-step.done .step-dot { background: #86efac; }
#' #'       .spinner-step.done .step-label::after { content: '  ✓'; color: #86efac; font-size: 11px; }
#' #' 
#' #'       /* ── DataTables ───────────────────────────────────────────────────────── */
#' #'       .dataTables_wrapper { font-size: 12px; color: #334155; }
#' #' 
#' #'       .dataTables_wrapper .dataTables_filter,
#' #'       .dataTables_wrapper .dataTables_length {
#' #'         margin-bottom: 10px;
#' #'       }
#' #'       .dataTables_wrapper .dataTables_filter input,
#' #'       .dataTables_wrapper .dataTables_length select {
#' #'         border: 1px solid #e2e8f0;
#' #'         border-radius: 6px;
#' #'         padding: 4px 9px;
#' #'         font-size: 12px;
#' #'         color: #334155;
#' #'         background: #f8fafc;
#' #'         outline: none;
#' #'         transition: border .15s;
#' #'       }
#' #'       .dataTables_wrapper .dataTables_filter input:focus,
#' #'       .dataTables_wrapper .dataTables_length select:focus {
#' #'         border-color: #93c5fd;
#' #'       }
#' #'       .dataTables_wrapper .dataTables_info { font-size: 11px; color: #94a3b8; }
#' #' 
#' #'       table.dataTable {
#' #'         border-collapse: separate !important;
#' #'         border-spacing: 0;
#' #'         width: 100% !important;
#' #'         border-radius: 8px;
#' #'         overflow: hidden;
#' #'         border: 1px solid #e2e8f0;
#' #'       }
#' #'       table.dataTable thead tr th {
#' #'         background: #f8fafc !important;
#' #'         color: #475569 !important;
#' #'         font-weight: 700 !important;
#' #'         font-size: 10.5px !important;
#' #'         text-transform: uppercase !important;
#' #'         letter-spacing: .5px !important;
#' #'         border-bottom: 1px solid #e2e8f0 !important;
#' #'         padding: 9px 12px !important;
#' #'         white-space: nowrap;
#' #'       }
#' #'       table.dataTable tbody td {
#' #'         padding: 8px 12px !important;
#' #'         border-bottom: 1px solid #f1f5f9 !important;
#' #'         vertical-align: middle;
#' #'         color: #334155;
#' #'       }
#' #'       table.dataTable tbody tr:last-child td { border-bottom: none !important; }
#' #'       table.dataTable tbody tr.odd  { background: #ffffff !important; }
#' #'       table.dataTable tbody tr.even { background: #fafbfc !important; }
#' #'       table.dataTable tbody tr:hover td { background: #eff6ff !important; color: #1e40af; }
#' #' 
#' #'       .dataTables_paginate { margin-top: 10px; }
#' #'       .dataTables_paginate .paginate_button {
#' #'         border-radius: 5px !important;
#' #'         font-size: 12px !important;
#' #'         padding: 3px 9px !important;
#' #'         border: 1px solid transparent !important;
#' #'         color: #475569 !important;
#' #'         transition: all .12s;
#' #'       }
#' #'       .dataTables_paginate .paginate_button:hover {
#' #'         background: #eff6ff !important;
#' #'         border-color: #bfdbfe !important;
#' #'         color: #2563eb !important;
#' #'       }
#' #'       .dataTables_paginate .paginate_button.current,
#' #'       .dataTables_paginate .paginate_button.current:hover {
#' #'         background: #2563eb !important;
#' #'         border-color: #2563eb !important;
#' #'         color: #fff !important;
#' #'         box-shadow: 0 1px 4px rgba(37,99,235,.3);
#' #'       }
#' #'       .dataTables_paginate .paginate_button.disabled,
#' #'       .dataTables_paginate .paginate_button.disabled:hover {
#' #'         color: #cbd5e1 !important;
#' #'         cursor: default;
#' #'       }
#' #' 
#' #'       /* ── Shiny overrides ──────────────────────────────────────────────────── */
#' #'       .container-fluid { padding: 0 !important; }
#' #'       .shiny-title-panel { display: none; }   /* hidden — we use top-bar instead */
#' #'       .row { margin: 0 !important; }
#' #'       .col-sm-3, .col-sm-9, .col-sm-12 { padding: 0 !important; }
#' #'     "))
#' #'   ),
#' #'   
#' #'   # ── Top nav bar ──────────────────────────────────────────────────────────────
#' #'   div(class = "top-bar",
#' #'       div(class = "top-bar-left",
#' #'           div(class = "top-bar-logo", "Research ", tags$span("Finance Tool")),
#' #'           div(class = "top-bar-divider"),
#' #'           div(class = "top-bar-badge",
#' #'               div(class = "badge-dot"),
#' #'               "MVP — Preview Build"
#' #'           )
#' #'       ),
#' #'       div(class = "top-bar-right",
#' #'           paste0("v0.1-alpha  ·  ", format(Sys.Date(), "%B %Y"))
#' #'       )
#' #'   ),
#' #'   
#' #'   # ── MVP notice banner ─────────────────────────────────────────────────────────
#' #'   div(class = "mvp-banner",
#' #'       div(class = "mvp-banner-text",
#' #'           tags$strong("This is an early-access MVP for internal review only. "),
#' #'           "Core pipeline logic is validated; the full production system — including ",
#' #'           "authentication, audit trails, and role-based access — is currently in development. ",
#' #'           "Results should be reviewed before use."
#' #'       ),
#' #'       div(class = "mvp-banner-pill", "Production release in development")
#' #'   ),
#' #'   
#' #'   # ── Spinner overlay ───────────────────────────────────────────────────────────
#' #'   div(id = "pipeline-overlay",
#' #'       div(class = "spinner-card",
#' #'           div(class = "spinner-title", "Running pipeline…"),
#' #'           div(class = "spinner-ring"),
#' #'           div(class = "spinner-steps",
#' #'               div(id = "step-1", class = "spinner-step",
#' #'                   div(class = "step-dot"), span(class = "step-label", "Processing workbook")),
#' #'               div(id = "step-2", class = "spinner-step",
#' #'                   div(class = "step-dot"), span(class = "step-label", "Generating posting plan")),
#' #'               div(id = "step-3", class = "spinner-step",
#' #'                   div(class = "step-dot"), span(class = "step-label", "Adjusting amounts")),
#' #'               div(id = "step-4", class = "spinner-step",
#' #'                   div(class = "step-dot"), span(class = "step-label", "Assigning EDGE keys")),
#' #'               div(id = "step-5", class = "spinner-step",
#' #'                   div(class = "step-dot"), span(class = "step-label", "Building EDGE templates"))
#' #'           )
#' #'       )
#' #'   ),
#' #'   
#' #'   # ── JS: overlay control ───────────────────────────────────────────────────────
#' #'   tags$script(HTML("
#' #'     Shiny.addCustomMessageHandler('pipeline_step', function(data) {
#' #'       var overlay = document.getElementById('pipeline-overlay');
#' #'       if (data.step === 0) {
#' #'         overlay.classList.add('active');
#' #'         document.querySelectorAll('.spinner-step').forEach(function(el) {
#' #'           el.classList.remove('active', 'done');
#' #'         });
#' #'       } else if (data.step === -1) {
#' #'         overlay.classList.remove('active');
#' #'       } else {
#' #'         for (var i = 1; i < data.step; i++) {
#' #'           var prev = document.getElementById('step-' + i);
#' #'           if (prev) { prev.classList.remove('active'); prev.classList.add('done'); }
#' #'         }
#' #'         var cur = document.getElementById('step-' + data.step);
#' #'         if (cur) { cur.classList.add('active'); cur.classList.remove('done'); }
#' #'       }
#' #'     });
#' #' 
#' #'     function toggleView(v) {
#' #'       Shiny.setInputValue('view_mode', v, { priority: 'event' });
#' #'       ['posting', 'edge'].forEach(function(b) {
#' #'         var el = document.getElementById('btn_' + b);
#' #'         if (el) el.classList.toggle('active', b === v);
#' #'       });
#' #'     }
#' #'   ")),
#' #'   
#' #'   # ── Page wrap ─────────────────────────────────────────────────────────────────
#' #'   div(class = "page-wrap",
#' #'       
#' #'       # ── Sidebar ──────────────────────────────────────────────────────────────
#' #'       div(class = "sidebar-card",
#' #'           
#' #'           div(class = "card",
#' #'               div(class = "card-label", "Input"),
#' #'               fileInput("file", "Workbook", accept = ".xlsx", buttonLabel = "Browse…"),
#' #'               selectInput("scenario", "Scenario", choices = get_scenarios()),
#' #'               actionButton("run", icon("play"), "Run Pipeline")
#' #'           ),
#' #'           
#' #'           div(class = "card",
#' #'               div(class = "card-label", "Pipeline Log"),
#' #'               div(class = "log-box", verbatimTextOutput("log"))
#' #'           )
#' #'       ),
#' #'       
#' #'       # ── Main ─────────────────────────────────────────────────────────────────
#' #'       div(class = "main-area",
#' #'           div(class = "card",
#' #'               
#' #'               # View toggle
#' #'               div(class = "view-toggle",
#' #'                   tags$button(id = "btn_posting", class = "view-btn active",
#' #'                               onclick = "toggleView('posting')",
#' #'                               tags$span(class = "btn-icon", "☰"),
#' #'                               "Posting Lines"),
#' #'                   tags$button(id = "btn_edge", class = "view-btn",
#' #'                               onclick = "toggleView('edge')",
#' #'                               tags$span(class = "btn-icon", "⬡"),
#' #'                               "EDGE Templates")
#' #'               ),
#' #'               
#' #'               # ── Posting lines panel ─────────────────────────────────────────────
#' #'               conditionalPanel("input.view_mode === 'posting' || !input.view_mode",
#' #'                                div(class = "section-row",
#' #'                                    div(class = "section-left",
#' #'                                        span(class = "section-title", "Posting Lines"),
#' #'                                        uiOutput("posting_count_badge")
#' #'                                    ),
#' #'                                    tags$a(class = "toggle-link", id = "posting-toggle",
#' #'                                           onclick = "
#' #'                      var el  = document.getElementById('posting-body');
#' #'                      var lnk = document.getElementById('posting-toggle');
#' #'                      var vis = el.style.display !== 'none';
#' #'                      el.style.display  = vis ? 'none' : '';
#' #'                      lnk.textContent   = vis ? 'Show' : 'Hide';
#' #'                    ", "Hide")
#' #'                                ),
#' #'                                div(id = "posting-body",
#' #'                                    div(class = "sheet-select-wrap", uiOutput("posting_sheet_select")),
#' #'                                    DTOutput("posting_table")
#' #'                                )
#' #'               ),
#' #'               
#' #'               # ── EDGE templates panel ────────────────────────────────────────────
#' #'               conditionalPanel("input.view_mode === 'edge'",
#' #'                                div(class = "section-row",
#' #'                                    div(class = "section-left",
#' #'                                        span(class = "section-title", "EDGE Templates"),
#' #'                                        uiOutput("edge_count_badge")
#' #'                                    ),
#' #'                                    downloadButton("dl_edge", icon = icon("download"), label = "Export .xlsx")
#' #'                                ),
#' #'                                div(class = "sheet-select-wrap", uiOutput("edge_sheet_select")),
#' #'                                DTOutput("edge_table")
#' #'               )
#' #'           )
#' #'       )
#' #'   )
#' #' )
#' #' 
#' #' # ── Server ─────────────────────────────────────────────────────────────────────
#' #' 
#' #' server <- function(input, output, session) {
#' #'   
#' #'   log_lines      <- reactiveVal(character(0))
#' #'   posting_adj    <- reactiveVal(NULL)
#' #'   edge_templates <- reactiveVal(NULL)
#' #'   
#' #'   step <- function(n) session$sendCustomMessage("pipeline_step", list(step = n))
#' #'   
#' #'   log <- function(...) {
#' #'     msg <- paste0("[", format(Sys.time(), "%H:%M:%S"), "] ", ...)
#' #'     log_lines(c(log_lines(), msg))
#' #'   }
#' #'   
#' #'   output$log <- renderText(paste(log_lines(), collapse = "\n"))
#' #'   
#' #'   # ── Run pipeline ──────────────────────────────────────────────────────────────
#' #'   observeEvent(input$run, {
#' #'     req(input$file)
#' #'     log_lines(character(0))
#' #'     posting_adj(NULL)
#' #'     edge_templates(NULL)
#' #'     step(0)
#' #'     
#' #'     withCallingHandlers(
#' #'       tryCatch({
#' #'         log("Starting pipeline...")
#' #'         
#' #'         step(1)
#' #'         log("Stage A/B: processing workbook...")
#' #'         processed <- process_workbook(
#' #'           input_path = input$file$datapath,
#' #'           db_dir     = DATA_DIR
#' #'         )
#' #'         
#' #'         step(2)
#' #'         log("Generating posting plan (scenario ", input$scenario, ")...")
#' #'         out <- generate_posting_plan(
#' #'           ict           = processed,
#' #'           rules_db_path = RULES_DB_PATH,
#' #'           scenario_id   = input$scenario,
#' #'           ict_db_path   = ICT_DB_PATH
#' #'         )
#' #'         
#' #'         step(3)
#' #'         log("Adjusting posting amounts...")
#' #'         adj <- adjust_posting_lines(out)
#' #'         
#' #'         step(4)
#' #'         log("Assigning EDGE keys...")
#' #'         adj <- assign_edge_keys(adj)
#' #'         posting_adj(adj)
#' #'         
#' #'         step(5)
#' #'         log("Building EDGE templates...")
#' #'         templates <- build_all_edge_templates(adj)
#' #'         edge_templates(templates)
#' #'         
#' #'         step(-1)
#' #'         log("Done — ", nrow(adj), " posting lines · ", length(templates), " EDGE templates.")
#' #'         
#' #'       }, error = function(e) {
#' #'         step(-1)
#' #'         log("ERROR: ", conditionMessage(e))
#' #'       }),
#' #'       message = function(m) {
#' #'         log(trimws(conditionMessage(m)))
#' #'         invokeRestart("muffleMessage")
#' #'       }
#' #'     )
#' #'   })
#' #'   
#' #'   # ── Posting lines ──────────────────────────────────────────────────────────────
#' #'   posting_sheets <- reactive({
#' #'     req(posting_adj())
#' #'     sort(unique(posting_adj()$sheet_name))
#' #'   })
#' #'   
#' #'   output$posting_count_badge <- renderUI({
#' #'     req(posting_adj())
#' #'     span(class = "section-count", format(nrow(posting_adj()), big.mark = ","), " rows")
#' #'   })
#' #'   
#' #'   output$posting_sheet_select <- renderUI({
#' #'     req(posting_sheets())
#' #'     selectInput("posting_sheet", NULL, choices = posting_sheets())
#' #'   })
#' #'   
#' #'   output$posting_table <- renderDT({
#' #'     req(posting_adj(), input$posting_sheet)
#' #'     df <- posting_adj() %>% filter(sheet_name == input$posting_sheet)
#' #'     
#' #'     money_cols <- intersect(
#' #'       c("posting_amount", "adjusted_amount", "contract_price",
#' #'         "contract_cost", "base_sum", "activity_cost_num"),
#' #'       names(df)
#' #'     )
#' #'     
#' #'     dt <- datatable(
#' #'       df,
#' #'       options  = list(
#' #'         pageLength = 20,
#' #'         scrollX    = TRUE,
#' #'         dom        = "<'row'<'col-sm-6'l><'col-sm-6'f>>tip",
#' #'         autoWidth  = FALSE
#' #'       ),
#' #'       rownames = FALSE,
#' #'       class    = "stripe hover"
#' #'     )
#' #'     
#' #'     if (length(money_cols) > 0)
#' #'       dt <- formatCurrency(dt, columns = money_cols, currency = "£", digits = 2)
#' #'     
#' #'     num_cols <- setdiff(names(df)[sapply(df, is.numeric)], money_cols)
#' #'     if (length(num_cols) > 0)
#' #'       dt <- formatRound(dt, columns = num_cols, digits = 4)
#' #'     
#' #'     dt
#' #'   })
#' #'   
#' #'   # ── EDGE templates ─────────────────────────────────────────────────────────────
#' #'   output$edge_count_badge <- renderUI({
#' #'     req(edge_templates())
#' #'     span(class = "section-count", length(edge_templates()), " templates")
#' #'   })
#' #'   
#' #'   output$edge_sheet_select <- renderUI({
#' #'     req(edge_templates())
#' #'     selectInput("edge_sheet", NULL, choices = names(edge_templates()))
#' #'   })
#' #'   
#' #'   output$edge_table <- renderDT({
#' #'     req(edge_templates(), input$edge_sheet)
#' #'     df <- edge_templates()[[input$edge_sheet]]
#' #'     
#' #'     money_cols <- intersect(c("Default Cost", "Overhead Cost", "total"), names(df))
#' #'     
#' #'     dt <- datatable(
#' #'       df,
#' #'       options  = list(
#' #'         pageLength = 20,
#' #'         scrollX    = TRUE,
#' #'         dom        = "<'row'<'col-sm-6'l><'col-sm-6'f>>tip",
#' #'         autoWidth  = FALSE
#' #'       ),
#' #'       rownames = FALSE,
#' #'       class    = "stripe hover"
#' #'     )
#' #'     
#' #'     if (length(money_cols) > 0)
#' #'       dt <- formatCurrency(dt, columns = money_cols, currency = "£", digits = 2)
#' #'     
#' #'     dt
#' #'   })
#' #'   
#' #'   # ── Export ─────────────────────────────────────────────────────────────────────
#' #'   output$dl_edge <- downloadHandler(
#' #'     filename = function() {
#' #'       paste0("EDGE_templates_scenario_", input$scenario, "_",
#' #'              format(Sys.Date(), "%Y%m%d"), ".xlsx")
#' #'     },
#' #'     content = function(file) {
#' #'       req(edge_templates())
#' #'       wb <- createWorkbook()
#' #'       for (nm in names(edge_templates())) {
#' #'         safe_nm <- substr(gsub("[\\[\\]\\*\\?:/\\\\]", "_", nm), 1, 31)
#' #'         addWorksheet(wb, safe_nm)
#' #'         writeData(wb, safe_nm, edge_templates()[[nm]])
#' #'       }
#' #'       saveWorkbook(wb, file, overwrite = TRUE)
#' #'     }
#' #'   )
#' #' }
#' #' 
#' #' shinyApp(ui, server)
#' 
#' #' # ── Helpers ────────────────────────────────────────────────────────────────────
#' #' 
#' #' get_scenarios <- function() {
#' #'   tryCatch({
#' #'     con <- dbConnect(duckdb::duckdb(), dbdir = RULES_DB_PATH, read_only = TRUE)
#' #'     on.exit(dbDisconnect(con, shutdown = TRUE))
#' #'     dbGetQuery(con, "SELECT DISTINCT scenario_id FROM dist_rules ORDER BY scenario_id;")$scenario_id
#' #'   }, error = function(e) LETTERS[1:8])
#' #' }
#' #' 
#' #' adjust_posting_lines <- function(out) {
#' #'   out %>%
#' #'     mutate(contract_price = round(contract_cost, 0)) %>%
#' #'     group_by(row_id, scenario_id) %>%
#' #'     mutate(
#' #'       base_sum        = sum(posting_amount, na.rm = TRUE),
#' #'       contract_price  = first(contract_price),
#' #'       multiplier      = if_else(base_sum == 0, NA_real_, contract_price / base_sum),
#' #'       adjusted_amount = if_else(base_sum == 0, 0, round(posting_amount * multiplier, 2))
#' #'     ) %>%
#' #'     mutate(
#' #'       residual        = round(contract_price - sum(adjusted_amount, na.rm = TRUE), 2),
#' #'       has_direct      = any(posting_line_type_id == "DIRECT"),
#' #'       is_residual_row = if_else(
#' #'         has_direct,
#' #'         posting_line_type_id == "DIRECT" & row_number() == min(which(posting_line_type_id == "DIRECT")),
#' #'         row_number() == 1L
#' #'       ),
#' #'       adjusted_amount = if_else(
#' #'         is_residual_row,
#' #'         round(adjusted_amount + residual, 2),
#' #'         adjusted_amount
#' #'       )
#' #'     ) %>%
#' #'     mutate(
#' #'       adjusted_sum_check = round(sum(adjusted_amount, na.rm = TRUE), 2),
#' #'       diff_check         = round(contract_price - adjusted_sum_check, 2)
#' #'     ) %>%
#' #'     select(-has_direct) %>%
#' #'     ungroup()
#' #' }
#' #' 
#' #' assign_edge_keys <- function(data) {
#' #'   special_sheets <- c("Unscheduled Activities", "Setup & Closedown", "Pharmacy")
#' #'   special_keys <- data %>%
#' #'     filter(sheet_name %in% special_sheets) %>%
#' #'     distinct(sheet_name, Activity, row_id, staff_group, Study_Arm) %>%
#' #'     mutate(edge_key = paste0("EDGE-", str_pad(row_number(), width = 4, pad = "0")))
#' #'   main_keys <- data %>%
#' #'     filter(!sheet_name %in% special_sheets) %>%
#' #'     distinct(Study_Arm, Visit) %>%
#' #'     mutate(edge_key = paste0("EDGE-", str_pad(row_number() + nrow(special_keys), width = 4, pad = "0")))
#' #'   data %>%
#' #'     left_join(special_keys, by = c("sheet_name", "Activity", "row_id", "staff_group", "Study_Arm")) %>%
#' #'     left_join(main_keys,    by = c("Study_Arm", "Visit")) %>%
#' #'     mutate(edge_key = coalesce(edge_key.x, edge_key.y)) %>%
#' #'     select(-edge_key.x, -edge_key.y)
#' #' }
#' #' 
#' #' # ── Currency formatter for DT ──────────────────────────────────────────────────
#' #' fmt_currency_cols <- function(dt, cols) {
#' #'   formatCurrency(dt, columns = cols, currency = "£", digits = 2)
#' #' }
#' #' 
#' #' # ── UI ─────────────────────────────────────────────────────────────────────────
#' #' 
#' #' ui <- fluidPage(
#' #'   
#' #'   tags$head(
#' #'     tags$style(HTML("
#' #' 
#' #'       /* ── Base ── */
#' #'       body {
#' #'         font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
#' #'         font-size: 13px;
#' #'         background: #f4f6f9;
#' #'         color: #2c3e50;
#' #'       }
#' #' 
#' #'       h2 { font-size: 18px; font-weight: 700; color: #1a253c; margin-bottom: 4px; }
#' #' 
#' #'       /* ── Sidebar ── */
#' #'       .well {
#' #'         background: #ffffff;
#' #'         border: 1px solid #e0e4ea;
#' #'         border-radius: 8px;
#' #'         box-shadow: 0 1px 4px rgba(0,0,0,.06);
#' #'         padding: 18px;
#' #'       }
#' #' 
#' #'       /* ── Run button ── */
#' #'       #run {
#' #'         background: #2563eb;
#' #'         border-color: #2563eb;
#' #'         color: #fff;
#' #'         font-weight: 600;
#' #'         letter-spacing: .3px;
#' #'         border-radius: 6px;
#' #'         transition: background .15s;
#' #'       }
#' #'       #run:hover { background: #1d4ed8; border-color: #1d4ed8; }
#' #'       #run:disabled { background: #93b4f5; border-color: #93b4f5; cursor: not-allowed; }
#' #' 
#' #'       /* ── Log box ── */
#' #'       .log-box {
#' #'         font-size: 11px;
#' #'         font-family: 'Courier New', monospace;
#' #'         background: #0f172a;
#' #'         color: #86efac;
#' #'         padding: 10px 12px;
#' #'         border-radius: 6px;
#' #'         min-height: 90px;
#' #'         max-height: 200px;
#' #'         overflow-y: auto;
#' #'         white-space: pre-wrap;
#' #'         line-height: 1.5;
#' #'         border: 1px solid #1e293b;
#' #'       }
#' #' 
#' #'       /* ── Spinner overlay ── */
#' #'       #pipeline-overlay {
#' #'         display: none;
#' #'         position: fixed;
#' #'         inset: 0;
#' #'         background: rgba(15, 23, 42, 0.55);
#' #'         z-index: 9999;
#' #'         align-items: center;
#' #'         justify-content: center;
#' #'         flex-direction: column;
#' #'         gap: 16px;
#' #'       }
#' #'       #pipeline-overlay.active { display: flex; }
#' #' 
#' #'       .spinner-ring {
#' #'         width: 56px; height: 56px;
#' #'         border: 5px solid rgba(255,255,255,.15);
#' #'         border-top-color: #60a5fa;
#' #'         border-radius: 50%;
#' #'         animation: spin .8s linear infinite;
#' #'       }
#' #'       @keyframes spin { to { transform: rotate(360deg); } }
#' #' 
#' #'       .spinner-steps {
#' #'         color: #e2e8f0;
#' #'         font-size: 13px;
#' #'         font-family: 'Courier New', monospace;
#' #'         text-align: center;
#' #'         max-width: 320px;
#' #'         line-height: 1.6;
#' #'       }
#' #'       .spinner-step { opacity: .45; transition: opacity .3s; }
#' #'       .spinner-step.active { opacity: 1; font-weight: 700; color: #93c5fd; }
#' #'       .spinner-step.done   { opacity: .7; }
#' #'       .spinner-step.done::after { content: ' ✓'; color: #86efac; }
#' #' 
#' #'       /* ── Main card ── */
#' #'       .main-card {
#' #'         background: #ffffff;
#' #'         border: 1px solid #e0e4ea;
#' #'         border-radius: 8px;
#' #'         box-shadow: 0 1px 4px rgba(0,0,0,.06);
#' #'         padding: 18px 20px;
#' #'         min-height: 400px;
#' #'       }
#' #' 
#' #'       /* ── View toggle ── */
#' #'       .view-toggle {
#' #'         display: flex;
#' #'         gap: 6px;
#' #'         margin-bottom: 14px;
#' #'       }
#' #'       .view-btn {
#' #'         padding: 5px 16px;
#' #'         border-radius: 20px;
#' #'         border: 1.5px solid #cbd5e1;
#' #'         background: #f1f5f9;
#' #'         color: #475569;
#' #'         font-size: 12px;
#' #'         font-weight: 500;
#' #'         cursor: pointer;
#' #'         transition: all .15s;
#' #'       }
#' #'       .view-btn.active {
#' #'         background: #2563eb;
#' #'         border-color: #2563eb;
#' #'         color: #fff;
#' #'       }
#' #' 
#' #'       /* ── Section header row ── */
#' #'       .section-row {
#' #'         display: flex;
#' #'         align-items: center;
#' #'         justify-content: space-between;
#' #'         margin-bottom: 10px;
#' #'       }
#' #'       .section-title {
#' #'         font-size: 14px;
#' #'         font-weight: 700;
#' #'         color: #1e3a5f;
#' #'         letter-spacing: .2px;
#' #'       }
#' #' 
#' #'       /* ── Sheet selector ── */
#' #'       .sheet-select { max-width: 340px; }
#' #' 
#' #'       /* ── Toggle link ── */
#' #'       .toggle-link {
#' #'         font-size: 12px;
#' #'         color: #2563eb;
#' #'         cursor: pointer;
#' #'         text-decoration: none;
#' #'         margin-left: 12px;
#' #'       }
#' #'       .toggle-link:hover { text-decoration: underline; }
#' #' 
#' #'       /* ── Export button ── */
#' #'       #dl_edge {
#' #'         background: #059669;
#' #'         border-color: #059669;
#' #'         color: #fff;
#' #'         font-size: 12px;
#' #'         font-weight: 600;
#' #'         border-radius: 6px;
#' #'         padding: 5px 14px;
#' #'       }
#' #'       #dl_edge:hover { background: #047857; border-color: #047857; }
#' #' 
#' #'       /* ── DataTables tweaks ── */
#' #'       .dataTables_wrapper { font-size: 12px; }
#' #'       .dataTables_wrapper .dataTables_filter input {
#' #'         border: 1px solid #cbd5e1;
#' #'         border-radius: 5px;
#' #'         padding: 3px 8px;
#' #'         font-size: 12px;
#' #'       }
#' #'       .dataTables_wrapper .dataTables_length select {
#' #'         border: 1px solid #cbd5e1;
#' #'         border-radius: 5px;
#' #'         font-size: 12px;
#' #'         padding: 2px 4px;
#' #'       }
#' #'       table.dataTable thead th {
#' #'         background: #f1f5f9;
#' #'         color: #334155;
#' #'         font-weight: 700;
#' #'         font-size: 11px;
#' #'         text-transform: uppercase;
#' #'         letter-spacing: .4px;
#' #'         border-bottom: 2px solid #cbd5e1 !important;
#' #'         white-space: nowrap;
#' #'       }
#' #'       table.dataTable tbody tr:hover { background: #eff6ff !important; }
#' #'       table.dataTable tbody tr.odd  { background: #fafafa; }
#' #'       table.dataTable tbody tr.even { background: #ffffff; }
#' #'       .dataTables_paginate .paginate_button.current {
#' #'         background: #2563eb !important;
#' #'         border-color: #2563eb !important;
#' #'         color: #fff !important;
#' #'         border-radius: 4px;
#' #'       }
#' #'       .dataTables_paginate .paginate_button:hover {
#' #'         background: #eff6ff !important;
#' #'         border-color: #93c5fd !important;
#' #'         color: #1d4ed8 !important;
#' #'         border-radius: 4px;
#' #'       }
#' #'     "))
#' #'   ),
#' #'   
#' #'   # ── Spinner overlay (shown via JS) ──────────────────────────────────────────
#' #'   div(id = "pipeline-overlay",
#' #'       div(class = "spinner-ring"),
#' #'       div(class = "spinner-steps",
#' #'           div(id = "step-1", class = "spinner-step", "① Processing workbook"),
#' #'           div(id = "step-2", class = "spinner-step", "② Generating posting plan"),
#' #'           div(id = "step-3", class = "spinner-step", "③ Adjusting amounts"),
#' #'           div(id = "step-4", class = "spinner-step", "④ Assigning EDGE keys"),
#' #'           div(id = "step-5", class = "spinner-step", "⑤ Building EDGE templates")
#' #'       )
#' #'   ),
#' #'   
#' #'   # ── JS: overlay + step highlighting ─────────────────────────────────────────
#' #'   tags$script(HTML("
#' #'     Shiny.addCustomMessageHandler('pipeline_step', function(data) {
#' #'       var overlay = document.getElementById('pipeline-overlay');
#' #'       if (data.step === 0) {
#' #'         // start
#' #'         overlay.classList.add('active');
#' #'         document.querySelectorAll('.spinner-step').forEach(function(el) {
#' #'           el.classList.remove('active','done');
#' #'         });
#' #'       } else if (data.step === -1) {
#' #'         // done or error
#' #'         overlay.classList.remove('active');
#' #'       } else {
#' #'         for (var i = 1; i < data.step; i++) {
#' #'           var prev = document.getElementById('step-' + i);
#' #'           if (prev) { prev.classList.remove('active'); prev.classList.add('done'); }
#' #'         }
#' #'         var cur = document.getElementById('step-' + data.step);
#' #'         if (cur) { cur.classList.add('active'); cur.classList.remove('done'); }
#' #'       }
#' #'     });
#' #'   ")),
#' #'   
#' #'   titlePanel("Research Finance Tool"),
#' #'   
#' #'   sidebarLayout(
#' #'     sidebarPanel(width = 3,
#' #'                  
#' #'                  fileInput("file", "Upload ICT Workbook (.xlsx)",
#' #'                            accept = ".xlsx", buttonLabel = "Browse…"),
#' #'                  selectInput("scenario", "Scenario", choices = get_scenarios()),
#' #'                  actionButton("run", "Run Pipeline", icon = icon("play"), width = "100%"),
#' #'                  
#' #'                  hr(style = "border-color:#e2e8f0; margin: 14px 0;"),
#' #'                  
#' #'                  tags$label("Pipeline Log", style = "font-size:11px; font-weight:600;
#' #'                   color:#64748b; text-transform:uppercase; letter-spacing:.5px;"),
#' #'                  div(class = "log-box", verbatimTextOutput("log"))
#' #'     ),
#' #'     
#' #'     mainPanel(width = 9,
#' #'               div(class = "main-card",
#' #'                   
#' #'                   # ── View toggle ────────────────────────────────────────────────────────
#' #'                   div(class = "view-toggle",
#' #'                       actionButton("btn_posting", "Posting Lines", class = "view-btn active",
#' #'                                    onclick = "toggleView('posting')"),
#' #'                       actionButton("btn_edge",    "EDGE Templates", class = "view-btn",
#' #'                                    onclick = "toggleView('edge')")
#' #'                   ),
#' #'                   tags$script(HTML("
#' #'           function toggleView(v) {
#' #'             Shiny.setInputValue('view_mode', v, {priority: 'event'});
#' #'             ['posting','edge'].forEach(function(b) {
#' #'               var el = document.getElementById('btn_' + b);
#' #'               if (el) el.classList.toggle('active', b === v);
#' #'             });
#' #'           }
#' #'         ")),
#' #'                   
#' #'                   # ── POSTING LINES ──────────────────────────────────────────────────────
#' #'                   conditionalPanel("input.view_mode === 'posting' || !input.view_mode",
#' #'                                    div(class = "section-row",
#' #'                                        span(class = "section-title", "Posting Lines"),
#' #'                                        tags$a(class = "toggle-link", id = "posting-toggle",
#' #'                                               onclick = "
#' #'                      var el = document.getElementById('posting-body');
#' #'                      var lnk = document.getElementById('posting-toggle');
#' #'                      if (el.style.display === 'none') {
#' #'                        el.style.display = '';
#' #'                        lnk.textContent = 'Hide';
#' #'                      } else {
#' #'                        el.style.display = 'none';
#' #'                        lnk.textContent = 'Show';
#' #'                      }
#' #'                    ", "Hide")
#' #'                                    ),
#' #'                                    div(id = "posting-body",
#' #'                                        div(class = "sheet-select", uiOutput("posting_sheet_select")),
#' #'                                        DTOutput("posting_table")
#' #'                                    )
#' #'                   ),
#' #'                   
#' #'                   # ── EDGE TEMPLATES ─────────────────────────────────────────────────────
#' #'                   conditionalPanel("input.view_mode === 'edge'",
#' #'                                    div(class = "section-row",
#' #'                                        span(class = "section-title", "EDGE Templates"),
#' #'                                        downloadButton("dl_edge", icon = icon("download"),
#' #'                                                       label = "Export .xlsx")
#' #'                                    ),
#' #'                                    div(class = "sheet-select", uiOutput("edge_sheet_select")),
#' #'                                    DTOutput("edge_table")
#' #'                   )
#' #'               )
#' #'     )
#' #'   )
#' #' )
#' #' 
#' #' # ── Server ─────────────────────────────────────────────────────────────────────
#' #' 
#' #' server <- function(input, output, session) {
#' #'   
#' #'   log_lines      <- reactiveVal(character(0))
#' #'   posting_adj    <- reactiveVal(NULL)
#' #'   edge_templates <- reactiveVal(NULL)
#' #'   
#' #'   step <- function(n) session$sendCustomMessage("pipeline_step", list(step = n))
#' #'   
#' #'   log <- function(...) {
#' #'     msg <- paste0("[", format(Sys.time(), "%H:%M:%S"), "] ", ...)
#' #'     log_lines(c(log_lines(), msg))
#' #'   }
#' #'   
#' #'   output$log <- renderText(paste(log_lines(), collapse = "\n"))
#' #'   
#' #'   # ── View mode default ────────────────────────────────────────────────────────
#' #'   observe({
#' #'     if (is.null(input$view_mode)) updateTextInput(session, "view_mode", value = "posting")
#' #'   })
#' #'   
#' #'   # ── Run pipeline ─────────────────────────────────────────────────────────────
#' #'   observeEvent(input$run, {
#' #'     req(input$file)
#' #'     log_lines(character(0))
#' #'     posting_adj(NULL)
#' #'     edge_templates(NULL)
#' #'     step(0)   # show overlay
#' #'     
#' #'     withCallingHandlers(
#' #'       tryCatch({
#' #'         
#' #'         log("Starting pipeline...")
#' #'         
#' #'         step(1)
#' #'         log("Stage A/B: processing workbook...")
#' #'         processed <- process_workbook(
#' #'           input_path = input$file$datapath,
#' #'           db_dir     = DATA_DIR
#' #'         )
#' #'         
#' #'         step(2)
#' #'         log("Generating posting plan (scenario ", input$scenario, ")...")
#' #'         out <- generate_posting_plan(
#' #'           ict           = processed,
#' #'           rules_db_path = RULES_DB_PATH,
#' #'           scenario_id   = input$scenario,
#' #'           ict_db_path   = ICT_DB_PATH
#' #'         )
#' #'         
#' #'         step(3)
#' #'         log("Adjusting posting amounts...")
#' #'         adj <- adjust_posting_lines(out)
#' #'         
#' #'         step(4)
#' #'         log("Assigning EDGE keys...")
#' #'         adj <- assign_edge_keys(adj)
#' #'         posting_adj(adj)
#' #'         
#' #'         step(5)
#' #'         log("Building EDGE templates...")
#' #'         templates <- build_all_edge_templates(adj)
#' #'         edge_templates(templates)
#' #'         
#' #'         step(-1)  # hide overlay
#' #'         log("Done — ", nrow(adj), " posting lines, ",
#' #'             length(templates), " EDGE template sheets.")
#' #'         
#' #'       }, error = function(e) {
#' #'         step(-1)
#' #'         log("ERROR: ", conditionMessage(e))
#' #'       }),
#' #'       message = function(m) {
#' #'         log(trimws(conditionMessage(m)))
#' #'         invokeRestart("muffleMessage")
#' #'       }
#' #'     )
#' #'   })
#' #'   
#' #'   # ── Posting lines ─────────────────────────────────────────────────────────────
#' #'   posting_sheets <- reactive({
#' #'     req(posting_adj())
#' #'     sort(unique(posting_adj()$sheet_name))
#' #'   })
#' #'   
#' #'   output$posting_sheet_select <- renderUI({
#' #'     req(posting_sheets())
#' #'     selectInput("posting_sheet", "Sheet", choices = posting_sheets())
#' #'   })
#' #'   
#' #'   output$posting_table <- renderDT({
#' #'     req(posting_adj(), input$posting_sheet)
#' #'     df <- posting_adj() %>% filter(sheet_name == input$posting_sheet)
#' #'     
#' #'     # identify money cols
#' #'     money_cols <- intersect(
#' #'       c("posting_amount", "adjusted_amount", "contract_price",
#' #'         "contract_cost", "base_sum", "activity_cost_num"),
#' #'       names(df)
#' #'     )
#' #'     
#' #'     dt <- datatable(
#' #'       df,
#' #'       options = list(
#' #'         pageLength  = 20,
#' #'         scrollX     = TRUE,
#' #'         dom         = "<'row'<'col-sm-6'l><'col-sm-6'f>>tip",
#' #'         columnDefs  = list(list(className = "dt-right", targets = "_all")),
#' #'         autoWidth   = FALSE
#' #'       ),
#' #'       rownames  = FALSE,
#' #'       class     = "stripe hover compact"
#' #'     )
#' #'     
#' #'     if (length(money_cols) > 0)
#' #'       dt <- formatCurrency(dt, columns = money_cols, currency = "£", digits = 2)
#' #'     
#' #'     # round numeric non-money cols to 4dp
#' #'     num_cols <- setdiff(names(df)[sapply(df, is.numeric)], money_cols)
#' #'     if (length(num_cols) > 0)
#' #'       dt <- formatRound(dt, columns = num_cols, digits = 4)
#' #'     
#' #'     dt
#' #'   })
#' #'   
#' #'   # ── EDGE templates ────────────────────────────────────────────────────────────
#' #'   output$edge_sheet_select <- renderUI({
#' #'     req(edge_templates())
#' #'     selectInput("edge_sheet", "Template", choices = names(edge_templates()))
#' #'   })
#' #'   
#' #'   output$edge_table <- renderDT({
#' #'     req(edge_templates(), input$edge_sheet)
#' #'     df <- edge_templates()[[input$edge_sheet]]
#' #'     
#' #'     money_cols <- intersect(c("Default Cost", "Overhead Cost", "total"), names(df))
#' #'     
#' #'     dt <- datatable(
#' #'       df,
#' #'       options = list(
#' #'         pageLength = 20,
#' #'         scrollX    = TRUE,
#' #'         dom        = "<'row'<'col-sm-6'l><'col-sm-6'f>>tip",
#' #'         autoWidth  = FALSE
#' #'       ),
#' #'       rownames = FALSE,
#' #'       class    = "stripe hover compact"
#' #'     )
#' #'     
#' #'     if (length(money_cols) > 0)
#' #'       dt <- formatCurrency(dt, columns = money_cols, currency = "£", digits = 2)
#' #'     
#' #'     dt
#' #'   })
#' #'   
#' #'   # ── Export ────────────────────────────────────────────────────────────────────
#' #'   output$dl_edge <- downloadHandler(
#' #'     filename = function() {
#' #'       paste0("EDGE_templates_scenario_", input$scenario, "_",
#' #'              format(Sys.Date(), "%Y%m%d"), ".xlsx")
#' #'     },
#' #'     content = function(file) {
#' #'       req(edge_templates())
#' #'       wb <- createWorkbook()
#' #'       for (nm in names(edge_templates())) {
#' #'         safe_nm <- substr(gsub("[\\[\\]\\*\\?:/\\\\]", "_", nm), 1, 31)
#' #'         addWorksheet(wb, safe_nm)
#' #'         writeData(wb, safe_nm, edge_templates()[[nm]])
#' #'       }
#' #'       saveWorkbook(wb, file, overwrite = TRUE)
#' #'     }
#' #'   )
#' #' }
#' #' 
#' #' shinyApp(ui, server)
