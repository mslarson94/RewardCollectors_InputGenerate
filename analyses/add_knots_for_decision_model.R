# add_knots_for_decision_model.R
suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
})

add_knot_features <- function(df,
                              knots = c(15L, 20L, 25L),
                              totsesh_col = "TotSesh_actTest_RoundNum",
                              center = FALSE) {
  if (!totsesh_col %in% names(df)) {
    stop(sprintf("Missing required column: %s", totsesh_col), call. = FALSE)
  }
  
  t <- suppressWarnings(as.numeric(df[[totsesh_col]]))
  
  for (k in knots) {
    k <- as.integer(k)
    if (is.na(k) || k <= 0L) stop(sprintf("Knot must be positive; got %s", k), call. = FALSE)
    
    early_name <- sprintf("t_early_%d", k)
    late_name  <- sprintf("t_late_%d",  k)
    
    df[[early_name]] <- pmin(t, k)
    df[[late_name]]  <- pmax(t - k, 0)
    
    if (isTRUE(center)) {
      df[[paste0(early_name, "_c")]] <- df[[early_name]] - mean(df[[early_name]], na.rm = TRUE)
      df[[paste0(late_name, "_c")]]  <- df[[late_name]]  - mean(df[[late_name]],  na.rm = TRUE)
    }
  }
  
  df
}

# CLI usage example (edit paths)
# in_csv  <- "input.csv"
# out_csv <- "output.csv"
# df <- read_csv(in_csv, show_col_types = FALSE)
# df2 <- add_knot_features(df, knots = c(15,20,25), center = TRUE)
# write_csv(df2, out_csv)