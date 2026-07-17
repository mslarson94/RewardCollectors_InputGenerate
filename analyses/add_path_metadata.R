# file: add_path_metadata.R

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
})

input_csv <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain_PO/EventSegmentation/megaFiles/allIntervalDataKnotted_AN.csv"
output_csv <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain_PO/EventSegmentation/megaFiles/allIntervalData_AN_knotted_pathValueOrdered.csv"

path_mapping <- tibble::tribble(
  ~path_order_round, ~path_order_round_num_map, ~orderedCollect, ~pathValue,
  "HV -> LV -> NV",  1,                         1,               30,
  "LV -> HV -> NV",  2,                         0,               30,
  "NV -> HV -> LV",  3,                         0,               25,
  "HV -> NV -> LV",  4,                         1,               25,
  "NV -> LV -> HV",  5,                         0,               20,
  "LV -> NV -> HV",  6,                         1,               20
)

df <- read_csv(input_csv, show_col_types = FALSE)

if (!"path_order_round" %in% names(df)) {
  stop("Missing required column: path_order_round")
}

df_out <- df %>%
  left_join(path_mapping, by = "path_order_round") %>%
  mutate(
    path_order_round_num = case_when(
      !is.na(path_order_round_num_map) ~ path_order_round_num_map,
      "path_order_round_num" %in% names(df) ~ suppressWarnings(as.numeric(path_order_round_num)),
      TRUE ~ NA_real_
    )
  ) %>%
  select(-path_order_round_num_map)

matched_rows <- sum(!is.na(df_out$orderedCollect))
unmatched_rows <- sum(is.na(df_out$orderedCollect))

cat("\n=== Path metadata join summary ===\n")
cat("Input rows:   ", nrow(df), "\n")
cat("Matched rows: ", matched_rows, "\n")
cat("Unmatched rows:", unmatched_rows, "\n")

unmatched_levels <- df_out %>%
  filter(is.na(orderedCollect)) %>%
  distinct(path_order_round) %>%
  arrange(path_order_round) %>%
  pull(path_order_round)

if (length(unmatched_levels) > 0) {
  cat("\nUnmatched path_order_round levels:\n")
  cat(paste0(" - ", unmatched_levels, collapse = "\n"))
  cat("\n")
}

mapped_summary <- df_out %>%
  filter(!is.na(orderedCollect), !is.na(pathValue)) %>%
  count(path_order_round, path_order_round_num, orderedCollect, pathValue, sort = TRUE)

cat("\n=== Mapped level summary ===\n")
print(mapped_summary, n = Inf)

write_csv(df_out, output_csv)
cat("\nWrote:", output_csv, "\n")