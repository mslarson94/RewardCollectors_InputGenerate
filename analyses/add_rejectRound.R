# file: add_rejectWalkDist.R

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
})


output_csv <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain_PO/EventSegmentation/megaFiles/allIntervalData_AN_with_rejectWalkDist.csv"
input_csv <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain_PO/EventSegmentation/megaFiles/allIntervalData_AN_knotted_pathValueOrdered.csv"

df <- read_csv(input_csv, show_col_types = FALSE)

df_out <- df %>%
  group_by(roundID, roundID_int) %>%
  mutate(
    rejectWalkDist = as.integer(any(!is.na(WalkDist) & WalkDist <= 0.5))
  ) %>%
  ungroup()

n_flagged_rows <- sum(df_out$rejectWalkDist == 1, na.rm = TRUE)
n_flagged_rounds <- df_out %>%
  distinct(roundID, roundID_int, rejectWalkDist) %>%
  filter(rejectWalkDist == 1) %>%
  nrow()

write_csv(df_out, output_csv)

cat("Rows flagged (rejectWalkDist = 1):", n_flagged_rows, "\n")
cat("Rounds flagged:", n_flagged_rounds, "\n")
cat("Output file:", output_csv, "\n")

cat("N rows =", nrow(model_df), "\n")
cat("N unique rounds (roundID) =", dplyr::n_distinct(model_df$roundID), "\n")
cat("N unique rounds (roundID_int) =", dplyr::n_distinct(model_df$roundID_int), "\n")