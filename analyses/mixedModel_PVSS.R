# mixedModel_PVSS.R
suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(stringr)
  library(tidyr)
  library(lme4)
  library(lmerTest)
  library(lmtest)
  library(sandwich)
  library(here)
  source(here::here("RC_utilities/rUtilities", "r_helpers.R"))
  source(here::here("RC_utilities/rUtilities", "run_lmer_diagnostics.R"))
})
out_dir <- "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/analyses/results_R/mixedLinear"
if (!dir.exists(out_dir)) {
  dir.create(out_dir, recursive = TRUE)
}
out_txt <- file.path(out_dir, "mixedLinear_output.txt")

con <- file(out_txt, open = "wt", encoding = "UTF-8")
sink(con, split = TRUE)

cat("\n================================================\n")
cat("Mixed Models Elapsed Time\n")
cat("All Participants Over All Time In All Coin Layouts\n")
cat("\n================================================\n")
# ----------------------------
# Helpers
# ----------------------------
trim_lower <- function(x) str_to_lower(str_trim(as.character(x)))

cluster_vcov <- function(lm_fit, cluster) {
  # cluster-robust VCOV similar to statsmodels cov_type="cluster"
  sandwich::vcovCL(lm_fit, cluster = cluster, type = "HC1")
}

print_clustered <- function(lm_fit, cluster, title) {
  cat("\n\n\n\n\n")
  cat(title, "\n\n")
  vc <- cluster_vcov(lm_fit, cluster)
  print(lmtest::coeftest(lm_fit, vcov. = vc))
  cat("\n")
}

# ----------------------------
# Inputs / outputs
# ----------------------------
dataFile <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/allIntervalDataKnotted_L1.csv"

out_offending <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/offending_rows_model_ElapsedTime_all.csv"
out_used <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/model_ElapsedTime_all_rows_used.csv"

# ----------------------------
# Load + clean
# ----------------------------
df <- readr::read_csv(dataFile, show_col_types = FALSE)

df <- df %>%
  filter(trim_lower(BlockType) != "collecting") %>%
  mutate(
    CoinSetID_num = suppressWarnings(as.numeric(CoinSetID))
  ) %>%
  filter(is.na(CoinSetID_num) | CoinSetID_num < 4) %>%  # match your "lt(4)" behavior, keeps NA
  mutate(
    participantID = as.factor(participantID),
    coinLabel     = as.factor(coinLabel),
    coinSet       = as.factor(coinSet),
    isSwap        = as.factor(isSwap),
    main_RR       = as.factor(main_RR)
  )

# ----------------------------------
# Offending rows + model_df  (FIXED)
# ----------------------------------
needed <- c(
  "roundElapsed_s", "coinLabel", "TotSesh_runTot_RoundNum", "recentSwapRate_all",
  "main_RR", "WalkAvgSpeed", "coinSet", "isSwap", "participantID",
  "t_early_15", "t_late_15", "t_early_20", "t_late_20", "t_early_25", "t_late_25"
)

# Safety: confirm columns exist (optional but helpful)
missing_needed <- setdiff(needed, names(df))
if (length(missing_needed) > 0) {
  stop(sprintf("These required columns are missing: %s", paste(missing_needed, collapse = ", ")), call. = FALSE)
}

# Logical NA matrix (no mixed-type combining)
na_mat <- as.data.frame(lapply(df[needed], is.na))  # data.frame of logicals
row_has_any_na <- Reduce(`|`, na_mat)

# Build missing_cols per row from logicals
missing_cols_vec <- vapply(
  seq_len(nrow(df)),
  function(i) paste(needed[unlist(na_mat[i, ], use.names = FALSE)], collapse = ","),
  character(1)
)

offending_rows <- df[row_has_any_na, , drop = FALSE]
offending_rows$missing_cols <- missing_cols_vec[row_has_any_na]

readr::write_csv(offending_rows, out_offending)

model_df <- df[!row_has_any_na, , drop = FALSE]
readr::write_csv(model_df, out_used)

# ----------------------------
# Quick inspection
# ----------------------------
cat("\nCounts (model_df)\n")
print(table(model_df$coinSet, useNA = "ifany"))
print(table(model_df$coinLabel, useNA = "ifany"))
print(table(model_df$isSwap, useNA = "ifany"))
print(table(model_df$main_RR, useNA = "ifany"))
cat("N =", nrow(model_df), "\n")

# Split coinSet A vs non-A (case/trim insensitive)
coinset_str <- trim_lower(model_df$coinSet)

model_df_A   <- model_df %>% filter(coinset_str == "a")
model_df_B   <- model_df %>% filter(coinset_str == "b")
model_df_C   <- model_df %>% filter(coinset_str == "c")
model_df_D   <- model_df %>% filter(coinset_str == "d")
model_df_Ax  <- model_df %>% filter(coinset_str == "ax")
model_df_Bx  <- model_df %>% filter(coinset_str == "bx")

model_df_noA <- model_df %>% filter(coinset_str != "a")

# ------------------------------
# Mixed model with PVSS (subset)
# ------------------------------
df_pvss <- model_df %>%
  filter(!is.na(PVSS_TotalScore))

cat("\n\n\n\n\n")
cat("lmer (PVSS subset): roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR + WalkAvgSpeed + coinSet  + PVSS_TotalScore + (1|participantID)\n\n")

m_pvss <- lmer(
  roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum * recentSwapRate_all +
    main_RR + WalkAvgSpeed + coinSet + PVSS_TotalScore + (1 | participantID),
  data = df_pvss,
  REML = FALSE
)
print(summary(m_pvss))
run_lmer_diagnostics(ols_all, name = "m_pvss", out_dir = out_dir)
sink()
