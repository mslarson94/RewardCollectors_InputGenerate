# mixedModel_ElapsedTime_A.R
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
out_dir <- "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/analyses/results_R/mixedLm_A"
if (!dir.exists(out_dir)) {
  dir.create(out_dir, recursive = TRUE)
}
# 
# t_dir <- file.path(out_dir, "model_mixedLm")
out_ols <- file.path(out_dir, "model_ols")

out_txt <- file.path(out_dir, "PinDropLatencyModels_output.txt")

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

# --------------------------------
# Split model_df by Coin Layout 
# --------------------------------
coinset_str <- trim_lower(model_df$coinSet)
model_df_A   <- model_df %>% filter(coinset_str == "a")
model_df_B   <- model_df %>% filter(coinset_str == "b")
model_df_C   <- model_df %>% filter(coinset_str == "c")
model_df_D   <- model_df %>% filter(coinset_str == "d")
model_df_Ax  <- model_df %>% filter(coinset_str == "ax")
model_df_Bx  <- model_df %>% filter(coinset_str == "bx")

model_df_noA <- model_df %>% filter(coinset_str != "a")

model_dfList <- c(model_df_A, model_df_Ax, model_df_B, model_df_Bx, model_df_C, model_df_D, model_df_noA)

# -------------------------------------------
# log transform model_df (roundElapsed_s > 0)
# -------------------------------------------
model_df2 <- model_df %>%
  filter(roundElapsed_s > 0) %>%
  mutate(log_roundElapsed_s = log(roundElapsed_s))

model_df2_A   <- model_df2 %>% filter(coinset_str == "a")
model_df2_B   <- model_df2 %>% filter(coinset_str == "b")
model_df2_C   <- model_df2 %>% filter(coinset_str == "c")
model_df2_D   <- model_df2 %>% filter(coinset_str == "d")
model_df2_Ax  <- model_df2 %>% filter(coinset_str == "ax")
model_df2_Bx  <- model_df2 %>% filter(coinset_str == "bx")

model_df2_noA <- model_df2 %>% filter(coinset_str != "a")

model_df2List <- c(model_df2_A, model_df2_Ax, model_df2_B, model_df2_Bx, model_df2_C, model_df2_D, model_df2_noA)

# # -------------------------------------------------------------------
# # Mixed model | M1: Pin Drop Latency (Raw Elapsed Time) & Coin Label
# # -------------------------------------------------------------------
# cat("\n\n\n\n\n")
# cat("lmer: roundElapsed_s ~ coinLabel\n\n")
# 
# m_1 <- lmer(
#   roundElapsed_s ~ coinLabel + (1 | participantID),
#   data = model_df_A,
#   REML = FALSE
# )
# print(summary(m_1))
# run_lmer_diagnostics(m_1, name = "M1_coinLabel_Raw", out_dir = out_dir)

# # ----------------------------------------------------------------------------------
# # Mixed model | M2: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label + Coin Layout
# # ----------------------------------------------------------------------------------
# cat("\n\n\n\n\n")
# cat("lmer: roundElapsed_s ~ coinLabel\n\n")
# 
# m_2 <- lmer(
#   roundElapsed_s ~ coinLabel + (1 | participantID),
#   data = model_df_A,
#   REML = FALSE
# )
# print(summary(m_2))
# run_lmer_diagnostics(m_2, name = "M2_coinSet_Raw", out_dir = out_dir)

# ------------------------------------------------------------------------------------------------
# Mixed model | M3: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label + Session Time + Coin Layout 
# ------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: roundElapsed_s ~ coinLabel + TotSesh_runTot_RoundNum\n\n")

m_3 <- lmer(
  roundElapsed_s ~ coinLabel + TotSesh_runTot_RoundNum + (1 | participantID),
  data = model_df_A,
  REML = FALSE
)
print(summary(m_3))
run_lmer_diagnostics(m_3, name = "M3_time", out_dir = out_dir)

# ------------------------------------------------------------------------------------------------
# Mixed model | M4: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label * SessionTime + Coin Layout  
# ------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum\n\n")

m_4 <- lmer(
  roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum + (1 | participantID),
  data = model_df_A,
  REML = FALSE
)
print(summary(m_4))
run_lmer_diagnostics(m_4, name = "M4_timeInteract", out_dir = out_dir)

# -------------------------------------------------------------------------------------------------------------
# Mixed model | M5: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label + Volatility + Coin Layout  
# -------------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum + recentSwapRate_all\n\n")

m_5 <- lmer(
  roundElapsed_s ~ coinLabel + recentSwapRate_all + (1 | participantID),
  data = model_df_A,
  REML = FALSE
)
print(summary(m_5))
run_lmer_diagnostics(m_5, name = "M5_Volatility", out_dir = out_dir)

# -------------------------------------------------------------------------------------------------------------
# Mixed model | M6: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label * SessionTime + Volatility + Coin Layout  
# -------------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum + recentSwapRate_all\n\n")

m_6 <- lmer(
  roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum + recentSwapRate_all + (1 | participantID),
  data = model_df_A,
  REML = FALSE
)
print(summary(m_6))
run_lmer_diagnostics(m_6, name = "M6_timeVolatility", out_dir = out_dir)


###########################
#   Piece-Wise Learning
###########################

# --------------------------------------------------------------------------------------------------------------------
# Mixed model | L1: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label + Coin Layout + Early Learning + Late Learning 
# --------------------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: roundElapsed_s ~ coinLabel + t_early_20 + t_late_20\n\n")

mL_1 <- lmer(
  roundElapsed_s ~ coinLabel + t_early_20 + t_late_20 + (1 | participantID),
  data = model_df_A,
  REML = FALSE
)
print(summary(mL_1))
run_lmer_diagnostics(mL_1, name = "L1_learning_Raw", out_dir = out_dir)

# -----------------------------------------------------------------------------------------------------------
# Mixed model | L2: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label + t_early_20 + t_late_20 + Coin Layout
# -----------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: roundElapsed_s ~ coinLabel * t_early_20 * t_late_20\n\n")

mL_2 <- lmer(
  roundElapsed_s ~ coinLabel * t_early_20 * t_late_20 + (1 | participantID),
  data = model_df_A,
  REML = FALSE
)
print(summary(mL_2))
run_lmer_diagnostics(mL_2, name = "L2_learningInteract_Raw", out_dir = out_dir)

# -----------------------------------------------------------------------------------------------------------
# Mixed model | L3: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label * t_early_20 * t_late_20 + Coin Layout
# -----------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: roundElapsed_s ~ coinLabel  * t_early_20 * t_late_20 + recentSwapRate_all\n\n")

mL_3 <- lmer(
  roundElapsed_s ~ coinLabel * t_early_20 * t_late_20 + recentSwapRate_all + (1 | participantID),
  data = model_df_A,
  REML = FALSE
)
print(summary(mL_3))
run_lmer_diagnostics(mL_3, name = "L3_learningVolatility_Raw", out_dir = out_dir)

# -------------------------
# 6) Model comparison table
# -------------------------
model_list <- list(
  M3_time = m_3,
  M4_timeInteract = m_4,
  M5_Volatility = m_5,
  M6_timeVolatility = m_6,
  L1_learning_Raw = mL_1,
  L2_learningInteract_Raw = mL_2,
  L3_learningVolatility_Raw = mL_3
)

cmp <- model_compare_tbl(model_list)
readr::write_csv(cmp, file.path(out_dir, "model_compare.csv"))

cat("\n\n==============================\n")
cat("Model Comparisons\n")
cat("==============================\n")
print(cmp)

sink()
