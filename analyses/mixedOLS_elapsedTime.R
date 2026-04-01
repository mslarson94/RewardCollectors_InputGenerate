# mixedModel_ElapsedTime_Redo.R
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
out_dir <- "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/analyses/results_R/mixedLinear/ols_elapsedTime"
if (!dir.exists(out_dir)) {
  dir.create(out_dir, recursive = TRUE)
}

out_mixed <- file.path(out_dir, "model_mixedLm")
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
#   roundElapsed_s ~ coinLabel,
#   data = model_df,
#   REML = FALSE
# )
# print(summary(m_1))
# run_lmer_diagnostics(m_1, name = "M1_coinLabel_Raw", out_dir = out_mixed)
# 
# # ----------------------------------------------------------------------------------
# # Mixed model | M2: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label + Coin Layout
# # ----------------------------------------------------------------------------------
# cat("\n\n\n\n\n")
# cat("lmer: roundElapsed_s ~ coinLabel + coinSet\n\n")
# 
# m_2 <- lmer(
#   roundElapsed_s ~ coinLabel + coinSet,
#   data = model_df,
#   REML = FALSE
# )
# print(summary(m_2))
# run_lmer_diagnostics(m_2, name = "M2_coinSet_Raw", out_dir = out_mixed)
# 
# # ------------------------------------------------------------------------------------------------
# # Mixed model | M3: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label + Session Time + Coin Layout 
# # ------------------------------------------------------------------------------------------------
# cat("\n\n\n\n\n")
# cat("lmer: roundElapsed_s ~ coinLabel + TotSesh_runTot_RoundNum + coinSet\n\n")
# 
# m_3 <- lmer(
#   roundElapsed_s ~ coinLabel + TotSesh_runTot_RoundNum + coinSet,
#   data = model_df,
#   REML = FALSE
# )
# print(summary(m_3))
# run_lmer_diagnostics(m_3, name = "M3_time", out_dir = out_ols)
# 
# # ------------------------------------------------------------------------------------------------
# # Mixed model | M4: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label * SessionTime + Coin Layout  
# # ------------------------------------------------------------------------------------------------
# cat("\n\n\n\n\n")
# cat("lmer: roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum + coinSet\n\n")
# 
# m_4 <- lmer(
#   roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum + coinSet,
#   data = model_df,
#   REML = FALSE
# )
# print(summary(m_4))
# run_lmer_diagnostics(m_4, name = "M4_timeInteract", out_dir = out_ols)
# 
# # -------------------------------------------------------------------------------------------------------------
# # Mixed model | M5: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label + Volatility + Coin Layout  
# # -------------------------------------------------------------------------------------------------------------
# cat("\n\n\n\n\n")
# cat("lmer: roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum + recentSwapRate_all + coinSet\n\n")
# 
# m_5 <- lmer(
#   roundElapsed_s ~ coinLabel + recentSwapRate_all + coinSet,
#   data = model_df,
#   REML = FALSE
# )
# print(summary(m_5))
# run_lmer_diagnostics(m_5, name = "M5_Volatility", out_dir = out_ols)
# 
# # -------------------------------------------------------------------------------------------------------------
# # Mixed model | M6: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label * SessionTime + Volatility + Coin Layout  
# # -------------------------------------------------------------------------------------------------------------
# cat("\n\n\n\n\n")
# cat("lmer: roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum + recentSwapRate_all + coinSet\n\n")
# 
# m_6 <- lmer(
#   roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum + recentSwapRate_all + coinSet,
#   data = model_df,
#   REML = FALSE
# )
# print(summary(m_6))
# run_lmer_diagnostics(m_6, name = "M6_timeVolatility", out_dir = out_ols)
# 
# 
# ###########################
# #   Piece-Wise Learning
# ###########################
# 
# # --------------------------------------------------------------------------------------------------------------------
# # Mixed model | L1: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label + Coin Layout + Early Learning + Late Learning 
# # --------------------------------------------------------------------------------------------------------------------
# cat("\n\n\n\n\n")
# cat("lmer: roundElapsed_s ~ coinLabel + t_early_20 + t_late_20 + coinSet\n\n")
# 
# mL_1 <- lmer(
#   roundElapsed_s ~ coinLabel + t_early_20 + t_late_20 + coinSet,
#   data = model_df,
#   REML = FALSE
# )
# print(summary(mL_1))
# run_lmer_diagnostics(mL_1, name = "L1_learning_Raw", out_dir = out_ols)
# 
# # -----------------------------------------------------------------------------------------------------------
# # Mixed model | L2: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label + t_early_20 + t_late_20 + Coin Layout
# # -----------------------------------------------------------------------------------------------------------
# cat("\n\n\n\n\n")
# cat("lmer: roundElapsed_s ~ coinLabel * t_early_20 * t_late_20 + coinSet\n\n")
# 
# mL_2 <- lmer(
#   roundElapsed_s ~ coinLabel * t_early_20 * t_late_20 + coinSet,
#   data = model_df,
#   REML = FALSE
# )
# print(summary(mL_2))
# run_lmer_diagnostics(mL_2, name = "L2_learningInteract_Raw", out_dir = out_ols)
# 
# # -----------------------------------------------------------------------------------------------------------
# # Mixed model | L3: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label * t_early_20 * t_late_20 + Coin Layout
# # -----------------------------------------------------------------------------------------------------------
# cat("\n\n\n\n\n")
# cat("lmer: roundElapsed_s ~ coinLabel  * t_early_20 * t_late_20 + recentSwapRate_all + coinSet\n\n")
# 
# mL_3 <- lmer(
#   roundElapsed_s ~ coinLabel * t_early_20 * t_late_20 + recentSwapRate_all + coinSet,
#   data = model_df,
#   REML = FALSE
# )
# print(summary(mL_3))
# run_lmer_diagnostics(mL_3, name = "L3_learningVolatility_Raw", out_dir = out_ols)


# chosen ~
#   points + idealDistance +
#   points:t_early_20 + idealDistance:t_early_20 +
#   points:t_late_20  + idealDistance:t_late_20 +
#   # swap-rate moderates sensitivities (identifiable)
#   points:recentSwapRate_all_z +
#   idealDistance:recentSwapRate_all_z +
#   strata(roundID) + cluster(participantID),

# ---------------------------------------------------------
# OLS with participant-clustered SEs (statsmodels-like)
# ---------------------------------------------------------
ols_all <- lm(
  roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum * recentSwapRate_all +
    main_RR + coinSet + isSwap,
  data = model_df
)
print_clustered(
  ols_all,
  cluster = model_df$participantID,
  title = "ols (clustered by participantID): roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR + coinSet"
)
print(summary(ols_all))
run_lmer_diagnostics(ols_all, name = "ols_all", out_dir = out_dir)

# log transform model (roundElapsed_s > 0)
model_df2 <- model_df %>%
  filter(roundElapsed_s > 0) %>%
  mutate(log_roundElapsed_s = log(roundElapsed_s))

ols_all2 <- lm(
  log_roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum * recentSwapRate_all +
    main_RR + coinSet,
  data = model_df2
)
run_lmer_diagnostics(ols_all, name = "ols_all2", out_dir = out_dir)
print_clustered(
  ols_all2,
  cluster = model_df2$participantID,
  title = "ols (clustered): log_roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR + coinSet"
)

# roundFrac full model
ols_all3 <- lm(
  roundFrac ~ coinLabel * TotSesh_runTot_RoundNum * recentSwapRate_all +
    main_RR + coinSet + isSwap,
  data = model_df
)
print_clustered(
  ols_all3,
  cluster = model_df$participantID,
  title = "ols (clustered): roundFrac ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR + coinSet"
)
run_lmer_diagnostics(ols_all, name = "ols_all3", out_dir = out_dir)
# collinearity check
cat("\n\n\n\n\n")
cat("collinearity checks\n")
cat("running total round num vs. recentSwapRate_all\n")
print(cor(
  model_df %>% select(TotSesh_runTot_RoundNum, recentSwapRate_all),
  use = "complete.obs"
))
cat("\n")

# reduced roundFrac model
ols_all4 <- lm(
  roundFrac ~ coinLabel * recentSwapRate_all + main_RR + coinSet,
  data = model_df
)
print_clustered(
  ols_all4,
  cluster = model_df$participantID,
  title = "ols (clustered): roundFrac ~ coinLabel*recentSwapRate_all + main_RR + coinSet "
)

ols_all4_A <- lm(
  roundFrac ~ coinLabel * recentSwapRate_all + main_RR + isSwap,
  data = model_df_A
)
run_lmer_diagnostics(ols_all, name = "ols_all4_A", out_dir = out_dir)
print_clustered(
  ols_all4_A,
  cluster = model_df_A$participantID,
  title = "Coin Set A (clustered): roundFrac ~ coinLabel*recentSwapRate_all + main_RR + isSwap"
)

# runningSwapRate = swapRate_t-1_all
model_df   <- model_df   %>% mutate(runningSwapRate = `swapRate_t-1_all`)
model_df_A <- model_df_A %>% mutate(runningSwapRate = `swapRate_t-1_all`)

ols_all5 <- lm(
  roundFrac ~ coinLabel * runningSwapRate + main_RR + coinSet,
  data = model_df
)
run_lmer_diagnostics(ols_all, name = "ols_all5", out_dir = out_dir)
print_clustered(
  ols_all5,
  cluster = model_df$participantID,
  title = "ols (clustered): roundFrac ~ coinLabel*runningSwapRate + main_RR + coinSet"
)

ols_all5_A <- lm(
  roundFrac ~ coinLabel * runningSwapRate + main_RR + isSwap,
  data = model_df_A
  
)
print_clustered(
  ols_all5_A,
  cluster = model_df_A$participantID,
  title = "Coin Set A (clustered): roundFrac ~ coinLabel*runningSwapRate + main_RR"
)

ols_all5 <- lm(
  roundFrac ~ coinLabel * runningSwapRate + main_RR + coinSet,
  data = model_df
)
run_lmer_diagnostics(ols_all, name = "ols_all5", out_dir = out_dir)
print_clustered(
  ols_all5,
  cluster = model_df$participantID,
  title = "ols (clustered): roundFrac ~ coinLabel*runningSwapRate + main_RR + coinSet"
)


run_lmer_diagnostics(ols_all, name = "ols_all5_A", out_dir = out_dir)
ols_all6 <- lm(
  roundFrac ~ coinLabel * TotSesh_runTot_RoundNum + main_RR + coinSet + isSwap,
  data = model_df
)
run_lmer_diagnostics(ols_all, name = "ols_all6", out_dir = out_dir)
print_clustered(
  ols_all6,
  cluster = model_df$participantID,
  title = "ols (clustered): roundFrac ~ coinLabel*TotSesh_runTot_RoundNum + main_RR + coinSet"
)

ols_all6_A <- lm(
  roundFrac ~ coinLabel * TotSesh_runTot_RoundNum + main_RR,
  data = model_df_A
)
print_clustered(
  ols_all6_A,
  cluster = model_df_A$participantID,
  title = "Coin Set A (clustered): roundFrac ~ coinLabel*TotSesh_runTot_RoundNum + main_RR"
)
run_lmer_diagnostics(ols_all, name = "ols_all6_A", out_dir = out_dir)
ols_all7 <- lm(
  roundFrac ~ coinLabel + main_RR + coinSet,
  data = model_df
)
run_lmer_diagnostics(ols_all, name = "ols_all7", out_dir = out_dir)
print_clustered(
  ols_all7,
  cluster = model_df$participantID,
  title = "ols (clustered): roundFrac ~ coinLabel + main_RR + coinSet"
)

ols_all7_A <- lm(
  roundFrac ~ coinLabel + main_RR,
  data = model_df_A
)
print_clustered(
  ols_all7_A,
  cluster = model_df_A$participantID,
  title = "Coin Set A (clustered): roundFrac ~ coinLabel + main_RR"
)
run_lmer_diagnostics(ols_all, name = "ols_all7A", out_dir = out_dir)



###########################
#   Piece-Wise Learning
###########################

# --------------------------------------------------------------------------------------------------------------------
# Mixed model | L1: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label + Coin Layout + Early Learning + Late Learning 
# --------------------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: roundElapsed_s ~ coinLabel + t_early_20 + t_late_20\n\n")

mL_1 <- lm(
  roundElapsed_s ~ coinLabel + t_early_20 + t_late_20,
  cluster = model_df$participantID,
  data = model_df_A
)
print(summary(mL_1))
run_lmer_diagnostics(mL_1, name = "L1_learning_Raw", out_dir = out_dir)

# -----------------------------------------------------------------------------------------------------------
# Mixed model | L2: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label + t_early_20 + t_late_20 + Coin Layout
# -----------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: roundElapsed_s ~ coinLabel * t_early_20 * t_late_20\n\n")

mL_2 <- lm(
  roundElapsed_s ~ coinLabel * t_early_20 * t_late_20,
  cluster = model_df$participantID,
  data = model_df_A
)
print(summary(mL_2))
run_lmer_diagnostics(mL_2, name = "L2_learningInteract_Raw", out_dir = out_dir)

# -----------------------------------------------------------------------------------------------------------
# Mixed model | L3: Pin Drop Latency (Raw Elapsed Time)  ~ Coin Label * t_early_20 * t_late_20 + Coin Layout
# -----------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: roundElapsed_s ~ coinLabel  * t_early_20 * t_late_20 + recentSwapRate_all\n\n")

mL_3 <- lm(
  roundElapsed_s ~ coinLabel * t_early_20 * t_late_20 + recentSwapRate_all,
  cluster = model_df$participantID,
  data = model_df_A
)
print(summary(mL_3))
run_lmer_diagnostics(mL_3, name = "L3_learningVolatility_Raw", out_dir = out_dir)

# -------------------------
# 6) Model comparison table
# -------------------------
model_list <- list(
  ols_all = ols_all,
  ols_all2 = ols_all2,
  ols_all5 = ols_all5,
  L1_learning_Raw = mL_1,
  L2_learningInteract_Raw = mL_2,
  L3_learningVolatility_Raw = mL_3
)

cmp <- model_compare_tbl(model_list)
readr::write_csv(cmp, file.path(out_dir, "model_compare.csv"))

# # ------------------------------
# # Mixed model with PVSS (subset)
# # ------------------------------
# df_pvss <- model_df %>%
#   filter(!is.na(PVSS_TotalScore))
# 
# cat("\n\n\n\n\n")
# cat("lmer (PVSS subset): roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR + WalkAvgSpeed + coinSet  + PVSS_TotalScore + (1|participantID)\n\n")
# 
# m_pvss <- lmer(
#   roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum * recentSwapRate_all +
#     main_RR + WalkAvgSpeed + coinSet + PVSS_TotalScore + (1 | participantID),
#   data = df_pvss,
#   REML = FALSE
# )
# print(summary(m_pvss))
# run_lmer_diagnostics(ols_all, name = "m_pvss", out_dir = out_dir)
sink()
