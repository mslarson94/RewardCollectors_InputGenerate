# mixedModel_path_eff_raw_noA.R
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
  library(FSA)
  source(here::here("RC_utilities/rUtilities", "r_helpers.R"))
  source(here::here("RC_utilities/rUtilities", "run_lmer_diagnostics.R"))
  source(here::here("RC_utilities/rUtilities", "plot_fixed_effects.R"))
})
out_dir <- "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/analyses/results_R/mixedLm_pathEff"
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
cat("Mixed Models Round Fraction\n")
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
dataFile <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain_PO/EventSegmentation/megaFiles/allIntervalData_AN_with_rejectWalkDist.csv"

out_offending <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/offending_rows_model_ElapsedTime_all_1.csv"
out_used <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/model_ElapsedTime_all_rows_used_1.csv"

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
    participantID         = as.factor(participantID),
    coinLabel             = as.factor(coinLabel),
    coinSet               = as.factor(coinSet),
    isSwap                = as.factor(isSwap),
    main_RR               = as.factor(main_RR),
    path_order_round_num  = as.factor(path_order_round_num),
    orderedCollect	      = as.factor(orderedCollect),
    pathValue             = as.factor(pathValue)
    
  )

# ----------------------------------
# Offending rows + model_df  (FIXED)
# ----------------------------------
needed <- c(
  "roundElapsed_s", "coinLabel", "TotSesh_runTot_RoundNum", "recentSwapRate_all", "path_eff_raw",
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

model_df_raw <- df[!row_has_any_na, , drop = FALSE]
readr::write_csv(model_df_raw, out_used)

# ----------------------------
# Quick inspection
# ----------------------------
cat("\nCounts (model_df_raw)\n")
print(table(model_df_raw$coinSet, useNA = "ifany"))
print(table(model_df_raw$coinLabel, useNA = "ifany"))
print(table(model_df_raw$isSwap, useNA = "ifany"))
print(table(model_df_raw$main_RR, useNA = "ifany"))
cat("N =", nrow(model_df_raw), "\n")
model_df <- model_df %>%
  filter(WalkDist > 0.5)
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
model_df_noCD <- model_df %>% filter(!trim_lower(coinSet) %in% c("c", "d"))
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


analysis_df <- model_df_noCD


labelMap <- c(
  "(Intercept)" = "Intercept",
  "coinLabelLV" = "Low Value Coin",
  "coinLabelNV" = "Null Value Coin",
  "t_early_20" = "Early learning",
  "t_late_20" = "Late learning",
  "recentSwapRate_all" = "Volatility",
  "coinSetAx" = "Coin Layout Ax",
  "coinSetB" = "Coin Layout B",
  "coinSetBx" = "Coin Layout Bx",
  "coinSetC" = "Coin Layout C",
  "coinSetD" = "Coin Layout D",
  "coinLabelLV:t_early_20" = "LV × Early learning",
  "coinLabelNV:t_early_20" = "NV × Early learning",
  "coinLabelLV:t_late_20" = "LV × Late learning",
  "coinLabelNV:t_late_20" = "NV × Late learning",
  "orderedCollect1" = "Ordered Collection",
  "orderedCollect0" = "Unordered Collection",
  "pathValue30" = "30 points",
  "pathValue25" = "25 points",
  "pathValue20" = "20 points",
  "orderedCollect1:pathValue30" = "Ordered Collect x 30 pt Path",
  "orderedCollect1:pathValue25" = "Ordered Collect x 25 pt Path",
  "orderedCollect1:pathValue20" = "Ordered Collect x 20 pt Path",
  "orderedCollect0:pathValue30" = "Unordered Collect x 30 pt Path",
  "orderedCollect0:pathValue25" = "Unordered Collect x 25 pt Path",
  "orderedCollect0:pathValue20" = "Unordered Collect x 20 pt Path"
)


# # -------------------------------------------------------------------
# # Mixed model | M1: Pin Drop Latency (Raw Round Fraction) & Coin Label
# # -------------------------------------------------------------------
# cat("\n\n\n\n\n")
# cat("lmer: path_eff_raw ~ coinLabel\n\n")
# 
# m_1 <- lmer(
#   path_eff_raw ~  (1 | participantID),
#   data = model_df_noA,
#   REML = FALSE
# )
# print(summary(m_1))
# run_lmer_diagnostics(m_1, name = "M1_coinLabel_Raw", out_dir = out_dir)

# ----------------------------------------------------------------------------------
# Mixed model | M2: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label + Coin Layout
# ----------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: path_eff_raw ~  coinSet\n\n")

check_CoinSet <- kruskal.test(path_eff_raw ~ coinSet, data = model_df)

check_CoinSet_pairwise <- pairwise.wilcox.test(
  x = model_df$path_eff_raw,
  g = model_df$coinSet,
  p.adjust.method = "holm"
)
model_df %>%
  group_by(coinSet) %>%
  summarise(
    n = n(),
    median = median(path_eff_raw, na.rm = TRUE),
    q1 = quantile(path_eff_raw, 0.25, na.rm = TRUE),
    q3 = quantile(path_eff_raw, 0.75, na.rm = TRUE)
  )

modelDF_summary <- model_df %>%
  group_by(coinSet) %>%
  summarise(
    n = n(),
    median_path_eff = median(path_eff_raw, na.rm = TRUE),
    mean_path_eff = mean(path_eff_raw, na.rm = TRUE),
    q1 = quantile(path_eff_raw, 0.25, na.rm = TRUE),
    q3 = quantile(path_eff_raw, 0.75, na.rm = TRUE)
  )
print(modelDF_summary)
coinSetPostHoc <- FSA::dunnTest(
  path_eff_raw ~ coinSet,
  data = model_df,
  method = "holm"
)
print(check_CoinSet)
print(check_CoinSet_pairwise)
print(coinSetPostHoc)
# ------------------------------------------------------------------------------------------------
# Mixed model | M3: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label + Session Time + Coin Layout 
# ------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: path_eff_raw ~  TotSesh_runTot_RoundNum + coinSet\n\n")

m_3 <- lmer(
  path_eff_raw ~  TotSesh_runTot_RoundNum + coinSet + (1 | participantID),
  data = analysis_df,
  REML = FALSE
)
print(summary(m_3))
run_lmer_diagnostics(m_3, name = "M3_time", out_dir = out_dir)

# ------------------------------------------------------------------------------------------------
# Mixed model | M4: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label * SessionTime + Coin Layout  
# ------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: path_eff_raw ~  TotSesh_runTot_RoundNum + coinSet\n\n")

m_4 <- lmer(
  path_eff_raw ~  TotSesh_runTot_RoundNum + coinSet + (1 | participantID),
  data = analysis_df,
  REML = FALSE
)
print(summary(m_4))
run_lmer_diagnostics(m_4, name = "M4_timeInteract", out_dir = out_dir)

# -------------------------------------------------------------------------------------------------------------
# Mixed model | M5: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label + Volatility + Coin Layout  
# -------------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: path_eff_raw ~  TotSesh_runTot_RoundNum + recentSwapRate_all + coinSet\n\n")

m_5 <- lmer(
  path_eff_raw ~  recentSwapRate_all + coinSet + (1 | participantID),
  data = analysis_df,
  REML = FALSE
)
print(summary(m_5))
run_lmer_diagnostics(m_5, name = "M5_Volatility", out_dir = out_dir)

# -------------------------------------------------------------------------------------------------------------
# Mixed model | M6: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label * SessionTime + Volatility + Coin Layout  
# -------------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: path_eff_raw ~  TotSesh_runTot_RoundNum + recentSwapRate_all + coinSet\n\n")

m_6 <- lmer(
  path_eff_raw ~  TotSesh_runTot_RoundNum + recentSwapRate_all + coinSet + (1 | participantID),
  data = analysis_df,
  REML = FALSE
)
print(summary(m_6))
run_lmer_diagnostics(m_6, name = "M6_timeVolatility", out_dir = out_dir)


###########################
#   Piece-Wise Learning
###########################

# --------------------------------------------------------------------------------------------------------------------
# Mixed model | L1: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label + Coin Layout + Early Learning + Late Learning 
# --------------------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: path_eff_raw ~  t_early_20 + t_late_20 + coinSet\n\n")

mL_1 <- lmer(
  path_eff_raw ~  t_early_20 + t_late_20 + coinSet + (1 | participantID),
  data = analysis_df,
  REML = FALSE
)
print(summary(mL_1))
run_lmer_diagnostics(mL_1, name = "L1_learning_Raw", out_dir = out_dir)

# -----------------------------------------------------------------------------------------------------------
# Mixed model | L2: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label + t_early_20 + t_late_20 + Coin Layout
# -----------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: path_eff_raw ~  t_early_20 * t_late_20 + coinSet\n\n")

mL_2 <- lmer(
  path_eff_raw ~  t_early_20 * t_late_20 + coinSet + (1 | participantID),
  data = analysis_df,
  REML = FALSE
)
print(summary(mL_2))
run_lmer_diagnostics(mL_2, name = "L2_learningInteract_Raw", out_dir = out_dir)

# -----------------------------------------------------------------------------------------------------------
# Mixed model | L3: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label * t_early_20 * t_late_20 + Coin Layout
# -----------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: path_eff_raw ~ coinLabel  * t_early_20 * t_late_20 + recentSwapRate_all + coinSet\n\n")

mL_3 <- lmer(
  path_eff_raw ~  t_early_20 + t_late_20 + recentSwapRate_all + coinSet + (1 | participantID),
  data = analysis_df,
  REML = FALSE
)
print(summary(mL_3))
run_lmer_diagnostics(mL_3, name = "L3_learningVolatility_Raw", out_dir = out_dir)

p <- plot_fixed_effects(mL_3, labelMap, drop_intercept = TRUE)
print(p)

# -----------------------------------------------------------------------------------------------------------
# Mixed model | L4: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label * t_early_20 * t_late_20 + Coin Layout
# -----------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: path_eff_raw ~ coinLabel  * t_early_20 * t_late_20 + recentSwapRate_all + coinSet\n\n")

mL_4 <- lmer(
  path_eff_raw ~  t_early_20 + t_late_20 + recentSwapRate_all + coinSet +  orderedCollect + pathValue + (1 | participantID),
  data = analysis_df,
  REML = FALSE
)
print(summary(mL_4))
run_lmer_diagnostics(mL_4, name = "L4_learningVolatility_Raw", out_dir = out_dir)

p <- plot_fixed_effects(mL_4, labelMap, drop_intercept = TRUE)
print(p)

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
  L3_learningVolatility_Raw = mL_3,
  L4_learningVolatility_Raw = mL_4
)

cmp <- model_compare_tbl(model_list)
readr::write_csv(cmp, file.path(out_dir, "model_compare.csv"))

cat("\n\n==============================\n")
cat("Model Comparisons\n")
cat("==============================\n")
print(cmp)


cat("\n\n==============================\n")
cat("Model Comparisons\n")
cat("==============================\n")
alias(lm(path_eff_raw ~  t_early_20 * t_late_20 + recentSwapRate_all + coinSet,
         data = analysis_df))

X <- model.matrix(~  t_early_20 * t_late_20 + recentSwapRate_all + coinSet,
                  data = analysis_df)
qr(X)$rank
ncol(X)
colnames(X)
caret::findLinearCombos(X)

sink()
