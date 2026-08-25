# mixedModel_dropDist_first50.R
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
  here::i_am("analyses/mixedModel_dropDist_first50.R")
  source(here::here("RC_utilities/rUtilities", "buildPaths.R"))
  source(here::here("RC_utilities/rUtilities", "r_helpers.R"))
  source(here::here("RC_utilities/rUtilities", "run_lmer_diagnostics.R"))
  source(here::here("RC_utilities/rUtilities", "plot_fixed_effects.R"))
})

voi <- "dropDist"
which_subset = "TotSesh_actTest_RoundNum" 
# "TotSesh_actTest_RoundNum", "TotSesh_TP1_roundNum", "TotSesh_TP2_roundNum"

paths <- buildPaths(
  proc_dir = "FreshStart_redoAgainAgainAgain_PO_redo",
  round_dur_filter = "2.0",
  round_set = "1st50Rounds",
  condition = "noCD",
  inFile = "1st50IntervalDataKnotted_AN_noCD_all.csv",
  outDirName = paste0("mixedLm_bothTPs_perf_", voi)
)

dir.create(
  paths$outdir,
  recursive = TRUE,
  showWarnings = FALSE
)

dataFile <- paths$data_file
outdir <- paths$outdir

if (!dir.exists(outdir)) {
  dir.create(outdir, recursive = TRUE)
}

out_txt <- file.path(outdir, "modelsOutput.txt")

con <- file(out_txt, open = "wt", encoding = "UTF-8")
sink(con, split = TRUE)

out_offending = file.path(outdir, "offending_rows.csv")
out_used = file.path(outdir, "all_rows_used.csv")

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
make_formula <- function(rhs) {
  as.formula(sprintf("%s ~ %s", voi, rhs))
}
# ----------------------------
# Inputs / outputs
# ----------------------------

print(paths$dataFile)
# ----------------------------
# Load + clean
# ----------------------------
df <- readr::read_csv(dataFile, show_col_types = FALSE)
print(df)
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
# df <- df %>%
#   #filter(is.na(rejectWalkDist) | rejectWalkDist != 1) %>%
#   filter(which_subset <= 50) %>%
#   filter(dropDist <= 1.1)
# ----------------------------------
# Offending rows + model_df  (FIXED)
# ----------------------------------
needed <- c(
  which_subset, voi,
  "roundElapsed_s", "coinLabel", "recentSwapRate_all", 
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
if ("coinLabel" %in% names(model_df)) {
  model_df$coinLabel <- droplevels(model_df$coinLabel)
  if ("LV" %in% levels(model_df$coinLabel)) {
    model_df$coinLabel <- relevel(model_df$coinLabel, ref = "LV")
  }
}
# ----------------------------
# Quick inspection
# ----------------------------
cat("\nCounts (model_df_raw)\n")
print(table(model_df$coinSet, useNA = "ifany"))
print(table(model_df$coinLabel, useNA = "ifany"))
print(table(model_df$isSwap, useNA = "ifany"))
print(table(model_df$main_RR, useNA = "ifany"))
cat("N =", nrow(model_df), "\n")
# model_df <- model_df %>%
#   filter(WalkDist > 0.5)
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
model_df_noB <- model_df %>% filter(coinset_str != "b")
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
model_df2_noB <- model_df2 %>% filter(coinset_str != "b")

model_df2List <- c(model_df2_A, model_df2_Ax, model_df2_B, model_df2_Bx, model_df2_C, model_df2_D, model_df2_noA)

model_df_corr <- model_df %>% filter(dropQual == "good")
model_df_noB_corr <- model_df_noB %>% filter(dropQual == "good")

analysis_df <- model_df_corr


labelMap <- c(
  "(Intercept)" = "Intercept",
  "coinLabelLV" = "Low Value Coin",
  "coinLabelHV" = "High Value Coin",
  "coinLabelNV" = "Null Value Coin",
  "t_early_15" = "Early learning",
  "t_late_15" = "Late learning",
  "recentSwapRate_all" = "Volatility",
  "coinSetAx" = "Coin Layout Ax",
  "coinSetB" = "Coin Layout B",
  "coinSetBx" = "Coin Layout Bx",
  "coinSetC" = "Coin Layout C",
  "coinSetD" = "Coin Layout D",
  "coinLabelLV:t_early_15" = "LV × Early learning",
  "coinLabelNV:t_early_15" = "NV × Early learning",
  "coinLabelLV:t_late_15" = "LV × Late learning",
  "coinLabelNV:t_late_15" = "NV × Late learning",
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
# cat("lmer: dropDist ~ coinLabel\n\n")
# 
# m_1 <- lmer(
#   dropDist ~  (1 | participantID),
#   data = model_df_noA,
#   REML = FALSE
# )
# print(summary(m_1))
# run_lmer_diagnostics(m_1, name = "M1_coinLabel_Raw", out_dir = out_dir)

# ----------------------------------------------------------------------------------
# Mixed model | M2: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label + Coin Layout
# ----------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: dropDist ~  coinSet\n\n")

check_CoinSet <- kruskal.test(
  make_formula("coinSet"),
  data = analysis_df
)

check_CoinSet_pairwise <- pairwise.wilcox.test(
  x = analysis_df[[voi]],
  g = analysis_df$coinSet,
  p.adjust.method = "holm"
)

analysis_df %>%
  group_by(coinSet) %>%
  summarise(
    n = n(),
    median = median(.data[[voi]], na.rm = TRUE),
    q1 = quantile(.data[[voi]], 0.25, na.rm = TRUE),
    q3 = quantile(.data[[voi]], 0.75, na.rm = TRUE)
  )

modelDF_summary <- analysis_df %>%
  group_by(coinSet) %>%
  summarise(
    n = n(),
    median = median(.data[[voi]], na.rm = TRUE),
    mean = mean(.data[[voi]], na.rm = TRUE),
    q1 = quantile(.data[[voi]], 0.25, na.rm = TRUE),
    q3 = quantile(.data[[voi]], 0.75, na.rm = TRUE)
  )

print(modelDF_summary)
coinSetPostHoc <- FSA::dunnTest(
  make_formula("coinSet"),
  data = analysis_df,
  method = "holm"
)
print(check_CoinSet)
print(check_CoinSet_pairwise)
print(coinSetPostHoc)
# ------------------------------------------------------------------------------------------------
# Mixed model | M3: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label + Session Time + Coin Layout 
# ------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: dropDist ~  which_subset + coinSet\n\n")

m3_formula <- as.formula(
  make_formula(
    paste(which_subset, " + coinSet + coinLabel + (1 | participantID)")
  )
)

m_3 <- lmer(
  m3_formula,
  data = analysis_df,
  REML = FALSE
)

print(summary(m_3))
run_lmer_diagnostics(m_3, name = "M3_time", out_dir = outdir)

# ------------------------------------------------------------------------------------------------
# Mixed model | M4: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label * SessionTime + Coin Layout  
# ------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: dropDist ~  which_subset + coinSet\n\n")

m4_formula <- as.formula(
  make_formula(
    paste(which_subset, " + coinSet + (1 | participantID)")
  )
)

m_4 <- lmer(
  m4_formula,
  data = analysis_df,
  REML = FALSE
)

print(summary(m_4))
run_lmer_diagnostics(m_4, name = "M4_timeInteract", out_dir = outdir)

# -------------------------------------------------------------------------------------------------------------
# Mixed model | M5: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label + Volatility + Coin Layout  
# -------------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: dropDist ~  recentSwapRate_all + coinSet\n\n")


m5_formula <- as.formula(
  make_formula(
    "recentSwapRate_all + coinSet + coinLabel  + (1 | participantID)"
  )
)

m_5 <- lmer(
  m5_formula,
  data = analysis_df,
  REML = FALSE
)
print(summary(m_5))
run_lmer_diagnostics(m_5, name = "M5_Volatility", out_dir = outdir)

# -------------------------------------------------------------------------------------------------------------
# Mixed model | M6: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label * SessionTime + Volatility + Coin Layout  
# -------------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: dropDist ~  which_subset + recentSwapRate_all + coinSet\n\n")
m6_formula <- as.formula(
  make_formula(
    paste(which_subset, " + recentSwapRate_all + coinSet + coinLabel + (1 | participantID)")
  )
)

m_6 <- lmer(
  m6_formula,
  data = analysis_df,
  REML = FALSE
)

print(summary(m_6))
run_lmer_diagnostics(m_6, name = "M6_timeVolatility", out_dir = outdir)

# -------------------------------------------------------------------------------------------------------------
# Mixed model | M6: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label * SessionTime + Volatility + Coin Layout  
# -------------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: dropDist ~  which_subset + recentSwapRate_all + coinSet\n\n")
m7_formula <- as.formula(
  make_formula(
    paste(which_subset, "+ recentSwapRate_all +  coinLabel  + (1 | participantID)")
  )
)
m_7 <- lmer(
  m7_formula,
  data = analysis_df,
  REML = FALSE
)
print(summary(m_7))
run_lmer_diagnostics(m_7, name = "M7_timeVolatility", out_dir = outdir)
###########################
#   Piece-Wise Learning
###########################

# --------------------------------------------------------------------------------------------------------------------
# Mixed model | L1: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label + Coin Layout + Early Learning + Late Learning 
# --------------------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: dropDist ~  t_early_15 + t_late_15 + coinSet\n\n")
mL_1_formula <- as.formula(
  make_formula("t_early_15 + t_late_15 + coinSet + coinLabel  + (1 | participantID)"
  )
)
mL_1 <- lmer(
  mL_1_formula,
  data = analysis_df,
  REML = FALSE
)
print(summary(mL_1))
run_lmer_diagnostics(mL_1, name = "L1_learning_Raw", out_dir = outdir)

# -----------------------------------------------------------------------------------------------------------
# Mixed model | L2: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label + t_early_20 + t_late_20 + Coin Layout
# -----------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: dropDist ~  t_early_15 * t_late_15 + coinSet\n\n")

mL_2_formula <- as.formula(
  make_formula(" t_early_15 * t_late_15 + coinSet  + (1 | participantID)"
  )
)
mL_2 <- lmer(
  mL_2_formula,
  data = analysis_df,
  REML = FALSE
)
print(summary(mL_2))
run_lmer_diagnostics(mL_2, name = "L2_learningInteract_Raw", out_dir = outdir)

# -----------------------------------------------------------------------------------------------------------
# Mixed model | L3: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label * t_early_20 * t_late_20 + Coin Layout
# -----------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: dropDist ~ coinLabel  * t_early_15 * t_late_15 + recentSwapRate_all + coinSet\n\n")

mL_3_formula <- as.formula(
  make_formula("t_early_15 + t_late_15 + recentSwapRate_all + coinSet + coinLabel  + (1 | participantID)"
  )
)

mL_3 <- lmer(
  mL_3_formula,
  data = analysis_df,
  REML = FALSE
)
print(summary(mL_3))
run_lmer_diagnostics(mL_3, name = "L3_learningVolatility_Raw", out_dir = outdir)

p <- plot_fixed_effects(mL_3, labelMap, drop_intercept = TRUE)
print(p)

# -----------------------------------------------------------------------------------------------------------
# Mixed model | L4: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label * t_early_15 * t_late_15 + Coin Layout
# -----------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: dropDist ~ coinLabel  * t_early_15 * t_late_15 + recentSwapRate_all + coinSet\n\n")

mL_4_formula <- as.formula(
  make_formula("t_early_15 + t_late_15 + recentSwapRate_all + coinSet +  orderedCollect + coinLabel  +  (1 | participantID)"
  )
)

mL_4 <- lmer(
  mL_4_formula,
  data = analysis_df,
  REML = FALSE
)
print(summary(mL_4))
run_lmer_diagnostics(mL_4, name = "L4_learningVolatility_Raw", out_dir = outdir)

p <- plot_fixed_effects(mL_4, labelMap, drop_intercept = TRUE)
print(p)


# -----------------------------------------------------------------------------------------------------------
# Mixed model | L5: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label * t_early_15 * t_late_20 
# -----------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: dropDist ~ coinLabel  * t_early_15 * t_late_15 + recentSwapRate_all\n\n")

mL_5_formula <- as.formula(
  make_formula("t_early_15 + t_late_15 + recentSwapRate_all + coinLabel  + (1 | participantID)"
  )
)

mL_5 <- lmer(
  mL_5_formula,
  data = analysis_df,
  REML = FALSE
)
print(summary(mL_5))
run_lmer_diagnostics(mL_5, name = "L5_learningVolatility_Raw2", out_dir = outdir)

p <- plot_fixed_effects(mL_5, labelMap, drop_intercept = TRUE)
print(p)

# -----------------------------------------------------------------------------------------------------------
# Mixed model | L6: Pin Drop Latency (Raw Round Fraction)  ~ Coin Label * t_early_15 * t_late_20 
# -----------------------------------------------------------------------------------------------------------
cat("\n\n\n\n\n")
cat("lmer: dropDist ~ coinLabel  * t_early_15 * t_late_15 + recentSwapRate_all\n\n")

mL_6_formula <- as.formula(
  make_formula("t_early_15 + t_late_15 + coinLabel  + (1 | participantID)"
  )
)

mL_6 <- lmer(
  mL_6_formula,
  data = analysis_df,
  REML = FALSE
)
print(summary(mL_6))
run_lmer_diagnostics(mL_6, name = "L6_learningVolatility_Raw3", out_dir = outdir)

p <- plot_fixed_effects(mL_6, labelMap, drop_intercept = TRUE)
print(p)

# -------------------------
# 6) Model comparison table
# -------------------------
model_list <- list(
  M3_time = m_3,
  M4_timeInteract = m_4,
  M5_Volatility = m_5,
  M6_timeVolatility = m_6,
  M7_timeVolatility = m_7,
  L1_learning_Raw = mL_1,
  L2_learningInteract_Raw = mL_2,
  L3_learningVolatility_Raw = mL_3,
  L4_learningVolatility_Raw = mL_4,
  L5_learningVolatility_Raw2 = mL_5,
  L6_learningVolatility_Raw3 = mL_6
)

cmp <- model_compare_tbl(model_list)
readr::write_csv(cmp, file.path(outdir, "model_compare.csv"))

cat("\n\n==============================\n")
cat("Model Comparisons\n")
cat("==============================\n")
print(cmp)


cat("\n\n==============================\n")
cat("Model Comparisons\n")
cat("==============================\n")
alias(
  lm(
    make_formula(
      "t_early_15 * t_late_15 + recentSwapRate_all + coinSet "
    ),
    data = analysis_df
  )
)

X <- model.matrix(
  ~ t_early_15 * t_late_15 + recentSwapRate_all + coinSet,
  data = analysis_df
)

qr(X)$rank
ncol(X)
colnames(X)
caret::findLinearCombos(X)

sink()
