# /path/to/analysis.R
suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(stringr)
  library(tidyr)
  library(lme4)
  library(lmerTest)   # p-values for lmer (Satterthwaite); optional but handy
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

cat("\n==================================\n")
cat("Mixed Models Output\n")
cat("All Participants Over All Time In All Coin Layouts\n")
cat("\n==================================\n")
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
out_used <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/model_all_rows_ElapsedTime_used.csv"

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

# ----------------------------
# Offending rows + model_df  (FIXED)
# ----------------------------
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
model_df_A  <- model_df %>% filter(coinset_str == "a")
model_df    <- model_df %>% filter(coinset_str != "a")

# ----------------------------
# Mixed model (random intercept only, like re_formula="1")
# ----------------------------
cat("\n\n\n\n\n")
cat("lmer: roundElapsed_s ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR + coinSet + (1|participantID)\n\n")

m_all <- lmer(
  roundElapsed_s ~ coinLabel * TotSesh_runTot_RoundNum * recentSwapRate_all +
    main_RR + coinSet + (1 | participantID),
  data = model_df,
  REML = FALSE
)
print(summary(m_all))

# ----------------------------
# OLS with participant-clustered SEs (statsmodels-like)
# ----------------------------
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
# ----------------------------
# Mixed model with PVSS (subset)
# ----------------------------
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
