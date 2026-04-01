# /path/to/analysis.R

# Disable reticulate autoconfig unless you explicitly need Python in this script
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
  library(performance)
  library(ggplot2)
  library(emmeans)
  source(here::here("RC_utilities/rUtilities", "r_helpers.R"))
  source(here::here("RC_utilities/rUtilities", "run_lmer_diagnostics.R"))
})


out_dir <- "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/analyses/results_R/mixedLinear_Other"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
out_txt <- file.path(out_dir, "mixedLinear_output.txt")
con <- file(out_txt, open = "wt", encoding = "UTF-8")
sink(con, split = TRUE)


cat("\n==================================\n")
cat("Mixed Models Output\n")
cat("All Participants Over All Time In All Coin Layouts\n")
cat("\n==================================\n")

trim_lower <- function(x) str_to_lower(str_trim(as.character(x)))

cluster_vcov <- function(lm_fit, cluster) {
  sandwich::vcovCL(lm_fit, cluster = cluster, type = "HC1")
}

print_clustered <- function(lm_fit, cluster, title) {
  cat("\n\n\n\n\n")
  cat(title, "\n\n")
  vc <- cluster_vcov(lm_fit, cluster)
  print(lmtest::coeftest(lm_fit, vcov. = vc))
  cat("\n")
}

dataFile <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/allIntervalDataKnotted_L1.csv"

out_offending <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/offending_rows_model_all.csv"
out_used <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/model_all_rows_used.csv"

df <- readr::read_csv(dataFile, show_col_types = FALSE)

df <- df %>%
  filter(trim_lower(BlockType) != "collecting") %>%
  mutate(CoinSetID_num = suppressWarnings(as.numeric(CoinSetID))) %>%
  filter(is.na(CoinSetID_num) | CoinSetID_num < 4) %>%
  mutate(
    participantID = as.factor(participantID),
    coinLabel     = relevel(as.factor(coinLabel), ref = "LV"),
    coinSet       = as.factor(coinSet),
    isSwap        = as.factor(isSwap),
    main_RR       = relevel(as.factor(main_RR), ref = "main"),
    path_order_round_num = relevel(factor(path_order_round_num), ref = "6")
  )

needed <- c(
  "roundElapsed_s", "coinLabel", "TotSesh_runTot_RoundNum", "recentSwapRate_all",
  "main_RR", "WalkAvgSpeed", "coinSet", "isSwap", "participantID",
  "t_early_15", "t_late_15", "t_early_20", "t_late_20", "t_early_25", "t_late_25",
  "path_eff_raw", "path_order_round_num"
)

missing_needed <- setdiff(needed, names(df))
if (length(missing_needed) > 0) {
  stop(sprintf("These required columns are missing: %s", paste(missing_needed, collapse = ", ")), call. = FALSE)
}

na_mat <- as.data.frame(lapply(df[needed], is.na))
row_has_any_na <- Reduce(`|`, na_mat)

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

cat("\nCounts (model_df)\n")
print(table(model_df$coinSet, useNA = "ifany"))
print(table(model_df$coinLabel, useNA = "ifany"))
print(table(model_df$isSwap, useNA = "ifany"))
print(table(model_df$main_RR, useNA = "ifany"))
cat("N =", nrow(model_df), "\n")

cat("\n\n\n\n\n")
cat("lmer:  WalkAvgSpeed ~ coinLabel*TotSesh_runTot_RoundNum*path_order_round_num + recentSwapRate_all + path_eff_raw +  main_RR  + coinSet (1|participantID)\n\n")

walkSpeed_round <- lmer(
  WalkAvgSpeed ~ coinLabel * TotSesh_runTot_RoundNum * path_order_round_num +
    recentSwapRate_all + path_eff_raw +
    main_RR + coinSet + (1 | participantID),
  data = model_df,
  REML = FALSE
)
print(summary(walkSpeed_round))
run_lmer_diagnostics(walkSpeed_round, name = "walkSpeed_round", out_dir = out_dir)

cat("\n\n\n\n\n")
cat("lmer:  WalkAvgSpeed ~ path_order_round_num * coinLabel + t_early_20+ t_late_20+ recentSwapRate_all + path_eff_raw +  main_RR  + coinSet (1|participantID)\n\n")

walkSpeed_knotted <- lmer(
  WalkAvgSpeed ~ path_order_round_num * coinLabel + t_early_20 + t_late_20 +
    recentSwapRate_all + path_eff_raw +
    main_RR + coinSet + (1 | participantID),
  data = model_df,
  REML = FALSE
)
print(summary(walkSpeed_knotted))
run_lmer_diagnostics(walkSpeed_knotted, name = "walkSpeed_knotted", out_dir = out_dir)


emm <- emmeans(walkSpeed_knotted, ~ path_order_round_num * coinLabel)
print(emm)
print(pairs(emm))
print(summary(emm))
print(plot(emm))

cat("\n\n\n\n\n")
cat("lmer:  WalkAvgSpeed ~ path_order_round_num * coinLabel (1|participantID)\n\n")

walkSpeed_knottedSmall <- lmer(
  WalkAvgSpeed ~ path_order_round_num + coinLabel + (1 | participantID),
  data = model_df,
  REML = FALSE
)
print(summary(walkSpeed_knottedSmall))
run_lmer_diagnostics(walkSpeed_knottedSmall, name = "walkSpeed_knottedSmall", out_dir = out_dir)


# emm2 <- emmeans(walkSpeed_knottedSmall, ~ path_order_round_num * coinLabel)
# print(emm2)
# print(pairs(emm2))
# print(summary(emm2))
# print(plot(emm2))


cat("\n\n\n\n\n")
cat("lmer:  path_eff_raw ~ coinLabel*TotSesh_runTot_RoundNum*recentSwapRate_all + main_RR +  coinSet (1|participantID)\n\n")

df_pe <- model_df %>% filter((chestPin_num) != 1)

pathEfficiency_round <- lmer(
  path_eff_raw ~ TotSesh_runTot_RoundNum * path_order_round_num +
    recentSwapRate_all +
    main_RR + coinSet + (1 | participantID),
  data = df_pe,
  REML = FALSE
)
print(summary(pathEfficiency_round))
run_lmer_diagnostics(pathEfficiency_round, name = "pathEfficiency_round", out_dir = out_dir)

cat("\n\n\n\n\n")
cat("lmer:  path_eff_raw ~  path_order_round_num  + t_early_20 + t_late_20 + recentSwapRate_all + main_RR +  coinSet (1|participantID)\n\n")

pathEfficiency_knotted <- lmer(
  path_eff_raw ~ path_order_round_num + t_early_20 + t_late_20 +
    recentSwapRate_all +
    main_RR + coinSet + (1 | participantID),
  data = df_pe,
  REML = FALSE
)
print(summary(pathEfficiency_knotted))
run_lmer_diagnostics(pathEfficiency_knotted, name = "pathEfficiency_knotted", out_dir = out_dir)

cat("\n\n\n\n\n")
cat("lmer:  path_eff_raw ~  path_order_round_num * (t_early_20 + t_late_20) + recentSwapRate_all + main_RR +  coinSet (1|participantID)\n\n")

pathEff_sepLearnPt <- lmer(
  path_eff_raw ~ path_order_round_num * (t_early_20 + t_late_20) +
    recentSwapRate_all + main_RR + coinSet +
    (1 + t_early_20 + t_late_20 | participantID),
  data = df_pe,
  REML = FALSE
)
print(summary(pathEff_sepLearnPt))
run_lmer_diagnostics(pathEff_sepLearnPt, name = "pathEff_sepLearnPt", out_dir = out_dir)

cat("\n\n\n\n\n")
cat("Path Efficiency No Baseline Treatment of Path Order Round Num\n\n")

df_pe$path_order_round_num <- factor(df_pe$path_order_round_num)
contrasts(df_pe$path_order_round_num) <- contr.sum

pathEff_noBaslinePathChoice <- lmer(
  path_eff_raw ~ path_order_round_num * (t_early_20 + t_late_20) +
    recentSwapRate_all + main_RR + coinSet +
    (1 | participantID),
  data = df_pe,
  REML = FALSE
)

print(summary(pathEff_noBaslinePathChoice))
run_lmer_diagnostics(pathEff_noBaslinePathChoice, name = "pathEff_noBaslinePathChoice", out_dir = out_dir)
print('done!')
sink()