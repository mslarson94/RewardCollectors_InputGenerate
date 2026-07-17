# mixedModel_dropDist_coinLabel_coinSet.R

rm(list = ls())

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tidyr)
  library(purrr)
  library(lme4)
  library(lmerTest)
  library(emmeans)
  library(broom.mixed)
})

options(width = 140)
options(stringsAsFactors = FALSE)

cat("\n================================================\n")
cat("Mixed Model: dropDist ~ coinLabel * coinSet\n")
cat("All participants over all valid non-tutorial data\n")
cat("================================================\n\n")

# ----------------------------
# Paths / user settings
# ----------------------------
dataFile <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain_PO_redo/EventSegmentation/megaFiles_filtered/roundDur_optionA_output/allIntervalData_AN_baseQC_roundDur_optionA_patched_noCD.csv"

outDir <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain_PO_redo/Analyses/mixedModel_dropDist_coinLabel_coinSet_noCD"
dir.create(outDir, recursive = TRUE, showWarnings = FALSE)

# Filtering switches
use_base_eligibility   <- TRUE
use_corrected_dropqual <- TRUE
keep_only_good_drops   <- FALSE
exclude_duration_outs  <- FALSE

# Tutorial handling
tutorial_coinsetid_max <- 3

# Random effects structure
random_formula <- "(1 | participantID)"

# ----------------------------
# Helpers
# ----------------------------
trim_lower <- function(x) {
  stringr::str_to_lower(trimws(as.character(x)))
}

pick_dropqual_col <- function(df) {
  if ("dropQual_corrected" %in% names(df)) {
    return("dropQual_corrected")
  }
  if ("dropQual" %in% names(df)) {
    return("dropQual")
  }
  stop("Neither dropQual_corrected nor dropQual found in data.")
}

factor_if_present <- function(df, cols) {
  cols_present <- intersect(cols, names(df))
  df %>% mutate(across(all_of(cols_present), as.factor))
}

write_section <- function(con, title, x = NULL) {
  writeLines(c("", paste0("==== ", title, " ====")), con = con)
  if (!is.null(x)) {
    writeLines(capture.output(print(x)), con = con)
  }
}

safe_coinset_name <- function(x) {
  x <- as.character(x)
  x <- gsub("[^A-Za-z0-9_]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  ifelse(nchar(x) == 0, "NA", x)
}

# ----------------------------
# Load + clean
# ----------------------------
cat("Input file:\n")
print(dataFile)

df <- readr::read_csv(dataFile, show_col_types = FALSE)

cat("\nData dimensions:\n")
print(dim(df))

cat("\nFirst few rows:\n")
print(dplyr::glimpse(df, width = 100))

dropqual_col <- pick_dropqual_col(df)
cat("\nUsing drop quality column:", dropqual_col, "\n")

df <- df %>%
  mutate(
    CoinSetID_num = suppressWarnings(as.numeric(CoinSetID)),
    dropDist = suppressWarnings(as.numeric(dropDist))
  )

# Optional legacy-style exclusion of collecting rows
if ("BlockType" %in% names(df)) {
  df <- df %>%
    filter(trim_lower(BlockType) != "collecting")
}

# Tutorial removal based on CoinSetID
if ("CoinSetID_num" %in% names(df)) {
  df <- df %>%
    filter(is.na(CoinSetID_num) | CoinSetID_num <= tutorial_coinsetid_max)
}

# Base eligibility
if (use_base_eligibility && "isEligibleBase" %in% names(df)) {
  df <- df %>%
    filter(isEligibleBase %in% c(TRUE, 1, "1"))
}

# Optional duration outlier exclusion
if (exclude_duration_outs && "roundDur_pref_out" %in% names(df)) {
  df <- df %>%
    filter(!(roundDur_pref_out %in% c(TRUE, 1, "1")))
}

# Optional good-drop restriction
if (keep_only_good_drops) {
  df <- df %>%
    filter(trim_lower(.data[[dropqual_col]]) == "good")
}

# Basic modeling subset
model_df <- df %>%
  filter(
    !is.na(dropDist),
    !is.na(coinLabel),
    !is.na(coinSet),
    !is.na(participantID)
  ) %>%
  factor_if_present(c(
    "participantID",
    "sessionID",
    "coinLabel",
    "coinSet",
    "isSwap",
    "main_RR",
    "path_order_round_num",
    "orderedCollect",
    "pathValue"
  ))

# Relevel coinLabel if present
if ("coinLabel" %in% names(model_df)) {
  model_df$coinLabel <- droplevels(model_df$coinLabel)
  if ("LV" %in% levels(model_df$coinLabel)) {
    model_df$coinLabel <- relevel(model_df$coinLabel, ref = "LV")
  }
}

if ("coinSet" %in% names(model_df)) {
  model_df$coinSet <- droplevels(model_df$coinSet)
}

cat("\nModel data dimensions:\n")
print(dim(model_df))

cat("\nCounts by coinLabel:\n")
print(table(model_df$coinLabel, useNA = "ifany"))

cat("\nCounts by coinSet:\n")
print(table(model_df$coinSet, useNA = "ifany"))

cat("\nCounts by coinSet x coinLabel:\n")
print(with(model_df, table(coinSet, coinLabel, useNA = "ifany")))

# ----------------------------
# Define specific coinSet dfs
# ----------------------------
coinset_levels <- levels(factor(model_df$coinSet))
model_df_list <- split(model_df, model_df$coinSet)

# Also create individual objects to match your earlier style
for (cs in coinset_levels) {
  obj_name <- paste0("model_df_coinSet_", safe_coinset_name(cs))
  assign(obj_name, droplevels(model_df_list[[cs]]), envir = .GlobalEnv)
}

# ----------------------------
# Save data actually used
# ----------------------------
out_used <- file.path(outDir, "model_data_used.csv")
readr::write_csv(model_df, out_used)

out_counts_coin <- file.path(outDir, "counts_by_coinLabel.csv")
out_counts_set <- file.path(outDir, "counts_by_coinSet.csv")
out_counts_cross <- file.path(outDir, "counts_by_coinSet_x_coinLabel.csv")

model_df %>%
  count(coinLabel, name = "n") %>%
  readr::write_csv(out_counts_coin)

model_df %>%
  count(coinSet, name = "n") %>%
  readr::write_csv(out_counts_set)

model_df %>%
  count(coinSet, coinLabel, name = "n") %>%
  readr::write_csv(out_counts_cross)

# ----------------------------
# Fit models
# ----------------------------
formula_interaction <- as.formula(
  paste("dropDist ~ coinLabel * coinSet +", random_formula)
)

formula_main <- as.formula(
  paste("dropDist ~ coinLabel + coinSet +", random_formula)
)

cat("\nFitting interaction model:\n")
print(formula_interaction)

mod_interaction <- lmer(
  formula_interaction,
  data = model_df,
  REML = FALSE
)

cat("\nFitting main-effects model:\n")
print(formula_main)

mod_main <- lmer(
  formula_main,
  data = model_df,
  REML = FALSE
)

# Likelihood-ratio comparison for interaction
model_cmp <- anova(mod_main, mod_interaction)

# Type III-ish tests from lmerTest
anova_interaction <- anova(mod_interaction, type = 3)
anova_main <- anova(mod_main, type = 3)

# Fixed effects tables
tidy_interaction <- broom.mixed::tidy(mod_interaction, effects = "fixed", conf.int = TRUE)
tidy_main <- broom.mixed::tidy(mod_main, effects = "fixed", conf.int = TRUE)

# EMMs
emm_coin_within_set <- emmeans(mod_interaction, ~ coinLabel | coinSet)
pairs_coin_within_set <- contrast(emm_coin_within_set, method = "pairwise", adjust = "holm") %>%
  as.data.frame()

emm_set_within_coin <- emmeans(mod_interaction, ~ coinSet | coinLabel)
pairs_set_within_coin <- contrast(emm_set_within_coin, method = "pairwise", adjust = "holm") %>%
  as.data.frame()

emm_coin_overall <- emmeans(mod_main, ~ coinLabel)
pairs_coin_overall <- contrast(emm_coin_overall, method = "pairwise", adjust = "holm") %>%
  as.data.frame()

emm_set_overall <- emmeans(mod_main, ~ coinSet)
pairs_set_overall <- contrast(emm_set_overall, method = "pairwise", adjust = "holm") %>%
  as.data.frame()

# ----------------------------
# Write outputs
# ----------------------------
readr::write_csv(tidy_interaction, file.path(outDir, "fixed_effects_interaction.csv"))
readr::write_csv(tidy_main, file.path(outDir, "fixed_effects_main.csv"))

readr::write_csv(as.data.frame(anova_interaction), file.path(outDir, "anova_interaction.csv"))
readr::write_csv(as.data.frame(anova_main), file.path(outDir, "anova_main.csv"))
readr::write_csv(as.data.frame(model_cmp), file.path(outDir, "model_comparison_interaction_vs_main.csv"))

readr::write_csv(as.data.frame(emm_coin_within_set), file.path(outDir, "emmeans_coinLabel_within_coinSet.csv"))
readr::write_csv(pairs_coin_within_set, file.path(outDir, "pairwise_coinLabel_within_coinSet.csv"))

readr::write_csv(as.data.frame(emm_set_within_coin), file.path(outDir, "emmeans_coinSet_within_coinLabel.csv"))
readr::write_csv(pairs_set_within_coin, file.path(outDir, "pairwise_coinSet_within_coinLabel.csv"))

readr::write_csv(as.data.frame(emm_coin_overall), file.path(outDir, "emmeans_coinLabel_overall_main_model.csv"))
readr::write_csv(pairs_coin_overall, file.path(outDir, "pairwise_coinLabel_overall_main_model.csv"))

readr::write_csv(as.data.frame(emm_set_overall), file.path(outDir, "emmeans_coinSet_overall_main_model.csv"))
readr::write_csv(pairs_set_overall, file.path(outDir, "pairwise_coinSet_overall_main_model.csv"))

# ----------------------------
# TXT report
# ----------------------------
report_file <- file.path(outDir, "model_report.txt")
con <- file(report_file, open = "wt")

writeLines("================================================", con)
writeLines("Mixed Model: dropDist ~ coinLabel * coinSet", con)
writeLines("================================================", con)

write_section(con, "Input file", dataFile)
write_section(con, "Output directory", outDir)

write_section(
  con,
  "Filtering flags",
  list(
    use_base_eligibility = use_base_eligibility,
    use_corrected_dropqual = use_corrected_dropqual,
    keep_only_good_drops = keep_only_good_drops,
    exclude_duration_outs = exclude_duration_outs,
    tutorial_coinsetid_max = tutorial_coinsetid_max,
    dropqual_col = dropqual_col
  )
)

write_section(con, "Model data dimensions", dim(model_df))
write_section(con, "Counts by coinLabel", model_df %>% count(coinLabel, name = "n"))
write_section(con, "Counts by coinSet", model_df %>% count(coinSet, name = "n"))
write_section(con, "Counts by coinSet x coinLabel", model_df %>% count(coinSet, coinLabel, name = "n"))

write_section(con, "Interaction model formula", formula_interaction)
write_section(con, "Main-effects model formula", formula_main)

write_section(con, "Interaction model summary", summary(mod_interaction))
write_section(con, "Main-effects model summary", summary(mod_main))
write_section(con, "Model comparison: main vs interaction", model_cmp)

write_section(con, "ANOVA interaction model", anova_interaction)
write_section(con, "ANOVA main model", anova_main)

write_section(con, "EMMs: coinLabel within coinSet", as.data.frame(emm_coin_within_set))
write_section(con, "Pairwise: coinLabel within coinSet", pairs_coin_within_set)

write_section(con, "EMMs: coinSet within coinLabel", as.data.frame(emm_set_within_coin))
write_section(con, "Pairwise: coinSet within coinLabel", pairs_set_within_coin)

write_section(con, "EMMs: coinLabel overall (main model)", as.data.frame(emm_coin_overall))
write_section(con, "Pairwise: coinLabel overall (main model)", pairs_coin_overall)

write_section(con, "EMMs: coinSet overall (main model)", as.data.frame(emm_set_overall))
write_section(con, "Pairwise: coinSet overall (main model)", pairs_set_overall)

close(con)

cat("\nDone.\n")
cat("Outputs written to:\n")
print(outDir)