# clogit_learning_plot.R

library(survival)
library(readr)
library(dplyr)
library(ggplot2)
library(tibble)
library(here)
source(here::here("RC_utilities/rUtilities", "r_helpers.R"))
source(here::here("RC_utilities/rUtilities", "run_lmer_diagnostics.R"))
# -------------------------
# 1) Read data
# -------------------------
dataFile <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgain/EventSegmentation/megaFiles/decisionExpandedKnotted_L1.csv"
df <- read_csv(dataFile, show_col_types = FALSE)

out_dir <- "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/analyses/results_R/cLogit_L4"
if (!dir.exists(out_dir)) {
  dir.create(out_dir, recursive = TRUE)
}
out_txt <- file.path(out_dir, "clogit_L4_output.txt")
out_png <- file.path(out_dir, "L4_coef_plot.png")
sink(out_txt, split = TRUE)
cat("\n==================================\n")
cat("Conditional Logistic Regression Learning Model Output\n")
cat("All Participants Over All Time In All Coin Layouts\n")
cat("\n==================================\n")
# -------------------------
# 2) Basic cleaning
# -------------------------
dat <- df %>%
  select(
    roundID,
    participantID,
    alt,
    chosen,
    coinSet,
    currentRole,
    points,
    idealDistance,
    t_early_20,
    t_late_20
  ) %>%
  filter(
    !is.na(roundID),
    !is.na(chosen),
    !is.na(points),
    !is.na(idealDistance),
    !is.na(t_early_20),
    !is.na(t_late_20)
  ) %>%
  mutate(
    roundID = as.factor(roundID),
    participantID = as.factor(participantID),
    alt = as.factor(alt),
    chosen = as.integer(chosen),
    coinSet = as.factor(coinSet),
    currentRole = as.factor(currentRole)
  )

# Optional sanity checks
cat("\nRows:", nrow(dat), "\n")
cat("Choice sets:", n_distinct(dat$roundID), "\n")
cat("Participants:", n_distinct(dat$participantID), "\n")

# Each round should have exactly 6 rows and exactly one chosen==1
check_sets <- dat %>%
  group_by(roundID) %>%
  summarise(
    n_rows = n(),
    n_chosen = sum(chosen),
    .groups = "drop"
  )

cat("\nChoice-set QC:\n")
print(table(check_sets$n_rows))
print(table(check_sets$n_chosen))

# -------------------------
# 4) Learning + Participant model 
# -------------------------
mL4_learning_clustered <- clogit(
  chosen ~ points + idealDistance +
    points:t_early_20 + idealDistance:t_early_20 +
    points:t_late_20  + idealDistance:t_late_20 +
    strata(roundID) + cluster(participantID),
  data = dat,
  method = "efron"
)

# -------------------------
# 5) Print summaries
# -------------------------

cat("\n\n=================================================\n")
cat("L4: (points + distance * learning)*participantCluster\n")
cat("=====================================================\n")
print(summary(mL4_learning_clustered))

# -------------------------
# 6) Model comparison table
# -------------------------
model_list <- list(
  L4_learning_clustered = mL4_learning_clustered
)

model_compare <- tibble(
  model = names(model_list),
  logLik = sapply(model_list, function(m) as.numeric(logLik(m)[1])),
  AIC = sapply(model_list, AIC),
  BIC = sapply(model_list, BIC)
) %>%
  arrange(AIC)


cat("\n\n==============================\n")
cat("Model Comparisons\n")
cat("==============================\n")
print(model_compare)

# --------------------------
# 9) Implied lambda from L4
# --------------------------
bL4 <- coef(mL4_learning_clustered)

lambda_impliedL4 <- -bL4["idealDistance"] / bL4["points"]

cat("\n\n==============================\n")
cat("Implied lambda from L4\n")
cat("==================================\n")
cat("lambda =", as.numeric(lambda_impliedL4), "\n")


# -----------------------------------
# Coefficient plot for L4
# -----------------------------------
cat("\nReached plotting block\n")

coef_mat <- vcov(mL4_learning_clustered)
cat("vcov dims:", dim(coef_mat), "\n")

coef_df <- tibble(
  term = names(coef(mL4_learning_clustered)),
  estimate = as.numeric(coef(mL4_learning_clustered)),
  se = sqrt(diag(coef_mat))
)

cat("Built coef_df\n")
print(coef_df)

coef_df <- coef_df %>%
  mutate(
    conf.low = estimate - 1.96 * se,
    conf.high = estimate + 1.96 * se,
    term_label = dplyr::recode(
      term,
      "points" = "Value",
      "idealDistance" = "Distance",
      "points:t_early_20" = "Value × Early learning",
      "idealDistance:t_early_20" = "Distance × Early learning",
      "points:t_late_20" = "Value × Late learning",
      "idealDistance:t_late_20" = "Distance × Late learning"
    )
  )

cat("Finished mutate\n")

p1 <- ggplot(coef_df, aes(x = estimate, y = reorder(term_label, estimate))) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  geom_point(size = 3) +
  geom_errorbar(aes(xmin = conf.low, xmax = conf.high), orientation = "y", width = 0.15) +
  labs(
    title = "Conditional logit coefficients with participant-clustered SEs",
    x = "Coefficient estimate (log-odds scale)",
    y = NULL
  ) +
  theme_minimal(base_size = 18)

cat("Built p1\n")
print(p1)

ggsave(out_png, plot = p1, width = 20, height = 4.5, dpi = 300)
cat("Saved plot to L4_coef_plot.png\n")

# -------------------------
# 8) Plots + model diagnostics (saved to out_dir)
# -------------------------
ensure_dir(out_dir)

save_pdf <- function(plot, path, w = 10, h = 6) {
  ggplot2::ggsave(path, plot = plot, width = w, height = h, device = grDevices::cairo_pdf)
  invisible(path)
}

save_plot_list_pdf <- function(plot_list, path, w = 10, h = 6) {
  grDevices::pdf(path, width = w, height = h)
  for (p in plot_list) print(p)
  grDevices::dev.off()
  invisible(path)
}

save_clogit_bundle <- function(
    fit,
    name,
    out_dir,
    data,
    strata_col = "roundID",
    chosen_col = "chosen",
    term_recode = NULL,
    calib_bins = 15
) {
  cat("\n\n--- Diagnostics:", name, "---\n")
  
  td <- tidy_clogit(fit, term_recode = term_recode)
  
  p_coef <- plot_coef_dot(td, title = paste0(name, " (coef, log-odds)"), annotate_stars = TRUE)
  p_or   <- plot_or_dot(td, title = paste0(name, " (odds ratios)"), annotate_stars = TRUE)
  
  save_pdf(p_coef, file.path(out_dir, paste0(name, "_coef.pdf")), w = 11, h = 7)
  save_pdf(p_or,   file.path(out_dir, paste0(name, "_OR.pdf")),   w = 11, h = 7)
  
  diag <- plot_clogit_residuals(fit, title_prefix = name)
  save_plot_list_pdf(diag, file.path(out_dir, paste0(name, "_residual_diagnostics.pdf")), w = 11, h = 7)
  
  pred <- clogit_pred_choice_probs(fit, data = data, strata_col = strata_col)
  p_cal <- plot_clogit_calibration(
    pred,
    chosen_col = chosen_col,
    bins = calib_bins,
    title = paste0(name, ": calibration (binned)")
  )
  save_pdf(p_cal, file.path(out_dir, paste0(name, "_calibration.pdf")), w = 11, h = 7)
  
  p_inf <- plot_clogit_dfbeta(fit, top_k = 15, title_prefix = name)
  if (!is.null(p_inf)) {
    save_pdf(p_inf, file.path(out_dir, paste0(name, "_dfbeta.pdf")), w = 11, h = 7)
  } else {
    cat("No dfbeta terms available for:", name, "\n")
  }
  
  fs <- clogit_fit_stats(fit)
  print(fs)
  
  readr::write_csv(td, file.path(out_dir, paste0(name, "_coef_table.csv")))
  
  invisible(list(tidy = td, pred = pred, fit_stats = fs))
}

term_recode <- c(
  "points" = "Points",
  "idealDistance" = "Ideal distance",
  "points:t_early_20" = "Points × early",
  "idealDistance:t_early_20" = "Distance × early",
  "points:t_late_20" = "Points × late",
  "idealDistance:t_late_20" = "Distance × late"
)

save_clogit_bundle(mL4_learning_clustered, "L4_learning_clustered",  out_dir, data = dat, term_recode = term_recode)

cat("\n\nDone. Outputs saved in:\n", out_dir, "\n")

# sink cleanup happens via on.exit()

sink()