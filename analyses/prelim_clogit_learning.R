# prelim_clogit_learning.R
# End-to-end prelim conditional logit + diagnostics + participant breakdown

suppressPackageStartupMessages({
  library(survival)
  library(readr)
  library(dplyr)
  library(here)
  library(ggplot2)
  
  source(here::here("RC_utilities/rUtilities", "r_helpers.R"))
})

# -------------------------
# 1) Read data + logging
# -------------------------
dataFile <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain/EventSegmentation/megaFiles/decisionExpanded_L1.csv"
df <- readr::read_csv(dataFile, show_col_types = FALSE)

out_dir <- "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/analyses/results_R/prelim_cLogit"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

out_txt <- file.path(out_dir, "prelim_cLogit_output.txt")

# Robust sink cleanup (prevents stuck sinks on re-run / error)
con <- file(out_txt, open = "wt", encoding = "UTF-8")
sink(con, split = TRUE)

cat("\n==================================\n")
cat("Conditional Logistic Regression Modeling Output\n")
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
    points,
    idealDistance,
    t_early_20,
    t_late_20,
    recentSwapRate_all
  ) %>%
  filter(
    !is.na(roundID),
    !is.na(chosen),
    !is.na(points),
    !is.na(idealDistance),
    !is.na(t_early_20),
    !is.na(t_late_20),
    !is.na(recentSwapRate_all)
  ) %>%
  mutate(
    roundID = as.factor(roundID),
    participantID = as.factor(participantID),
    alt = as.factor(alt),
    chosen = as.integer(chosen),
    coinSet = as.factor(coinSet),
    recentSwapRate_all = as.numeric(recentSwapRate_all),
    recentSwapRate_all_z = as.numeric(scale(recentSwapRate_all))
  )

cat("\nRows:", nrow(dat), "\n")
cat("Choice sets:", dplyr::n_distinct(dat$roundID), "\n")
cat("Participants:", dplyr::n_distinct(dat$participantID), "\n")

check_sets <- dat %>%
  group_by(roundID) %>%
  summarise(
    n_rows = dplyr::n(),
    n_chosen = sum(chosen),
    .groups = "drop"
  )

cat("\nChoice-set QC:\n")
print(table(check_sets$n_rows))
print(table(check_sets$n_chosen))

# -------------------------
# 3) Fit models
# -------------------------
m1_points <- clogit(
  chosen ~ points + strata(roundID),
  data = dat
)

m2_distance <- clogit(
  chosen ~ idealDistance + strata(roundID),
  data = dat
)

m3_value_distance <- clogit(
  chosen ~ points + idealDistance + strata(roundID),
  data = dat
)

mL3_learning <- clogit(
  chosen ~ points + idealDistance +
    points:t_early_20 + idealDistance:t_early_20 +
    points:t_late_20  + idealDistance:t_late_20 +
    strata(roundID),
  data = dat
)

mL4_learning_clustered <- clogit(
  chosen ~ points + idealDistance +
    points:t_early_20 + idealDistance:t_early_20 +
    points:t_late_20  + idealDistance:t_late_20 +
    strata(roundID) + cluster(participantID),
  data = dat,
  method = "efron"
)

mL5_learning_clustered <- clogit(
  chosen ~
    points + idealDistance +
    points:t_early_20 + idealDistance:t_early_20 +
    points:t_late_20  + idealDistance:t_late_20 +
    # swap-rate moderates sensitivities (identifiable)
    points:recentSwapRate_all_z +
    idealDistance:recentSwapRate_all_z +
    strata(roundID) + cluster(participantID),
  data = dat,
  method = "efron"
)

# -------------------------
# 4) Print summaries
# -------------------------
cat("\n\n==============================\n")
cat("M1: points only\n")
cat("==================================\n")
print(summary(m1_points))

cat("\n\n==============================\n")
cat("M2: distance only\n")
cat("==================================\n")
print(summary(m2_distance))

cat("\n\n==============================\n")
cat("M3: points + distance\n")
cat("==================================\n")
print(summary(m3_value_distance))

cat("\n\n==============================\n")
cat("L3: points + distance * learning\n")
cat("==================================\n")
print(summary(mL3_learning))

cat("\n\n=================================================\n")
cat("L4: (points + distance * learning)*participantCluster\n")
cat("=====================================================\n")
print(summary(mL4_learning_clustered))

cat("\n\n=================================================\n")
cat("L5: (points + distance * learning)*participant + recentSwapRate_all Cluster\n")
cat("=====================================================\n")
print(summary(mL5_learning_clustered))

# -------------------------
# 5) Participant diagnostics (coinSet / currentRole breakdown)
# -------------------------
cat("\n\n=====================\n")
cat("Participant Diagnostics\n")
cat("=======================\n")

run_participant_diagnostics <- function(
    fit,
    name,
    data,
    out_dir,
    strata_col = "roundID",
    chosen_col = "chosen",
    participant_col = "participantID",
    group_cols = c("coinSet"),
    bins = 10,
    top_n = 10,
    save_group_calibration_pdf = TRUE
) {
  stopifnot(all(group_cols %in% names(data)))
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  
  pred <- clogit_pred_choice_probs(fit, data = data, strata_col = strata_col) %>%
    dplyr::mutate(.err = .data[[chosen_col]] - .p)
  
  metrics_tbl <- function(df, by_cols) {
    top1 <- df %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(c(by_cols, participant_col, strata_col)))) %>%
      dplyr::summarise(
        hit = as.integer(which.max(.p) == which.max(.data[[chosen_col]])),
        .groups = "drop"
      ) %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(c(by_cols, participant_col)))) %>%
      dplyr::summarise(top1 = mean(hit), .groups = "drop")
    
    base <- df %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(c(by_cols, participant_col)))) %>%
      dplyr::summarise(
        n_rows = dplyr::n(),
        n_sets = dplyr::n_distinct(.data[[strata_col]]),
        mean_p_chosen = mean(.p[.data[[chosen_col]] == 1], na.rm = TRUE),
        brier = mean((.data[[chosen_col]] - .p)^2, na.rm = TRUE),
        mae = mean(abs(.err), na.rm = TRUE),
        .groups = "drop"
      )
    
    base %>%
      dplyr::left_join(top1, by = c(by_cols, participant_col)) %>%
      dplyr::arrange(dplyr::desc(brier))
  }
  
  per_pt <- metrics_tbl(pred, by_cols = character(0))
  readr::write_csv(per_pt, file.path(out_dir, paste0(name, "_per_participant_metrics.csv")))
  
  out_by <- list()
  for (g in group_cols) {
    tbl <- metrics_tbl(pred, by_cols = g)
    out_by[[g]] <- tbl
    readr::write_csv(tbl, file.path(out_dir, paste0(name, "_per_participant_by_", g, ".csv")))
  }
  
  both_key <- paste(group_cols, collapse = "_")
  tbl_both <- metrics_tbl(pred, by_cols = group_cols)
  out_by[[both_key]] <- tbl_both
  readr::write_csv(tbl_both, file.path(out_dir, paste0(name, "_per_participant_by_", both_key, ".csv")))
  
  plot_worst_groups <- function(tbl, by_cols, suffix) {
    agg <- tbl %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(by_cols))) %>%
      dplyr::summarise(
        brier_w = stats::weighted.mean(brier, w = .data[["n_rows"]], na.rm = TRUE),
        top1_w  = stats::weighted.mean(top1,  w = .data[["n_rows"]], na.rm = TRUE),
        n_rows_sum = sum(.data[["n_rows"]], na.rm = TRUE),
        n_sets_sum = sum(.data[["n_sets"]], na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::arrange(dplyr::desc(brier_w)) %>%
      dplyr::slice_head(n = min(top_n, nrow(.))) %>%
      dplyr::mutate(label = do.call(
        paste,
        c(dplyr::across(dplyr::all_of(by_cols)), sep = " | ")
      )) %>%
      dplyr::mutate(label = stats::reorder(label, brier_w))
    
    p <- ggplot2::ggplot(agg, ggplot2::aes(x = brier_w, y = label)) +
      ggplot2::geom_col() +
      ggplot2::labs(
        title = paste0(name, ": worst groups by Brier (", suffix, ")"),
        x = "Weighted Brier (mean (y - p)^2)",
        y = NULL
      ) +
      ggplot2::theme_minimal(base_size = 13)
    
    ggplot2::ggsave(
      file.path(out_dir, paste0(name, "_worst_groups_brier_", suffix, ".pdf")),
      p,
      width = 11,
      height = 7
    )
    
    readr::write_csv(
      agg,
      file.path(out_dir, paste0(name, "_worst_groups_brier_", suffix, ".csv"))
    )
  }
  
  plot_worst_groups(tbl_both, group_cols, both_key)
  for (g in group_cols) plot_worst_groups(out_by[[g]], g, g)
  
  if (isTRUE(save_group_calibration_pdf)) {
    worst_combos <- tbl_both %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) %>%
      dplyr::summarise(
        brier_w = stats::weighted.mean(brier, w = .data[["n_rows"]], na.rm = TRUE),
        n_rows_sum = sum(.data[["n_rows"]], na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::arrange(dplyr::desc(brier_w)) %>%
      dplyr::slice_head(n = min(top_n, nrow(.)))
    
    pdf_path <- file.path(out_dir, paste0(name, "_worst_group_calibration_", both_key, ".pdf"))
    grDevices::pdf(pdf_path, width = 10, height = 6)
    for (i in seq_len(nrow(worst_combos))) {
      filt <- rep(TRUE, nrow(pred))
      for (g in group_cols) {
        filt <- filt & (as.character(pred[[g]]) == as.character(worst_combos[[g]][i]))
      }
      sub <- pred[filt, , drop = FALSE]
      title <- paste0(
        name, ": calibration (",
        paste(
          paste0(group_cols, "=", sapply(group_cols, function(g) as.character(worst_combos[[g]][i]))),
          collapse = ", "
        ),
        ")"
      )
      print(plot_clogit_calibration(sub, chosen_col = chosen_col, bins = bins, title = title))
    }
    grDevices::dev.off()
  }
  
  invisible(list(pred = pred, per_participant = per_pt, by_group = out_by))
}

run_participant_diagnostics(
  fit = mL4_learning_clustered,
  name = "L4_learning_clustered",
  data = dat,
  out_dir = out_dir,
  top_n = 10
)

run_participant_diagnostics(
  fit = mL5_learning_clustered,
  name = "L5_learning_clustered",
  data = dat,
  out_dir = out_dir,
  top_n = 10
)

# -------------------------
# 6) Model comparison table
# -------------------------
model_list <- list(
  M1_points = m1_points,
  M2_distance = m2_distance,
  M3_value_distance = m3_value_distance,
  L3_learning = mL3_learning,
  L4_learning_clustered = mL4_learning_clustered,
  L5_learning_clustered = mL5_learning_clustered
)

cmp <- model_compare_tbl(model_list)
readr::write_csv(cmp, file.path(out_dir, "model_compare.csv"))

cat("\n\n==============================\n")
cat("Model Comparisons\n")
cat("==============================\n")
print(cmp)

# -------------------------
# 7) Implied lambda
# -------------------------
lambda_from <- function(fit) {
  b <- coef(fit)
  -b[["idealDistance"]] / b[["points"]]
}

cat("\n\n==============================\n")
cat("Implied lambda from M3\n")
cat("==================================\n")
cat("lambda =", as.numeric(lambda_from(m3_value_distance)), "\n")

cat("\n\n==============================\n")
cat("Implied lambda from L3\n")
cat("==================================\n")
cat("lambda =", as.numeric(lambda_from(mL3_learning)), "\n")

cat("\n\n==============================\n")
cat("Implied lambda from L4\n")
cat("==================================\n")
cat("lambda =", as.numeric(lambda_from(mL4_learning_clustered)), "\n")

cat("\n\n==============================\n")
cat("Implied lambda from L5\n")
cat("==================================\n")
cat("lambda =", as.numeric(lambda_from(mL5_learning_clustered)), "\n")


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
  "idealDistance:t_late_20" = "Distance × late",
  "points:recentSwapRate_all" = "Points x volatility",
  "idealDistance:recentSwapRate_all" = "Distance x volatility"
)

save_clogit_bundle(m1_points,              "M1_points",              out_dir, data = dat, term_recode = term_recode)
save_clogit_bundle(m2_distance,            "M2_distance",            out_dir, data = dat, term_recode = term_recode)
save_clogit_bundle(m3_value_distance,      "M3_value_distance",      out_dir, data = dat, term_recode = term_recode)
save_clogit_bundle(mL3_learning,           "L3_learning",            out_dir, data = dat, term_recode = term_recode)
save_clogit_bundle(mL4_learning_clustered, "L4_learning_clustered",  out_dir, data = dat, term_recode = term_recode)
save_clogit_bundle(mL5_learning_clustered, "L5_Learning_clustered",  out_dir, data = dat, term_recode = term_recode)

cat("\n\nDone. Outputs saved in:\n", out_dir, "\n")
sink()
# sink cleanup happens via on.exit()