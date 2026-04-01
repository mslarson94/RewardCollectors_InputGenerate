# r_helpers.R
# Lightweight reusable helpers for discrete-choice (survival::clogit/coxph) and lme4::lmer models.
#
# Usage (in any analysis script):
#   if (!requireNamespace("here", quietly = TRUE)) install.packages("here")
#   source(here::here("utilities", "r_helpers.R"))
#
# Notes:
# - No side effects on load (only function definitions).
# - Functions use explicit package namespaces where practical.
# - Prefer dot+whisker (estimate + 95% CI). Optional p-value "stars" supported.

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

assert_pkg <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(sprintf("Package '%s' is required. Install with: install.packages('%s')", pkg, pkg), call. = FALSE)
  }
  invisible(TRUE)
}

ensure_dir <- function(path) {
  if (!base::dir.exists(path)) base::dir.create(path, recursive = TRUE)
  invisible(path)
}

save_png <- function(plot, path, w = 10, h = 5, dpi = 300) {
  assert_pkg("ggplot2")
  ggplot2::ggsave(path, plot = plot, width = w, height = h, dpi = dpi)
  invisible(path)
}

p_stars <- function(p) {
  dplyr::case_when(
    is.na(p) ~ "",
    p < 0.001 ~ "***",
    p < 0.01  ~ "**",
    p < 0.05  ~ "*",
    p < 0.1   ~ "·",
    TRUE      ~ ""
  )
}

# -------------------------
# Conditional logit helpers (survival::clogit)
# -------------------------

#' Tidy a survival::clogit (or coxph) model summary into a dataframe suitable for plotting.
#' Computes Wald 95% CI on the coefficient scale and converts to odds ratios.
#'
#' @param fit A fitted survival::clogit model (inherits from "coxph").
#' @param term_recode Named character vector for term relabeling (old_name = "New label").
#' @return tibble with term, estimate, se, conf.low, conf.high, p, stars, or, or.low, or.high, term_label.
tidy_clogit <- function(fit, term_recode = NULL) {
  assert_pkg("survival")
  assert_pkg("dplyr")
  assert_pkg("tibble")

  s <- base::summary(fit)
  co <- base::as.data.frame(s$coefficients)

  # survival summary commonly includes: coef, exp(coef), se(coef), robust se (optional), z, Pr(>|z|)
  se_col <- if ("robust se" %in% base::names(co)) "robust se" else "se(coef)"

  out <- tibble::tibble(
    term = base::rownames(co),
    estimate = base::unname(co[["coef"]]),
    se = base::unname(co[[se_col]]),
    z = base::unname(co[["z"]]),
    p = base::unname(co[["Pr(>|z|)"]])
  ) |>
    dplyr::mutate(
      conf.low = estimate - 1.96 * se,
      conf.high = estimate + 1.96 * se,
      or = base::exp(estimate),
      or.low = base::exp(conf.low),
      or.high = base::exp(conf.high),
      stars = p_stars(p)
    )

  if (!is.null(term_recode)) {
    out <- out |>
      dplyr::mutate(term_label = dplyr::recode(term, !!!term_recode))
  } else {
    out <- out |>
      dplyr::mutate(term_label = term)
  }

  out
}

#' Dot + whisker coefficient plot on the log-odds scale.
plot_coef_dot <- function(tidy_df, title, xlab = "Coefficient (log-odds)", annotate_stars = TRUE) {
  assert_pkg("ggplot2")
  assert_pkg("dplyr")

  df <- tidy_df |>
    dplyr::mutate(term_label = stats::reorder(term_label, estimate))

  p <- ggplot2::ggplot(df, ggplot2::aes(x = estimate, y = term_label)) +
    ggplot2::geom_vline(xintercept = 0, linetype = "dashed") +
    ggplot2::geom_errorbar(ggplot2::aes(xmin = conf.low, xmax = conf.high), width = 0.15) +
    ggplot2::geom_point(size = 2.8) +
    ggplot2::labs(title = title, x = xlab, y = NULL) +
    ggplot2::theme_minimal(base_size = 14)

  if (annotate_stars) {
    p <- p + ggplot2::geom_text(ggplot2::aes(x = conf.high, label = stars), hjust = -0.2, size = 5)
  }
  p
}

#' Odds ratio dot + whisker plot (log x-scale).
plot_or_dot <- function(tidy_df, title, annotate_stars = TRUE) {
  assert_pkg("ggplot2")
  assert_pkg("dplyr")

  df <- tidy_df |>
    dplyr::mutate(term_label = stats::reorder(term_label, or))

  p <- ggplot2::ggplot(df, ggplot2::aes(x = or, y = term_label)) +
    ggplot2::geom_vline(xintercept = 1, linetype = "dashed") +
    ggplot2::geom_errorbar(ggplot2::aes(xmin = or.low, xmax = or.high), width = 0.15) +
    ggplot2::geom_point(size = 2.8) +
    ggplot2::scale_x_log10() +
    ggplot2::labs(title = title, x = "Odds ratio (log scale)", y = NULL) +
    ggplot2::theme_minimal(base_size = 14)

  if (annotate_stars) {
    p <- p + ggplot2::geom_text(ggplot2::aes(x = or.high, label = stars), hjust = -0.2, size = 5)
  }
  p
}

#' Simple fit stats for model annotation.
clogit_fit_stats <- function(fit) {
  assert_pkg("tibble")
  tibble::tibble(
    logLik = base::as.numeric(stats::logLik(fit)[1]),
    AIC = stats::AIC(fit),
    BIC = stats::BIC(fit)
  )
}

#' Compare models by logLik/AIC/BIC; adds delta AIC/BIC.
model_compare_tbl <- function(model_list) {
  assert_pkg("tibble")
  assert_pkg("dplyr")
  tibble::tibble(
    model = base::names(model_list),
    logLik = base::sapply(model_list, function(m) base::as.numeric(stats::logLik(m)[1])),
    AIC = base::sapply(model_list, stats::AIC),
    BIC = base::sapply(model_list, stats::BIC)
  ) |>
    dplyr::arrange(AIC) |>
    dplyr::mutate(
      dAIC = AIC - base::min(AIC, na.rm = TRUE),
      dBIC = BIC - base::min(BIC, na.rm = TRUE)
    )
}

#' Delta IC (AIC/BIC) dot plot.
plot_delta_ic <- function(compare_df, which = c("dAIC", "dBIC"), title = NULL) {
  assert_pkg("ggplot2")
  assert_pkg("dplyr")
  which <- base::match.arg(which)

  df <- compare_df |>
    dplyr::mutate(model = stats::reorder(model, .data[[which]]))

  ggplot2::ggplot(df, ggplot2::aes(x = .data[[which]], y = model)) +
    ggplot2::geom_point(size = 3) +
    ggplot2::geom_vline(xintercept = 0, linetype = "dashed") +
    ggplot2::labs(
      title = title %||% paste0(which, " (lower is better)"),
      x = which,
      y = NULL
    ) +
    ggplot2::theme_minimal(base_size = 14)
}

#' Compute per-alternative predicted probabilities within each choice set via softmax on linear predictor.
#' Requires that `data` contains the strata column used in clogit (e.g., roundID).
clogit_pred_choice_probs <- function(fit, data, strata_col = "roundID") {
  assert_pkg("dplyr")

  lp <- base::as.numeric(stats::predict(fit, newdata = data, type = "lp"))
  data |>
    dplyr::mutate(.lp = lp) |>
    dplyr::group_by(.data[[strata_col]]) |>
    dplyr::mutate(
      .exp_lp = base::exp(.lp - base::max(.lp, na.rm = TRUE)),
      .p = .exp_lp / base::sum(.exp_lp, na.rm = TRUE)
    ) |>
    dplyr::ungroup()
}

#' Top-1 accuracy within each choice set: does the highest-prob alternative match the chosen one?
clogit_top1_accuracy <- function(pred_df, strata_col = "roundID", chosen_col = "chosen") {
  assert_pkg("dplyr")
  per_set <- pred_df |>
    dplyr::group_by(.data[[strata_col]]) |>
    dplyr::summarise(
      chosen_alt = base::which.max(.data[[chosen_col]]),
      pred_alt = base::which.max(.p),
      hit = base::as.integer(chosen_alt == pred_alt),
      .groups = "drop"
    )
  base::mean(per_set$hit)
}

#' Calibration plot: binned observed choice rate vs mean predicted probability.
plot_clogit_calibration <- function(pred_df, chosen_col = "chosen", bins = 10, title = "Calibration (binned)") {
  assert_pkg("dplyr")
  assert_pkg("ggplot2")

  df <- pred_df |>
    dplyr::mutate(bin = dplyr::ntile(.p, bins)) |>
    dplyr::group_by(bin) |>
    dplyr::summarise(
      p_mean = base::mean(.p),
      chosen_rate = base::mean(.data[[chosen_col]]),
      n = dplyr::n(),
      .groups = "drop"
    )

  ggplot2::ggplot(df, ggplot2::aes(x = p_mean, y = chosen_rate)) +
    ggplot2::geom_point(size = 3) +
    ggplot2::geom_abline(intercept = 0, slope = 1, linetype = "dashed") +
    ggplot2::labs(title = title, x = "Mean predicted probability (bin)", y = "Observed choice rate (bin)") +
    ggplot2::theme_minimal(base_size = 14)
}

#' Basic residual diagnostics for clogit: deviance residuals vs linear predictor and QQ plot.
plot_clogit_residuals <- function(fit, title_prefix = "clogit") {
  assert_pkg("ggplot2")
  r <- base::as.numeric(stats::residuals(fit, type = "deviance"))
  lp <- base::as.numeric(stats::predict(fit, type = "lp"))
  df <- tibble::tibble(lp = lp, resid = r)

  p1 <- ggplot2::ggplot(df, ggplot2::aes(x = lp, y = resid)) +
    ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
    ggplot2::geom_point(alpha = 0.4) +
    ggplot2::labs(
      title = paste0(title_prefix, ": deviance residuals vs linear predictor"),
      x = "Linear predictor",
      y = "Deviance residual"
    ) +
    ggplot2::theme_minimal(base_size = 14)

  p2 <- ggplot2::ggplot(df, ggplot2::aes(sample = resid)) +
    ggplot2::stat_qq() +
    ggplot2::stat_qq_line() +
    ggplot2::labs(title = paste0(title_prefix, ": QQ deviance residuals"), x = NULL, y = NULL) +
    ggplot2::theme_minimal(base_size = 14)

  list(resid_vs_lp = p1, qq = p2)
}

#' Influence summary: max absolute dfbeta per term.
plot_clogit_dfbeta <- function(fit, top_k = 12, title_prefix = "clogit") {
  assert_pkg("ggplot2")
  assert_pkg("dplyr")
  assert_pkg("tidyr")
  
  db <- tryCatch(
    stats::residuals(fit, type = "dfbeta"),
    error = function(e) NULL
  )
  
  if (is.null(db)) return(NULL)
  
  db <- base::as.matrix(db)
  if (base::ncol(db) == 0) return(NULL)
  
  df <- base::as.data.frame(db)
  df$.idx <- base::seq_len(base::nrow(df))
  
  long <- df |>
    tidyr::pivot_longer(cols = - .idx, names_to = "term", values_to = "dfbeta") |>
    dplyr::group_by(term) |>
    dplyr::summarise(max_abs = base::max(base::abs(dfbeta), na.rm = TRUE), .groups = "drop") |>
    dplyr::arrange(dplyr::desc(max_abs))
  
  top_n <- base::min(top_k, base::nrow(long))
  long <- utils::head(long, top_n) |>
    dplyr::mutate(term = stats::reorder(term, max_abs))
  
  ggplot2::ggplot(long, ggplot2::aes(x = max_abs, y = term)) +
    ggplot2::geom_col() +
    ggplot2::labs(
      title = paste0(title_prefix, ": max |dfbeta| by term"),
      x = "Max absolute dfbeta",
      y = NULL
    ) +
    ggplot2::theme_minimal(base_size = 14)
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

  ggplot2::ggsave(file.path(out_dir, paste0(name, "_coef.pdf")), p_coef, width = 11, height = 7)
  ggplot2::ggsave(file.path(out_dir, paste0(name, "_OR.pdf")),   p_or,   width = 11, height = 7)

  # Residual diagnostics
  diag <- plot_clogit_residuals(fit, title_prefix = name)
  grDevices::pdf(file.path(out_dir, paste0(name, "_residual_diagnostics.pdf")), width = 11, height = 7)
  for (p in diag) print(p)
  grDevices::dev.off()

  # Calibration (within-strata predicted probs)
  pred <- clogit_pred_choice_probs(fit, data = data, strata_col = strata_col)
  p_cal <- plot_clogit_calibration(pred, chosen_col = chosen_col, bins = calib_bins,
                                  title = paste0(name, ": calibration (binned)"))
  ggplot2::ggsave(file.path(out_dir, paste0(name, "_calibration.pdf")), p_cal, width = 10, height = 6)

  # Influence (dfbeta) if available
  p_inf <- plot_clogit_dfbeta(fit, top_k = 15, title_prefix = name)
  if (!is.null(p_inf)) {
    ggplot2::ggsave(file.path(out_dir, paste0(name, "_dfbeta.pdf")), p_inf, width = 11, height = 7)
  } else {
    cat("dfbeta not available for:", name, "\n")
  }

  fs <- clogit_fit_stats(fit)
  print(fs)

  invisible(list(tidy = td, pred = pred, fit_stats = fs))
}

# -------------------------
# lmer helpers (lme4 / lmerTest)
# -------------------------

#' Tidy fixed effects from an lmer model summary (with optional p-values if lmerTest used).
lmer_fixef_df <- function(fit) {
  assert_pkg("lme4")
  assert_pkg("dplyr")
  assert_pkg("tibble")

  sm <- base::summary(fit)
  co <- base::as.data.frame(sm$coefficients)

  pcol <- if ("Pr(>|t|)" %in% base::names(co)) "Pr(>|t|)" else NA_character_

  out <- tibble::tibble(
    term = base::rownames(co),
    estimate = base::unname(co[["Estimate"]]),
    se = base::unname(co[["Std. Error"]]),
    t = base::unname(co[[base::grep("^t value$|^t value", base::names(co), value = TRUE)[1]]]),
    p = if (!is.na(pcol)) base::unname(co[[pcol]]) else NA_real_
  ) |>
    dplyr::mutate(
      conf.low = estimate - 1.96 * se,
      conf.high = estimate + 1.96 * se,
      stars = p_stars(p),
      term_label = term
    )

  out
}

#' Dot + whisker plot for lmer fixed effects.
plot_lmer_fixef <- function(df, title, annotate_stars = TRUE) {
  assert_pkg("ggplot2")
  assert_pkg("dplyr")

  df <- df |>
    dplyr::mutate(term_label = stats::reorder(term_label, estimate))

  p <- ggplot2::ggplot(df, ggplot2::aes(x = estimate, y = term_label)) +
    ggplot2::geom_vline(xintercept = 0, linetype = "dashed") +
    ggplot2::geom_errorbar(ggplot2::aes(xmin = conf.low, xmax = conf.high), width = 0.15) +
    ggplot2::geom_point(size = 2.8) +
    ggplot2::labs(title = title, x = "Fixed effect estimate", y = NULL) +
    ggplot2::theme_minimal(base_size = 14)

  if (annotate_stars) {
    p <- p + ggplot2::geom_text(ggplot2::aes(x = conf.high, label = stars), hjust = -0.2, size = 5)
  }
  p
}

#' Variance components and ICC for random-intercept lmer models.
lmer_vcomp_icc <- function(fit) {
  assert_pkg("lme4")
  assert_pkg("dplyr")
  assert_pkg("tibble")

  vc <- base::as.data.frame(lme4::VarCorr(fit))
  re_var <- vc |>
    dplyr::filter(var1 == "(Intercept)") |>
    dplyr::summarise(re_var = base::sum(vcov, na.rm = TRUE), .groups = "drop") |>
    dplyr::pull(re_var)

  resid_var <- stats::sigma(fit)^2
  icc <- re_var / (re_var + resid_var)

  tibble::tibble(re_var = re_var, resid_var = resid_var, icc = icc)
}

#' Caterpillar plot of random intercept BLUPs.
plot_lmer_ranef_caterpillar <- function(fit, group = "participantID", title = NULL, top_n = NULL) {
  assert_pkg("lme4")
  assert_pkg("ggplot2")
  assert_pkg("dplyr")
  assert_pkg("tibble")

  re <- lme4::ranef(fit)[[group]]
  if (is.null(re)) stop(sprintf("No random effects group named '%s' found.", group), call. = FALSE)

  df <- tibble::tibble(level = base::rownames(re), intercept = re[["(Intercept)"]])
  if (!is.null(top_n)) {
    df <- df |>
      dplyr::slice_max(order_by = base::abs(intercept), n = top_n, with_ties = FALSE)
  }
  df <- df |>
    dplyr::mutate(level = factor(level, levels = level[base::order(intercept)]))

  ggplot2::ggplot(df, ggplot2::aes(x = intercept, y = level)) +
    ggplot2::geom_vline(xintercept = 0, linetype = "dashed") +
    ggplot2::geom_point(size = 2.5) +
    ggplot2::labs(
      title = title %||% paste0("Random intercepts: ", group),
      x = "BLUP (random intercept)",
      y = NULL
    ) +
    ggplot2::theme_minimal(base_size = 14)
}

#' QQ plot of random intercepts for a given grouping factor.
plot_lmer_ranef_qq <- function(fit, group = "participantID", title = NULL) {
  assert_pkg("lme4")
  assert_pkg("ggplot2")
  assert_pkg("tibble")

  re <- lme4::ranef(fit)[[group]]
  if (is.null(re)) return(NULL)

  df <- tibble::tibble(x = re[["(Intercept)"]])

  ggplot2::ggplot(df, ggplot2::aes(sample = x)) +
    ggplot2::stat_qq() +
    ggplot2::stat_qq_line() +
    ggplot2::labs(title = title %||% paste0("QQ random intercepts: ", group), x = NULL, y = NULL) +
    ggplot2::theme_minimal(base_size = 14)
}

#' Residual diagnostics for lmer: residuals vs fitted, QQ residuals, observed vs fitted.
plot_lmer_diag <- function(fit, title_prefix = "lmer") {
  assert_pkg("ggplot2")
  assert_pkg("tibble")

  df <- tibble::tibble(
    fitted = base::as.numeric(stats::fitted(fit)),
    resid = base::as.numeric(stats::residuals(fit))
  )

  p1 <- ggplot2::ggplot(df, ggplot2::aes(x = fitted, y = resid)) +
    ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
    ggplot2::geom_point(alpha = 0.4) +
    ggplot2::labs(title = paste0(title_prefix, ": residuals vs fitted"), x = "Fitted", y = "Residual") +
    ggplot2::theme_minimal(base_size = 14)

  p2 <- ggplot2::ggplot(df, ggplot2::aes(sample = resid)) +
    ggplot2::stat_qq() +
    ggplot2::stat_qq_line() +
    ggplot2::labs(title = paste0(title_prefix, ": QQ residuals"), x = NULL, y = NULL) +
    ggplot2::theme_minimal(base_size = 14)

  p3 <- ggplot2::ggplot(df, ggplot2::aes(x = fitted, y = fitted + resid)) +
    ggplot2::geom_point(alpha = 0.3) +
    ggplot2::geom_abline(intercept = 0, slope = 1, linetype = "dashed") +
    ggplot2::labs(title = paste0(title_prefix, ": observed vs fitted"), x = "Fitted", y = "Observed") +
    ggplot2::theme_minimal(base_size = 14)

  list(resid_vs_fitted = p1, qq_resid = p2, obs_vs_fit = p3)
}

#' Simple variance components plot with ICC in the title.
plot_vcomp <- function(vcomp_tbl, title = "Variance components") {
  assert_pkg("ggplot2")
  assert_pkg("tibble")
  df <- tibble::tibble(
    component = c("Random intercept", "Residual"),
    variance = c(vcomp_tbl$re_var[1], vcomp_tbl$resid_var[1])
  )

  ggplot2::ggplot(df, ggplot2::aes(x = component, y = variance)) +
    ggplot2::geom_col() +
    ggplot2::labs(
      title = paste0(title, sprintf(" (ICC = %.3f)", vcomp_tbl$icc[1])),
      x = NULL,
      y = "Variance"
    ) +
    ggplot2::theme_minimal(base_size = 14)
}
