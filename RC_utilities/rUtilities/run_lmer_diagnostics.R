# diagnostics/run_lmer_diagnostics.R

.extract_raw_predictors <- function(model) {
  requireNamespace("lme4", quietly = TRUE)
  
  fixed_form <- lme4::nobars(stats::formula(model))
  mf <- stats::model.frame(fixed_form, data = stats::model.frame(model), na.action = stats::na.pass)
  
  # Drop response column (first column in model.frame(fixed_form) is response if present)
  resp_name <- all.vars(stats::update(fixed_form, . ~ 0))
  # Safer: remove the response by using terms information
  tt <- stats::terms(fixed_form)
  resp <- as.character(attr(tt, "variables")[[2]])
  if (!is.null(resp) && resp %in% names(mf)) mf[[resp]] <- NULL
  
  is_num <- vapply(
    mf,
    function(x) is.numeric(x) || is.integer(x) || is.logical(x),
    logical(1)
  )
  
  dropped <- names(mf)[!is_num]
  X <- mf[, is_num, drop = FALSE]
  
  if (ncol(X) == 0) {
    stop("No numeric raw predictors found in the model frame to compute correlations.")
  }
  
  attr(X, "dropped_non_numeric") <- dropped
  X
}

.make_corr_heatmap <- function(
    X,
    title = "Predictor correlation (raw numeric predictors)",
    digits = 2,
    text_size = 3,
    label_threshold = 0
) {
  requireNamespace("ggplot2", quietly = TRUE)
  
  C <- stats::cor(X, use = "pairwise.complete.obs")
  df <- as.data.frame(as.table(C), stringsAsFactors = FALSE)
  names(df) <- c("Var1", "Var2", "r")
  
  df$label <- ""
  keep <- !is.na(df$r) & abs(df$r) >= label_threshold
  df$label[keep] <- formatC(df$r[keep], format = "f", digits = digits)
  
  ggplot2::ggplot(df, ggplot2::aes(Var1, Var2, fill = r)) +
    ggplot2::geom_tile() +
    ggplot2::geom_text(ggplot2::aes(label = label), size = text_size, na.rm = TRUE) +
    ggplot2::coord_equal() +
    ggplot2::theme_minimal(base_size = 11) +
    ggplot2::theme(
      axis.text.x = ggplot2::element_text(angle = 90, hjust = 1, vjust = 0.5),
      axis.title = ggplot2::element_blank()
    ) +
    ggplot2::labs(title = title, fill = "r")
}

run_lmer_diagnostics <- function(
    model,
    name = deparse(substitute(model)),
    out_dir = ".",
    pdf_width = 11,
    pdf_height = 8.5,
    vif_width = 11,
    vif_height = 11,
    corr_width = 11,
    corr_height = 11,
    vif_flip = TRUE,
    overwrite = TRUE
) {
  stopifnot(is.character(name), length(name) == 1)
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  
  requireNamespace("performance", quietly = TRUE)
  requireNamespace("ggplot2", quietly = TRUE)
  
  f_summary   <- file.path(out_dir, paste0(name, "_summary.txt"))
  f_fixef_png <- file.path(out_dir, paste0(name, ".png"))
  f_check_pdf <- file.path(out_dir, paste0(name, ".pdf"))
  f_vif_pdf   <- file.path(out_dir, paste0(name, "_VIF.pdf"))
  f_corr_pdf  <- file.path(out_dir, paste0(name, "_Corr.pdf"))
  
  if (!overwrite) {
    existing <- c(f_summary, f_fixef_png, f_check_pdf, f_vif_pdf, f_corr_pdf)
    if (any(file.exists(existing))) {
      stop("Some output files already exist and overwrite=FALSE.")
    }
  }
  
  # 1) Summary -> txt
  writeLines(capture.output(print(summary(model))), f_summary)
  
  # 2) Fixed effects plot -> png (your helpers)
  td <- lmer_fixef_df(model)
  p_fixef <- plot_lmer_fixef(td, paste0(name, " coefficients"))
  save_png(p_fixef, f_fixef_png)
  
  # 3) Full model performance check -> pdf
  p_check <- performance::check_model(model)
  grDevices::pdf(f_check_pdf, width = pdf_width, height = pdf_height)
  print(p_check)
  grDevices::dev.off()
  
  # 4) VIF plot -> pdf
  vif_obj <- performance::check_collinearity(model)
  p_vif <- plot(vif_obj)
  if (isTRUE(vif_flip)) p_vif <- p_vif + ggplot2::coord_flip()
  
  ggplot2::ggsave(
    filename = f_vif_pdf,
    plot = p_vif,
    width = vif_width,
    height = vif_height
  )
  
  # 5) Correlation heatmap (RAW numeric predictors) -> pdf
  X_raw <- .extract_raw_predictors(model)
  dropped <- attr(X_raw, "dropped_non_numeric")
  if (length(dropped) > 0) {
    message(
      "Correlation heatmap: dropped non-numeric raw predictors: ",
      paste(dropped, collapse = ", ")
    )
  }
  
  p_corr <- .make_corr_heatmap(X_raw, title = paste0(name, " predictor correlation (raw)"))
  
  ggplot2::ggsave(
    filename = f_corr_pdf,
    plot = p_corr,
    width = corr_width,
    height = corr_height
  )
  
  invisible(list(
    summary_file = f_summary,
    fixef_plot = p_fixef,
    check_plot = p_check,
    vif_plot = p_vif,
    corr_plot = p_corr,
    dropped_non_numeric_predictors = dropped,
    files = list(
      summary = f_summary,
      fixef_png = f_fixef_png,
      check_pdf = f_check_pdf,
      vif_pdf = f_vif_pdf,
      corr_pdf = f_corr_pdf
    )
  ))
}

# Example:
# run_lmer_diagnostics(pathEfficiency_knotted, out_dir = out_dir)