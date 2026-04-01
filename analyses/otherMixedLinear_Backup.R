# run_lmer_diagnostics.R

run_lmer_diagnostics <- function(
    model,
    name = deparse(substitute(model)),
    out_dir = ".",
    pdf_width = 11,
    pdf_height = 8.5,
    vif_width = 12,
    vif_height = 12,
    corr_width = 8.5,
    corr_height = 11,
    vif_flip = TRUE,
    overwrite = TRUE
) {
  stopifnot(is.character(name), length(name) == 1)
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  
  requireNamespace("performance", quietly = TRUE)
  requireNamespace("ggplot2", quietly = TRUE)
  
  f_summary <- file.path(out_dir, paste0(name, "_summary.txt"))
  f_fixef_png <- file.path(out_dir, paste0(name, ".png"))
  f_check_pdf <- file.path(out_dir, paste0(name, ".pdf"))
  f_vif_pdf <- file.path(out_dir, paste0(name, "_VIF.pdf"))
  f_corr_pdf <- file.path(out_dir, paste0(name, "_Corr.pdf"))
  
  if (!overwrite) {
    existing <- c(f_summary, f_fixef_png, f_check_pdf, f_vif_pdf, f_corr_pdf)
    if (any(file.exists(existing))) {
      stop("Some output files already exist and overwrite=FALSE.")
    }
  }
  
  # 1) Summary -> txt
  summary_lines <- capture.output(print(summary(model)))
  writeLines(summary_lines, f_summary)
  
  # 2) Fixed effects coefficients plot -> png (your custom helpers)
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
  
  # 5) Correlation matrix -> pdf
  corr_obj <- performance::check_collinearity(model, component = "correlation")
  p_corr <- plot(corr_obj)
  
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
    files = list(
      summary = f_summary,
      fixef_png = f_fixef_png,
      check_pdf = f_check_pdf,
      vif_pdf = f_vif_pdf,
      corr_pdf = f_corr_pdf
    )
  ))
}

# Example usage:
# run_lmer_diagnostics(pathEfficiency_knotted, out_dir = out_dir)
# run_lmer_diagnostics(pathEfficiency_knotted, name = "pathEfficiency_knotted", out_dir = out_dir)