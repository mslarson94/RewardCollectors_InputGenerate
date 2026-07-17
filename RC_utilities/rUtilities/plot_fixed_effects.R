suppressPackageStartupMessages({
  library(lme4)
  library(lmerTest)
  library(sjPlot)
  library(broom.mixed)
  library(dplyr)
  library(ggplot2)
  library(forcats)
})



plot_fixed_effects <- function(m, label_map, drop_intercept = FALSE, conf_level = 0.95) {
  coef_df <- broom.mixed::tidy(
    m,
    effects = "fixed",
    conf.int = TRUE
  ) %>%
    mutate(
      term = ifelse(term %in% names(label_map), label_map[term], term),
      term = fct_reorder(term, estimate)
    )
  
  if (drop_intercept) {
    coef_df <- coef_df %>% filter(term != "Intercept")
  }
  
  coef_df <- coef_df %>%
    mutate(term = forcats::fct_reorder(term, estimate))
  
  ggplot(coef_df, aes(x = estimate, y = term)) +
    geom_vline(xintercept = 0, linetype = 2, linewidth = 0.5) +
    geom_errorbarh(aes(xmin = conf.low, xmax = conf.high), height = 0.15, linewidth = 0.6) +
    geom_point(size = 2.2) +
    labs(
      title = "Fixed effect coefficients",
      x = paste0("Estimate (", conf_level * 100, "% CI)"),
      y = NULL
    ) +
    theme_classic(base_size = 12)
}