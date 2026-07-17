# file: plot_dropDist_over_time_by_coinLabel.R

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(ggplot2)
  library(broom)
  library(purrr)
  library(tidyr)
})

input_csv <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain_PO/EventSegmentation/megaFiles/allIntervalData_AN_with_rejectWalkDist.csv"
output_png <- "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/analyses/results_R/dropDist/dropDist_over_time_by_coinLabel_Correct.png"
output_stats_csv <- "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/analyses/results_R/dropDist/dropDist_over_time_by_coinLabel_stats_Correct.csv"

df <- read_csv(input_csv, show_col_types = FALSE)

plot_df <- df %>%
  filter(
    !is.na(dropDist),
    !is.na(TotSesh_actTest_RoundNum),
    TotSesh_actTest_RoundNum < 51,
    dropDist <= 1.1,
    !is.na(coinLabel)
  ) %>%
  mutate(
    coinLabel = factor(coinLabel, levels = c("HV", "LV", "NV"))
  )

# ----------------------------
# Stats by coinLabel
# ----------------------------
stats_df <- plot_df %>%
  group_by(coinLabel) %>%
  nest() %>%
  mutate(
    pearson = map(data, ~ cor.test(
      .x$TotSesh_actTest_RoundNum,
      .x$dropDist,
      method = "pearson"
    )),
    spearman = map(data, ~ cor.test(
      .x$TotSesh_actTest_RoundNum,
      .x$dropDist,
      method = "spearman",
      exact = FALSE
    )),
    lm_fit = map(data, ~ lm(dropDist ~ TotSesh_actTest_RoundNum, data = .x)),
    lm_tidy = map(lm_fit, broom::tidy),
    n = map_int(data, nrow),
    pearson_r = map_dbl(pearson, ~ unname(.x$estimate)),
    pearson_p = map_dbl(pearson, ~ .x$p.value),
    spearman_rho = map_dbl(spearman, ~ unname(.x$estimate)),
    spearman_p = map_dbl(spearman, ~ .x$p.value),
    slope = map_dbl(lm_tidy, ~ .x %>%
                      filter(term == "TotSesh_actTest_RoundNum") %>%
                      pull(estimate)),
    slope_p = map_dbl(lm_tidy, ~ .x %>%
                        filter(term == "TotSesh_actTest_RoundNum") %>%
                        pull(p.value))
  ) %>%
  select(
    coinLabel,
    n,
    pearson_r,
    pearson_p,
    spearman_rho,
    spearman_p,
    slope,
    slope_p
  ) %>%
  ungroup()

write_csv(stats_df, output_stats_csv)

cat("\n==============================\n")
cat("dropDist over time by coinLabel\n")
cat("==============================\n")
print(stats_df)

# ----------------------------
# Labels for each facet
# ----------------------------
label_df <- stats_df %>%
  mutate(
    label = paste0(
      "n = ", n,
      "\nPearson r = ", round(pearson_r, 3),
      "\nSpearman rho = ", round(spearman_rho, 3),
      "\nSlope = ", round(slope, 4),
      "\nSlope p = ", format.pval(slope_p, digits = 3)
    )
  )

y_max_df <- plot_df %>%
  group_by(coinLabel) %>%
  summarise(
    x = min(TotSesh_actTest_RoundNum, na.rm = TRUE) + 1,
    y = max(dropDist, na.rm = TRUE),
    .groups = "drop"
  )

label_df <- left_join(label_df, y_max_df, by = "coinLabel")

# ----------------------------
# Plot
# ----------------------------
p <- ggplot(
  plot_df,
  aes(x = TotSesh_actTest_RoundNum, y = dropDist)
) +
  geom_point(alpha = 0.4, size = 1.8) +
  geom_smooth(method = "lm", se = TRUE, linewidth = 0.9) +
  facet_wrap(~ coinLabel, nrow = 1) +
  geom_label(
    data = label_df %>% mutate(x = Inf, y = Inf),
    aes(x = x, y = y, label = label),
    inherit.aes = FALSE,
    hjust = 1.02,
    vjust = 1.02,
    size = 3.5,
    label.size = 0.25,
    fill = "white",
    alpha = 0.9
  ) +
  labs(
    title = "Recall Precision over time by Coin Type",
    subtitle = "Filtered to Correct Recall Only and Rounds < 51",
    x = "Rounds",
    y = "Pin Drop Distance (m)"
  ) +
  theme_classic(base_size = 13)

ggsave(
  filename = output_png,
  plot = p,
  width = 12,
  height = 4.5,
  dpi = 300
)

print(p)