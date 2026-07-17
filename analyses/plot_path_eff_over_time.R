# file: plot_path_eff_over_time.R

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(ggplot2)
})

input_csv <- "/Users/mairahmac/Desktop/RC_TestingNotes/FreshStart_redoAgainAgainAgain_PO/EventSegmentation/megaFiles/allIntervalData_AN_with_rejectWalkDist.csv"
output_png <- "/Users/mairahmac/Desktop/myra_code/Python/RewardCollectors_InputGenerate/analyses/results_R/mixedLm_pathEff/dropDist_over_time_scatter_all_correct.png"

df <- read_csv(input_csv, show_col_types = FALSE)

plot_df <- df %>%
  #filter(is.na(rejectWalkDist) | rejectWalkDist != 1) %>%
  filter(TotSesh_runTot_RoundNum <= 50) %>%
  filter(dropDist <= 1.1) %>%
  select(roundID, roundID_int, TotSesh_runTot_RoundNum, dropDist) %>%
  filter(
    !is.na(roundID),
    !is.na(TotSesh_runTot_RoundNum),
    !is.na(dropDist)
  ) %>%
  group_by(roundID, roundID_int) %>%
  summarise(
    #TotSesh_runTot_RoundNum = first(TotSesh_runTot_RoundNum),
    TotSesh_runTot_RoundNum,
    dropDist = mean(dropDist),
    .groups = "drop"
  ) %>%
  arrange(TotSesh_runTot_RoundNum)

pearson_test <- cor.test(
  ~ TotSesh_runTot_RoundNum + dropDist,
  data = plot_df,
  method = "pearson"
)

spearman_test <- cor.test(
  ~ TotSesh_runTot_RoundNum + dropDist,
  data = plot_df,
  method = "spearman",
  exact = FALSE
)

cat("N rows after filtering =", nrow(plot_df), "\n")
cat("Pearson r =", round(unname(pearson_test$estimate), 3),
    ", p =", format.pval(pearson_test$p.value, digits = 3), "\n")
cat("Spearman rho =", round(unname(spearman_test$estimate), 3),
    ", p =", format.pval(spearman_test$p.value, digits = 3), "\n")

p <- ggplot(
  plot_df,
  aes(x = TotSesh_runTot_RoundNum, y = dropDist)
) +
  geom_point(alpha = 0.5, size = 1.8) +
  geom_smooth(method = "lm", se = TRUE) +
  labs(
    title = "Recall Precision Over Time | Correct Recall Only",
    subtitle = paste0(
      "One point per pin drop | Pearson r = ",
      round(unname(pearson_test$estimate), 3),
      " (p = ", format.pval(pearson_test$p.value, digits = 3), ")",
      " | Spearman rho = ",
      round(unname(spearman_test$estimate), 3),
      " (p = ", format.pval(spearman_test$p.value, digits = 3), ")"
    ),
    x = "Rounds",
    y = "Pin Drop Distance (m)"
  ) +
  theme_classic(base_size = 13)

ggsave(
  filename = output_png,
  plot = p,
  width = 8,
  height = 6,
  dpi = 300
)

print(p)