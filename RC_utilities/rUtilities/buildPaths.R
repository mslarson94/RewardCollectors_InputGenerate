# buildPaths.R

buildPaths <- function(
    proc_dir,
    round_dur_filter = "2.0",
    round_set = "1st50Rounds",
    condition = "noCD",
    inFile= "1st50IntervalDataKnotted_AN_noCD_all.csv",
    outDirName= "mixedModel_X"
) {
  
  true_base_dir <- "/Users/mairahmac/Desktop/RC_TestingNotes"
  
  mega_dir <- file.path(
    true_base_dir,
    proc_dir,
    "EventSegmentation",
    "megaFiles"
  )
  
  mega_final_dir <- file.path(
    paste0(mega_dir, "_final"),
    paste0("mad_", round_dur_filter)
  )
  
  data_file <- file.path(
    mega_final_dir,
    inFile
  )
  
  outdir <- file.path(
    true_base_dir,
    proc_dir,
    "Plotting",
    paste0("mad_", round_dur_filter),
    round_set,
    "all",
    condition,
    "R",
    outDirName
  )
  
  list(
    base_dir = true_base_dir,
    mega_dir = mega_dir,
    mega_final_dir = mega_final_dir,
    data_file = data_file,
    outdir = outdir
  )
}