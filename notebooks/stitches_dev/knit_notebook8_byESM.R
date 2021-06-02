# a wrapper script for iteratively compiling `Notebook8-emulate-validate.Rmd` for each ESM separately.

# what we will emulate:
esm_name_vec <- c("ACCESS-ESM1-5",
              "CanESM5",
              "GISS-E2-1-G",
              "MIROC6",
              "MPI-ESM1-2-HR",
              "MPI-ESM1-2-LR",
              "NorCPM1",
              "UKESM1-0-LL"  )
esm_experiment_vec <- c('ssp245', 'ssp370')

# How many times do we want to draw full generated ense mbles:
Ndraws <- 10

for (i in 2:2){# 1:length(esm_name_vec)){
  for (j in 1:1){#1:length(esm_experiment_vec)){
    
    # define our target esm name and experiment values
    # as global variables so that the markdown can access
    esm_name <- esm_name_vec[i]
    esm_experiment <- esm_experiment_vec[j]
    
    
    rmarkdown::render(input = "Notebook8-emulate-validate.Rmd", output_file = paste0(esm_name, "_", esm_experiment, "_tgav_validation_results.html"))
  }
}


