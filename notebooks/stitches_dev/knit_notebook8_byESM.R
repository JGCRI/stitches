# a wrapper script for iteratively compiling `Notebook8-emulate-validate.Rmd` for each ESM separately.

# what we will emulate:
esm_name_vec <- c("ACCESS-ESM1-5",
              "CanESM5",
              #"GISS-E2-1-G", # no psl or pr data on pangeo
              "MIROC6",
              "MPI-ESM1-2-HR",
              "MPI-ESM1-2-LR",
              # "NorCPM1", # don't have archive data past 2025/raw tgav data past 2029
              "UKESM1-0-LL")
esm_experiment_vec <- c('ssp245', 'ssp370')

# How many times do we want to draw full generated ensembles:
Ndraws <- 10

for (name_ind in 1:length(esm_name_vec)){
  for (exp_ind in 1:length(esm_experiment_vec)){
    
    rm(esm_name, esm_experiment)
    
    # define our target esm name and experiment values
    # as global variables so that the markdown can access
    esm_name <- esm_name_vec[name_ind]
    esm_experiment <- esm_experiment_vec[exp_ind]
    
    print(name_ind)
    print(exp_ind)
    print(esm_name)
    print(esm_experiment)
    
    rmarkdown::render(input = "Notebook8-emulate-validate.Rmd", output_file = paste0("Notebook8_", esm_name, "_", esm_experiment, "_tgav_validation_results.html"))
  }
}


