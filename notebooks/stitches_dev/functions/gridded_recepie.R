

# Generate recpcie for the stiched gridded product 
# (based on the notebook 5 code)
# Args 
#   target_data: a data frame of the target data 
#   archive_data: the data frame of the archive data to match on
#   n: int number of permutations based on the neighborhood matching TODO write some test to account for when tol is small 
#   tol: default set to 0.1, it defines the threshold for the neighborhood matching process
# Return: data frame containing the following columns, stitching_id, target_start_yr, target_end_yr, 
#     archive_start_yr, archive_end_yr, and file. This is all of the information that will be needed 
#     by python to stitch the gridded product. 
generate_gridded_recepie <- function(target_data, archive_data, n, tol = 0.1){
  
  # Load the complete archive. 
  complete_archive <- read.csv("inputs/main_raw_pasted_tgav_anomaly_all_pangeo_list_models.csv", 
                               stringsAsFactors = FALSE) %>% 
    dplyr::select(-X) %>% 
    mutate(experiment = ifelse(grepl('historical', file), 'historical', experiment)) %>%  
    select(model, experiment, ensemble, variable, file) %>%  
    distinct()
  
  # This code is taken from Notebook 5
  # ----------------------------------------------------------------
  # Get all your matches in your neighborhood and convert them to
  # the number of recipes you want:
  match_neighborhood(target_data = target_data,
                     archive_data = archive_subset,
                     tol=tol) %>% 
    # Convert them to a sample of individual Tgav Recipes:
    permute_stitching_recipes(N_matches = n, matched_data = .) -> 
    recipes
  
  # stitch the tgavs not allowing duplicates within a single
  # generated Tgav. Specifically,the duplicates will be re-matched on
  # Nearest neighbor criteria with an archive that has all previously
  # matched points removed
  lapply(split(recipes, recipes$stitching_id),
         function(df){
           df %>% 
             remove_duplicates(matched_data = ., 
                               archive = archive_subset) %>%
             mutate(stitching_id = unique(df$stitching_id))
         }) %>%
    do.call(rbind, .) %>% 
    dplyr::select(target_start_yr, target_end_yr, archive_experiment,archive_variable,
                  archive_model, archive_ensemble, archive_start_yr, archive_end_yr, archive_year, stitching_id) -> 
    unformatted_recepie 
  
  # TODO so I think that there is some problem with the periods that straddle the historical/future sccenario line. 
  # I think we need to revisit it. 
  
  # Because of the way that we've removed the historical achive but will want to pull from pangeo 
  # we need to properly treat the historical years.
  unformatted_recepie %>% 
    mutate(archive_experiment = ifelse(archive_year <= 2015, 'historical', archive_experiment)) %>% 
    left_join(complete_archive, by = c("archive_experiment" = "experiment", 
                                       "archive_model" = "model", 
                                       "archive_ensemble" = "ensemble")) %>% 
    select(stitching_id, target_start_yr, target_end_yr, archive_start_yr, archive_end_yr, file) -> 
    gridded_recipes
  
  return(gridded_recipes)
}