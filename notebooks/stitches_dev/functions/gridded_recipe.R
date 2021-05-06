
# Go through the recipe and when there is a transition period, aka the archive years span both the 
# historical and future scenarios go through and insert in an extra period so that they don't do 
# this over lap any more. 
# Args 
#   rp: data frame of the recipe
# Return: a data frame of of the recipe with no over lapping historical/future experiments, 
# this is now ready to join with pangeo information. 
handle_transition_periods <- function(rp){
  
  apply(rp, 1, function(x){
    
    # First check to see the archive period spans the historical to future scenario 
    transition_period <- 2014 %in% x["archive_start_yr"][[1]]:x["archive_end_yr"][[1]]
    
    # If the period is a transition period aka it contains both historical and future years. 
    # Then we will need to break it out into two periods to reflect the different files 
    # we will need to pull from pangeo. 
    if(transition_period){
      target_yrs <- x["target_start_yr"][[1]]:x["target_end_yr"][[1]]
      
      archive_yrs <- x["archive_start_yr"][[1]]:x["archive_end_yr"][[1]]
      
      hist_cut_off <- 2014 # the final complete year of the historical epxeriment for CanESM
      historical_yrs <- archive_yrs[archive_yrs <= hist_cut_off]
      future_yrs <- archive_yrs[!archive_yrs %in% historical_yrs]
      
      
      # This is the information that is constant between the historical and future periods. 
      constant_info <- data.frame(archive_variable = x["archive_variable"][[1]], 
                                  archive_model = x["archive_model"][[1]], 
                                  archive_ensemble = x["archive_ensemble"][[1]], 
                                  stitching_id = x["stitching_id"][[1]], stringsAsFactors = FALSE)
      
      # Construct the historical period information 
      historical_period <- data.frame(target_start_yr = target_yrs[1], 
                                      target_end_yr = target_yrs[length(historical_yrs)], 
                                      archive_experiment = "historical", stringsAsFactors = FALSE) %>%  
        cbind(constant_info) %>%  
        cbind.data.frame(archive_start_yr = min(historical_yrs), 
                         archive_end_yr = max(historical_yrs)) 
      
      # Now construct the future period information 
      future_period <- data.frame(target_start_yr = target_yrs[length(historical_yrs) + 1], 
                                  target_end_yr = target_yrs[length(target_yrs)], 
                                  archive_experiment = x["archive_experiment"][[1]], 
                                  stringsAsFactors = FALSE) %>%  
        cbind(constant_info) %>%  
        cbind.data.frame(archive_start_yr = min(future_yrs), 
                         archive_end_yr = max(future_yrs)) 
      
      # Combine the period information 
      updated_period <- rbind(historical_period, future_period) 
      
      return(updated_period)
      
    } else {
      
      # This is redundant but reformat the original input to return as output that matches 
      # the data frame returned by the other if statement. 
      out <- data.frame(target_start_yr = as.integer(x["target_start_yr"][[1]]), 
                        target_end_yr = as.integer(x["target_end_yr"][[1]]), 
                        archive_experiment = x["archive_experiment"][[1]],
                        archive_variable = x["archive_variable"][[1]], 
                        archive_model = x["archive_model"][[1]], 
                        archive_ensemble = x["archive_ensemble"][[1]], 
                        stitching_id = x["stitching_id"][[1]], 
                        archive_start_yr = as.integer(x["archive_start_yr"[[1]]]), 
                        archive_end_yr = as.integer(x["archive_end_yr"][[1]]), stringsAsFactors = FALSE)
      
      return(out)
      
      
    }
    
  }) %>% 
    dplyr::bind_rows() -> 
    out 
  
  return(out)
  
}



# Go through a recipe and ensure that all of the periods have the same archive 
# and target period length, if not update to reflect the target period length. 
# Otherwise you'll end up with extra years in the stitched data. This is really 
# only an issue for the final period of target data because sometimes that period is somewhat short. 
# Args 
# rp: a data frame of the recipe
# Return: a recipe data frame that has target and archive periods of the same length.
handle_final_period <- function(rp){
  
  out <- data.frame(stringsAsFactors = FALSE)
  
  # For each row in rp check to see if the length 
  # of the target and the archive years are the same. 
  for(row in 1:nrow(rp)){
    print(row)
    len_target <- rp$target_end_yr[row] - rp$target_start_yr[row]
    len_archive <- rp$archive_end_yr[row] - rp$archive_start_yr[row]
    
    if(len_target == len_archive){
      
      out <- rbind(out, rp[row, ])
      
    } else if (len_target < len_archive) {
      
      # Figure out how much shorter the target period is than the archive period. 
      diff <- len_archive - len_target
      updated <- rp[row, ]
      updated$archive_end_yr <- updated$archive_end_yr - 1
      
      # Add the updated row to the data frame. 
      out <- rbind(out, updated)
    } else {
      # TODO need to revisit, just added an extra year to the archive length but that seems 
      # pretty sus. 
      # Figure out how much longer the target period is than the archive period. 
      diff <- len_archive - len_target
      updated <- rp[row, ]
      updated$archive_start_yr <- updated$archive_start_yr - 1
      
      # Add the updated row to the data frame. 
      out <- rbind(out, updated)
    }
  }
  
  return(out)
  
}




# Generate recipe for the stiched gridded product 
# (based on the notebook 5 code)
# Args 
#   target_data: a data frame of the target data 
#   archive_data: the data frame of the archive data to match on
#   n: int number of permutations based on the neighborhood matching TODO write some test to account for when tol is small 
#   tol: default set to 0.1, it defines the threshold for the neighborhood matching process
# Return: data frame containing the following columns, stitching_id, target_start_yr, target_end_yr, 
#     archive_start_yr, archive_end_yr, and file. This is all of the information that will be needed 
#     by python to stitch the gridded product. 
generate_gridded_recipe <- function(target_data, archive_data, n, tol = 0.1){
  
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
  matched_data <- match_neighborhood(target_data = target_data,
                                     archive_data = archive_data,
                                     tol=tol)
  
  # Convert them to a sample of individual Tgav Recipes:
  permute_stitching_recipes(N_matches = n, matched_data = matched_data, archive = archive_data) %>% 
    dplyr::select(target_start_yr, target_end_yr, archive_experiment,archive_variable,
                  archive_model, archive_ensemble, archive_start_yr, archive_end_yr, archive_year, stitching_id) -> 
    unformatted_recipe 
  
  
  # Because of the way that we've removed the historical achive but will want to pull from pangeo 
  # we need to properly treat the historical years.
  unformatted_recipe %>% 
    # When a archive period spans the historical and future scenario we need to make sure there is 
    # it is split up into seprate periods otherwise it will be impossible to pair the chunks with 
    # the pangeo data.
    handle_transition_periods %>% 
    # Make sure that the length of the target and archive periods are the same to prevent 
    # extra data from being assigned to that period. 
    handle_final_period %>% 
    # Make sure that if there are historical years of data being used assign the experiment name 
    # to historical. 
    mutate(archive_experiment = ifelse(archive_end_yr <= 2014, 'historical', archive_experiment)) -> 
    formatted_recipe 
  
  # Now add the pangeo file information! 
  formatted_recipe %>%  
    left_join(complete_archive, by = c("archive_experiment" = "experiment", 
                                       "archive_model" = "model", 
                                       "archive_ensemble" = "ensemble")) %>% 
    select(stitching_id, target_start_yr, target_end_yr, archive_start_yr, archive_end_yr, file) -> 
    gridded_recipes
  
  return(gridded_recipes)
}