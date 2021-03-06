# here are the helper functions that take the data frame of the matched observations and pulls from 
# global mean values and stitches together the final product. 
library(assertthat)

# we actually need two of the helper functions rom this script:
source("functions/gridded_recipe.R") 

# Sometimes, like when the archive window matched in is the last window
# of a time series, length(archive_yrs) will be less than length(target_yr).
# This will introduce an NA in the time series for global avg temperature.
# We don't want that, so to be consistent, with the recipes that go to
# python for adding in gridded data, we use the exact same functions here.

###############################################################################
# create a stitched together global mean time series from tgav data that
# has historical pasted into every scenario, as in the archive
# Args 
#   recipes: A table of stitching recipes - each target year has been either matched
#          to the nearest neighor or matched to a neighborhood and then
#          permuted so that what is input to this function is a single tgav recipe. 
#   data: the data of the global mean temp that will be stitched together
# return: a time series of data (year, value, variable)
stitch_global_mean <- function(recipes, data){
  
  # call the helper function from gridded_recipe.R to make
  # sure we aren't trying to match 8 years of archive data
  # to 9 years of target data or vice versa. 
  # For example if target window 2084-2092 (9 years)
  # gets a match of 2093-2100 (8 years), we go ahead
  # and make it 2084-2092 getting a mat ch of 2092-2100.
  # For something like target window 2093-2100 (8 years)
  # getting a match of 2084-2092 (9 years), we go ahead
  # and just use 2084-2091 data. 
  recipes %>% 
    handle_final_period ->
    recipes
  
  # check the match input make sure that the values are 
  req_target_cols <- paste0("target_", c("variable", "start_yr", "end_yr"))
  req_archive_cols <- paste0("archive_",  c("model", "start_yr", "end_yr", 
                                            "experiment", "variable", "model", "ensemble"))
  
  
  
  lapply(split(recipes, recipes$stitching_id),
         function(match){
           
           if(length(unique(match$target_year)) < nrow(match)){
             stop("You have multiple matches to a single target year, you need to call `permute_stitching_recipes` before this function")
           }
           
           assert_that(has_name(x = match, which = c(req_archive_cols, req_target_cols)))
           
           # note that when we ware actually working with pangeo one of the columns should be file... 
           req_cols <- c("model", "experiment", "ensemble", "year", "value", "variable")
           assert_that(has_name(x = data, which = req_cols))
           
           # Subset the data so that we are only working with archive data we are intrested with.
           mod <- unique(match$archive_model)
           exp <- unique(match$archive_experiment) 
           ens <- unique(match$archive_ensemble)
           
           
           index <- c(data$model %in% mod & data$experiment %in% exp & data$ensemble %in% ens)
           data  <- data[index, ]
           
           # quick test to make sure that the data is pasted data, with years 1850-2100 for each
           # SSP. Writing tests is not my strength so this is not the best test
           data %>%
             group_by(model, experiment, ensemble, timestep, grid_type, variable) %>%
             summarize(max_year = max(year),
                       min_year = min(year)) %>%
             ungroup ->
             data_years
           
           #assert_that(all(data_years$max_year == 2100))
           #assert_that(all(data_years$min_year == 1850))
           rm(data_years)
           
           
           data_list <- lapply(1:nrow(match), 
                               FUN = function(i){
                                 # select a single row of matched data, this is akward but not using a lapply here 
                                 # because it will transform the int data into strings 
                                 m <- match[i, ]
                                 yr <- m$target_start_yr:m$target_end_yr
                                 
                                 select_yrs <- m$archive_start_yr:m$archive_end_yr
                                 
                                 rows <- c(data$year %in% select_yrs & data$ensemble == m$archive_ensemble & data$experiment == m$archive_experiment)
                                 values <- data[rows, ]$value
                                 
                                
                                 out <- data.frame(year = yr, 
                                                   value = values,
                                                   variable = m$archive_variable)
                                 
                                 return(out)
                               })
           
           stitched_ts <- do.call(what = "rbind", args = data_list)
           
           stitched_ts %>%
             mutate(stitching_id = unique(match$stitching_id))
         }) %>%
    do.call(rbind, .) ->
    new_tgavs
  
  return(new_tgavs)
  
  
}





