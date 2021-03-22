# here are the helper functions that take the data frame of the matched observations and pulls from 
# global mean values and stitches together the final product. 
library(assertthat)

# create a stitched together global mean time series 
# Args 
#   match: the data frame returned by the match function 
#   data: the data of the global mean temp that will be stitched together
# return: a time series of data (year, value, variable)
stitch_global_mean <- function(match, data){
  
  # check the match input make sure that the values are 
  req_target_cols <- paste0("target_", c("variable", "start_yr", "end_yr"))
  req_archive_cols <- paste0("archive_",  c("model", "start_yr", "end_yr", 
                                            "experiment", "variable", "model", "ensemble"))
  
  assert_that(has_name(x = match, which = c(req_archive_cols, req_target_cols)))
  
  # note that when we ware actually working with pangeo one off the columns should be file... 
  req_cols <- c("model", "experiment", "ensemble", "year", "value", "variable")
  assert_that(has_name(x = data, which = req_cols))
  
  # Subset the data so that we are only working with archive data we are intrested with.
  mod <- unique(match$archive_model)
  exp <- c(unique(match$archive_experiment), "historical") # the historical experiments were renamed in our archive 
  ens <- unique(match$archive_ensemble)
  
  data <- tgav_data
  index <- c(data$model %in% mod & data$experiment %in% exp & data$ensemble %in% ens)
  data  <- data[index, ]
  
  data_list <- lapply(1:nrow(match), 
                      FUN = function(i){
                        # select a single row of matched data, this is akward but not using a lapply here 
                        # because it will transform the int data into strings 
                        m <- match[i, ]
                        yr <- m$target_start_yr:m$target_end_yr
                        
                        select_yrs <- m$archive_start_yr:m$archive_end_yr
                        
                        if(any(select_yrs < 2015)){
                          rows <- c(data$year %in% select_yrs & data$ensemble == m$archive_ensemble & data$experiment == "historical")
                          values <- data[rows, ]$value
                          } else {
                            rows <- c(data$year %in% select_yrs & data$ensemble == m$archive_ensemble & data$experiment == m$archive_experiment)
                            values <- data[rows, ]$value
                            }
                        out <- data.frame(year = yr, 
                                          value = values[1:length(yr)], # somewhat confused why this is happenging
                                          variable = m$archive_variable)
                        return(out)
                        })
  
  stiched_ts <- do.call(what = "rbind", args = data_list)
  return(stiched_ts)
}










