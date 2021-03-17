library(assertthat)

# This function calculates the elucidian distance between the target values (dx and dy) with 
# and the arhcive values contained in the data frame. It will be used to help select which 
# of the arhive values best matches the target values. 
# Args 
#   fx: a single value of the target fx value
#   dx: a single value of the target dx value 
#   data: a data frame of the archive fx and dx values 
# Return: a dt
internal_dist <- function(fx, dx, data){
  
  #message(data)
  # calculate the elucian distance between the target point 
  # and the archive obversations 
  dist <- unlist(sqrt((data['dx'] - dx)^2 + (data['fx'] - fx)^2))
  # this returns the first minimum run into, which is not how we are going to want to do it, 
  # we will want some way to keep track of the min and combine to havev different realizations 
  # or have some random generation. But for now I think that this is probably sufficent. 
  index <- which.min(dist) 
  # if there are mulitple matches then an error should be thrown! Why 
  # is this not happening for the historiccal period? The ensemble memebers of 
  # different experiments should be identical to one another! 
  if(length(index) > 1){
    stop("more than one identical match found!")
    }
  
  out <- data[index, ]
  names(out) <- paste0('archive-', names(out))
  out$distance <- dist[index]
  return(out)
  
}

# match a target data point with corresponding nearest neighboor from an archive data set. 
# 
# Args 
#   target_data: data frame created by the get_chunk_info containg information from the target time series. 
#   archive_data: data frame created by the get_chunk_info containing information from the archive. 
# Return: a data frame of the target data matched with the archive data, this is the information 
# that will be used to look up and stich the archive values together, this is our "recepie card".
match_nearest_neighboor <- function(target_data, archive_data){
  
# Check the inputs of the functions 
req_cols <- c("experiment", "variable", "ensemble", "start_yr", "end_yr", "fx", "dx")  
assert_that(has_name(which = req_cols, x = archive_data))
req_cols <- c("start_yr", "end_yr", "fx", "dx")
assert_that(has_name(which = req_cols, x = target_data))


# For every entry in the target data frame find its nearest neighboor from the archive data. 
mapply(FUN = function(fx, dx){internal_dist(fx = fx, dx = dx, data = archive_data)},
        fx = target_data$fx, dx = target_data$dx, SIMPLIFY = FALSE, USE.NAMES = FALSE) %>%  
  # concatenate the results into a sinngle data frame 
  do.call('rbind', args = .) -> 
  matched


# Now add the information about the matches to the target data
# Make sure it if clear which columns contain  data that comes from the target compared
# to which ones correspond to the archive information. Right now there are lots of columns
# that contain duplicate information for now it is probably fine to be moving these things around. 
names(target_data) <- paste0('target-', names(target_data)) 
out <- cbind(target_data, matched)

# Return the data frame of target values matched with the archive values with the distance. 
return(out)

  
}






