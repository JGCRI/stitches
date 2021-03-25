library(assertthat)

# This function calculates the euclidean distance between the target values (fx and dx)  
# and the archive values contained in the data frame. It will be used to help select which 
# of the archive values best matches the target values. To ensure a consisten unit across
# all dimensions of the space, dx is updated to be windowsize*dx so that it has units of 
# degC. This results in a distance metric (Euclidean/l2) in units of degC.
# Could _very_ easily make that choice of unit consistency optional via arg and if-statement.
# Args 
#   fx_pt: a single value of the target fx value
#   dx_pt: a single value of the target dx value 
#   archivedata: a data frame of the archive fx and dx values
#   tol: a tolerance for the neighborhood of matching. defaults to 0.01 degC about the nearest-
#        neighbor. If tol=0, only the nearest-neighbor is returned
# Return: a data frame with the target data and the corresponding matched archive data. 
internal_dist <- function(fx_pt, dx_pt, archivedata, tol = 0.01){

  # Compute the window size of the archive data to use to update
  # dx values to be windowsize*dx so that it has units of degC
  windowsize <- max(archivedata$end_yr - archivedata$start_yr)
  
 # # For now comment out the code that enforces that there is only type of window size, 
 # # however we do know that when the full ts is not divisible by a window size of 9 
 # # or what not then we will run into issues with this. We do know that is will be 
 # # important to ennsure that the size of the target chunk and archive chunks are equivalent. 
 #TODO address this issue at a latter time. 
 # if(length(windowsize) > 1){
 #  stop("you've made your archive by chunking with multiple window sizes, don't do that!")
 # }

  # Update the names of the columns in the archive data.
  names(archivedata) <- paste0('archive_', names(archivedata))
  
  # calculate the euclidean distance between the target point 
  # and the archive observations.
  # Calculating distance d(target, archive_i) for each point i in the archive.
  # calculating the distance in the dx and fx dimensions separately because
  # now we want to track those in addition to the l2 distance.
  archivedata %>% 
    mutate(dist_dx = windowsize*abs(archive_dx - dx_pt),
           dist_fx = abs(archive_fx - fx_pt),
           dist_l2 = sqrt( (dist_fx)^2 + (dist_dx)^2 )) ->
    dist
  
  # this returns the first minimum run into, which is not how we are going to want to do it, 
  # we will want some way to keep track of the min and combine to have different realizations 
  # or have some random generation. But for now I think that this is probably sufficent. 
  #
  # probably don't actually need the if-statement to treat tol=0 separately; in theory, the 
  # else condition would return the nearest neighbor for tol=0. But just in case of rounding
  # issues, keeping it separate for now to be sure we can replicate previous behavior.
  if (tol == 0) { 
    index <- which.min(dist$dist_l2) 
    
    # if there are mulitple matches then an error should be thrown! Why 
    # is this not happening for the historiccal period? The ensemble members of 
    # different experiments should be identical to one another! 
    if(length(index) > 1){
      stop("more than one identical match found and you only want the nearest neighbor!")
    }
  }
  else {
    min_dist <- min(dist$dist_l2)
    dist_radius <- min_dist + tol
    
    index <- which(dist$dist_l2 <= dist_radius)
  }
  
  out <- dist[index, ]

  return(out)
  
}

# match a target data point with corresponding nearest neighboor from an archive data set. 
# 
# Args 
#   target_data: data frame created by the get_chunk_info containg information from the target time series. 
#   archive_data: data frame created by the get_chunk_info containing information from the archive. 
#   tol: a tolerance for the neighborhood of matching. defaults to 0.01 degC about the nearest-
#        neighbor. If tol=0, only the nearest-neighbor is returned

# Return: a data frame of the target data matched with the archive data, this is the information 
# that will be used to look up and stich the archive values together, this is our "recepie card".
match_nearest_neighbor <- function(target_data, archive_data){
  
# Check the inputs of the functions 
req_cols <- c("experiment", "variable", "ensemble", "start_yr", "end_yr", "fx", "dx")  
assert_that(has_name(which = req_cols, x = archive_data))
req_cols <- c("start_yr", "end_yr", "fx", "dx")
assert_that(has_name(which = req_cols, x = target_data))

# shufflle the the archive data 
archive_data <- shuffle_function(archive_data)

# For every entry in the target data frame find its nearest neighboor from the archive data. 
mapply(FUN = function(fx, dx){internal_dist(fx_pt = fx, dx_pt = dx, archivedata = archive_data, tol=0)},
        fx = target_data$fx, dx = target_data$dx, SIMPLIFY = FALSE, USE.NAMES = FALSE) %>%  
  # concatenate the results into a sinngle data frame 
  do.call('rbind', args = .) -> 
  matched


# Now add the information about the matches to the target data
# Make sure it if clear which columns contain  data that comes from the target compared
# to which ones correspond to the archive information. Right now there are lots of columns
# that contain duplicate information for now it is probably fine to be moving these things around. 
names(target_data) <- paste0('target_', names(target_data)) 
out <- cbind(target_data, matched)

# Return the data frame of target values matched with the archive values with the distance. 
return(out)

  
}



# match a target data point with corresponding nearest neighboor from an archive data set. 
# 
# Args 
#   target_data: data frame created by the get_chunk_info containg information from the target time series. 
#   archive_data: data frame created by the get_chunk_info containing information from the archive. 
#   tol: a tolerance for the neighborhood of matching. defaults to 0.01 degC about the nearest-
#        neighbor. If tol=0, only the nearest-neighbor is returned

# Return: a data frame of the target data matched with the archive data, this is the information 
# that will be used to look up and stich the archive values together, this is our "recepie card".
match_neighborhood <- function(target_data, archive_data, tol = 0.01){
  
  # Check the inputs of the functions 
  req_cols <- c("experiment", "variable", "ensemble", "start_yr", "end_yr", "fx", "dx")  
  assert_that(has_name(which = req_cols, x = archive_data))
  req_cols <- c("start_yr", "end_yr", "fx", "dx")
  assert_that(has_name(which = req_cols, x = target_data))
  
  # shufflle the the archive data 
  archive_data <- shuffle_function(archive_data)
  
  # For every entry in the target data frame find its nearest neighboor from the archive data. 
  mapply(FUN = function(fx, dx){internal_dist(fx_pt = fx, dx_pt = dx, archivedata = archive_data, tol=tol) %>%
      dplyr::mutate(target_fx = fx, target_dx = dx)},
         fx = target_data$fx, dx = target_data$dx, SIMPLIFY = FALSE, USE.NAMES = FALSE) %>%  
    # concatenate the results into a sinngle data frame 
    do.call('rbind', args = .) -> 
    matched
  
  
  # Now add the information about the matches to the target data
  # Make sure it if clear which columns contain  data that comes from the target compared
  # to which ones correspond to the archive information. Right now there are lots of columns
  # that contain duplicate information for now it is probably fine to be moving these things around. 
  names(target_data) <- paste0('target_', names(target_data)) 
  matched %>%
    left_join(target_data, by = c('target_fx', 'target_dx')) %>%
    # Keep all columns of data but order them the same as the original matching function:
    select(target_variable, target_experiment, target_ensemble, target_model, 
           target_start_yr, target_end_yr, target_year, target_fx, target_dx,
           archive_experiment, archive_variable, archive_model, archive_ensemble,
           archive_start_yr, archive_end_yr, archive_year, archive_fx, archive_dx,
           dist_dx, dist_fx, dist_l2) ->
    out
  
  # Return the data frame of target values matched with the archive values with the distance. 
  return(out)
  
  
}


# Randomly shuffle the deck, this should help with the matching process. 
# Args 
#   dt: a data of archive values that will be used in the matching process. 
# Return: a randomly ordered data frame
# TODO this will be removed when we have figured out the ensemble situation however it will only have 
# an effect if there are ties in the matching process, and it is unclear if that is the case yet. 
shuffle_function <- function(dt){
  
  rows <- sample(nrow(dt), replace = FALSE) 
  dt <-dt[rows, ]
  return(dt)
}






