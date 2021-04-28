library(assertthat)
###############################################################################
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

###############################################################################
# A helper function to remove false duplicate matches in the historical period. For
# example, target 1850 gets 1872 data from realization 13 of SSP126 and SSP585.
# The metadata of these archive values are different, but the actual data 
# values are identical because we just pasted in the same historical data to 
# every Experiment. So this function keeps only the first match.
# 
# Args: matched_data: data output from match_neighborhood.
#
# Returns: a data frame of matched data with the same structure as the 
# input, with false duplicates in the historical period dropped.
drop_hist_false_duplicates <- function(matched_data){
  
  # Deterine the historical cut off year based on the size of the chunks. 
  cut_off_yr <- 2015 - max(matched_data$target_end_yr-matched_data$target_start_yr)/2
  
  matched_data %>%
    # Because smoothing with window =9 has occured, 
    # historical is actually 2010 or earlier: the chunks
    # that had purely historical data in them and none 
    # from the future when smoothing.
    filter(target_year <= cut_off_yr) %>%
    mutate(exp2 = archive_experiment) %>%
    separate(exp2, c('trash', 'idvalue'), sep = 'p') %>%
    select(-trash) %>%
    mutate(idvalue = as.numeric(idvalue)) %>%
    group_by(target_variable, target_experiment,
             target_ensemble, target_model,
             target_start_yr, target_end_yr,
             target_year, target_fx, target_dx,
             archive_ensemble, archive_year) %>%
    mutate(min_id_value = min(idvalue)) %>%
    ungroup %>%
    filter(idvalue == min_id_value) %>%
    select(-idvalue, -min_id_value) -> 
    historical
  
  matched_data %>%
    filter(target_year > 2010) %>%
    bind_rows(historical) %>%
    arrange(target_year) ->
    matched
  
  return(matched)
  
}


###############################################################################
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


###############################################################################
# A function to remove duplicated matches for a single target trajectory.
# E.g. if target year 2070 and 2079 both get the same archive point matched
# in, let that point stay with the target year that had smaller `dist_l2`, 
# re-match the other target year on the archive minus that duplicated point.
# Runs recursively so with the re-matched other target year, it will again
# check the full set of matched data for duplicates, keep the match on the 
# target year with smallest distance, re-match the other target year that
# got the duplicate on the archive with the duplicated point removed, and so
# on. I guess with how we're removing points from the archive iteratively, it
# could potentially get trapped in an infinite loop bouncing between two different
# duplicate cases. 
#
# Args:
# matched_data: data frame with results of matching
# tol: a tolerance for the neighborhood of matching. defaults to 0.01 degC about the nearestneighbor. 
#     If tol=0, only the nearest-neighbor is returned
# drop_hist_duplicates: boolean, default set to TRUE, will discard historical duplicates from matching process.
# 
# Returns:
# matched_data: data frame with same structure as raw matched, with duplicate
# matches replaced. 

remove_duplicates <- function(matched_data, tol = 0.01, drop_hist_duplicates = TRUE){
  
  # Work with rows where the same archive match gets brought in 
  matched_data %>%
    group_by(archive_experiment, archive_variable,
             archive_model, archive_ensemble,
             archive_start_yr, archive_end_yr, archive_year,
             archive_fx, archive_dx) %>%
    filter(n() > 1) %>%
    ungroup ->
    duplicates
  
  while(nrow(duplicates) > 0){
    
    # within each set of duplicates, 
    # pull out the one with smallest dist_l2 - 
    # this is the one that gets to keep the match, and we use
    # as an index to work on the complement of (in case the same
    # archive point gets matched for more than 2 target years)
    duplicates %>%
      group_by(archive_experiment, archive_variable,
               archive_model, archive_ensemble,
               archive_start_yr, archive_end_yr, archive_year,
               archive_fx, archive_dx) %>%
      filter(dist_l2 == min(dist_l2)) %>%
      ungroup ->
      duplicates_min 
    
    # target points of duplicates-duplicates_min need to be 
    # refit on the archive - matched points
    duplicates[, grepl('target_', names(duplicates))  ] %>%
      filter(!(target_year %in% duplicates_min$target_year)) ->
      points_to_rematch
    names(points_to_rematch) <- gsub(pattern = 'target_', replacement = '', x = names(points_to_rematch))
    
    rm_from_archive <- matched_data[, grepl('archive_', names(matched_data))] 
    names(rm_from_archive) <- gsub(pattern = 'archive_', replacement = '', x = names(rm_from_archive))
    
    archive_subset %>%
      anti_join(rm_from_archive,
                by=c("experiment", "variable",
                     "model", "ensemble", 
                     "start_yr", "end_yr", "year",
                     "fx", "dx")) ->
      new_archive
    
    rematched <- match_neighborhood(target_data = points_to_rematch, archive_data = new_archive, 
                                    tol = tol, drop_hist_duplicates = FALSE)
    
    matched_data %>%
      filter(!(target_year %in% rematched$target_year)) %>%
      bind_rows(rematched) %>%
      arrange(target_year) ->
      matched_data
    
    matched_data  %>%
      group_by(archive_experiment, archive_variable,
               archive_model, archive_ensemble,
               archive_start_yr, archive_end_yr, archive_year,
               archive_fx, archive_dx) %>%
      filter(n() > 1) %>%
      ungroup ->
      duplicates
    
    # cleanup for next loop
    rm(duplicates_min)
    rm(points_to_rematch)
    rm(rm_from_archive)
    rm(new_archive)
    rm(rematched)
    
  }
  return(matched_data)
}

###############################################################################
# match a target data point with corresponding nearest neighboor from an archive data set. 
# 
# Args 
#   target_data: data frame created by the get_chunk_info containg information from the target time series. 
#   archive_data: data frame created by the get_chunk_info containing information from the archive. 
#   tol: a tolerance for the neighborhood of matching. defaults to 0.01 degC about the nearest-
#        neighbor. If tol=0, only the nearest-neighbor is returned
# drop_hist_duplicates: boolean, default set to TRUE, will discard historical duplicates from matching process.
#
# Return: a data frame of the target data matched with the archive data, this is the information 
# that will be used to look up and stich the archive values together, this is our "recepie card".
match_neighborhood <- function(target_data, archive_data, tol = 0.01,
                               drop_hist_duplicates = TRUE){
  
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
  
  
 # if (drop_hist_duplicates && all(out$archive_year <= 2015)){
#    out <- drop_hist_false_duplicates(out)
#  }
  if (drop_hist_duplicates){
      out <- drop_hist_false_duplicates(out)
    }

  # Return the data frame of target values matched with the archive values with the distance. 
  return(distinct(out))
  
  
}






