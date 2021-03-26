
###################################################################

# A function to give you the number of potential permutations from a 
# matched set of data. Ie Taking in the the results of 
# `match_neighborhood(target, archive)`.
#
# Args:
# matched_data: data output from match_neighborhood.
#
# Returns:
# A list with two entries. First, the total number of
# potential permutations of the matches that cover 1850-2100 of the 
# target data in the matched_data dataframe. The second, a data
# frame with the break down of how many matches are in each period
# of the target data 

get_num_perms <- function(matched_data){
  # TODO add testing to make sure the matched_data has all
  # the target_ and archive_ columns needed
  
  lapply(split(matched_data,
               f=list(matched_data$target_variable,
                      matched_data$target_experiment,
                      matched_data$target_ensemble,
                      matched_data$target_model,
                      matched_data$target_start_yr, 
                      matched_data$target_end_yr,
                      matched_data$target_year,
                      matched_data$target_fx, 
                      matched_data$target_dx),
               drop = TRUE),
         
         function(df){
           df %>% 
             dplyr::distinct() %>%
             dplyr::group_by(target_variable, target_experiment,
                             target_ensemble, target_model,
                             target_start_yr, target_end_yr,
                             target_year, target_fx, target_dx) %>%
             dplyr::summarise(n_matches = n()) %>%
             ungroup ->
             x
           return(x)
           } # end function in the lapply
         ) %>%  # end lapply
    do.call(rbind, .) %>%
    arrange(target_year)->
    num_matches
  
  return(list(data.frame(totalNumPerms = prod(num_matches$n_matches)),
              num_matches))
}


###################################################################

# A function to sample from input `matched_data` (the the results 
# of `match_neighborhood(target, archive)` to produce permutations
# of possible stitching recipes that will match the target data.
#
# Args:
# matched_data: data output from match_neighborhood.
# N_matches: the number of desired stitching recipes.
# optional something: a previous output of this function that contains a 
# list of already created recipes to avoid re-making.
# 
# Returns: something

permute_stitching_recipes <- function(N_matches, matched_data, optional=NULL){

  N_to_make <- N_matches
  
  N_data_max <- get_num_perms(matched_data)[[1]]$totalNumPerms
  if (N_matches > N_data_max){
    message(paste("You have requested more recipes than possible, returning", N_data_max))
    N_to_make <- N_data_max
  }
  
  
  
  
  # Initialize the number of matches to either 0 or the input read from optional:
  if( is.character(optional)){
    # initialize to the read-in:
    recipes <- read.csv(optional)

    # message: you requested N_matches
    # and Have NN new ones, will produce NN-N_matches new ones and then just 
    # Update N_matches to be NN-N_matches.
    # TODO need to relate the N_to_make here to N_data_max
    N_existing <- length(unique(recipes$stitching_id))
    
    message(paste('You requested', N_matches, 'and already have', N_existing))
    if (N_matches-N_existing <= 0){
      message("You have all you need")
      return(recipes)
    } else {
      message(paste("Creating", N_matches-N_existing, "new recipes and adding to those input."))
      N_to_make <- N_matches-N_existing
    }
  } else{
    # initialize to empty
    recipes <- data.frame()
  }
  
  
  # Somehow sample and construct, compare to existing recipes, keep if new, give an id, add to recipes
  # and repeat....
   
  ## TODO doing it this way, may not even need N_to_make.
  while(length(unique(recipes$stitching_id)) < N_matches){
    # Sample
    
    # compare
    
    # label
    
    # concat
    
  }
  
  
  
}



