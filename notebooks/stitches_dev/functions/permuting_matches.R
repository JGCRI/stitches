
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
  
  return(list(num_matches %>%
                group_by(target_variable, target_experiment, target_ensemble, target_model) %>%
                summarize(totalNumPerms = prod(n_matches),
                          minNumMatches = min(n_matches)) %>%
                ungroup,
              num_matches %>%
                arrange(target_variable, target_model, target_experiment, target_ensemble)))
}


###################################################################

# A function to sample from input `matched_data` (the the results 
# of `match_neighborhood(target, archive)` to produce permutations
# of possible stitching recipes that will match the target data.
#
# Args:
# matched_data: data output from match_neighborhood.
# N_matches: the number of desired stitching recipes.
# archive: The archive data to use for re-matching duplicate points
# optional something: a previous output of this function that contains a 
# list of already created recipes to avoid re-making.
# 
# Returns: something

permute_stitching_recipes <- function(N_matches, matched_data, archive, 
                                      optional=NULL){

  num_perms <- get_num_perms(matched_data)
  
  perm_guide <- num_perms[[2]]
  
  
  # how many target trajectories are we matching to,
  # how many collapse-free ensemble members can each
  # target support, and order them according to that
  # for construction.
  num_perms[[1]] %>%
    arrange(minNumMatches) %>%
    mutate(target_ordered_id = as.integer(row.names(.))) ->
    targets
  
  
  # max number of permutations per target without repeating across generated
  # ensemble members
  N_data_max <- min(num_perms[[1]]$minNumMatches)
  
  if (N_matches > N_data_max){
    message(paste("You have requested more recipes than possible for at least one target trajectories, returning what can"))
  }
  

  
  # Initialize the number of matches to either 0 or the input read from optional:
  if( is.character(optional)){
    
    # # initialize to the read-in:
    # recipes <- read.csv(optional)
    # 
    # # message: you requested N_matches
    # # and Have NN new ones, will produce NN-N_matches new ones and then just 
    # # Update N_matches to be NN-N_matches.
    # # TODO need to relate the N_to_make here to N_data_max
    # N_existing <- length(unique(recipes$stitching_id))
    # 
    # message(paste('You requested', N_matches, 'and already have', N_existing))
    # if (N_matches-N_existing <= 0){
    #   message("You have all you need")
    #   return(recipes)
    # } else {
    #   message(paste("Creating", N_matches-N_existing, "new recipes and adding to those input."))
    #   N_to_make <- N_matches-N_existing
    # }
  } else{
    # initialize to empty
    recipes <- data.frame()
  }
  
  # for each target
  for(target_id in unique(targets$target_ordered_id)) {
  
    # target info 
    targets %>%
      filter(target_ordered_id == target_id) ->
      target
    
    # initialize a recipes for each target
    recipes_by_target <- data.frame()
    
    # work with periods with more than one match, just for this target
    perm_guide %>% 
      filter(target_variable == unique(target$target_variable),
             target_experiment == unique(target$target_experiment),
             target_model == unique(target$target_model),
             target_ensemble == unique(target$target_ensemble),
             n_matches > 1) ->
      draw_periods
    
    while(length(unique(recipes_by_target$stitching_id)) < min(N_matches, target$minNumMatches)){
      
      
      # sample one match per target_year for each period with 
      # more than one match:
      matched_data %>%
        filter(target_year %in% draw_periods$target_year,
               target_variable == unique(target$target_variable),
               target_experiment == unique(target$target_experiment),
               target_model == unique(target$target_model),
               target_ensemble == unique(target$target_ensemble)) %>%
        split(f = list(.$target_variable, .$target_experiment, .$target_ensemble,
                       .$target_model, .$target_year)) -> 
        list_targets
      
      
      sampled_target <- lapply(list_targets, function(df){
        return(df[sample(nrow(df), 1), ])
      }) # end sampling
      
      matched_data %>%
        filter(!(target_year %in% draw_periods$target_year),
               target_variable == unique(target$target_variable),
               target_experiment == unique(target$target_experiment),
               target_model == unique(target$target_model),
               target_ensemble == unique(target$target_ensemble)) %>%
        bind_rows(do.call(rbind, sampled_target)) %>%
        arrange(target_year)  ->
        sampled_match
      
      # Remove duplicate archive matches within this sampled match:
      # In other words, the same archive match point cannot be paired
      # to two separate target years:
      sampled_match %>%
        remove_duplicates(matched_data = .,
                          archive = archive) %>%
        mutate(stitching_id = paste(target$target_experiment,
                                    target$target_ensemble,
                                    length(unique(recipes_by_target$stitching_id))+1,
                                    sep = "~")) ->
        sampled_match
      
      # Make sure you created a full match with no gaps:
      assert_that(nrow(sampled_match) == 28)
      assert_that(all(sampled_match$target_year == unique(matched_data$target_year)))
      
      # compare the sampled match to the existing matches.
      # Again, the challenge is seeing if our entire sample has
      # been included in recipes before, not just a row or two.
      do.call(rbind,
              lapply(split(recipes, recipes$stitching_id), function(df){
                # compare only columns without fx/dx info in case of rounding
                # issues.
                all_equal(df %>% select(-stitching_id, -target_fx, -target_dx,
                                        -archive_fx, -archive_dx, -dist_dx, 
                                        -dist_fx, -dist_l2), 
                          sampled_match %>% select(-stitching_id, -target_fx, -target_dx,
                                                   -archive_fx, -archive_dx, -dist_dx, 
                                                   -dist_fx, -dist_l2))
              })) ->
        comparison
      
      if(any(comparison == TRUE)){
        # if any entries of the comparison are true, then
        # the sampled_match agreed with one of the matches
        # in recipes, do nothing
      } else{
        # didn't match any previous recipes, add it in.
        bind_rows(recipes_by_target, sampled_match) ->
          recipes_by_target
        
        # And remove it from the matched points so it
        # can't be used to construct subsequent realizations.
        
        # each (target-year, archive-match) combination must
        # be removed from matched data for all target_ids
       sampled_match %>%
          select(target_year, target_start_yr, target_end_yr,
                 archive_experiment, archive_variable, archive_model,
                 archive_ensemble, archive_start_yr, archive_end_yr,
                 archive_year) ->
         to_remove
       
       matched_data %>%
         anti_join(to_remove,
                   by = c("target_start_yr", "target_end_yr", "target_year", 
                          "archive_experiment", "archive_variable",
                          "archive_model", "archive_ensemble", "archive_start_yr",
                          "archive_end_yr", "archive_year")) ->
         matched_data
       
       # update permutation count info with the
       # revised matched data
       num_perms <- get_num_perms(matched_data)
       
       perm_guide <- num_perms[[2]]
       
       
       # how many target trajectories are we matching to,
       # how many collapse-free ensemble members can each
       # target support, and order them according to that
       # for construction.
       num_perms[[1]] %>%
         arrange(minNumMatches) %>%
         mutate(target_ordered_id = as.integer(row.names(.))) ->
         targets
      } # end comparison if-else
      
    } # end while loop
    
    
    # Bind together the recipes for each target trajectory
    bind_rows(recipes_by_target, recipes) -> 
      recipes
    
  } # end for loop over target trajectories
    
  return(recipes)
  
} #end function



