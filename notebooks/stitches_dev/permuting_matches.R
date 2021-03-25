
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