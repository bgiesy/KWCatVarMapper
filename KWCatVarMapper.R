require(dplyr)
require(readr)

main <- function()
{
  #filter by descriptions of requirement
  dor_accounting_df <- load_and_prep("descriptonOfReq.csv", "FY_16ATOM_GW_Weps_Clth.csv")
  processed_accounting_df <- column_processor(dor_accounting_df, target_df, mode = "dor")
  dor_resultset <- collect_rows(processed_accounting_df, target_df)
  write_csv(dor_resultset, "dor_resultset.csv")
  
  #filter by vendor
  vendor_accounting_df <- load_and_prep("vendor.csv", "FY_16ATOM_GW_Weps_Clth.csv")
  processed_accounting_df <- column_processor(vendor_accounting_df, target_df, mode = "vendor")
  vendor_resultset <- collect_rows(processed_accounting_df, target_df)
  write_csv(vendor_resultset, "vendor_resultset.csv")
  
  #filter by psc_and_dor
  psc_dor_accounting_df <- psc_dor_load_and_prep("psc_dor.csv", "FY_16ATOM_GW_Weps_Clth.csv")
  processed_accounting_df <- column_processor(psc_dor_accounting_df, target_df, mode = "dor")  
  psc_dor_resultset <- collect_rows(processed_accounting_df, target_df) 
  write_csv(psc_dor_resultset, "psc_dor_resultset.csv")
  
  final_result_set <- rbind(dor_resultset, vendor_resultset, psc_dor_resultset)
  write.csv(final_result_set, "Initial Criteria Result Set.csv")
  distinct_criteria_df <- final_result_set
  distinct_criteria_df$product_or_service_code <- as.character(distinct_criteria_df$product_or_service_code)
  distinct_criteria_df <- distinct_criteria_df %>% distinct(product_or_service_code, naics_code, vendor_duns_number)
  
  first_filter <- filter(transaction_df, product_or_service_code %in% distinct_criteria_df$product_or_service_code)
  
  second_filter <- merge(distinct_criteria_df, first_filter)
  write_csv(second_filter, "Final Filtered Result Set.csv")
  }

psc_dor_load_and_prep <- function(term_file, df_file)
{
  ##read in dictionary terms from csv
  dictionary_df <<- read_csv(term_file)
  ## identify unique psc and dor search criteria 
  pscs <- dictionary_df %>% select(psc)%>% distinct() %>% .$psc
  terms <- dictionary_df %>% select(dor)%>% distinct() %>% .$dor
  
  
  ##read in transaction terms from csv
  transaction_df <<- read_csv(df_file)
  
  print("capturing requirement descriptons")
  
  target_df <<- transaction_df %>% 
    filter(product_or_service_code %in% pscs) %>%
    select(product_or_service_code, product_or_service_description, naics_code, naics_description, funding_agency_name, vendor_duns_number, vendor_name, description_of_requirement, dollars_obligated)
  
  #create transverse data frame for dictionary accounting
  t_dictionary <- t(terms)
  #set column names = first row = the dictionary terms
  colnames(t_dictionary)<- t_dictionary[1,]
  #remove the first row
  t_dictionary <- t_dictionary[-1,]
  #create empty rows in t_dictionary corresponding to number of rows in transaction_df
  accounting_df <- data.frame(matrix(NA, nrow = nrow(target_df), ncol = ncol(t_dictionary))) 
  colnames(accounting_df) <- c(colnames(t_dictionary))
  accounting_df <- accounting_df[, -1]
  
  accounting_df
  
  
  
}

load_and_prep <- function(term_file, df_file)
{
  ##read in dictionary terms from csv
  dictionary_df <<- read_csv(term_file)
  ##read in transaction terms from csv
  transaction_df <- read_csv(df_file)
  ##read in psc translation table
  psc_trans_table <<- read_csv("pscTransTable_rebaselining_March_8.csv")
  #pull IT PSC vector from psc translation table
  #cat_pscs <- psc_trans_table %>% filter(Level_1_Category == "Security and Protection") %>% select(`4_Digit_PSC`) %>% .$'4_Digit_PSC'
  cat_pscs <- psc_trans_table %>% filter(psc_trans_table$`V2 Level 1 Category` == "Security and Protection") %>% select(`PSC CODE`) %>% .$'PSC CODE'

  #load corresponding transactions from data frame
  print("capturing requirement descriptons")
  target_df <<- transaction_df %>% 
    filter(product_or_service_code %in% cat_pscs) %>%
    select(product_or_service_code, product_or_service_description, naics_code, naics_description, funding_agency_name, vendor_duns_number, vendor_name, description_of_requirement, dollars_obligated)
 
  #rebaselining_additions <- read_csv("pscTransTable_rebaselining_March8.csv")
  
   #create transverse data frame for dictionary accounting
  t_dictionary <- t(dictionary_df)
  #set column names = first row = the dictionary terms
  colnames(t_dictionary)<- t_dictionary[1,]
  #remove the first row
  t_dictionary <- t_dictionary[-1,]
  #create empty rows in t_dictionary corresponding to number of rows in transaction_df
  accounting_df <- data.frame(matrix(NA, nrow = nrow(target_df), ncol = ncol(t_dictionary))) 
  colnames(accounting_df) <- c(colnames(t_dictionary))
  accounting_df <- accounting_df[, -1]
  
  accounting_df
  
}

column_processor <- function(accounting_df, target_df, mode)
{
  column_list_count <- ncol(accounting_df)
  for(i in 1:column_list_count)
  {#call function to process one column
    if(mode == "dor")
    {truth_vector <- dor_scan_for_term(target_df, colnames(accounting_df[i]))}
    else 
    {if (mode == "vendor")
    {truth_vector <- vendor_scan_for_term(target_df, colnames(accounting_df[i]))}
      else print("Scan mode not identified")
    }
    #write one column's truth vector back out to accounting_df
    accounting_df[,i]<-truth_vector
  }
  accounting_df
}




dor_scan_for_term <- function(target_df, term)
{ #search entire dataset for one term
  offending_transaction_vector <- grepl(toupper(term), target_df$description_of_requirement)
  offending_transaction_vector
}

vendor_scan_for_term <- function(target_df, term)
{ #search entire dataset for one term
  offending_transaction_vector <- grepl(toupper(term), target_df$vendor_name)
  offending_transaction_vector
}

psc_dor_scan_for_term <- function(target_df, psc, term)
{ #search entire dataset for one term
  target_df <- target_df %>% filter(product_or_service_code == psc)
  offending_transaction_vector <- grepl(toupper(term), target_df$description_of_requirement)
  offending_transaction_vector
}


collect_rows <- function(filled_accounting_df, target_df)
{
  column_list_count <- ncol(filled_accounting_df)
  targeted_rows_nums <- numeric()
  targeted_row_terms <- character()
  for(i in 1:column_list_count)
  {#call function to process one column
    targeted_rows_nums <- append(targeted_rows_nums, which(filled_accounting_df[,i] == TRUE))
    term_count <- length(which(filled_accounting_df[,i] == TRUE))
    term <- colnames(filled_accounting_df[i])
    targeted_row_terms <- append(targeted_row_terms, rep(term, term_count))
  }
  
  targeted_rows <- target_df[targeted_rows_nums, ]
  targeted_rows <- cbind(targeted_rows, targeted_row_terms)
  targeted_rows
}

