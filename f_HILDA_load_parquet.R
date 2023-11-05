# LOAD HILDA DATA FROM PARQUET FILES
# This function loads the HILDA data from parquet files. The parquet files are 
# created by the script Convert_HILDA_dta_to_parquet.R
# The raw data follows the naming convention ftype_release_year.parquet. 
# For instance the Combined file for wave 1 from release 210u is 
# Combined_210u_2001.parquet


# Inputs:
# folder: folder where the parquet files are stored
# ftype: type of file to load. Options are Combined, Rperson, Eperson, Household
# release: release of the data. Options are 210u, 210c and so on
# years: years to load. Options are 2001, 2002 and so on. eg: years=c(2001:2021)
# varlist: list of variables to load. Options are all variables in the parquet

library(tidyverse)
library(arrow)
f_HILDA_load_parquet <- 
  function(folder,ftype,release,years,varlist) {
    A <- 
      open_dataset(
        str_glue("{folder}/{ftype}_{release}_{years}.parquet")
      ) %>%
      select(any_of(varlist)) %>% 
      collect()
  }