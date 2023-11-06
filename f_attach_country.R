# LINK COUNTRY, SUB-REGION AND MAIN REGION TO HILDA COUNTRY CODES
## This function loads the data SACC_Countries_2016.csv and 
## attaches the country name to the HILDA data. The SACC data is compiled using
## the script "Get_SACC_country_codes.R".
## Need to rename the country of birth : ancob, fmfcob, fmmcob to country_code.

f_attach_country <- 
  function(A){
    # Make region codes
    A <- 
      A %>% 
      filter(country_code>0) %>% 
      mutate(
        country_code=as.character(country_code),
        region_code=substr(country_code,1,1),
        sub_region_code=substr(country_code,1,2),
        across(ends_with('code'),~as.numeric(.))
      ) 
    
    # Load SACC data
    SACC <- 
      read_csv(
        str_glue("{here('Raw')}/SACC_Countries_2016.csv"),
        show_col_types = F
      ) 
    
    # Attach region names
    Z <- 
      SACC %>% 
      select(starts_with('region')) %>% 
      group_by(region) %>% 
      filter(row_number()==1)
    
    A <- left_join(A,Z,by='region_code')
    
    # Attach sub-region names
    Z <- 
      SACC %>% 
      select(starts_with('sub')) %>% 
      group_by(sub_region) %>% 
      filter(row_number()==1)
    
    A <- left_join(A,Z,by='sub_region_code')
    
    # Attach country names
    Z <- 
      SACC %>% 
      select(starts_with('country')) %>% 
      group_by(country) %>% 
      filter(row_number()==1)
    
    A <- 
      left_join(A,Z,by='country_code') %>% 
      # Some missing countries
      mutate(
        country=
          if_else(is.na(country)&str_detect(sub_region,'United Kingdom'),
                  'United Kingdom',country),
        country=
          if_else(country_code==3213|country_code==913,
                  'Yugoslavia ',country)
      )
  }