# Load required libraries
pacman::p_load(here, haven, arrow, tidyverse, foreach, doParallel)

# Specify HILDA files to convert
ftype = 'Combined'
waves = c(1:21)
release= '210u'


# Number of cores to use for parallel processing
num_cores <- detectCores() - 1  

# Initialize parallel backend
cl <- makeCluster(num_cores)
registerDoParallel(cl)

# Function to convert to parquet
f_HILDA_parquet <- 
  function(ftype, release, wave){
    letter <- letters[wave]
    year = 2000+wave
    read_dta(
        here("Raw", 
             str_glue("{ftype}_{letter}{release}.dta")
        )
      ) %>% 
      # Remove variable labels
      zap_label() %>% 
      # Remove value labels
      zap_labels() %>% 
      # Remove wave prefix
      rename_with(str_sub, start = 2L, .cols = -c("xwaveid","xhhraid")) %>%
      # Add wave number
      mutate(wave = wave, year=year) %>% 
      relocate(wave, year, .after = xwaveid) %>% 
      write_parquet(., 
                    here("Raw", str_glue("{ftype}_{release}_{year}.parquet")))
    
    gc()
  }

# Parallelized loop
foreach(i = seq_along(waves), .packages = c("here", "haven", "arrow", "tidyverse")) %dopar% {
  f_HILDA_parquet(ftype, release, waves[i])
}

stopCluster(cl)

# Convert stand alone files
ftype = 'longitudinal_weights_u' #'longitudinal_weights_u' 'Master_u'

read_dta(
  here("Raw", 
       str_glue("{ftype}{release}.dta")
  )
) %>% 
  write_parquet(., 
                here("Raw", str_glue("{ftype}{release}.parquet")))

# CNEF
read_dta(
  here("Raw", 
       str_glue("CNEF_Long_u210c.dta")
  )
) %>% 
  write_parquet(., 
                here("Raw", str_glue("CNEF_Long_u210c.parquet")))
