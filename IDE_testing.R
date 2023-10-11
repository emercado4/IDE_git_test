library(googleAuthR)
library(sparklyr)
library(dplyr)

options("googleAuthR.httr_oauth_cache"="gce.oauth")
googleAuthR::gar_gce_auth()


# Testing listing tables in BQ --------------------------------------------

library(bigQueryR)

# Authenticate with an email that has access to the BigQuery project you need
bigQueryR::bqr_auth()

# Generate a dataframe of projects the user has access to
projects <- bigQueryR::bqr_list_projects()

# Show projects
projects

# Generate a dataframe of datasets within the projects the user has access to
datasets <- purrr::map_dfr(.x = projects$id, .f = bigQueryR::bqr_list_datasets)

# Show datasets
datasets

# Generate a dataframe of all tables within projects the user has access to
tables <- purrr::map2_dfr(.x = datasets$projectId, .y = datasets$datasetId, .f = bigQueryR::bqr_list_tables)

# Show tables (there is only one in this example)
tables



bqr_list_tables(projectId = "sandbox-workstations",
                datasetId = "example_data")


# Testing gcs listing buckets and objects ---------------------------------


#googleCloudStorageR::gcs_list_buckets(projectId = "sandbox-workstations")

googleCloudStorageR::gcs_list_objects(bucket = "sandbox-workstations-example-data")


# Loading dataset from GCS ------------------------------------------------

# set global bucket
googleCloudStorageR::gcs_global_bucket("sandbox-workstations-example-data")

# Define object parsing function. Not sure why the default option of `gcs_parse_download` doesn't work
# httr::content extracts content from a request like that generated below using `gcs_get_object`
parseCsv <- function(request) {
  httr::content(request, type = 'text/csv')
}

# Get animal_rescue.csv bucket
rescue <- googleCloudStorageR::gcs_get_object("animal_rescue.csv", 
                                              bucket = googleCloudStorageR::gcs_get_global_bucket(), 
                                              parseFunction = parseCsv) 

rescue_agg <- rescue %>% 
  janitor::clean_names() %>% 
  group_by(cal_year, animal_group_parent) %>% 
  summarise(across(pump_count:incident_notional_cost, ~sum(.)), 
            no_incidents = n())



# Test loading from BQ - DBI method ---------------------------------------
library(DBI)

con <- DBI::dbConnect(
  bigrquery::bigquery(),
  project = "sandbox-workstations"
)
con

query <- "SELECT * FROM `example_data.pokemon_data` LIMIT 300"

DBI::dbGetQuery(con, query)


# Test loading from BQ - bigrquery method ---------------------------------

library(bigrquery)

project <- "sandbox-workstations"
query <- "SELECT * FROM example_data.pokemon_data LIMIT 300"

tb <- bigrquery::bq_project_query(project, query)

sample <- bigrquery::bq_table_download(tb, n_max = 10)

sample