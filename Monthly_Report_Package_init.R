
# Monthly_Report_Package_init.R

library(yaml)
library(glue)
library(future)

plan(multiprocess)

print(glue("{Sys.time()} Starting Package Script"))


if (Sys.info()["sysname"] == "Windows") {
    working_directory <- file.path(dirname(path.expand("~")), "Code", "GDOT", "GDOT-Flexdashboard-Report")
} else if (Sys.info()["sysname"] == "Linux") {
    working_directory <- file.path("~", "Code", "GDOT", "GDOT-Flexdashboard-Report")
} else {
    stop("Unknown operating system.")
}
setwd(working_directory)

source("Monthly_Report_Functions.R")
# source("mark1_dynamodb.R")
conf <- read_yaml("Monthly_Report.yaml")

# startsWith(Sys.info()["nodename"], "ip-") # check for if on AWS EC2 instance

corridors <- read_feather(conf$corridors_filename)
signals_list <- corridors$SignalID[!is.na(corridors$SignalID)]
all_corridors <- read_feather(glue("all_{conf$corridors_filename}"))

# This is in testing as of 8/26
subcorridors <- corridors %>% 
    select(-Zone_Group) %>% 
    rename(
        Zone_Group = Zone, 
        Zone = Corridor, 
        Corridor = Subcorridor)

# First attempt. Only replace Corridor with Subcorridor. Lost the actual corridor. Didn't work.
# subcorridors <- corridors %>%
#     mutate(
#         Subcorridor = as.character(Subcorridor),
#         Subcorridor = if_else(!is.na(Subcorridor), Subcorridor, as.character(Corridor))
#     ) %>%
#     mutate(Corridor = factor(Subcorridor)) %>%
#     select(-Subcorridor)

conn <- get_athena_connection()

cam_config <- get_cam_config(object = "Cameras_Latest.xlsx", bucket = "gdot-spm")

# cam_config <- aws.s3::get_object(conf$cctv_config_filename, bucket = "gdot-spm") %>%
#     rawToChar() %>%
#     read_csv() %>%
#     separate(col = CamID, into = c("CameraID", "Location"), sep = ": ")
# 
# if (class(cam_config$As_of_Date) != "character") {
#     cam_config <- cam_config %>%
#         mutate(As_of_Date = if_else(grepl("\\d{4}-\\d{2}-\\d{2}", As_of_Date),
#                                     ymd(As_of_Date),
#                                     mdy(As_of_Date)
#         ))
# }


usable_cores <- get_usable_cores()
doParallel::registerDoParallel(cores = usable_cores)

# system("aws s3 sync s3://gdot-spm/mark MARK --exclude *counts_*")

# # ###########################################################################

# # Package everything up for Monthly Report back 13 months

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#

report_start_date <- conf$report_start_date
if (conf$report_end_date == "yesterday") {
    report_end_date <- Sys.Date() - days(1)
} else {
    report_end_date <- conf$report_end_date
}

#calcs_start_date <- conf$calcs_start_date
if (conf$calcs_start_date == "auto") {
    if (day(Sys.Date()) < 15) {
        calcs_start_date <- Sys.Date() - months(1)
    } else {
        calcs_start_date <- Sys.Date()
    }
    day(calcs_start_date) <- 1
} else {
    calcs_start_date <- conf$calcs_start_date
}

dates <- seq(ymd(report_start_date), ymd(report_end_date), by = "1 month")
month_abbrs <- get_month_abbrs(report_start_date, report_end_date)

report_start_date <- as.character(report_start_date)
report_end_date <- as.character(report_end_date)
print(month_abbrs)

date_range <- seq(ymd(report_start_date), ymd(report_end_date), by = "1 day")
date_range_str <- paste0("{", paste0(as.character(date_range), collapse = ","), "}")