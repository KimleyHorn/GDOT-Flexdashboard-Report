
# Monthly_Report_Functions.R

#Sys.unsetenv("JAVA_HOME")
suppressMessages(library(DBI))
suppressMessages(library(rJava))
#.jinit()
suppressMessages(library(RJDBC))
suppressMessages(library(RAthena))
suppressMessages(library(readxl))
suppressMessages(library(readr))
suppressMessages(library(dplyr))
suppressMessages(library(tidyr))
suppressMessages(library(stringr))
suppressMessages(library(purrr))
suppressMessages(library(lubridate))
suppressMessages(library(glue))
suppressMessages(library(data.table))
suppressMessages(library(formattable))

suppressMessages(library(forcats))
suppressMessages(library(feather))
suppressMessages(library(fst))
suppressMessages(library(parallel))
suppressMessages(library(doParallel))
suppressMessages(library(future))
suppressMessages(library(multidplyr))
#suppressMessages(library(pool))
suppressMessages(library(httr))
suppressMessages(library(aws.s3))
suppressMessages(library(sf))
suppressMessages(library(yaml))
suppressMessages(library(utils))
suppressMessages(library(readxl))

suppressMessages(library(plotly))
suppressMessages(library(crosstalk))

suppressMessages(library(reticulate))

suppressMessages(library(runner))
suppressMessages(library(fitdistrplus))

#suppressMessages(library(sparklyr))
suppressMessages(library(foreach))

suppressMessages(library(arrow))
# https://arrow.apache.org/install/
# install.packages("arrow")

select <- dplyr::select
filter <- dplyr::filter

conf <- read_yaml("Monthly_Report.yaml")

if (Sys.info()["sysname"] == "Windows") {
    home_path <- dirname(path.expand("~"))
    python_path <- file.path(home_path, "Anaconda3", "python.exe")
    
} else if (Sys.info()["sysname"] == "Linux") {
    home_path <- "~"
    python_path <- file.path(home_path, "miniconda3", "bin", "python")
    
} else {
    stop("Unknown operating system.")
}

use_python(python_path)

pqlib <- reticulate::import_from_path("parquet_lib", path = ".")

# Colorbrewer Paired Palette Colors
LIGHT_BLUE = "#A6CEE3";   BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A";  GREEN = "#33A02C"
LIGHT_RED = "#FB9A99";    RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"; ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"; PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99";  BROWN = "#B15928"

RED2 = "#e41a1c"
GDOT_BLUE = "#256194"

SUN = 1; MON = 2; TUE = 3; WED = 4; THU = 5; FRI = 6; SAT = 7

AM_PEAK_HOURS = conf$AM_PEAK_HOURS
PM_PEAK_HOURS = conf$PM_PEAK_HOURS

if (Sys.info()["nodename"] %in% c("GOTO3213490", "Larry")) { # The SAM or Larry
    set_config(
        use_proxy("gdot-enterprise", port = 8080,
                  username = Sys.getenv("GDOT_USERNAME"),
                  password = Sys.getenv("GDOT_PASSWORD")))
    
} else { # shinyapps.io
    Sys.setenv(TZ="America/New_York")
}

sizeof <- function(x) {
    format(object.size(x), units = "Mb")
}

get_most_recent_monday <- function(date_) {
    date_ + days(1 - lubridate::wday(date_, week_start  = 1))
}

get_usable_cores <- function() {
    # Usable cores is one per 8 GB of RAM. 
    # Get RAM from system file and divide
    
    if (Sys.info()["sysname"] == "Windows") {
        x <- shell('systeminfo | findstr Memory', intern = TRUE)
        
        memline <- x[grepl("Total Physical Memory", x)]
        mem <- stringr::str_extract(string =  memline, pattern = "\\d+,\\d+")
        mem <- as.numeric(gsub(",", "", mem))
        mem <- round(mem, -3)
        max(floor(mem/8e3), 1)
        
    } else if (Sys.info()["sysname"] == "Linux") {
        x <- readLines('/proc/meminfo')
        
        memline <- x[grepl("MemTotal", x)]
        mem <- stringr::str_extract(string =  memline, pattern = "\\d+")
        mem <- as.integer(mem)
        mem <- round(mem, -6)
        max(floor(mem/8e6), 1)
        
    } else {
        stop("Unknown operating system.")
    }
}

# From: https://billpetti.github.io/2017-10-13-retry-scrape-function-automatically-r-rstats/
retry_function <- function(.f, max_attempts = 5,
                           wait_seconds = 5) {
    
    force(max_attempts)
    force(wait_seconds)
    
    for (i in seq_len(max_attempts)) {
        
        tryCatch({
            output <- .f
        }, error = function(e) {
            if (wait_seconds > 0) {
                message(paste0("Retrying at ", Sys.time() + wait_seconds))
                Sys.sleep(wait_seconds)
            }
        })
    }
    
    stop()
}

read_zipped_feather <- function(x) {
    read_feather(unzip(x))
}

get_atspm_connection <- function(conf_atspm) {
    
    if (Sys.info()["sysname"] == "Windows") {
        
        dbConnect(odbc::odbc(),
                  dsn = conf_atspm$odbc_dsn,
                  uid = Sys.getenv(conf_atspm$uid_env),
                  pwd = Sys.getenv(conf_atspm$pwd_env))
        
    } else if (Sys.info()["sysname"] == "Linux") {
        
        dbConnect(odbc::odbc(),
                  driver = "FreeTDS",
                  server = Sys.getenv(conf_atspm$svr_env),
                  database = Sys.getenv(conf_atspm$db_env),
                  uid = Sys.getenv(conf_atspm$uid_env),
                  pwd = Sys.getenv(conf_atspm$pwd_env))
    }
}

get_maxview_connection <- function(dsn = "MaxView") {
    
    if (Sys.info()["sysname"] == "Windows") {
        
        dbConnect(odbc::odbc(),
                  dsn = dsn,
                  uid = Sys.getenv("ATSPM_USERNAME"),
                  pwd = Sys.getenv("ATSPM_PASSWORD"))
        
    } else if (Sys.info()["sysname"] == "Linux") {
        
        dbConnect(odbc::odbc(),
                  driver = "FreeTDS",
                  server = Sys.getenv("MAXV_SERVER_INSTANCE"),
                  database = Sys.getenv("MAXV_EVENTLOG_DB"),
                  uid = Sys.getenv("ATSPM_USERNAME"),
                  pwd = Sys.getenv("ATSPM_PASSWORD"))
    }
}

get_maxview_eventlog_connection <- function() {
    get_maxview_connection(dsn = "MaxView_EventLog")
}

get_cel_connection <- get_maxview_eventlog_connection

get_athena_connection_broken <- function(conf_athena) {
    
    drv <- JDBC(driverClass = "com.simba.athena.jdbc.Driver",
                classPath = conf_athena$jar_path,
                identifier.quote = "'")
    
    if (Sys.info()["nodename"] == "GOTO3213490") { # The SAM
        
        dbConnect(drv, "jdbc:awsathena://athena.us-east-1.amazonaws.com:443/",
                  s3_staging_dir = conf_athena$staging_dir,
                  user = Sys.getenv("AWS_ACCESS_KEY_ID"),
                  password = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
                  ProxyHost = "gdot-enterprise",
                  ProxyPort = "8080",
                  ProxyUID = Sys.getenv("GDOT_USERNAME"),
                  ProxyPWD = Sys.getenv("GDOT_PASSWORD"))
    } else {
        
        dbConnect(drv, "jdbc:awsathena://athena.us-east-1.amazonaws.com:443/",
                  s3_staging_dir = conf_athena$staging_dir,
                  user = Sys.getenv("AWS_ACCESS_KEY_ID"),
                  password = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
                  UseResultsetStreaming = 1)
    }
}

get_athena_connection <- function(conf_athena) {
    dbConnect(
        RAthena::athena(),
        aws_access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
        s3_staging_dir = conf_athena$staging_dir,
        region_name = 'us-east-1')
}

get_aurora_connection <- function() {
    
    RMySQL::dbConnect(RMySQL::MySQL(),
                      host = Sys.getenv("RDS_HOST"),
                      port = 3306,
                      dbname = Sys.getenv("RDS_DATABASE"),
                      username = Sys.getenv("RDS_USERNAME"),
                      password = Sys.getenv("RDS_PASSWORD"))
}


get_spark_context <- function() {
    conf <- spark_config()
    conf$sparklyr.defaultPackages <- c("com.amazonaws:aws-java-sdk-pom:1.10.34",
                                       "org.apache.hadoop:hadoop-aws:2.7.3")
    
    
    conf$`sparklyr.cores.local` <- 7
    conf$`sparklyr.shell.driver-memory` <- "16G"
    conf$spark.memory.fraction <- 0.9
    
    
    sc <- spark_connect(master = "local", config = conf)
    
    ctx <- sparklyr::spark_context(sc)
    
    jsc <- invoke_static(sc, 
                         "org.apache.spark.api.java.JavaSparkContext", 
                         "fromSparkContext", 
                         ctx)
    
    hconf <- jsc %>% 
        invoke("hadoopConfiguration")
    hconf %>% 
        invoke("set", "com.amazonaws.services.s3a.enableV4", "true")
    hconf %>% 
        invoke("set", "fs.s3a.fast.upload", "true")
    
    hconf %>% 
        invoke("set", "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf %>% 
        invoke("set","fs.s3a.access.key", Sys.getenv("AWS_ACCESS_KEY_ID")) 
    hconf %>% 
        invoke("set","fs.s3a.secret.key", Sys.getenv("AWS_SECRET_ACCESS_KEY"))
    
    sparklyr::spark_connection_is_open(sc=sc)
    sc
}

# Because of issue with Apache Arrow (feather, parquet)
# where R wants to convert UTC to local time zone on read
# Switch date or datetime fields back to UTC. Run on read.
convert_to_utc <- function(df) {
    
    # -- This may be a more elegant alternative. Needs testing. --
    #df %>% mutate_if(is.POSIXct, ~with_tz(., "UTC"))

    is_datetime <- sapply(names(df), function(x) sum(class(df[[x]]) == "POSIXct"))
    datetimes <- names(is_datetime[is_datetime==1])
    
    for (col in datetimes) {
        df[[col]] <- with_tz(df[[col]], "UTC")
    }
    df
}

# Use Python to upload R dataframe to s3 in parquet format
s3_upload_parquet <- function(df, date_, fn, bucket, table_name, athena_db) {
    
    df <- ungroup(df)
    
    if ("Detector" %in% names(df)) { 
        df <- mutate(df, Detector = as.character(Detector)) 
    }
    if ("CallPhase" %in% names(df)) { 
        df <- mutate(df, CallPhase = as.character(CallPhase)) 
    }
    
    # s3write_using(select(df, -Date),
    #               write_parquet,
    #               bucket = bucket,
    #               object = glue("mark/{table_name}/date={date_}/{fn}.parquet"))
    
    feather_filename = paste0(fn, ".feather")
    write_feather(df, feather_filename)

    pqlib$upload_parquet(Bucket = bucket,
                         Key = glue("mark/{table_name}/date={date_}/{fn}.parquet"),
                         Filename = feather_filename,
                         Database = athena_db)
    file.remove(feather_filename)
}

s3_upload_parquet_date_split <- function(df, prefix, bucket, table_name, athena_db) {
    
    if (!("Date" %in% names(df))) {
        if ("Timeperiod" %in% names(df)) { df <- mutate(df, Date = date(Timeperiod)) }
        else if ("Hour" %in% names(df)) { df <- mutate(df, Date = date(Hour)) }
    }
    
    df %>% 
        split(.$Date) %>% 
        lapply(function(x) {
            date_ <- as.character(x$Date[1])
            s3_upload_parquet(x, date_,
                              fn = glue("{prefix}_{date_}"), 
                              bucket = bucket,
                              table_name = table_name, 
                              athena_db = athena_db)
            Sys.sleep(1)
        })
}
s3_read_parquet <- function(table_name, 
                            start_date, 
                            end_date, 
                            signals_list = NULL, 
                            bucket = NULL,
                            callback = function(x) {x}) {
    
    if (is.null(bucket)) {
        fn <- pqlib$read_parquet_local(table_name, start_date, end_date, signals_list)
    } else{
        fn <- pqlib$read_parquet(bucket, table_name, start_date, end_date, signals_list)
    }
    
    if (!is.null(fn)) {
        df = read_feather(fn) %>% 
            convert_to_utc()
        if (nrow(df)) {
            df <- callback(df)
        }
        file.remove(fn)
    } else {
        df <- data.frame()
    }
    as_tibble(df)
}

# The function below uses the python function for parallel reads
# which would be nice except it prevents us from using callbacks from R
# which causes it to return very large data sets.
s3_read_parquet_parallel_newer <- function(
    table_name, 
    start_date, 
    end_date, 
    signals_list = NULL, 
    bucket = NULL,
    callback = function(x) {x}) {
    
    if (is.null(bucket)) {
        fn <- pqlib$read_parquet_local(table_name, start_date, end_date, signals_list)
    } else{
        fn <- pqlib$read_parquet(bucket, table_name, start_date, end_date, signals_list)
    }
    
    if (!is.null(fn)) {
        df = read_feather(fn) %>% 
            convert_to_utc()
        if (nrow(df)) {
            df <- callback(df)
        }
        file.remove(fn)
    } else {
        df <- data.frame()
    }
    as_tibble(df)
}

s3_read_parquet_parallel <- function(table_name, 
                                     start_date, 
                                     end_date, 
                                     signals_list = NULL, 
                                     bucket = NULL,
                                     callback = function(x) {x}) {
    
    usable_cores <- detectCores() - 1 # get_usable_cores()
    
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")
    
    doParallel::registerDoParallel(cores = usable_cores)
    
    df <- foreach(date_ = date_range, .combine = rbind) %dopar% {
        date_ <- as.character(date_)
        x <- s3_read_parquet(
            table_name, 
            date_, 
            date_, 
            signals_list = signals_list, 
            bucket = bucket)
        if (nrow(x)) callback(x)
    }
    if (length(df) > 1) {
        bind_rows(df)
    } else {
        df
    }
}
# The below is getting this errors;
# SSL read: error:1408F119:SSL routines:SSL3_GET_RECORD:decryption failed or bad record mac, errno 0
s3_read_parquet_parallel2 <- function(table_name, 
                                      start_date, 
                                      end_date, 
                                      signals_list = NULL, 
                                      bucket = NULL) {
    
    objs <- aws.s3::get_bucket_df(bucket, prefix = glue("mark/{table_name}")) %>% 
        filter(Key >= glue("mark/{table_name}/date={start_date}"), 
               Key <= glue("mark/{table_name}/date={end_date}"))
    
    
    doParallel::registerDoParallel(cores = detectCores() - 1)
    
    foreach(key = objs$Key, .combine = rbind) %dopar% {
        date_ <- stringr::str_extract(key, "\\d+-\\d+-\\d+")
        s3read_using(read_parquet, 
                     object = key, 
                     bucket = bucket) %>%
            filter(SignalID %in% signals_list) %>%
            mutate(Date = ymd(date_))
    }
}
week <- function(d) {
    d0 <- ymd("2016-12-25")
    as.integer(trunc((ymd(d) - d0)/dweeks(1)))
}

get_month_abbrs <- function(start_date, end_date) {
    start_date <- ymd(start_date)
    day(start_date) <- 1
    end_date <- ymd(end_date)
    
    sapply(seq(start_date, end_date, by = "1 month"), function(d) { format(d, "%Y-%m")} )
}

bind_rows_keep_factors <- function(dfs) {
    ## Identify all factors
    factors <- unique(unlist(
        map(list(dfs[[1]]), ~ select_if(dfs[[1]], is.factor) %>% names())
    ))
    ## Bind dataframes, convert characters back to factors
    suppressWarnings(bind_rows(dfs)) %>% 
        mutate_at(vars(one_of(factors)), factor)  
}

match_type <- function(val, val_type_to_match) {
    eval(parse(text=paste0('as.',class(val_type_to_match), "(", val, ")")))
}

addtoRDS <- function(df, fn, delta_var, rsd, csd) {
    
    #' combines data frame in local rds file with newly calculated data
    #' trimming the current data and appending the new data to prevent overlaps
    #' and/or duplicates. Used throughout Monthly_Report_Package code
    #' to avoid having to recalculate entire report period (13 months) every time
    #' which takes too long and runs into memory issues frequently.
    #' 
    #' @param df newly calculated data frame on most recent data
    #' @param fn filename of same data over entire reporting period (13 months)
    #' @param rsd report_start_date: start of current report period (13 months prior)
    #' @param csd calculation_start_date: start date of most recent data
    #' @return a combined data frame
    #' @examples
    #' addtoRDS(avg_daily_detector_uptime, "avg_daily_detector_uptime.rds", report_start_date, calc_start_date)
    
    combine_dfs <- function(df0, df, delta_var, rsd, csd) {
        
        # Extract aggregation period from the data fields
        periods <- intersect(c("Month", "Date", "Hour"), names(df0))
        per_ <- as.name(periods)
        
        # Remove everything after calcs_start_date (csd) in original df
        df0 <- df0 %>% filter(!!per_ >= rsd, !!per_ < csd)
        
        # Extract aggregation groupings from the data fields
        # to calculate period-to-period deltas
        
        groupings <- intersect(c("Zone_Group", "Corridor", "SignalID"), names(df0))
        groups_ <- sapply(groupings, as.name)

        group_arrange <- c(periods, groupings) %>%
            sapply(as.name)
        
        var_ <- as.name(delta_var)
        
        # Combine old and new
        x <- bind_rows_keep_factors(list(df0, df)) %>% 
            
            # Recalculate deltas from prior periods over combined df
            group_by(!!!groups_) %>% 
            arrange(!!!group_arrange) %>% 
            mutate(lag_ = lag(!!var_), 
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(-lag_)
        
        x
    }
    
    if (!file.exists(fn)) {
        saveRDS(df, fn)
    } else {
        df0 <- readRDS(fn)
        if (is.list(df) && is.list(df0) && 
            !is.data.frame(df) && !is.data.frame(df0) && 
            (names(df) == names(df0))) {
            x <- purrr::map2(df0, df, combine_dfs, delta_var, rsd, csd)
        } else {
            x <- combine_dfs(df0, df, delta_var, rsd, csd)
        }
        saveRDS(x, fn)
        x
    }
    
}

write_fst_ <- function(df, fn, append = FALSE) {
    if (append == TRUE & file.exists(fn)) {
        
        factors <- unique(unlist(
            map(list(df), ~ select_if(df, is.factor) %>% names())
        ))
        
        df_ <- read_fst(fn)
        df_ <- bind_rows(df, df_) %>% 
            mutate_at(vars(one_of(factors)), factor)
    } else {
        df_ <- df
    }
    write_fst(distinct(df_), fn)
}

get_corridors <- function(corr_fn, filter_signals = TRUE) {
    
    # Keep this up to date to reflect the Corridors_Latest.xlsx file
    cols <- list(SignalID = "numeric", #"text",
                 Zone_Group = "text",
                 Zone = "text",
                 Corridor = "text",
                 Subcorridor = "text",
                 Agency = "text",
                 `Main Street Name` = "text",
                 `Side Street Name` = "text",
                 Milepost = "numeric",
                 Asof = "date",
                 Duplicate = "numeric",
                 Include = "logical",
                 Modified = "date",
                 Note = "text",
                 Asof2 = "date")
    
    df <- readxl::read_xlsx(corr_fn, col_types = unlist(cols)) %>% 

        # Get the last modified record for the Signal|Zone|Corridor combination
        replace_na(replace = list(Modified = ymd("1900-01-01"))) %>% 
        group_by(SignalID, Zone, Corridor) %>% 
        filter(Modified == max(Modified)) %>% 
        ungroup() %>%

        filter(!is.na(Corridor))
    
    # if filter_signals == FALSE, this creates all_corridors, which
    #   includes corridors without signals
    #   which is used for manual ped/det uptimes and camera uptimes
    if (filter_signals) {
        df <- df %>%
            filter(
                SignalID > 0,
                Include == TRUE)
    }
    
    df %>%        
        tidyr::unite(Name, c(`Main Street Name`, `Side Street Name`), sep = ' @ ') %>%
        transmute(SignalID = factor(SignalID), 
                  Zone = as.factor(Zone), 
                  Zone_Group = factor(Zone_Group),
                  Corridor = as.factor(Corridor),
                  Subcorridor = as.factor(Subcorridor),
                  Milepost = as.numeric(Milepost),
                  Agency = Agency,
                  Name = Name,
                  Asof = date(Asof)) %>%
        mutate(Description = paste(SignalID, Name, sep = ": "))
    
}

get_corridor_name <- function(string) {
    dplyr::case_when(
        
        # From Inrix, Excel Monthly Report Files
        
        grepl(pattern = "Z1.*( |-)13", string) ~ "SR 13/42/155",
        grepl(pattern = "Z1.*( |-)141", string) ~ "SR 141S",
        grepl(pattern = "Z1.*( |-)155", string) ~ "SR 13/42/155",
        grepl(pattern = "Z1.*( |-)237", string) ~ "SR 237",
        grepl(pattern = "Z1.*( |-)42", string) ~ "SR 13/42/155",
        grepl(pattern = "Z1.*( |-)9", string) ~ "SR 9-Atlanta",
        
        grepl(pattern = "Z2.*( |-)120", string) ~ "SR 120W",
        grepl(pattern = "Z2.*( |-)140", string) ~ "SR 92/140",
        grepl(pattern = "Z2.*( |-)280", string) ~ "SR 280",
        grepl(pattern = "Z2.*( |-)360", string) ~ "SR 360",
        grepl(pattern = "Z2.*( |-)3", string) ~ "SR 3N",
        grepl(pattern = "Z2.*( |-)92", string) ~ "SR 92",
        grepl(pattern = "Z2.*( |-)9", string) ~ "SR 9",
        grepl(pattern = "Z2.*( |-)9", string) ~ "SR 9",
        
        grepl(pattern = "Z3.*( |-)138E", string) ~ "SR 138E",
        grepl(pattern = "Z3.*( |-)138S", string) ~ "SR 138S",
        grepl(pattern = "Z3.*( |-)3S", string) ~ "SR 3S",
        grepl(pattern = "Z3.*( |-)85", string) ~ "SR 85",
        grepl(pattern = "Z3.*( |-)54", string) ~ "SR 54/Jonesboro Road",
        
        grepl(pattern = "Z4.*( |-)278", string) ~ "US 278",
        grepl(pattern = "Z4.*( |-)3", string) ~ "SR 3",
        grepl(pattern = "Z4.*( |-)5", string) ~ "SR 5",
        grepl(pattern = "Z4.*( |-)6", string) ~ "SR 6",
        
        grepl(pattern = "Z5.*SR( |-)10", string) ~ "SR 10",
        grepl(pattern = "Z5.*SR( |-)12", string) ~ "SR 12",
        grepl(pattern = "Z5.*( |-)154", string) ~ "SR 154",
        grepl(pattern = "Z5.*( |-)155", string) ~ "SR 155S",
        grepl(pattern = "Z5.*( |-)42", string) ~ "SR 42",
        grepl(pattern = "Z5.*( |-)8W( |-)De[kK]alb", string) ~ "SR 8W-DeKalb",
        grepl(pattern = "Z5.*( |-)8W( |-)", string) ~ "SR 8W-Ponce",
        grepl(pattern = "Z5.*( |-)236", string) ~ "SR 236",
        grepl(pattern = "Z5.*( |-)Ponce", string) ~ "East Ponce de Leon Blvd",
        grepl(pattern = "Z5.*( |-)MIB", string) ~ "Mountain Industrial Blvd",
        
        grepl(pattern = "Z6.*( |-)120", string) ~ "SR 120E",
        grepl(pattern = "Z6.*( |-)140", string) ~ "SR 140-Gwinnett",
        grepl(pattern = "Z6.*( |-)141", string) ~ "SR 141N",
        grepl(pattern = "Z6.*( |-)20", string) ~ "SR 20",
        grepl(pattern = "Z6.*( |-)8.*( |-)DeKalb", string) ~ "SR 8E-DeKalb",
        grepl(pattern = "Z6.*( |-)8.*( |-)Gwinnett", string) ~ "SR 8E-Gwinnett",
        grepl(pattern = "Z6.*( |-)9", string) ~ "SR 9-Alpharetta",
        
        grepl(pattern = "North(.*)Ave", string) ~ "North Ave",
        
        grepl(pattern = "Z7.*( |-)10th", string) ~ "10th St",
        grepl(pattern = "Z7.*( |-)14th", string) ~ "14th St",
        grepl(pattern = "Z7.*( |-)17th", string) ~ "17th St",
        grepl(pattern = "Z7.*( |-)Ivan-Allen", string) ~ "Ivan Allen/Ralph McGill Blvd-Downtown",
        grepl(pattern = "Z7.*( |-)COP", string) ~ "Centennial Olympic Park Dr",
        grepl(pattern = "Z7.*( |-)Marietta", string) ~ "Marietta St",
        grepl(pattern = "Z7.*( |-)Monroe", string) ~ "Monroe Dr",
        grepl(pattern = "Z7.*( |-)W.*( |-)Peachtree", string) ~ "W Peachtree St",
        
        (grepl(pattern = "Z7.*( |-)", string) &
             grepl("Downtown", string) &
             (grepl("Juniper", string) | grepl("Courtland", string))) ~ "Courtland St",
        grepl(pattern = "Z7.*( |-)", string) & 
            grepl("Downtown", string) & 
            grepl("Ivan Allen", string) ~ "Courtland St",
        grepl(pattern = "Z7.*( |-)", string) & 
            grepl("Downtown", string) & 
            grepl("Peachtree", string) ~ "Peachtree St-Downtown",
        grepl(pattern = "Z7.*( |-)", string) & 
            grepl("Downtown", string) & 
            grepl("Piedmont", string) ~ "Piedmont Ave-Downtown",
        grepl(pattern = "Z7.*( |-)", string) & 
            grepl("Downtown", string) & 
            (grepl("Ted Turner", string) | grepl("Spring", string)) ~ "Ted Turner Dr",
        
        grepl(pattern = "Z7.*( |-)", string) & 
            grepl("Midtown", string) & 
            grepl("Juniper", string) ~ "Juniper St-Midtown",
        grepl(pattern = "Z7.*( |-)", string) & 
            grepl("Midtown", string) & 
            grepl("Peachtree", string) ~ "Peachtree St-Midtown",
        grepl(pattern = "Z7.*( |-)", string) & 
            grepl("Midtown", string) & 
            grepl("Piedmont", string) ~ "Piedmont Ave-Midtown",
        grepl(pattern = "Z7.*( |-)", string) & 
            grepl("Midtown", string) & 
            grepl("Spring", string) ~ "Spring St-Midtown",
        
        
        grepl(pattern = "Z8.*( |-)140", string) ~ "SR 92/140",
        grepl(pattern = "Z8.*( |-)Ashford-Dunwoody", string) ~ "Perimeter",
        grepl(pattern = "Z8.*( |-)Glenridge", string) ~ "Perimeter",
        grepl(pattern = "Z8.*( |-)Johnson-Ferry", string) ~ "Perimeter",
        grepl(pattern = "Z8.*( |-)Peachtree-Dunwoody", string) ~ "Perimeter",
        grepl(pattern = "Z8.*( |-)Perimeter-Center", string) ~ "Perimeter",
        grepl(pattern = "Z8.*( |-)SR( |-)9", string) ~ "SR 9",
        grepl(pattern = "Z8.*( |-)SR-9-ITP", string) ~ "SR 9-ITP",
        grepl(pattern = "Z8.*( |-)SR-9-OTP", string) ~ "SR 9-OTP",
        grepl(pattern = "Z8.*( |-)Dunwoody", string) ~ "Perimeter",
        grepl(pattern = "Z8.*( |-)Sandy Springs", string) ~ "Perimeter",
        
        startsWith(string, "-") ~ "",
        
        # Catchall. Return itself.
        TRUE ~ string)
}

get_cam_config <- function(object, bucket) {
    aws.s3::s3read_using(read_excel, object = object, bucket = bucket) %>%
        filter(!is.na(Corridor)) %>%
        transmute(
            CameraID = factor(CameraID), 
            Location, 
            Corridor = factor(Corridor), 
            #Zone = factor(Zone), 
            As_of_Date = date(As_of_Date)) %>%
        distinct()

}

# get_tmc_routes <- function(pth = "TMC_Identification") {
#     
#     lapply(aws.s3::get_bucket(bucket = 'gdot-spm', prefix = pth), function(x) { 
#         print(x$Key)
#         if (!file.exists(file.path(pth, basename(x$Key)))) { 
#             print("downloading...")
#             aws.s3::save_object(x$Key, file = file.path(pth, basename(x$Key)), bucket = 'gdot-spm')
#         }
#     })
# 
#     fns <- list.files(pth, pattern = "*.zip", recursive = TRUE)
#     
#     df <- lapply(fns, function(fn) {
#         x <- read_csv(unz(file.path(pth, fn), "TMC_Identification.csv"))
#         
#         # there may be multiple segments, differentiated by "active_start_date"
#         # take the most recent
#         if ("active_start_date" %in% names(x)) {
#             x <- x %>% 
#                 group_by_at(vars(-active_start_date)) %>% 
#                 filter(active_start_date == max(active_start_date)) %>%
#                 ungroup()
#         }
#         x <- x %>%
#             mutate(Corridor = get_corridor_name(fn),
#                    Filename = fn) %>% 
#             dplyr::select(c("tmc", "road", "direction", "intersection", "state", "county", "zip", "start_latitude", "start_longitude", "end_latitude", "end_longitude", "miles", "road_order", "timezone_name", "type", "country", "Corridor", "Filename"))
#     }) %>% bind_rows()
#     df
# }

# get_det_config_precountpriority <- function(date_) {
#     
#     s3path <- glue('atspm_det_config_good/date={date_}')
#     s3key <- "ATSPM_Det_Config_Good.feather"
#     s3bucket <- "gdot-devices"
#     local_filename <- sub(".feather", glue("_{date_}.feather"), s3key)
#     
#     aws.s3::save_object(object = file.path(s3path, s3key), 
#                         bucket = s3bucket,
#                         file = local_filename)
#     df <- read_feather(local_filename) %>%
#         transmute(SignalID = factor(SignalID), 
#                   Detector = factor(Detector), 
#                   CallPhase = factor(CallPhase),
#                   CallPhase.atspm = as.integer(CallPhase_atspm),
#                   CallPhase.maxtime = as.integer(CallPhase_maxtime),
#                   TimeFromStopBar = TimeFromStopBar,
#                   Date = date(date_))
#     file.remove(local_filename)
#     df
# }

get_ped_config <- function(date_) {
    
    date_ <- max(date_, ymd("2019-01-01"))
    
    s3key <- glue("maxtime_ped_plans/date={date_}/MaxTime_Ped_Plans.csv")
    s3bucket <- "gdot-devices"
    
    if (nrow(aws.s3::get_bucket_df(s3bucket, s3key)) > 0) {
        col_spec <- cols(
            .default = col_character(),
            SignalID = col_character(),
            IP = col_character(),
            PrimaryName = col_character(),
            SecondaryName = col_character(),
            Detector = col_character(),
            CallPhase = col_character())
        
        s3read_using(function(x) read_csv(x, col_types = col_spec), 
                     object = s3key, 
                     bucket = s3bucket) %>%
            transmute(SignalID = factor(SignalID), 
                      Detector = factor(Detector), 
                      CallPhase = factor(CallPhase)) %>%
            distinct()
    } else {
        data.frame()
    }
    
}


get_unique_timestamps <- function(df) {
    df %>% 
        dplyr::select(Timestamp) %>% 
        
        distinct() %>%
        mutate(SignalID = 0) %>%
        dplyr::select(SignalID, Timestamp)
}

get_uptime <- function(df, start_date, end_time) {
    
    signals <- collect(distinct(df, SignalID))$SignalID
    bookend1 <- expand.grid(SignalID = as.integer(signals), Timestamp = ymd_hms(glue("{start_date} 00:00:00")))
    bookend2 <- expand.grid(SignalID = as.integer(signals), Timestamp = ymd_hms(end_time))
    
    
    ts_sig <- df %>% 
        mutate(timestamp = date_trunc('minute', timestamp)) %>%
        distinct(signalid, timestamp) %>%
        collect() %>%
        transmute(SignalID = signalid, Timestamp = ymd_hms(timestamp)) %>%
        
        bind_rows(., bookend1, bookend2) %>%
        distinct() %>%
        arrange(SignalID, Timestamp)
    
    ts_all <- ts_sig %>%
        distinct(Timestamp) %>%
        mutate(SignalID = 0) %>%
        arrange(Timestamp)
    
    uptime <- lapply(list(ts_sig, ts_all), function (x) {
        x %>%
            mutate(Date = date(Timestamp)) %>%
            group_by(SignalID, Date) %>%
            mutate(lag_Timestamp = lag(Timestamp),
                span = as.numeric(Timestamp - lag_Timestamp, units = "mins")) %>% 
            select(-lag_Timestamp) %>%
            drop_na() %>%
            mutate(span = if_else(span > 15, span, 0)) %>%
            
            group_by(SignalID, Date) %>% 
            summarize(uptime = 1 - sum(span, na.rm = TRUE)/(60 * 24)) %>%
            ungroup()
    })
    names(uptime) <- c("sig", "all")
    uptime$all <- uptime$all %>%
        dplyr::select(-SignalID) %>% rename(uptime_all = uptime)
    uptime
}

get_counts <- function(df, det_config, units = "hours", date_, event_code = 82, TWR_only = FALSE) {
    
    if (lubridate::wday(date_, label = TRUE) %in% c("Tue", "Wed", "Thu") || (TWR_only == FALSE)) {
        
        df <- df %>%
            filter(eventcode == event_code)
        
        # Group by hour using Athena/Presto SQL
        if (units == "hours") {
            df <- df %>% 
                group_by(timeperiod = date_trunc('hour', timestamp),
                         signalid,
                         eventparam)
            
        # Group by 15 minute interval using Athena/Presto SQL
        } else if (units == "15min") {
            df <- df %>% 
                mutate(timeperiod = date_trunc('minute', timestamp)) %>%
                group_by(timeperiod = date_add('second',
                                               as.integer(-1 * mod(to_unixtime(timeperiod), 15*60)),
                                               timeperiod),
                         signalid,
                         eventparam)
        }
        
        df <- df %>%
            count() %>%
            ungroup() %>%
            collect() %>%
            transmute(Timeperiod = ymd_hms(timeperiod),
                      SignalID = factor(signalid),
                      Detector = factor(eventparam),
                      vol = as.integer(n)) %>%
            left_join(det_config, by = c("SignalID", "Detector")) %>%
            
            dplyr::select(SignalID, Timeperiod, Detector, CallPhase, vol) %>%
            mutate(SignalID = factor(SignalID),
                   Detector = factor(Detector))
        #tz(df$Timeperiod) <- "America/New_York"
        df
    } else {
        data.frame()
    }
}


get_counts2 <- function(date_, bucket, conf_athena, uptime = TRUE, counts = TRUE) {
    
    #gc()
    
    conn <- get_athena_connection(conf_athena)
    
    start_date <- date_
    end_time <- format(date(date_) + days(1) - seconds(0.1), "%Y-%m-%d %H:%M:%S.9")
    
    if (counts == TRUE) {
        det_config <- get_det_config(start_date) %>%
            transmute(SignalID = factor(SignalID), 
                      Detector = factor(Detector), 
                      CallPhase = factor(CallPhase))
        
        ped_config <- get_ped_config(start_date) %>%
            dplyr::select(SignalID, Detector, CallPhase)
    }
    
    df <- tbl(conn, sql(glue("select distinct * from {conf_athena$database}.{conf_athena$atspm_table}"))) %>%
        filter(date == start_date)
    
    print(head(arrange(df, timestamp)))
    
    if (uptime == TRUE) {
        
        # get uptime$sig, uptime$all
        uptime <- get_uptime(df, start_date, end_time)
        
        
        # Reduce to comm uptime for signals_sublist
        print(glue("Communications uptime {date_}"))
        
        cu <- uptime$sig %>%
            ungroup() %>%
            left_join(uptime$all) %>%
            mutate(SignalID = factor(SignalID),
                   CallPhase = factor(0),
                   uptime = uptime + (1 - uptime_all),
                   Date_Hour = ymd_hms(paste(start_date, "00:00:00")),
                   Date = date(start_date),
                   DOW = wday(start_date), 
                   Week = week(start_date)) %>%
            dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, uptime)
        #tz(cu$Date_Hour) <- "America/New_York"
        
        s3_upload_parquet(cu, date_, 
                          fn = glue("cu_{date_}"), 
                          bucket = bucket,
                          table_name = "comm_uptime",
                          athena_db = conf_athena$database)
    }
    
    if (counts == TRUE) {
        
        counts_1hr_fn <- glue("counts_1hr_{date_}")
        counts_ped_1hr_fn <- glue("counts_ped_1hr_{date_}")
        counts_15min_fn <- glue("counts_15min_TWR_{date_}")
        
        filtered_counts_1hr_fn <- glue("filtered_counts_1hr_{date_}")
        filtered_counts_15min_fn <- glue("filtered_counts_15min_{date_}")
        
        # get 1hr counts
        print("1-hour counts")
        counts_1hr <- get_counts(
            df, 
            det_config, 
            "hours", 
            date_, 
            event_code = 82, 
            TWR_only = FALSE
        ) %>% 
            arrange(SignalID, Detector, Timeperiod)

        s3_upload_parquet(counts_1hr, date_, 
                          fn = counts_1hr_fn, 
                          bucket = bucket,
                          table_name = "counts_1hr", 
                          athena_db = conf_athena$database)
        
        print("1-hr filtered counts")
        if (nrow(counts_1hr) > 0) {
            filtered_counts_1hr <- get_filtered_counts(
                counts_1hr, 
                interval = "1 hour")
            s3_upload_parquet(filtered_counts_1hr, date_, 
                              fn = filtered_counts_1hr_fn, 
                              bucket = bucket,
                              table_name = "filtered_counts_1hr", 
                              athena_db = conf_athena$database)
        # } else { # empty data frame
        #     filtered_counts_1hr <- data.frame(
        #         SignalID = character(),
        #         Timeperiod = as.POSIXct(character()),
        #         Detector = character(),
        #         CallPhase = character(),
        #         vol = numeric(),
        #         delta_vol = numeric(),
        #         Good = numeric(),
        #         mean_abs_delta = numeric(),
        #         Good_Day = integer(),
        #         Month_Hour = as.POSIXct(character()),
        #         Hour = as.POSIXct(character()),
        #         stringsAsFactors = FALSE)
        #     
        }
    
        
        
        # get 1hr ped counts
        print("1-hour pedestrian counts")
        counts_ped_1hr <- get_counts(
            df, 
            ped_config, 
            "hours", 
            date_, 
            event_code = 90, 
            TWR_only = FALSE
        ) %>% 
            arrange(SignalID, Detector, Timeperiod)

        s3_upload_parquet(counts_ped_1hr, date_, 
                          fn = counts_ped_1hr_fn, 
                          bucket = bucket,
                          table_name = "counts_ped_1hr", 
                          athena_db = conf_athena$database)
        
        
        # get 15min counts
        print("15-minute counts")
        counts_15min <- get_counts(
            df, 
            det_config, 
            "15min", 
            date_,
            event_code = 82, 
            TWR_only = FALSE
        ) %>% 
            arrange(SignalID, Detector, Timeperiod)
        
        s3_upload_parquet(counts_15min, date_, 
                          fn = counts_15min_fn, 
                          bucket = bucket,
                          table_name = "counts_15min", 
                          athena_db = conf_athena$database)
        
        # get 15min filtered counts
        print("15-minute filtered counts")
        if (nrow(counts_15min) > 0) {
            filtered_counts_15min <- get_filtered_counts(
                counts_15min, 
                interval = "15 min")
            s3_upload_parquet(filtered_counts_15min, date_, 
                              fn = filtered_counts_15min_fn, 
                              bucket = bucket,
                              table_name = "filtered_counts_15min", 
                              athena_db = conf_athena$database)
        }
        
    }
    
    dbDisconnect(conn)
    #gc()
}

# revised version of get_filtered_counts.
# get_filtered_counts_precountpriority <- function(counts, interval = "1 hour") { # interval (e.g., "1 hour", "15 min")
#     
#     counts <- counts %>%
#         mutate(SignalID = factor(SignalID),
#                Detector = factor(Detector),
#                CallPhase = factor(CallPhase)) %>%
#         filter(!is.na(CallPhase))
#     
#     # Identify detectors/phases from detector config file. Expand.
#     #  This ensures all detectors are included in the bad detectors calculation.
#     all_days <- unique(date(counts$Timeperiod))
#     det_config <- lapply(all_days, function(d) {
#         all_timeperiods <- seq(ymd_hms(paste(d, "00:00:00")), ymd_hms(paste(d, "23:59:00")), by = interval)
#         get_det_config(d) %>%
#         #read_feather(glue("ATSPM_Det_Config_Good_{d}.feather")) %>% 
#         #    transmute(SignalID = factor(SignalID), 
#         #              Detector = factor(Detector), 
#         #              CallPhase = factor(CallPhase)) %>% 
#             expand(nesting(SignalID, Detector, CallPhase), Timeperiod = all_timeperiods)
#     }) %>% bind_rows() %>%
#         transmute(SignalID = factor(SignalID),
#                   Timeperiod = Timeperiod,
#                   Detector = factor(Detector),
#                   CallPhase = factor(CallPhase)) 
#     
#     
#     expanded_counts <- full_join(det_config, counts) %>%
#         transmute(SignalID = factor(SignalID), 
#                   Date = date(Timeperiod),
#                   Timeperiod = Timeperiod,
#                   Detector = factor(Detector), 
#                   CallPhase = factor(CallPhase),
#                   vol = as.double(vol),
#                   vol0 = if_else(is.na(vol), 0.0, vol)) %>%
#         group_by(SignalID, CallPhase, Detector) %>% 
#         arrange(SignalID, CallPhase, Detector, Timeperiod) %>% 
#         mutate(delta_vol = vol0 - lag(vol0),
#                Good = ifelse(is.na(vol) | 
#                                  vol > 1000 | 
#                                  is.na(delta_vol) | 
#                                  abs(delta_vol) > 500 | 
#                                  abs(delta_vol) == 0,
#                              0, 1)) %>%
#         dplyr::select(-vol0)
#     
#     # bad day = any of the following:
#     #    too many bad hours (60%) based on the above criteria
#     #    mean absolute change in hourly volume > 200 
#     bad_days <- expanded_counts %>% 
#         filter(hour(Timeperiod) >= 5) %>%
#         group_by(SignalID, CallPhase, Detector, Date = date(Timeperiod)) %>% 
#         summarize(Good = sum(Good, na.rm = TRUE), 
#                   All = n(), 
#                   Pct_Good = as.integer(sum(Good, na.rm = TRUE)/n()*100),
#                   mean_abs_delta = mean(abs(delta_vol), na.rm = TRUE)) %>% 
#         
#         # manually calibrated
#         mutate(Good_Day = as.integer(ifelse(Pct_Good >= 70 & mean_abs_delta < 200, 1, 0))) %>%
#         dplyr::select(SignalID, CallPhase, Detector, Date, mean_abs_delta, Good_Day)
#     
#     # counts with the bad days taken out
#     filtered_counts <- left_join(expanded_counts, bad_days) %>%
#         mutate(vol = if_else(Good_Day==1, vol, as.double(NA)),
#                Month_Hour = Timeperiod - days(day(Timeperiod) - 1),
#                Hour = Month_Hour - months(month(Month_Hour) - 1))
#     
#     filtered_counts
# }


multicore_decorator <- function(FUN) {
    
    usable_cores <- get_usable_cores()
    
    function(x) {
        x %>% 
            split(.$SignalID) %>% 
            mclapply(FUN, mc.cores = usable_cores) %>% #floor(parallel::detectCores()*3/4)) %>%
            bind_rows()
    }
}
# get_adjusted_counts_precountpriority <- function(filtered_counts) {
#     
#     usable_cores <- get_usable_cores()
#     
#     filtered_counts %>%
#         split(.$SignalID) %>% mclapply(function(fc) {
#             fc <- fc %>% 
#                 mutate(DOW = wday(Timeperiod),
#                        vol = as.double(vol))
#             
#             ph_contr <- fc %>%
#                 group_by(SignalID, CallPhase, Timeperiod) %>% 
#                 mutate(na.vol = sum(is.na(vol))) %>%
#                 ungroup() %>% 
#                 filter(na.vol == 0) %>% 
#                 dplyr::select(-na.vol) %>% 
#                 
#                 # phase contribution factors--fraction of phase volume a detector contributes
#                 group_by(SignalID, CallPhase, Detector) %>% 
#                 summarize(vol = sum(vol, na.rm = TRUE)) %>% 
#                 group_by(SignalID, CallPhase) %>% 
#                 mutate(Ph_Contr = vol/sum(vol, na.rm = TRUE)) %>% 
#                 dplyr::select(-vol) %>% ungroup()
#             
#             # fill in missing detectors from other detectors on that phase
#             fc_phc <- left_join(fc, ph_contr, by = c("SignalID", "CallPhase", "Detector")) %>%
#                 # fill in missing detectors from other detectors on that phase
#                 group_by(SignalID, Timeperiod, CallPhase) %>%
#                 mutate(mvol = mean(vol/Ph_Contr, na.rm = TRUE)) %>% ungroup()
#             
#             fc_phc$vol[is.na(fc_phc$vol)] <- as.integer(fc_phc$mvol[is.na(fc_phc$vol)] * fc_phc$Ph_Contr[is.na(fc_phc$vol)])
#             
#             #hourly volumes over the month to fill in missing data for all detectors in a phase
#             mo_hrly_vols <- fc_phc %>%
#                 group_by(SignalID, CallPhase, Detector, DOW, Month_Hour) %>% 
#                 summarize(Hourly_Volume = median(vol, na.rm = TRUE)) %>%
#                 ungroup()
#             # SignalID | Call.Phase | Detector | Month_Hour | Volume(median)
#             
#             # fill in missing detectors by hour and day of week volume in the month
#             left_join(fc_phc, mo_hrly_vols, by = (c("SignalID", "CallPhase", "Detector", "DOW", "Month_Hour"))) %>% 
#                 ungroup() %>%
#                 mutate(vol = if_else(is.na(vol), as.integer(Hourly_Volume), as.integer(vol))) %>%
#                 
#                 dplyr::select(SignalID, CallPhase, Detector, Timeperiod, vol) %>%
#                 
#                 filter(!is.na(vol))
#         }, mc.cores = usable_cores) %>% bind_rows() #ceiling(parallel::detectCores()*1/3)
# }
# get_adjusted_counts_single_threaded <- function(filtered_counts) {
#     
#     filtered_counts <- mutate(filtered_counts, DOW = wday(Timeperiod))
#     
#     
#     fc <- filtered_counts %>% 
#         group_by(SignalID, CallPhase, Timeperiod) %>% 
#         mutate(na.vol = sum(is.na(vol))) %>%
#         ungroup() %>% 
#         filter(na.vol == 0) %>% 
#         dplyr::select(-na.vol)
#     
#     # phase contribution factors--fraction of phase volume a detector contributes
#     ph_contr <- fc %>% 
#         group_by(SignalID, CallPhase, Detector) %>% 
#         summarize(vol = sum(vol, na.rm = TRUE)) %>% 
#         group_by(SignalID, CallPhase) %>% 
#         mutate(Ph_Contr = vol/sum(vol, na.rm = TRUE)) %>% 
#         dplyr::select(-vol) %>% ungroup()
#     
#     # SignalID | CallPhase | Detector | Ph_Contr
#     
#     
#     # fill in missing detectors from other detectors on that phase
#     fc_phc <- left_join(filtered_counts, ph_contr) %>% 
#         # fill in missing detectors from other detectors on that phase
#         group_by(SignalID, Timeperiod, CallPhase) %>%
#         mutate(mvol = mean(vol/Ph_Contr, na.rm = TRUE)) %>% ungroup()
#     
#     fc_phc$vol[is.na(fc_phc$vol)] <- as.integer(fc_phc$mvol[is.na(fc_phc$vol)] * fc_phc$Ph_Contr[is.na(fc_phc$vol)])
#     
#     #hourly volumes over the month to fill in missing data for all detectors in a phase
#     mo_hrly_vols <- fc_phc %>%
#         group_by(SignalID, CallPhase, Detector, DOW, Month_Hour) %>% 
#         summarize(Hourly_Volume = median(vol, na.rm = TRUE)) %>%
#         ungroup()
#     # SignalID | Call.Phase | Detector | Month_Hour | Volume(median)
#     
#     # fill in missing detectors by hour and day of week volume in the month
#     left_join(fc_phc, mo_hrly_vols) %>%
#         ungroup() %>%
#         mutate(vol = if_else(is.na(vol), as.integer(Hourly_Volume), as.integer(vol))) %>%
#         
#         dplyr::select(SignalID, CallPhase, Detector, Timeperiod, vol) %>%
#         
#         filter(!is.na(vol))
#     
#     # SignalID | CallPhase | Timeperiod | Detector | vol 
# }


## -- --- Adds CountPriority from detector config file --------------------- -- ##
## This determines which detectors to use for counts when there is more than
## one detector in a lane, such as for video, Gridsmart and Wavetronix Matrix
get_det_config_prev <- function(date_) {
    
    read_det_config <- function(s3object, s3bucket) {
        aws.s3::s3read_using(read_feather, object = s3object, bucket = s3bucket)
    }
    
    s3bucket <- conf$bucket 
    s3object = glue("atspm_det_config_good/date={date_}/ATSPM_Det_Config_Good.feather")
    
    # Are there any files for this date?
    s3objects <- aws.s3::get_bucket_df(
        bucket = s3bucket, 
        prefix = dirname(s3object))
    
    # If the s3 object exists, read it and return the data frame
    if (nrow(aws.s3::get_bucket_df(s3bucket, s3object)) > 0) {
        read_det_config(s3object, s3bucket) %>%
            mutate(SignalID = as.character(SignalID),
                   Detector = as.integer(Detector), 
                   CallPhase = as.integer(CallPhase))
    
    # If the s3 object does not exist, but where there are objects for this date,
    # read all files and bind rows (for when multiple ATSPM databases are contributing)
    } else if (nrow(s3objects) > 0) {
        lapply(s3objects$Key, function(x) {read_det_config(x, s3bucket)})  %>%
            rbindlist() %>% as_tibble() %>%
            mutate(SignalID = as.character(SignalID),
                   Detector = as.integer(Detector), 
                   CallPhase = as.integer(CallPhase))
    } else {
        stop(glue("No detector config file for {date_}"))
    }
}

# This is a "function factory" 
# It is meant to be used to create a get_det_config function that takes only the date:
# like: get_det_config <- get_det_config_(conf$bucket)
get_det_config_  <- function(bucket) { 

    function(date_) {
        read_det_config <- function(s3object, s3bucket) {
            aws.s3::s3read_using(read_feather, object = s3object, bucket = s3bucket)
        }
        
        s3bucket <- bucket 
        s3object = glue("atspm_det_config_good/date={date_}/ATSPM_Det_Config_Good.feather")
        
        # Are there any files for this date?
        s3objects <- aws.s3::get_bucket_df(
            bucket = s3bucket, 
            prefix = dirname(s3object))
        
        # If the s3 object exists, read it and return the data frame
        if (nrow(aws.s3::get_bucket_df(s3bucket, s3object)) > 0) {
            read_det_config(s3object, s3bucket) %>%
                mutate(SignalID = as.character(SignalID),
                       Detector = as.integer(Detector), 
                       CallPhase = as.integer(CallPhase))
        
        # If the s3 object does not exist, but where there are objects for this date,
        # read all files and bind rows (for when multiple ATSPM databases are contributing)
        } else if (nrow(s3objects) > 0) {
            lapply(s3objects$Key, function(x) {read_det_config(x, s3bucket)})  %>%
                rbindlist() %>% as_tibble() %>%
                mutate(SignalID = as.character(SignalID),
                       Detector = as.integer(Detector), 
                       CallPhase = as.integer(CallPhase))
        } else {
            stop(glue("No detector config file for {date_}"))
        }
    }
}

get_det_config  <- get_det_config_(conf$bucket)

get_det_config_aog <- function(date_) {
    
    get_det_config(date_) %>%
        filter(!is.na(Detector)) %>%
        mutate(AOGPriority = 
                   dplyr::case_when(
                       grepl("Exit", DetectionTypeDesc)  ~ 0,
                       grepl("Advanced Count", DetectionTypeDesc) ~ 1,
                       TRUE ~ 2)) %>%
        filter(AOGPriority < 2) %>%
        group_by(SignalID, CallPhase) %>% 
        filter(AOGPriority == min(AOGPriority)) %>%
        ungroup() %>%
        
        transmute(SignalID = factor(SignalID), 
                  Detector = factor(Detector), 
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))
}

get_det_config_qs <- function(date_) {
    
    get_det_config(date_) %>%
        filter(grepl("Advanced Count", DetectionTypeDesc)) %>%
        filter(!is.na(Detector)) %>%
        
        transmute(SignalID = factor(SignalID), 
                  Detector = factor(Detector), 
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))
}

get_det_config_sf <- function(date_) {
    
    get_det_config(date_) %>%
        filter(grepl("Stop Bar Presence", DetectionTypeDesc)) %>%
        filter(!is.na(Detector)) %>%
        
        
        transmute(SignalID = factor(SignalID), 
                  Detector = factor(Detector), 
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))
}

get_det_config_vol <- function(date_) {
    
    get_det_config(date_) %>%
        transmute(SignalID = factor(SignalID), 
                  Detector = factor(Detector), 
                  CallPhase = factor(CallPhase),
                  CountPriority = as.integer(CountPriority),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_)) %>%
        group_by(SignalID, CallPhase) %>%
        mutate(minCountPriority = min(CountPriority, na.rm = TRUE)) %>%
        ungroup() %>%
        filter(CountPriority == minCountPriority)
}


# Multidplyr. Uses a local cluster to partition and parallelize
# interval (e.g., "1 hour", "15 min")
# get_filtered_counts <- function(counts, interval = "1 hour") {
#     
#     if (interval == "1 hour") {
#         max_volume <- 1000
#         max_delta <- 500
#         max_abs_delta <- 200
#     } else if (interval == "15 min") {
#         max_volume <- 250
#         max_delta <- 125
#         max_abs_delta <- 50
#     } else {
#         stop("interval must be '1 hour' or '15 min'")
#     }
#     
#     counts <- counts %>%
#         ungroup() %>%
#         mutate(SignalID = factor(SignalID),
#                Detector = factor(Detector),
#                CallPhase = factor(CallPhase)) %>%
#         filter(!is.na(CallPhase))
#     
#     # Identify detectors/phases from detector config file. Expand.
#     #  This ensures all detectors are included in the bad detectors calculation.
#     all_days <- unique(date(counts$Timeperiod))
#     det_config <- lapply(all_days, function(d) {
#         all_timeperiods <- seq(ymd_hms(paste(d, "00:00:00")), 
#                                ymd_hms(paste(d, "23:59:00")), 
#                                by = interval)
#         #tz(all_timeperiods) <- "America/New_York"
#         get_det_config(d) %>%
#             expand(nesting(SignalID, Detector, CallPhase), 
#                    Timeperiod = all_timeperiods)
#     }) %>% bind_rows() %>%
#         transmute(SignalID = factor(SignalID),
#                   Timeperiod = Timeperiod,
#                   Detector = factor(Detector),
#                   CallPhase = factor(CallPhase)) 
#     
#     
#     expanded_counts <- full_join(det_config, counts) %>%
#         transmute(SignalID = factor(SignalID), 
#                   Date = date(Timeperiod),
#                   Timeperiod = Timeperiod,
#                   Detector = factor(Detector), 
#                   CallPhase = factor(CallPhase),
#                   vol = as.double(vol),
#                   vol0 = if_else(is.na(vol), 0.0, vol)) %>%
#         arrange(SignalID, CallPhase, Detector, Timeperiod)
#     
#     cluster <- create_cluster(min(2, usable_cores))
#     set_default_cluster(cluster)
#     
#     ec <- partition(expanded_counts, SignalID, CallPhase, Detector)
#     cluster_assign_value(cluster, "max_volume", max_volume)
#     cluster_assign_value(cluster, "max_delta", max_delta)
#     cluster_assign_value(cluster, "max_abs_delta", max_abs_delta)
#     cluster_library(cluster, list("lubridate", "dplyr"))
#     
#     ec <- ec %>% 
#         mutate(delta_vol = vol0 - lag(vol0),
#                Good = ifelse(is.na(vol) | 
#                                  vol > max_volume | 
#                                  is.na(delta_vol) | 
#                                  abs(delta_vol) > max_delta | 
#                                  abs(delta_vol) == 0,
#                              0, 1)) %>%
#         dplyr::select(-vol0)
#     
#     expanded_counts <- ec %>% 
#         collect() %>%
#         ungroup()
#     
#     # bad day = any of the following:
#     #    too many bad hours (60%) based on the above criteria
#     #    mean absolute change in hourly volume > 200 
#     bad_days <- expanded_counts  %>%
#         filter(hour(Timeperiod) >= 5) %>%
#         partition(SignalID, CallPhase, Detector, Date) %>% 
#         summarize(Good = sum(Good, na.rm = TRUE), 
#                   All = n(), 
#                   Pct_Good = as.integer(sum(Good, na.rm = TRUE)/n()*100),
#                   mean_abs_delta = mean(abs(delta_vol), na.rm = TRUE)) %>% 
#         collect() %>%
#         ungroup() %>%
#         
#         # manually calibrated
#         mutate(Good_Day = as.integer(ifelse(Pct_Good >= 70 & mean_abs_delta < max_abs_delta, 1, 0))) %>%
#         dplyr::select(SignalID, CallPhase, Detector, Date, mean_abs_delta, Good_Day)
#     
#     
#     # counts with the bad days taken out
#     filtered_counts <- left_join(expanded_counts, bad_days) %>%
#         mutate(vol = if_else(Good_Day==1, vol, as.double(NA)),
#                Month_Hour = Timeperiod - days(day(Timeperiod) - 1),
#                Hour = Month_Hour - months(month(Month_Hour) - 1)) %>%
#         ungroup()
#     
#     filtered_counts
# }

# Single threaded
get_filtered_counts <- function(counts, interval = "1 hour") { # interval (e.g., "1 hour", "15 min")

    if (interval == "1 hour") {
        max_volume <- 1000
        max_delta <- 500
        max_abs_delta <- 200
    } else if (interval == "15 min") {
        max_volume <- 250
        max_delta <- 125
        max_abs_delta <- 50
    } else {
        stop("interval must be '1 hour' or '15 min'")
    }

    counts <- counts %>%
        mutate(SignalID = factor(SignalID),
               Detector = factor(Detector),
               CallPhase = factor(CallPhase)) %>%
        filter(!is.na(CallPhase))

    # Identify detectors/phases from detector config file. Expand.
    #  This ensures all detectors are included in the bad detectors calculation.
    all_days <- unique(date(counts$Timeperiod))
    det_config <- lapply(all_days, function(d) {
        all_timeperiods <- seq(ymd_hms(paste(d, "00:00:00")), ymd_hms(paste(d, "23:59:00")), by = interval)
        get_det_config(d) %>%
            expand(nesting(SignalID, Detector, CallPhase),
                   Timeperiod = all_timeperiods)
    }) %>% bind_rows() %>%
        transmute(SignalID = factor(SignalID),
                  Timeperiod = Timeperiod,
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase))


    expanded_counts <- full_join(det_config, counts) %>%
        transmute(SignalID = factor(SignalID),
                  Date = date(Timeperiod),
                  Timeperiod = Timeperiod,
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  vol = as.double(vol),
                  vol0 = if_else(is.na(vol), 0.0, vol)) %>%
        group_by(SignalID, CallPhase, Detector) %>%
        arrange(SignalID, CallPhase, Detector, Timeperiod) %>%

        mutate(delta_vol = vol0 - lag(vol0)) %>%
        ungroup() %>%
        mutate(Good = ifelse(is.na(vol) |
                                 vol > max_volume |
                                 is.na(delta_vol) |
                                 abs(delta_vol) > max_delta |
                                 abs(delta_vol) == 0,
                             0, 1)) %>%
        dplyr::select(-vol0) %>%
        ungroup()


    # bad day = any of the following:
    #    too many bad hours (60%) based on the above criteria
    #    mean absolute change in hourly volume > 200
    bad_days <- expanded_counts %>%
        filter(hour(Timeperiod) >= 5) %>%
        group_by(SignalID, CallPhase, Detector, Date = date(Timeperiod)) %>%
        summarize(Good = sum(Good, na.rm = TRUE),
                  All = n(),
                  Pct_Good = as.integer(sum(Good, na.rm = TRUE)/n()*100),
                  mean_abs_delta = mean(abs(delta_vol), na.rm = TRUE)) %>%
        ungroup() %>%

        # manually calibrated
        mutate(Good_Day = as.integer(ifelse(Pct_Good >= 70 & mean_abs_delta < max_abs_delta, 1, 0))) %>%
        dplyr::select(SignalID, CallPhase, Detector, Date, mean_abs_delta, Good_Day)

    # counts with the bad days taken out
    filtered_counts <- left_join(expanded_counts, bad_days) %>%
        mutate(vol = if_else(Good_Day==1, vol, as.double(NA)),
               Month_Hour = Timeperiod - days(day(Timeperiod) - 1),
               Hour = Month_Hour - months(month(Month_Hour) - 1)) %>%
        ungroup()

    filtered_counts
}


get_adjusted_counts <- function(filtered_counts) {
    
    usable_cores <- get_usable_cores()
    
    det_config <- lapply(unique(filtered_counts$Date), get_det_config_vol) %>% 
        bind_rows() %>%
        mutate(SignalID = factor(SignalID))
   
    filtered_counts %>%
        left_join(det_config) %>%
        filter(!is.na(CountPriority)) %>%
    
        split(.$SignalID) %>% mclapply(function(fc) {
            fc <- fc %>% 
                mutate(DOW = wday(Timeperiod),
                       vol = as.double(vol))
            
            ph_contr <- fc %>%
                group_by(SignalID, CallPhase, Timeperiod) %>% 
                mutate(na.vol = sum(is.na(vol))) %>%
                ungroup() %>% 
                filter(na.vol == 0) %>% 
                dplyr::select(-na.vol) %>% 
                
                # phase contribution factors--fraction of phase volume a detector contributes
                group_by(SignalID, CallPhase, Detector) %>% 
                summarize(vol = sum(vol, na.rm = TRUE)) %>% 
                group_by(SignalID, CallPhase) %>% 
                mutate(sum_vol = sum(vol, na.rm = TRUE),
                       Ph_Contr = vol/sum_vol) %>% 
                dplyr::select(-vol, -sum_vol) %>% ungroup()
            
            # fill in missing detectors from other detectors on that phase
            fc_phc <- left_join(fc, ph_contr, by = c("SignalID", "CallPhase", "Detector")) %>%
                # fill in missing detectors from other detectors on that phase
                group_by(SignalID, Timeperiod, CallPhase) %>%
                mutate(mvol = mean(vol/Ph_Contr, na.rm = TRUE)) %>% ungroup()
            
            fc_phc$vol[is.na(fc_phc$vol)] <- as.integer(fc_phc$mvol[is.na(fc_phc$vol)] * fc_phc$Ph_Contr[is.na(fc_phc$vol)])
            
            #hourly volumes over the month to fill in missing data for all detectors in a phase
            mo_hrly_vols <- fc_phc %>%
                group_by(SignalID, CallPhase, Detector, DOW, Month_Hour) %>% 
                summarize(Hourly_Volume = median(vol, na.rm = TRUE)) %>%
                ungroup()
            # SignalID | Call.Phase | Detector | Month_Hour | Volume(median)
            
            # fill in missing detectors by hour and day of week volume in the month
            left_join(fc_phc, 
                      mo_hrly_vols, 
                      by = (c("SignalID", "CallPhase", "Detector", "DOW", "Month_Hour"))) %>% 
                ungroup() %>%
                mutate(vol = if_else(is.na(vol), as.integer(Hourly_Volume), as.integer(vol))) %>%
                
                dplyr::select(SignalID, CallPhase, Detector, Timeperiod, vol) %>%
                
                filter(!is.na(vol))
        }, mc.cores = usable_cores) %>% bind_rows() #ceiling(parallel::detectCores()*1/3)
}
## -- --- End of Adds CountPriority from detector config file -------------- -- ##


get_spm_data <- function(start_date, end_date, signals_list, conf_atspm, table, TWR_only=TRUE) {
    
    conn <- get_atspm_connection(conf_atspm)
    
    if (TWR_only==TRUE) {
        query_where <- "WHERE DATEPART(dw, CycleStart) in (3,4,5)"
    } else {
        query_where <- ""
    }
    
    query <- paste("SELECT * FROM", table, query_where)
    
    df <- tbl(conn, sql(query)) 
    
    end_date1 <- as.character(ymd(end_date) + days(1))
    
    dplyr::filter(df, CycleStart >= start_date & CycleStart < end_date1 &
                      SignalID %in% signals_list)
}
get_spm_data_aws <- function(start_date, end_date, signals_list, conf_athena, table, TWR_only=TRUE) {
    
    conn <- get_athena_connection(conf_athena)
    
    if (TWR_only == TRUE) {
        query_where <- "WHERE date_format(date_parse(date, '%Y-%m-%d'), '%W') in ('Tuesday','Wednesday','Thursday')"
    } else {
        query_where <- ""
    }
    
    #query <- paste("SELECT * FROM", paste0(conf_athena$database, ".", tolower(table)), query_where)
    query <- glue("SELECT * FROM {conf_athena$database}.{tolower(table)} {query_where}")
    
    df <- tbl(conn, sql(query))
    
    end_date1 <- ymd(end_date) + days(1)
    
    #signals_list <- as.integer(signals_list)
    
    dplyr::filter(df, date >= start_date & date < end_date1) # &
    #signalid %in% signals_list)
}
# Query Cycle Data
get_cycle_data <- function(start_date, end_date, conf_athena, signals_list = NULL) {
    get_spm_data_aws(
        start_date, 
        end_date, 
        signals_list, 
        conf_athena, 
        table = "CycleData", 
        TWR_only = FALSE)
}
# Query Detection Events
get_detection_events <- function(start_date, end_date, conf_athena, signals_list = NULL) {
    get_spm_data_aws(
        start_date, 
        end_date, 
        signals_list, 
        conf_athena, 
        table = "DetectionEvents", 
        TWR_only = FALSE)
}

get_detector_uptime <- function(filtered_counts_1hr) {
    filtered_counts_1hr %>%
        ungroup() %>%
        distinct(Date, SignalID, Detector, Good_Day) %>%
        complete(nesting(SignalID, Detector), Date = seq(min(Date), max(Date), by="day")) %>%
        arrange(Date, SignalID, Detector)
}

get_bad_detectors <- function(filtered_counts_1hr) {
    get_detector_uptime(filtered_counts_1hr) %>%
        filter(Good_Day == 0)
}

get_bad_ped_detectors <- function(pau) {
    pau %>% 
        filter(uptime == 0) %>%
        dplyr::select(SignalID, CallPhase, Detector, Date)
}

# Volume VPD
get_vpd <- function(counts, mainline_only = TRUE) {
    
    if (mainline_only == TRUE) {
        counts <- counts %>%
            filter(CallPhase %in% c(2,6)) # sum over Phases 2,6 # added 4/24/18
    }
    counts %>%
        mutate(DOW = wday(Timeperiod), 
               Week = week(date(Timeperiod)),
               Date = date(Timeperiod)) %>% 
        group_by(SignalID, CallPhase, Week, DOW, Date) %>% 
        summarize(vpd = sum(vol, na.rm = TRUE))
    
    # SignalID | CallPhase | Week | DOW | Date | vpd
}

# SPM Throughput
get_thruput <- function(counts) {
    
    counts %>%
        mutate(DOW = wday(Date), 
               Week = week(Date)) %>%
        group_by(SignalID, Week, DOW, Date, Timeperiod) %>%
        summarize(vph = sum(vol, na.rm = TRUE)) %>%
        
        group_by(SignalID, Week, DOW, Date) %>%
        summarize(vph = quantile(vph, probs=c(0.95), na.rm = TRUE) * 4) %>%
        ungroup() %>%
        
        arrange(SignalID, Date) %>%
        mutate(CallPhase = 0) %>%
        dplyr::select(SignalID, CallPhase, Date, Week, DOW, vph)
    
    # SignalID | CallPhase | Date | Week | DOW | vph
}



# SPM Arrivals on Green -- modified for use with dbplyr on AWS Athena
get_aog <- function(cycle_data) {

    # volumes in cycle_data use count detectors only
    
    df <- cycle_data %>%
        filter(Phase %in% c(2,6)) %>%
        group_by(SignalID, Phase, CycleStart, Duration, EventCode) %>% 
        summarize(Volume = sum(Volume, na.rm = TRUE)) %>% 
        group_by(SignalID, Phase, CycleStart) %>% 
        mutate(Total_Volume = sum(Volume, na.rm = TRUE),
               Total_Duration = sum(Duration, na.rm = TRUE),
               CallPhase = Phase) %>% 
        filter(EventCode == 1) %>%
        
        
        group_by(SignalID, CallPhase, 
                 Hour = date_trunc('hour', CycleStart)) %>%
        summarize(vol = sum(Total_Volume, na.rm = TRUE),
                  aog = sum(Volume, na.rm = TRUE)/pmax(1, sum(Total_Volume, na.rm = TRUE)),
                  gC = sum(Duration, na.rm = TRUE)/pmax(1, sum(Total_Duration, na.rm = TRUE))) %>%
        ungroup() %>%
        mutate(pr = aog/gC) %>%
        
        collect %>% 
        
        mutate(SignalID = factor(SignalID),
               vol = as.integer(vol),
               CallPhase = factor(CallPhase),
               Date_Hour = lubridate::ymd_hms(Hour),
               Date = date(Date_Hour),
               DOW = wday(Date), 
               Week = week(Date)) %>%

        dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, aog, pr, vol)
}

# get_aog_older_no_progression_ratio <- function(cycle_data) {
#     
#     df <- cycle_data %>% 
#         filter(Phase %in% c(2,6)) %>%
#         group_by(SignalID, Phase, CycleStart, EventCode) %>% 
#         summarize(Volume = sum(Volume, na.rm = TRUE)) %>% 
#         group_by(SignalID, Phase, CycleStart) %>% 
#         mutate(Total_Volume = sum(Volume, na.rm = TRUE),
#                Total_Volume = ifelse(Total_Volume == 0, 1, Total_Volume),
#                CallPhase = Phase) %>% 
#         filter(EventCode == 1) %>%
#         
#         
#         group_by(SignalID, CallPhase, 
#                  Hour = date_trunc('hour', CycleStart)) %>%
#         summarize(vol = sum(Total_Volume, na.rm = TRUE),
#                   aog = sum(Volume, na.rm = TRUE)/sum(Total_Volume, na.rm = TRUE)) %>%
#         collect %>% 
#         ungroup() %>%
#         
#         mutate(SignalID = factor(SignalID),
#                vol = as.integer(vol),
#                CallPhase = factor(CallPhase),
#                Date_Hour = lubridate::ymd_hms(Hour),
#                Date = date(Date_Hour),
#                DOW = wday(Date), 
#                Week = week(Date)) %>%
#         #ungroup() %>%
#         dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, aog, vol)
#     
#     # SignalID | CallPhase | Date_Hour | Date | Hour | aog | vol
# }

get_daily_aog <- function(aog) {
    
    aog %>%
        ungroup() %>%
        mutate(DOW = wday(Date), 
               Week = week(Date),
               CallPhase = factor(CallPhase)) %>%
        filter(DOW %in% c(TUE,WED,THU) & (hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))) %>%
        group_by(SignalID, CallPhase, Date, Week, DOW) %>%
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE), vol = sum(vol, na.rm = TRUE)) %>%
        ungroup()
    
    # SignalID | CallPhase | Date | Week | DOW | aog | vol
}

get_daily_pr <- function(aog) {
    
    aog %>%
        ungroup() %>%
        mutate(DOW = wday(Date), 
               Week = week(Date),
               CallPhase = factor(CallPhase)) %>%
        #filter(DOW %in% c(TUE,WED,THU) & (hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))) %>%
        group_by(SignalID, CallPhase, Date, Week, DOW) %>%
        summarize(pr = weighted.mean(pr, vol, na.rm = TRUE), vol = sum(vol, na.rm = TRUE)) %>%
        ungroup()
    
    # SignalID | CallPhase | Date | Week | DOW | pr | vol
}

# SPM Arrivals on Green using Utah method -- modified for use with dbplyr on AWS Athena
get_sf_utah <- function(cycle_data, detection_events, first_seconds_of_red = 5) {
    
    de <- detection_events %>% 
        filter(phase %in% c(3, 4, 7, 8)) %>%
        select(signalid,
               phase,
               detector,
               cyclestart,
               dettimestamp,
               detduration,
               date) %>%
        arrange(signalid, phase, cyclestart) %>%
        
        collect() %>% 

        transmute(signalid = factor(signalid),
                  phase = factor(phase), 
                  detector = factor(detector), 
                  cyclestart = ymd_hms(cyclestart),
                  deton = ymd_hms(dettimestamp),
                  detoff = ymd_hms(dettimestamp) + seconds(detduration),
                  date = ymd(date)) 
    
    # dates <- seq(min(de$date), max(de$date), by = "1 day")
    dates <- unique(de$date)

    dc <- lapply(dates, get_det_config_sf) %>% 
        bind_rows %>% 
        rename(signalid = SignalID, detector = Detector, phase = CallPhase, date = Date)
    
    de <- de %>%
        left_join(dc, by = c("signalid", "phase", "detector", "date")) %>% 
        filter(!is.na(TimeFromStopBar)) %>%
        mutate(signalid = factor(signalid),
               phase = factor(phase),
               detector = factor(detector))
    
    print(head(de))
    
    # cd <- cycle_data %>% 
    #     filter(phase %in% c(3, 4, 7, 8)) %>%
    #     arrange(signalid, phase, cyclestart, phasestart)
        

    cd <- cycle_data %>% 
        filter(phase %in% c(3, 4, 7, 8),
               eventcode %in% c(1,9)) %>%
        select(signalid, 
               phase, 
               cyclestart, 
               phasestart, 
               phaseend,
               date,
               eventcode) %>% 
        arrange(signalid, phase, cyclestart, phasestart) %>%
        collect()
    
    grn_interval <- cd %>% 
        filter(eventcode == 1) %>%
        mutate(signalid = factor(signalid),
               phase = factor(phase),
               cyclestart = ymd_hms(cyclestart),
               intervalstart = ymd_hms(phasestart),
               intervalend = ymd_hms(phaseend),
               date = ymd(date)) %>%
        select(-eventcode)
    
    sor_interval <- cd %>%
        filter(eventcode == 9) %>%
        mutate(signalid = factor(signalid),
               phase = factor(phase),
               cyclestart = ymd_hms(cyclestart),
               intervalstart = ymd_hms(phasestart),
               intervalend = ymd_hms(intervalstart) + seconds(first_seconds_of_red),
               date = ymd(date)) %>%
        select(-eventcode)
    rm(cd) 
    
    cat('.')
   
    
    de_dt <- data.table(de)
    rm(de)
    gc()
    gr_dt <- data.table(grn_interval)
    sr_dt <- data.table(sor_interval)
    
    setkey(de_dt, signalid, phase, deton, detoff)
    setkey(gr_dt, signalid, phase, intervalstart, intervalend)
    setkey(sr_dt, signalid, phase, intervalstart, intervalend)
    
    ## ---
    
    get_occupancy <- function(de_dt, int_dt, interval) {
        occdf <- foverlaps(de_dt, int_dt, type = "any") %>% 
            filter(!is.na(intervalstart)) %>% 
            
            mutate(signalid = factor(signalid),
                   detector = as.integer(as.character(detector)),
                   int_int = interval(intervalstart, intervalend), 
                   occ_int = interval(deton, detoff), 
                   occ_duration = as.duration(intersect(occ_int, int_int)),
                   int_duration = as.duration(int_int)) %>%
            
            dplyr::select(signalid, phase, detector, cyclestart,
                          intervalstart, intervalend,
                          int_int,
                          occ_int,
                          occ_duration, int_duration)
        
        
        occdf <- full_join(interval, 
                           occdf, 
                           by = c("signalid", "phase", 
                                  "cyclestart", "intervalstart", "intervalend")) %>% 
            tidyr::replace_na(list(detector = 0, occ_duration = 0, int_duration = 1)) %>%
            mutate(signalid = factor(signalid),
                   detector = factor(detector),
                   occ_duration = as.numeric(occ_duration),
                   int_duration = as.numeric(int_duration)) %>%
            
            group_by(signalid, phase, detector, cyclestart) %>%
            summarize(occ = sum(occ_duration)/max(int_duration)) %>%
            
            group_by(signalid, phase, cyclestart) %>%
            summarize(occ = max(occ)) %>%
            ungroup() %>%
            
            mutate(signalid = factor(signalid),
                   phase = factor(phase))
        
        
        occdf
    }
    
    
    grn_occ <- get_occupancy(de_dt, gr_dt, grn_interval) %>% 
        rename(gr_occ = occ)
    cat('.')
    sor_occ <- get_occupancy(de_dt, sr_dt, sor_interval) %>% 
        rename(sr_occ = occ)
    cat('.')

    
    
    df <- full_join(grn_occ, sor_occ, by = c("signalid", "phase", "cyclestart")) %>%
        mutate(sf = if_else((gr_occ > 0.80) & (sr_occ > 0.80), 1, 0))
    
    # if a split failure on any phase
    df0 <- df %>% group_by(signalid, phase = factor(0), cyclestart) %>% 
        summarize(sf = max(sf))
    
    sf <- bind_rows(df, df0) %>% 
        mutate(phase = factor(phase)) %>%
        
        group_by(signalid, phase, hour = floor_date(cyclestart, unit = "hour")) %>% 
        summarize(cycles = n(),
                  sf_freq = sum(sf, na.rm = TRUE)/cycles, 
                  sf = sum(sf, na.rm = TRUE)) %>%
        ungroup() %>%
        
        transmute(SignalID = factor(signalid), 
                  CallPhase = factor(phase), 
                  Date_Hour = ymd_hms(hour),
                  Date = date(Date_Hour),
                  DOW = wday(Date),
                  Week = week(Date),
                  sf = as.integer(sf),
                  cycles = cycles,
                  sf_freq = sf_freq)
    
    #tz(sf$Date_Hour) <- "US/Eastern"
    
    sf
    
        # transmute(SignalID = signalid, 
        #           CallPhase = phase, 
        #           Date_Hour = hour,
        #           Date = as.Date(hour),
        #           Hour = hour - days(day(hour) - 1),
        #           sf = sf,
        #           sf_freq = sf_freq,
        #           cycles = cycles)
    
    # SignalID | CallPhase | Date_Hour | Date | Hour | sf_freq | cycles
}

get_peak_sf_utah <- function(msfh) {
    
    msfh %>%
        group_by(SignalID, 
                 Date = date(Hour), 
                 Peak = if_else(hour(Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS),
                                "Peak", "Off_Peak")) %>%
        summarize(sf_freq = weighted.mean(sf_freq, cycles, na.rm = TRUE), 
                  cycles = sum(cycles, na.rm = TRUE)) %>%
        ungroup() %>%
        select(-cycles) %>% 
        split(.$Peak)
    
    # SignalID | CallPhase | Date | Week | DOW | Peak | sf_freq | cycles
}


# SPM Split Failures
get_sf <- function(df) {
    df %>% mutate(SignalID = factor(SignalID),
                  CallPhase = factor(Phase),
                  Date_Hour = lubridate::ymd_hms(Hour),
                  Date = date(Date_Hour),
                  DOW = wday(Date), 
                  Week = week(Date)) %>%
        dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, sf, cycles, sf_freq) %>%
        as_tibble()
    
    # SignalID | CallPhase | Date | Date_Hour | DOW | Week | sf | cycles | sf_freq
}


# SPM Queue Spillback
get_qs <- function(detection_events) {
    
    qs <- detection_events %>% 
        filter(Phase %in% c(2,6)) %>%
        
        group_by(SignalID,
                 CallPhase = Phase,
                 Detector,
                 CycleStart,
                 Date) %>%
        summarize(occ = approx_percentile(DetDuration, 0.95)) %>%
        ungroup() %>%
        group_by(SignalID, 
                 CallPhase,
                 Detector,
                 Hour = date_trunc('hour', CycleStart),
                 Date) %>%
        summarize(cycles = n(), 
                  qs = count_if(occ > 3)) %>%
        collect() %>%
        ungroup() %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase),
               Date_Hour = ymd_hms(Hour),
               Date = date(Date),
               DOW = wday(Date),
               Week = week(Date),
               qs = as.integer(qs),
               cycles = as.integer(cycles),
               qs_freq = as.double(qs)/as.double(cycles)) %>%
        dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, qs, cycles, qs_freq)
    
    # dates <- seq(min(qs$Date), max(qs$Date), by = "1 day")
    dates <- unique(qs$Date)
    
    dc <- lapply(dates, get_det_config_qs) %>% 
        bind_rows() %>% 
        dplyr::select(Date, SignalID, CallPhase, TimeFromStopBar) %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase))
    
    qs %>% left_join(dc) %>% 
        filter(TimeFromStopBar > 0) %>% 
        dplyr::select(-TimeFromStopBar) %>%
        group_by(SignalID, CallPhase, Date, Date_Hour, DOW, Week) %>%
        summarize(qs = sum(qs), cycles = sum(cycles), qs_freq = sum(qs)/sum(cycles)) %>%
        ungroup() %>%
        mutate(SignalID = factor(SignalID), CallPhase = factor(CallPhase))
    
    # SignalID | CallPhase | Date | Date_Hour | DOW | Week | qs | cycles | qs_freq
}

# SPM Queue Spillback --- before get_det_config_qs
# get_qs <- function(detection_events) {
#     
#     qs <- detection_events %>% 
#         filter(Phase %in% c(2,6)) %>%
#         
#         group_by(SignalID,
#                  CallPhase = Phase,
#                  CycleStart) %>%
#         summarize(occ = approx_percentile(DetDuration, 0.95)) %>%
#         ungroup() %>%
#         group_by(SignalID, 
#                  CallPhase,
#                  Hour = date_trunc('hour', CycleStart)) %>%
#         summarize(cycles = n(), 
#                   qs = count_if(occ > 3)) %>%
#         collect() %>%
#         ungroup() %>%
#         mutate(SignalID = factor(SignalID),
#                CallPhase = factor(CallPhase),
#                Date_Hour = ymd_hms(Hour),
#                Date = date(lubridate::floor_date(Date_Hour, unit="days")),
#                DOW = wday(Date_Hour),
#                Week = week(Date),
#                qs = as.integer(qs),
#                cycles = as.integer(cycles),
#                qs_freq = as.double(qs)/as.double(cycles)) %>%
#         dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, qs, cycles, qs_freq)
#     
#     dates <- seq(min(qs$Date), max(qs$Date), by = "1 day")
#     
#     dc <- lapply(dates, function(d) {
#         get_det_config(d)
#         #fn <- glue("ATSPM_Det_Config_Good_{d}.feather")
#         #if (!file.exists(fn)) {
#         #    aws.s3::save_object(object = glue("atspm_det_config_good/date={d}/ATSPM_Det_Config_Good.feather"),
#         #                        bucket = 'gdot-devices',
#         #                        file = fn)
#         #    file.remove(fn)
#         #}
#         #read_feather(fn) %>% mutate(Date = ymd(d))
#     }) %>% 
#         bind_rows %>% 
#         as_tibble() %>% 
#         dplyr::select(Date, SignalID, CallPhase, TimeFromStopBar) %>% # = TimeFromStopBar.atspm) %>%
#         mutate(SignalID = factor(SignalID),
#                CallPhase = factor(CallPhase))
#     
#     qs %>% left_join(dc) %>% 
#         filter(TimeFromStopBar > 0) %>% 
#         dplyr::select(-TimeFromStopBar)
#     
#     # SignalID | CallPhase | Date | Date_Hour | DOW | Week | qs | cycles | qs_freq
# }

get_tt_csv <- function(fns) {
    
    dfs <- lapply(fns, function(f) {
        data.table::fread(f) %>%
            mutate(measurement_tstamp = ymd_hms(measurement_tstamp),
                   travel_time_seconds = travel_time_minutes * 60,
                   Corridor = get_corridor_name(f)) %>%
            filter(wday(measurement_tstamp) %in% c(TUE,WED,THU)) %>%
            mutate(miles = speed /3600 * travel_time_seconds,
                   ref_sec = miles/reference_speed * 3600) %>%
            group_by(Corridor = factor(Corridor), measurement_tstamp) %>%
            summarize(travel_time_seconds = sum(travel_time_seconds, na.rm = TRUE),
                      ref_sec = sum(ref_sec, na.rm = TRUE),
                      miles = sum(miles, na.rm = TRUE)) %>%
            ungroup()
    })
    df <- bind_rows(dfs) %>%
        group_by(Corridor = factor(Corridor),
                 measurement_tstamp) %>%
        summarize(travel_time_seconds = sum(travel_time_seconds, na.rm = TRUE),
                  ref_sec = sum(ref_sec, na.rm = TRUE),
                  miles = sum(miles, na.rm = TRUE)) %>%
        ungroup() %>%
        mutate(tti = travel_time_seconds/ref_sec,
               date_hour = floor_date(measurement_tstamp, "hours"),
               hour = date_hour - days(day(date_hour) - 1)) %>%
        group_by(Corridor, hour) %>%
        summarize(mean_ref_sec = mean(ref_sec, na.rm = TRUE),
                  tti = mean(travel_time_seconds/ref_sec, na.rm = TRUE),
                  pti = quantile(travel_time_seconds, c(0.90))/mean_ref_sec) %>%
        ungroup()
    
    tti <- dplyr::select(df, Corridor, Hour = hour, tti) %>% as_tibble()
    pti <- dplyr::select(df, Corridor, Hour = hour, pti) %>% as_tibble()
    
    list("tti" = tti, "pti" = pti)
    
    # Corridor | hour | idx | value
}

get_daily_cctv_uptime <- function(table, cam_config) {
    dbGetQuery(conn, sql(glue("select cameraid, date, size from gdot_spm.{table}"))) %>% 
        transmute(CameraID = factor(cameraid),
                  Date = date(date),
                  Size = size) %>%
        as_tibble() %>%
        
        # CCTV image size variance by CameraID and Date
        #  -> reduce to 1 for Size > 0, 0 otherwise
        
        # Expanded out to include all available cameras on all days
        #  up/uptime is 0 if no data
        #daily_cctv_uptime <- read_feather(conf$cctv_parsed_filename) %>% #"parsed_cctv.feather"
        filter(Date >= report_start_date,
               Size > 0) %>%
        mutate(up = 1, num = 1) %>%
        dplyr::select(-Size) %>%
        distinct() %>%
        
        # Expanded out to include all available cameras on all days
        complete(CameraID, Date = full_seq(Date, 1), fill = list(up = 0, num = 1)) %>%
        
        mutate(uptime = up/num) %>%
        
        left_join(dplyr::select(cam_config, -Location)) %>% 
        filter(Date >= As_of_Date & Corridor != "") %>%
        dplyr::select(-As_of_Date) %>%
        mutate(CameraID = factor(CameraID),
               Corridor = factor(Corridor))
}

get_rsu_uptime <- function(report_start_date) {
    # rsu <- dbGetQuery(conn, sql(glue("select * from gdot_spm.rsu_uptime"))) %>%
    rsu <- tbl(conn, sql("select * from gdot_spm.rsu_uptime")) %>%
        filter(Date >= date_parse(report_start_date, "%Y-%m-%d")) %>%
        collect() %>%
        transmute(
            SignalID = factor(signalid), 
            Date = ymd(date), 
            uptime = uptime, 
            total = count)
}

# -- Generic Aggregation Functions
get_Tuesdays <- function(df) {
    dates_ <- seq(min(df$Date) - days(6), max(df$Date) + days(6), by = "days")
    tuesdays <- dates_[wday(dates_) == 3]
    
    tuesdays <- pmax(min(df$Date), tuesdays) # unsure of this. need to test
    #tuesdays <- tuesdays[between(tuesdays, min(df$Date), max(df$Date))]
    
    data.frame(Week = week(tuesdays), Date = tuesdays)
}

weighted_mean_by_corridor_ <- function(df, per_, corridors, var_, wt_ = NULL) {
    
    per_ <- as.name(per_)
    
    gdf <- left_join(df, corridors) %>%
        mutate(Corridor = factor(Corridor)) %>%
        group_by(Zone, Corridor, Zone_Group, !!per_) 
    
    if (is.null(wt_)) {
        gdf %>% 
            summarize(!!var_ := mean(!!var_, na.rm = TRUE)) %>%
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(Zone, Corridor, Zone_Group, !!per_, !!var_, delta)
    } else {
        gdf %>% 
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                      !!wt_ := sum(!!wt_, na.rm = TRUE)) %>%
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(Zone, Corridor, Zone_Group, !!per_, !!var_, !!wt_, delta)
    }
}
group_corridor_by_ <- function(df, per_, var_, wt_, corr_grp) {
    df %>%
        group_by(!!per_) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>%
        mutate(Corridor = factor(corr_grp)) %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        mutate(Zone_Group = corr_grp) %>% 
        dplyr::select(Corridor, Zone_Group, !!per_, !!var_, !!wt_, delta) 
}
group_corridor_by_sum_ <- function(df, per_, var_, wt_, corr_grp) {
    df %>%
        group_by(!!per_) %>%
        summarize(!!var_ := sum(!!var_, na.rm = TRUE)) %>%
        mutate(Corridor = factor(corr_grp)) %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        mutate(Zone_Group = corr_grp) %>% 
        dplyr::select(Corridor, Zone_Group, !!per_, !!var_, delta) 
}

group_corridor_by_date <- function(df, var_, wt_, corr_grp) {
    group_corridor_by_(df, as.name("Date"), var_, wt_, corr_grp)
}
group_corridor_by_month <- function(df, var_, wt_, corr_grp) {
    group_corridor_by_(df, as.name("Month"), var_, wt_, corr_grp)
}
group_corridor_by_hour <- function(df, var_, wt_, corr_grp) {
    group_corridor_by_(df, as.name("Hour"), var_, wt_, corr_grp)
}

group_corridors_ <- function(df, per_, var_, wt_, gr_ = group_corridor_by_) {
    
    per_ <- as.name(per_)
    
    # Get average for each Zone or District, according to corridors$Zone
    df_ <- df %>% 
        split(df$Zone)
    all_zones <- lapply(names(df_), function(x) { gr_(df_[[x]], per_, var_, wt_, x) })
    
    # Get average for All RTOP (RTOP1 and RTOP2)
    all_rtop <- df %>%
        filter(Zone_Group %in% c("RTOP1", "RTOP2")) %>%
        gr_(per_, var_, wt_, "All RTOP")
    
    # Get average for RTOP1
    all_rtop1 <- df %>%
        filter(Zone_Group == "RTOP1") %>%
        gr_(per_, var_, wt_, "RTOP1")
    
    # Get average for RTOP2
    all_rtop2 <- df %>%
        filter(Zone_Group == "RTOP2") %>%
        gr_(per_, var_, wt_, "RTOP2")
    
    
    # Get average for All Zone 7 (Zone 7m and Zone 7d)
    all_zone7 <- df %>%
        filter(Zone %in% c("Zone 7m", "Zone 7d")) %>%
        gr_(per_, var_, wt_, "Zone 7")
    
    # concatenate all summaries with corridor averages
    dplyr::bind_rows(select_(df, "Corridor", Zone_Group = "Zone", per_, var_, wt_, "delta"),
                     all_zones,
                     all_rtop,
                     all_rtop1,
                     all_rtop2,
                     all_zone7) %>%
        distinct() %>%
        mutate(Corridor = factor(Corridor),
               Zone_Group = factor(Zone_Group))
}
get_daily_avg <- function(df, var_, wt_ = "ones", peak_only = FALSE) {
    
    var_ <- as.name(var_)
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    
    if (peak_only == TRUE) {
        df <- df %>%
            filter(hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))
    }
    
    df %>%
        group_by(SignalID, Date) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>% # Sum of phases 2,6
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(SignalID, Date, !!var_, !!wt_, delta)
    
    # SignalID | Date | var_ | wt_ | delta
}
get_daily_avg_cctv <- function(df, var_ = "uptime", wt_ = "num", peak_only = FALSE) {
    
    var_ <- as.name(var_)
    wt_ <- as.name(wt_)
    
    df %>%
        group_by(CameraID, Date) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>% # Sum of phases 2,6
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(CameraID, Date, !!var_, !!wt_, delta)
    
    # CameraID | Date | var_ | wt_ | delta
}
get_daily_sum <- function(df, var_, per_) {
    
    var_ <- as.name(var_)
    per_ <- as.name(per_)
    
    
    df %>%
        complete(nesting(SignalID, CallPhase), !!var_ := full_seq(!!var_, 1)) %>%
        group_by(SignalID, !!per_) %>% 
        summarize(!!var_ := sum(!!var_, na.rm = TRUE)) %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(SignalID, !!per_, !!var_, delta)
}

get_weekly_sum_by_day <- function(df, var_) {
    
    var_ <- as.name(var_)
    
    Tuesdays <- get_Tuesdays(df)
    
    df %>%
        group_by(SignalID, CallPhase, Week) %>% 
        summarize(!!var_ := mean(!!var_, na.rm = TRUE)) %>% # Mean over 3 days in the week
        
        group_by(SignalID, Week) %>% 
        summarize(!!var_ := sum(!!var_, na.rm = TRUE)) %>% # Sum of phases 2,6
        
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        left_join(Tuesdays) %>%
        dplyr::select(SignalID, Date, Week, !!var_, delta)

    # SignalID | Date | var_
}
get_weekly_avg_by_day <- function(df, var_, wt_ = "ones", peak_only = TRUE) {
    
    var_ <- as.name(var_)
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    
    Tuesdays <- get_Tuesdays(df)
    
    if (peak_only == TRUE) {
        df <- df %>%
            filter((hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS)))
    }
    
    df %>%
        complete(nesting(SignalID, CallPhase), Week = full_seq(Week, 1)) %>%
        group_by(SignalID, CallPhase, Week) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>% # Mean over 3 days in the week
        
        group_by(SignalID, Week) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>% # Sum of phases 2,6
        
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        left_join(Tuesdays) %>%
        dplyr::select(SignalID, Date, Week, !!var_, !!wt_, delta) %>%
        ungroup()
    
    # SignalID | Date | vpd
}
get_weekly_avg_by_day_cctv <- function(df, var_ = "uptime", wt_ = "num") {
    
    var_ <- as.name(var_)
    wt_ <- as.name(wt_)
    
    Tuesdays <- get_Tuesdays(df)
    
    df %>% mutate(Week = week(Date)) %>%
        dplyr::select(-Date) %>%
        left_join(Tuesdays, by = c("Week")) %>%
        group_by(CameraID, Corridor, Week) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>% # Mean over 3 days in the week
        
        group_by(CameraID, Corridor, Week) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>% # Sum of phases 2,6
        
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        left_join(Tuesdays) %>%
        dplyr::select(CameraID, Date, Week, !!var_, !!wt_, delta, Corridor)
    
    # SignalID | Date | vpd
}
get_cor_weekly_avg_by_day <- function(df, corridors, var_, wt_ = "ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Date", corridors, var_, wt_) %>%
        filter(!is.nan(!!var_))
    
    group_corridors_(cor_df_out, "Date", var_, wt_) %>%
        mutate(Week = week(Date))
}
get_monthly_avg_by_day <- function(df, var_, wt_ = NULL, peak_only = FALSE) {
    
    var_ <- as.name(var_)
    # if (wt_ == "ones") { ##--- this needs to be revisited. this should be added and this function should look like the weekly avg function above
    #     wt_ <- as.name(wt_)
    #     df <- mutate(df, !!wt_ := 1)
    # } else {
    #     wt_ <- as.name(wt_)
    # }
    
    if (peak_only == TRUE) {
        df <- df %>%
            filter((hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS)))
    }
    
    df$Month <- df$Date
    day(df$Month) <- 1

    current_month <- max(df$Month)
    
    gdf <- df %>%
        #mutate(Month = Date - days(day(Date) - 1)) %>%
        group_by(SignalID, CallPhase) %>%
        complete(nesting(SignalID, CallPhase), 
                 Month = seq(min(Month), current_month, by = "1 month")) %>%
        group_by(SignalID, CallPhase, Month)
    
    if (is.null(wt_)) {
        gdf %>%
            summarize(!!var_ := mean(!!var_, na.rm = TRUE)) %>%
            group_by(SignalID, Month) %>%
            summarize(!!var_ := sum(!!var_, na.rm = TRUE)) %>% # Sum over Phases (2,6)
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(-lag_) %>%
            filter(!is.na(Month))
    } else {
        wt_ <- as.name(wt_)
        gdf %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                      !!wt_ := sum(!!wt_, na.rm = TRUE)) %>%
            group_by(SignalID, Month) %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean over Phases(2,6)
                      !!wt_ := sum(!!wt_, na.rm = TRUE)) %>%
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(-lag_) %>%
            filter(!is.na(Month))
    }
}

get_cor_monthly_avg_by_day <- function(df, corridors, var_, wt_="ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Month", corridors, var_, wt_) %>%
        filter(!is.nan(!!var_))
    
    group_corridors_(cor_df_out, "Month", var_, wt_)
}

get_weekly_avg_by_hr <- function(df, var_, wt_ = NULL) {
    
    var_ <- as.name(var_)
    
    Tuesdays <- df %>% 
        mutate(Date = date(Hour)) %>%
        get_Tuesdays()
    
    df_ <- left_join(df, Tuesdays) %>%
        filter(!is.na(Date))
    year(df_$Hour) <- year(df_$Date)
    month(df_$Hour) <- month(df_$Date)
    day(df_$Hour) <- day(df_$Date)
    
    if (is.null(wt_)) {
        df_ %>%
            group_by(SignalID, CallPhase, Week, Hour) %>%
            summarize(!!var_ := mean(!!var_, na.rm = TRUE)) %>% # Mean over 3 days in the week
            group_by(SignalID, Week, Hour) %>% 
            summarize(!!var_ := mean(!!var_, na.rm = TRUE)) %>% # Mean of phases 2,6
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(SignalID, Hour, Week, !!var_, delta)
    } else {
        wt_ <- as.name(wt_)
        df_ %>%
            group_by(SignalID, CallPhase, Week, Hour) %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                      !!wt_ := sum(!!wt_, na.rm = TRUE)) %>% # Mean over 3 days in the week
            group_by(SignalID, Week, Hour) %>% 
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                      !!wt_ := sum(!!wt_, na.rm = TRUE)) %>% # Sum of phases 2,6
                mutate(lag_ = lag(!!var_),
                       delta = ((!!var_) - lag_)/lag_) %>%
                ungroup() %>%
            dplyr::select(SignalID, Hour, Week, !!var_, !!wt_, delta)
    }
}
get_cor_weekly_avg_by_hr <- function(df, corridors, var_, wt_="ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Hour", corridors, var_, wt_)
    
    group_corridors_(cor_df_out, "Hour", var_, wt_)
}

get_sum_by_hr <- function(df, var_) {
    
    var_ <- as.name(var_)
    
    df %>%  
        mutate(DOW = wday(Timeperiod),
               Week = week(date(Timeperiod)),
               Hour = floor_date(Timeperiod, unit = '1 hour')) %>%
        group_by(SignalID, CallPhase, Week, DOW, Hour) %>%
        summarize(!!var_ := sum(!!var_, na.rm = TRUE)) %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(-lag_)
    
    # SignalID | CallPhase | Week | DOW | Hour | var_ | delta
    
} ## unused. untested

get_avg_by_hr <- function(df, var_, wt_ = NULL) {
    
    df_ <- as.data.table(df)
    
    df_[, c("DOW", "Week", "Hour") := list(wday(Date_Hour), 
                                           week(date(Date_Hour)), 
                                           floor_date(Date_Hour, unit = '1 hour'))]
    if (is.null(wt_)) {
        ret <- df_[, .(mean(get(var_)), 1), 
                   by = .(SignalID, CallPhase, Week, DOW, Hour)]
        ret <- ret[, c((var_), (wt_)) := .(V1, V2)][, -(V1:V2)]
        as_tibble(ret)
    } else {
        ret <- df_[, .(weighted.mean(get(var_), get(wt_), na.rm = TRUE),
                       sum(get(wt_), na.rm = TRUE)), 
                   by = .(SignalID, CallPhase, Week, DOW, Hour)]
        ret <- ret[, c((var_), (wt_)) := .(V1, V2)][, -(V1:V2)]
        ret[, delta := (get(var_) - shift(get(var_), 1, type = "lag"))/shift(get(var_), 1, type = "lag"),
            by = .(SignalID, CallPhase)]
        as_tibble(ret)
    }
}

get_monthly_avg_by_hr <- function(df, var_, wt_ = "ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    df %>% 
        group_by(SignalID, Hour) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(-lag_)
    
    # SignalID | CallPhase | Hour | vph
}
get_cor_monthly_avg_by_hr <- function(df, corridors, var_, wt_ = "ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Hour", corridors, var_, wt_)
    group_corridors_(cor_df_out, "Hour", var_, wt_)
}

# Used to get peak period metrics from hourly results, i.e., peak/off-peak split failures
summarize_by_peak <- function(df, date_col) {
    
    date_col_ <- as.name(date_col)
    
    df %>% 
        mutate(Peak = if_else(hour(!!date_col_) %in% AM_PEAK_HOURS | hour(!!date_col_) %in% PM_PEAK_HOURS, 
                              "Peak", 
                              "Off_Peak"), 
               Peak = factor(Peak)) %>%
        split(.$Peak) %>%
        lapply(function(x) {
            x %>% 
                group_by(SignalID, Period = date(!!date_col_), Peak) %>%
                summarize(sf_freq = weighted.mean(sf_freq, cycles),
                          cycles = sum(cycles)) %>%
                ungroup() %>%
                select(-Peak)
        })
}

# Device Uptime from Excel files from Field Engineers
get_device_uptime_from_xl <- function(fn, range) {
    
    h <- names(readxl::read_excel(fn, range = "Device Status!A5:U5"))
    df <- readxl::read_excel(fn, range = range)
    names(df) <- h
    
    df[1,1] <- "Functioning"
    df[2,1] <- "Total"
    
    df %>% gather(Month, num, -X__1) %>% 
        spread(key = X__1, value = num) %>%
        transmute(Corridor = "Placeholder",
                  Month = dmy(paste0("1-", Month)),
                  up = as.integer(Functioning),
                  num = as.integer(Total),
                  uptime = up/num,
                  Filename = fn) %>% 
        filter(!is.na(up)) %>% 
        arrange(Month) %>%
        mutate(Corridor = get_corridor_name(fn))
}
get_device_uptime_from_xl_multiple <- function(fns, range, corridors) {
    dfs <- lapply(fns, function(x) get_device_uptime_from_xl(x, range))
    df <- bind_rows(dfs)
    
    corrs <- corridors %>% distinct(Corridor, Zone_Group, Zone) %>%
        mutate(Corridor = factor(Corridor),
               SignalID = factor(0))
    df_ <- left_join(df, corrs)
    
    df_ %>% dplyr::select(Zone_Group, Zone, Corridor, Month, up, num, uptime)
    
}
# -- end Generic Aggregation Functions

get_daily_detector_uptime <- function(filtered_counts) {
    
    usable_cores <- get_usable_cores()
    
    # this seems ripe for optimization
    bad_comms <- filtered_counts %>%
        group_by(SignalID, Timeperiod) %>%
        summarize(vol = sum(vol, na.rm = TRUE)) %>%
        dplyr::filter(vol == 0) %>%
        dplyr::select(-vol)
    fc <- anti_join(filtered_counts, bad_comms, by = c("SignalID", "Timeperiod")) %>%
        ungroup()
    
    ddu <- fc %>% 
        mutate(Date_Hour = Timeperiod,
               Date = date(Date_Hour)) %>%
        dplyr::select(SignalID, CallPhase, Detector, Date, Date_Hour, Good_Day) %>%
        ungroup() %>%
        mutate(setback = ifelse(CallPhase %in% c(2,6), "Setback", "Presence"),
               setback = factor(setback),
               SignalID = factor(SignalID)) %>%
        split(.$setback) %>% lapply(function(x) {
            x %>%
                group_by(SignalID, CallPhase, Date, Date_Hour, setback) %>%
                summarize(uptime = sum(Good_Day, na.rm = TRUE), 
                          all = n()) %>%
                ungroup() %>% 
                mutate(uptime = uptime/all,
                       all = as.double(all))
        }) #, mc.cores = usable_cores)
    ddu
}

get_avg_daily_detector_uptime <- function(ddu) {
    
    #plan(multiprocess)
    
    sb_daily_uptime <- get_daily_avg(filter(ddu, setback == "Setback"), 
                                     "uptime", "all", 
                                     peak_only = FALSE)
    pr_daily_uptime <- get_daily_avg(filter(ddu, setback == "Presence"), 
                                     "uptime", "all", 
                                     peak_only = FALSE)
    all_daily_uptime <- get_daily_avg(ddu, 
                                      "uptime", "all", 
                                      peak_only = FALSE)
    
    sb_pr <- full_join(sb_daily_uptime, pr_daily_uptime, 
                       by = c("SignalID", "Date"), 
                       suffix = c(".sb", ".pr"))
    
    full_join(all_daily_uptime, sb_pr,
              by = c("SignalID", "Date")) %>%
        dplyr::select(-starts_with("delta.")) %>%
        rename(uptime = uptime)
}

get_cor_avg_daily_detector_uptime <- function(avg_daily_detector_uptime, corridors) {
    
    plan(multiprocess)
    
    cor_daily_sb_uptime %<-% (avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.sb)) %>%
        get_cor_weekly_avg_by_day(corridors, "uptime.sb", "all.sb"))
    
    cor_daily_pr_uptime %<-% (avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.pr)) %>%
        get_cor_weekly_avg_by_day(corridors, "uptime.pr", "all.pr"))
    
    cor_daily_all_uptime %<-% (avg_daily_detector_uptime %>% 
        filter(!is.na(uptime)) %>%
        # average corridor by ones instead of all: 
        #  to treat all signals equally instead of weighted by # of detectors
        get_cor_weekly_avg_by_day(corridors, "uptime", "ones")) 
    
    
    full_join(dplyr::select(cor_daily_sb_uptime, -c(all.sb, delta)),
              dplyr::select(cor_daily_pr_uptime, -c(all.pr, delta))) %>%
        left_join(dplyr::select(cor_daily_all_uptime, -c(ones, delta))) %>%
        mutate(Zone_Group = factor(Zone_Group))
}

get_weekly_vpd <- function(vpd) {
    vpd <- filter(vpd, DOW %in% c(TUE,WED,THU)) 
    get_weekly_sum_by_day(vpd, "vpd")
}
get_weekly_papd <- function(papd) {
    papd <- filter(papd, DOW %in% c(TUE,WED,THU))
    get_weekly_sum_by_day(papd, "papd")
}
get_weekly_thruput <- function(throughput) {
    get_weekly_sum_by_day(throughput, "vph")
}
get_weekly_aog_by_day <- function(daily_aog) {
    get_weekly_avg_by_day(daily_aog, "aog", "vol", peak_only = TRUE)
}
get_weekly_pr_by_day <- function(daily_pr) {
    get_weekly_avg_by_day(daily_pr, "pr", "vol", peak_only = TRUE)
}
get_weekly_sf_by_day <- function(sf) {
    get_weekly_avg_by_day(sf, "sf_freq", "cycles", peak_only = TRUE)
}
get_weekly_qs_by_day <- function(qs) {
    get_weekly_avg_by_day(qs, "qs_freq", "cycles", peak_only = TRUE)
}
get_weekly_detector_uptime <- function(avg_daily_detector_uptime) {
    avg_daily_detector_uptime %>% 
        mutate(CallPhase = 0, Week = week(Date)) %>%
        get_weekly_avg_by_day("uptime", "all", peak_only = FALSE) %>%
        replace_na(list(uptime = 0)) %>%
        arrange(SignalID, Date)
}

get_cor_weekly_vpd <- function(weekly_vpd, corridors) {
    get_cor_weekly_avg_by_day(weekly_vpd, corridors, "vpd")
}
get_cor_weekly_papd <- function(weekly_papd, corridors) {
    get_cor_weekly_avg_by_day(weekly_papd, corridors, "papd")
}
get_cor_weekly_thruput <- function(weekly_throughput, corridors) {
    get_cor_weekly_avg_by_day(weekly_throughput, corridors, "vph")
}
get_cor_weekly_aog_by_day <- function(weekly_aog, corridors) {
    get_cor_weekly_avg_by_day(weekly_aog, corridors, "aog", "vol")
}
get_cor_weekly_pr_by_day <- function(weekly_pr, corridors) {
    get_cor_weekly_avg_by_day(weekly_pr, corridors, "pr", "vol")
}
get_cor_weekly_sf_by_day <- function(weekly_sf, corridors) {
    get_cor_weekly_avg_by_day(weekly_sf, corridors, "sf_freq", "cycles")
}
get_cor_weekly_qs_by_day <- function(weekly_qs, corridors) {
    get_cor_weekly_avg_by_day(weekly_qs, corridors, "qs_freq", "cycles")
}
get_cor_weekly_detector_uptime <- function(avg_weekly_detector_uptime, corridors) {
    get_cor_weekly_avg_by_day(avg_weekly_detector_uptime, corridors, "uptime", "all")
}

get_monthly_vpd <- function(vpd) {
    vpd <- filter(vpd, DOW %in% c(TUE,WED,THU)) 
    get_monthly_avg_by_day(vpd, "vpd", peak_only = FALSE)
}
get_monthly_papd <- function(papd) {
    papd <- filter(papd, DOW %in% c(TUE,WED,THU)) 
    get_monthly_avg_by_day(papd, "papd", peak_only = FALSE)
}
get_monthly_thruput <- function(throughput) {
    get_monthly_avg_by_day(throughput, "vph", peak_only = FALSE)
}
get_monthly_aog_by_day <- function(daily_aog) {
    get_monthly_avg_by_day(daily_aog, "aog", "vol", peak_only = TRUE)
}
get_monthly_pr_by_day <- function(daily_pr) {
    get_monthly_avg_by_day(daily_pr, "pr", "vol", peak_only = TRUE)
}
get_monthly_sf_by_day <- function(sf) {
    get_monthly_avg_by_day(sf, "sf_freq", "cycles", peak_only = TRUE)
}
get_monthly_qs_by_day <- function(qs) {
    get_monthly_avg_by_day(qs, "qs_freq", "cycles", peak_only = TRUE)
}
get_monthly_detector_uptime <- function(avg_daily_detector_uptime) {
    avg_daily_detector_uptime %>% 
        mutate(CallPhase = 0) %>%
        get_monthly_avg_by_day("uptime", "all") %>%
        arrange(SignalID, Month)
}

get_cor_monthly_vpd <- function(monthly_vpd, corridors) {
    get_cor_monthly_avg_by_day(monthly_vpd, corridors, "vpd")
}
get_cor_monthly_papd <- function(monthly_papd, corridors) {
    get_cor_monthly_avg_by_day(monthly_papd, corridors, "papd")
}
get_cor_monthly_thruput <- function(monthly_throughput, corridors) {
    get_cor_monthly_avg_by_day(monthly_throughput, corridors, "vph")
}
get_cor_monthly_aog_by_day <- function(monthly_aog_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_aog_by_day, corridors, "aog", "vol")
}
get_cor_monthly_pr_by_day <- function(monthly_pr_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_pr_by_day, corridors, "pr", "vol")
}
get_cor_monthly_sf_by_day <- function(monthly_sf_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_sf_by_day, corridors, "sf_freq", "cycles")
}
get_cor_monthly_qs_by_day <- function(monthly_qs_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_qs_by_day, corridors, "qs_freq", "cycles")
}
get_cor_monthly_detector_uptime <- function(avg_daily_detector_uptime, corridors) {
    
    plan(multiprocess)
    
    cor_daily_sb_uptime %<-% (avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.sb)) %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        get_cor_monthly_avg_by_day(corridors, "uptime.sb", "all.sb"))
    cor_daily_pr_uptime %<-% (avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.pr)) %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        get_cor_monthly_avg_by_day(corridors, "uptime.pr", "all.pr"))
    cor_daily_all_uptime %<-% (avg_daily_detector_uptime %>% 
        filter(!is.na(uptime)) %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        get_cor_monthly_avg_by_day(corridors, "uptime", "all"))
    
    full_join(dplyr::select(cor_daily_sb_uptime, -c(all.sb, delta)),
              dplyr::select(cor_daily_pr_uptime, -c(all.pr, delta))) %>%
        left_join(dplyr::select(cor_daily_all_uptime, -c(all))) %>%
        mutate(Corridor = factor(Corridor),
               Zone_Group = factor(Zone_Group))
}

get_vph <- function(counts, mainline_only = TRUE) {
    
    if (mainline_only == TRUE) {
        counts <- counts %>%
            filter(CallPhase %in% c(2,6)) # sum over Phases 2,6
    }
    df <- counts %>% 
        mutate(Date_Hour = floor_date(Timeperiod, "1 hour"))
    get_sum_by_hr(df, "vol") %>% 
        group_by(SignalID, Week, DOW, Hour) %>% 
        summarize(vph = sum(vol, na.rm = TRUE))
    
    # SignalID | CallPhase | Week | DOW | Hour | aog | vph
}
get_aog_by_hr <- function(aog) {
    get_avg_by_hr(aog, "aog", "vol")
    
    # SignalID | CallPhase | Week | DOW | Hour | aog | vol
}
get_pr_by_hr <- function(aog) {
    get_avg_by_hr(aog, "pr", "vol")
}
get_sf_by_hr <- function(sf) {
    get_avg_by_hr(sf, "sf_freq", "cycles")
}
get_qs_by_hr <- function(qs) {
    get_avg_by_hr(qs, "qs_freq", "cycles")
}

get_monthly_vph <- function(vph) {
    
    vph %>% 
        ungroup() %>%
        filter(DOW %in% c(TUE,WED,THU)) %>%
        group_by(SignalID, Hour) %>% summarize(vph = sum(vph, na.rm = TRUE)) %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(vph = mean(vph, na.rm = TRUE)) %>%
        ungroup()
    
    # SignalID | CallPhase | Hour | vph
}
get_monthly_paph <- function(paph) {
    paph %>%
        rename(vph = paph) %>%
        get_monthly_vph() %>%
        rename(paph = vph)
}
get_monthly_aog_by_hr <- function(aog_by_hr) {
    
    aog_by_hr %>% 
        group_by(SignalID, Hour) %>% 
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE), 
                  vol = sum(vol, na.rm = TRUE)) %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE), 
                  vol = sum(vol, na.rm = TRUE)) %>%
        ungroup()
    
    # SignalID | CallPhase | Hour | vph
}
get_monthly_pr_by_hr <- function(pr_by_hr) {
    
    get_monthly_aog_by_hr(rename(pr_by_hr, aog = pr)) %>%
        rename(pr = aog)
}
get_monthly_sf_by_hr <- function(sf_by_hr) {
    
    get_monthly_aog_by_hr(rename(sf_by_hr, aog = sf_freq, vol = cycles)) %>%
        rename(sf_freq = aog, cycles = vol)
}
get_monthly_qs_by_hr <- function(qs_by_hr) {
    
    get_monthly_aog_by_hr(rename(qs_by_hr, aog = qs_freq, vol = cycles)) %>%
        rename(qs_freq = aog, cycles = vol)
}

get_cor_monthly_vph <- function(monthly_vph, corridors) {
    get_cor_monthly_avg_by_hr(monthly_vph, corridors, "vph")
    # Corridor | Hour | vph
}
get_cor_monthly_paph <- function(monthly_paph, corridors) {
    get_cor_monthly_avg_by_hr(monthly_paph, corridors, "paph")
    # Corridor | Hour | vph
}
get_cor_monthly_aog_by_hr <- function(monthly_aog_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_aog_by_hr, corridors, "aog", "vol")
    # Corridor | Hour | vph
}
get_cor_monthly_pr_by_hr <- function(monthly_pr_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_pr_by_hr, corridors, "pr", "vol")
}
get_cor_monthly_sf_by_hr <- function(monthly_sf_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_sf_by_hr, corridors, "sf_freq", "cycles")
}
get_cor_monthly_qs_by_hr <- function(monthly_qs_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_qs_by_hr, corridors, "qs_freq", "cycles")
}

get_cor_monthly_ti <- function(ti, cor_monthly_vph, corridors) {
    
    # Get share of volume (as pct) by hour over the day, for whole dataset
    day_dist <- cor_monthly_vph %>% 
        group_by(Corridor, month(Hour)) %>% 
        mutate(pct = vph/sum(vph, na.rm = TRUE)) %>% 
        group_by(Corridor, Zone_Group, hr = hour(Hour)) %>% 
        summarize(pct = mean(pct, na.rm = TRUE))
    
    left_join(ti, corridors %>% distinct(Zone_Group, Zone, Corridor), by = c("Corridor")) %>%
        mutate(hr = hour(Hour)) %>%
        left_join(day_dist) %>%
        ungroup() %>%
        tidyr::replace_na(list(pct = 1))
}
get_cor_monthly_ti_by_hr <- function(ti, cor_monthly_vph, corridors) {
    
    df <- get_cor_monthly_ti(ti, cor_monthly_vph, corridors)
    
    if ("tti" %in% colnames(ti)) {
        tindx = "tti"
    } else if ("pti" %in% colnames(ti)) {
        tindx = "pti"
    } else if ("bi" %in% colnames(ti)) {
        tindx = "bi"
    } else {
        tindx = "oops"
    }
    
    get_cor_monthly_avg_by_hr(df, corridors, tindx, "pct")
}
get_cor_monthly_ti_by_day <- function(ti, cor_monthly_vph, corridors) {
    
    df <- get_cor_monthly_ti(ti, cor_monthly_vph, corridors)
    
    if ("tti" %in% colnames(ti)) {
        tindx = "tti"
    } else if ("pti" %in% colnames(ti)) {
        tindx = "pti"
    } else if ("bi" %in% colnames(ti)) {
        tindx = "bi"
    } else {
        tindx = "oops"
    }
    
    df %>% 
        mutate(Month = as_date(Hour)) %>%
        get_cor_monthly_avg_by_day(corridors, tindx, "pct")
}


get_weekly_vph <- function(vph) {
    vph <- filter(vph, DOW %in% c(TUE,WED,THU))
    get_weekly_avg_by_hr(vph, "vph")
}
get_weekly_paph <- function(paph) {
    paph <- filter(paph, DOW %in% c(TUE,WED,THU))
    get_weekly_avg_by_hr(paph, "paph")
}
get_cor_weekly_vph <- function(weekly_vph, corridors) {
    get_cor_weekly_avg_by_hr(weekly_vph, corridors, "vph")
}
get_cor_weekly_paph <- function(weekly_paph, corridors) {
    get_cor_weekly_avg_by_hr(weekly_paph, corridors, "paph")
}
get_cor_weekly_vph_peak <- function(cor_weekly_vph) {
    dfs <- get_cor_monthly_vph_peak(cor_weekly_vph)
    lapply(dfs, function(x) {dplyr::rename(x, Date = Month)})
}
get_weekly_vph_peak <- function(weekly_vph) {
    dfs <- get_monthly_vph_peak(weekly_vph)
    lapply(dfs, function(x) {dplyr::rename(x, Date = Month)})
}
get_monthly_vph_peak <- function(monthly_vph) {
    
    am <- dplyr::filter(monthly_vph, hour(Hour) %in% AM_PEAK_HOURS) %>% 
        mutate(Date = date(Hour)) %>%
        get_daily_avg("vph") %>%
        rename(Month = Date)
    
    pm <- dplyr::filter(monthly_vph, hour(Hour) %in% PM_PEAK_HOURS) %>% 
        mutate(Date = date(Hour)) %>%
        get_daily_avg("vph") %>%
        rename(Month = Date)
    
    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}

# vph during peak periods
get_cor_monthly_vph_peak <- function(cor_monthly_vph) {
    
    am <- dplyr::filter(cor_monthly_vph, hour(Hour) %in% AM_PEAK_HOURS) %>% 
        mutate(Month = date(Hour)) %>%
        weighted_mean_by_corridor_("Month", corridors, as.name("vph"))
    
    pm <- dplyr::filter(cor_monthly_vph, hour(Hour) %in% PM_PEAK_HOURS) %>%
        mutate(Month = date(Hour)) %>%
        weighted_mean_by_corridor_("Month", corridors, as.name("vph"))
    
    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}
# aog during peak periods -- unused
get_cor_monthly_aog_peak <- function(cor_monthly_aog_by_hr) {
    
    am <- dplyr::filter(cor_monthly_aog_by_hr, hour(Hour) %in% AM_PEAK_HOURS) %>% 
        mutate(Date = date(Hour)) %>%
        weighted_mean_by_corridor_("Date", corridors, as.name("aog"), as.name("vol"))
    
    pm <- dplyr::filter(cor_monthly_aog_by_hr, hour(Hour) %in% PM_PEAK_HOURS) %>%
        mutate(Date = date(Hour)) %>% 
        weighted_mean_by_corridor_("Date", corridors, as.name("aog"), as.name("vol"))
    
    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}

# Device Uptime from Excel
get_veh_uptime_from_xl_monthly_reports <- function(fns, corridors) {
    get_device_uptime_from_xl_multiple(fns, "Device Status!A5:U7", corridors)
}
get_ped_uptime_from_xl_monthly_reports <- function(fn, corridors) {
    get_device_uptime_from_xl_multiple(fn, "Device Status!A9:U11", corridors)
}
get_cctv_uptime_from_xl_monthly_reports <- function(fns, corridors) {
    get_device_uptime_from_xl_multiple(fns, "Device Status!A13:U15", corridors)
}


get_det_uptime_from_manual_xl <- function(bucket, key, corridors) {
    
    month_string <- str_extract(basename(key), "^\\S+ \\d{4}")
    month_date <- dmy(paste("1", month_string))
    
    corridors <- corridors %>% 
        distinct(Zone_Group, Zone, Corridor)
    
    s3read_using(read_excel, bucket = bucket, object = key) %>% 
        dplyr::select(
            Corridor, 
            `Detector Type`, 
            `Total # of Detectors`, 
            `# of Operational Detectors`
        ) %>%
        fill(Corridor) %>%
        mutate_all(stringi::stri_trim) %>%
        transmute(xl_Corridor = Corridor,
                  Corridor = factor(get_corridor_name(Corridor)),
                  #Zone_Group = Zone_Group,
                  Month = month_date,
                  Type = factor(`Detector Type`),
                  up = as.integer(`# of Operational Detectors`),
                  num = as.integer(`Total # of Detectors`),
                  uptime = as.double(up)/num) %>% 
        left_join(corridors, by = c("Corridor")) %>%
        group_by(Corridor, Zone_Group, Zone, Month, Type) %>%
        summarize(uptime = weighted.mean(uptime, num, na.rm = TRUE),
                  up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE)) %>%
        ungroup() %>%
        dplyr::select(Zone_Group, Zone, Corridor, Month, Type, up, num, uptime) %>%
        arrange(Zone_Group, Zone, Corridor)
}

# get_det_uptime_from_manual_xl_older <- function(fn, date_string) {
#     
#     aws.s3::save_object(glue('manual_veh_ped_uptime/{fn}'), bucket = 'gdot-spm')
#     
#     xl <- readxl::read_excel(fn) %>% 
#         dplyr::select(
#             Corridor, 
#             `Detector Type`, 
#             `Total # of Detectors`, 
#             `# of Operational Detectors`
#         ) %>%
#         fill(Corridor) %>%
#         mutate_all(stringi::stri_trim) %>%
#         mutate(Zone_Group = case_when(
#             startsWith(as.character(Corridor), "Z1") ~ "RTOP1",
#             startsWith(as.character(Corridor), "Z2") ~ "RTOP1",
#             startsWith(as.character(Corridor), "Z3") ~ "RTOP1",
#             TRUE ~ "RTOP2")
#         ) %>%
#         transmute(xl_Corridor = Corridor,
#                   Corridor = factor(get_corridor_name(Corridor)),
#                   Zone_Group = Zone_Group,
#                   Month = ymd(date_string),
#                   Type = factor(`Detector Type`),
#                   up = as.integer(`# of Operational Detectors`),
#                   num = as.integer(`Total # of Detectors`),
#                   uptime = as.double(up)/num) %>% 
#         group_by(Corridor, Zone_Group, Month, Type) %>% 
#         summarize(uptime = weighted.mean(uptime, num, na.rm = TRUE),
#                   up = sum(up, na.rm = TRUE),
#                   num = sum(num, na.rm = TRUE)) %>%
#         ungroup() %>%
#         dplyr::select(Zone_Group, Corridor, Month, Type, up, num, uptime)
#     file.remove(fn)
#     xl
# }
get_cor_monthly_xl_uptime <- function(df, corridors) {
    
    # By Corridor
    a <- df %>% 
        left_join(corridors, by = "Corridor") %>%
        group_by(Zone_Group, Corridor, Month) %>%
        summarize(uptime = weighted.mean(uptime, num, na.rm = TRUE),
                  up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE)) %>%
        ungroup()
    
    # RTOP1 and RTOP2
    b <- a %>% 
        filter(Zone_Group %in% c("RTOP1", "RTOP2")) %>%
        group_by(Zone_Group, Corridor = Zone_Group, Month) %>%
        summarize(uptime = weighted.mean(uptime, num, na.rm = TRUE),
                  up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE))
    
    # All RTOP
    c <- a %>% 
        filter(Zone_Group %in% c("RTOP1", "RTOP2")) %>%
        group_by(Zone_Group = "All RTOP", Corridor = Zone_Group, Month) %>%
        summarize(uptime = weighted.mean(uptime, num, na.rm = TRUE),
                  up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE))
    
    bind_rows(a, b, c) %>%
        arrange(Zone_Group, Corridor, Month) %>% 
        group_by(Zone_Group, Corridor) %>%
        mutate(delta = (uptime - lag(uptime))/lag(uptime)) %>%
        ungroup()
    
}

get_cor_weekly_cctv_uptime <- function(daily_cctv_uptime) {
    
    df <- daily_cctv_uptime %>% 
        mutate(DOW = wday(Date), 
               Week = week(Date))
    
    Tuesdays <- get_Tuesdays(df)
    
    df %>% 
        dplyr::select(-Date) %>% 
        left_join(Tuesdays) %>% 
        group_by(Date, Corridor, Zone_Group) %>% 
        summarize(up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE),
                  uptime = sum(up, na.rm = TRUE)/sum(num, na.rm = TRUE)) %>%
        ungroup()
}
get_cor_monthly_cctv_uptime <- function(daily_cctv_uptime) {
    
    daily_cctv_uptime %>% 
        mutate(Month = Date - days(day(Date) - 1)) %>% 
        group_by(Month, Corridor, Zone_Group) %>% 
        summarize(up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE),
                  uptime = sum(up, na.rm = TRUE)/sum(num, na.rm = TRUE)) #%>%
    #get_cor_monthly_xl_uptime()
}



# Cross filter Daily Volumes Chart. For Monthly Report ------------------------
get_vpd_plot <- function(cor_weekly_vpd, cor_monthly_vpd) {
    
    sdw <- SharedData$new(cor_weekly_vpd, ~Corridor, group = "grp")
    sdm <- SharedData$new(dplyr::filter(cor_monthly_vpd, month(Month)==10), ~Corridor, group = "grp")
    
    font_ <- list(family = "Ubuntu")
    
    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)
    
    p1 <- base_m %>%
        summarise(vpd = mean(vpd, na.rm = TRUE)) %>% # This has to be just the current month's vpd
        arrange(vpd) %>%
        add_bars(x = ~vpd, 
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vpd)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = "October Volume (vpd)", zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, 
                  y = ~vpd, 
                  alpha = 0.3) %>%
        layout(xaxis = list(title = "Vehicles/day"),
               showlegend = FALSE)
    
    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80),
               title = "Volume (veh/day) Trend") %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), 
                                            textposition = "inside"))
}
# Cross filter Hourly Volumes Chart. For Monthly Report -----------------------
get_vphpl_plot <- function(df, group_name, chart_title, bar_subtitle, mo) {
    
    sdw <- SharedData$new(df, ~Corridor, group = group_name)
    sdm <- SharedData$new(dplyr::filter(df, month(Date) == mo), ~Corridor, group = group_name)
    
    font_ <- list(family = "Ubuntu")
    
    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)
    
    p1 <- base_m %>%
        summarise(vphpl = mean(vphpl, na.rm = TRUE)) %>% # This has to be just the current month's vphpl
        arrange(vphpl) %>%
        add_bars(x = ~vphpl, 
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vphpl)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = bar_subtitle, zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, y = ~vphpl, alpha = 0.3) %>%
        layout(xaxis = list(title = "Date"),
               showlegend = FALSE,
               annotations = list(text = chart_title,
                                  font = list(family = "Ubuntu",
                                              size = 12),
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE)
        )
    
    
    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80, r = 40)) %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), textposition = "inside"))
}
get_vph_peak_plot <- function(df, group_name, chart_title, bar_subtitle, mo) {
    
    sdw <- SharedData$new(df, ~Corridor, group = group_name)
    sdm <- SharedData$new(dplyr::filter(df, month(Date) == mo), ~Corridor, group = group_name)
    
    font_ <- list(family = "Ubuntu")
    
    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)
    
    p1 <- base_m %>%
        summarise(vph = mean(vph, na.rm = TRUE)) %>% # This has to be just the current month's vph
        arrange(vph) %>%
        add_bars(x = ~vph, 
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vph)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = bar_subtitle, zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, y = ~vph, alpha = 0.3) %>%
        layout(xaxis = list(title = "Date"),
               showlegend = FALSE,
               annotations = list(text = chart_title,
                                  font = list(family = "Ubuntu",
                                              size = 12),
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE)
        )
    
    
    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80, r = 40)) %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), textposition = "inside"))
}


# Convert Monthly data to quarterly for Quarterly Report ####
get_quarterly <- function(monthly_df, var_, wt_="ones", operation = "avg") {
    
    if (wt_ == "ones" & !"ones" %in% names(monthly_df)) {
        monthly_df <- monthly_df %>% mutate(ones = 1)
    }
    
    var_ <- as.name(var_)
    wt_ <- as.name(wt_)
    
    quarterly_df <- monthly_df %>% 
        group_by(Corridor, 
                 Zone_Group,
                 Quarter = as.character(lubridate::quarter(Month, with_year = TRUE)))
    if (operation == "avg") {
        quarterly_df <- quarterly_df %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                      !!wt_ := sum(!!wt_, na.rm = TRUE))
    } else if (operation == "sum") {
        quarterly_df <- quarterly_df %>%
            summarize(!!var_ := sum(!!var_, na.rm = TRUE))
    } else if (operation == "latest") {
        quarterly_df <- monthly_df %>% 
            group_by(Corridor, 
                     Zone_Group,
                     Quarter = as.character(lubridate::quarter(Month, with_year = TRUE))) %>%
            filter(Month == max(Month)) %>%
            dplyr::select(Corridor, 
                          Zone_Group,
                          Quarter,
                          !!var_,
                          !!wt_) %>%
            group_by(Corridor, Zone_Group) %>% arrange(Zone_Group, Corridor, Quarter)
    }
    
    quarterly_df %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(-lag_)
}

# Activities
# tidy_teams <- function(df) {
#     
#     # set unique id based on creation date, time, lat/long
#     df$cdn <- sapply(lapply(as.character(df$`Created by`), charToRaw), function(x) sum(as.numeric(x), na.rm = TRUE))
#     df$id <- as.numeric(mdy_hms(df$`Created on`))/1e8 + df$cdn + abs(df$Latitude) + abs(df$Longitude)
#     
#     df %>% distinct() %>%
#         
#         
#         mutate(`Task Type` = ifelse(`Task Type` == "- Preventative Maintenance", "04 - Preventative Maintenance", `Task Type`),
#                `Task Source` = ifelse(`Task Source` == "P Program", "RTOP Program", `Task Source`),
#                `Task Subtype` = ifelse(`Task Subtype` == "ection Check", "Detection Check", `Task Subtype`),
#                Priority = ifelse(Priority == "mal", "Normal", Priority)) %>%
#         
#         
#         #unite(Location, `Location Groups`, County, sep = "-") %>%
#         transmute(Id = id,
#                   Task_Type = factor(`Task Type`),
#                   Task_Subtype = factor(`Task Subtype`),
#                   Task_Source = factor(`Task Source`),
#                   Priority = factor(Priority),
#                   Status = factor(Status),
#                   #Corridor = get_corridor_name(Location),
#                   #Location = factor(Location),
#                   `Created on` = `Created on`,
#                   `Date Reported` = `Date Reported`,
#                   `Date Resolved` = `Date Resolved`,
#                   `Time To Resolve In Days` = `Time To Resolve In Days`,
#                   Maintained_by = ifelse(
#                       grepl(pattern = "District 1", `Maintained by`), "D1",
#                       ifelse(grepl(pattern = "District 6", `Maintained by`), "D6",
#                              ifelse(grepl(pattern = "Consultant|GDOT", `Maintained by`), "D6",
#                                     as.character(`Maintained by`))))) %>%
#         filter(!is.na(`Date Reported`)) %>% as_tibble()
# }

# ----- TEAMS Tasks Functions -------------------------------------------------

get_teams_locations <- function(
    locations_fn = "TEAMS_Reports/TEAMS_Locations_Report.csv") {
    
    conn <- get_atspm_connection()
    
    # Data Frames
    locs <- readr::read_csv(locations_fn)
    sigs <- dbReadTable(conn, "Signals") %>%
        as_tibble() %>%
        mutate(SignalID = factor(SignalID),
               Latitude = as.numeric(Latitude),
               Longitude = as.numeric(Longitude)) %>%
        filter(Latitude != 0)
    
    dbDisconnect(conn)
    
    corridors <- s3read_using(read_feather, 
                              object = "corridors.feather", 
                              bucket = "gdot-spm")
    
    # sf (spatial - point) objects
    locs.sp <- sf::st_as_sf(locs, coords = c("Latitude", "Longitude")) %>%
        sf::st_set_crs(4326)
    sigs.sp <- sf::st_as_sf(sigs, coords = c("Latitude", "Longitude")) %>%
        sf::st_set_crs(4326)
    
    # Join TEAMS Locations to Tasks via lat/long -- get closest, only if within 100 m
    
    # Get the row index in locs.sp that is closest to each row in ints.sp
    idx <- apply(st_distance(sigs.sp, locs.sp, byid = TRUE), 1, which.min)
    # Reorder locs.sp so each row is the one corresponding to each ints.sp row
    locs.sp <- locs.sp[idx,]
    
    # Get vector of distances between closest items (row-wise)
    dist_vector <- sf::st_distance(sigs.sp, locs.sp, by_element = TRUE)
    # Make into a data frame
    dist_dataframe <- data.frame(m = as.integer(dist_vector))
    
    # Bind data frames to map Locationid (TEAMS) to SignalID (ATSPM) with distance
    bind_cols(sigs.sp, locs.sp, dist_dataframe) %>% 
        dplyr::select(LocationId = `DB Id`, SignalID, m, `Maintained By`, City, County) %>%
        filter(m < 100)
}



tidy_teams_tasks <- function(tasks, locations, replicate = FALSE) {
    
    # Get tasks and join locations, corridors
    tasks <- tasks %>%
        dplyr::select(-`Maintained by`) %>%
        left_join(locations, by = c("LocationId")) %>%
        filter(!is.na(m),
               `Date Reported` < today(),
               (`Date Resolved` < today() | is.na(`Date Resolved`))) %>%
        mutate(SignalID = factor(SignalID)) %>%
        left_join(corridors, by = c("SignalID"))
    
    all_tasks <- tasks %>% 
        mutate(
            `Time To Resolve In Days` = floor((`Date Resolved` - `Date Reported`)/ddays(1)),
            `Time To Resolve In Hours` = floor((`Date Resolved` - `Date Reported`)/dhours(1)),
            
            SignalID = factor(SignalID),
            Task_Type = fct_explicit_na(`Task Type`, na_level = "Unspecified"),
            Task_Subtype = fct_explicit_na(`Task Subtype`, na_level = "Unspecified"),
            Task_Source = fct_explicit_na(`Task Source`, na_level = "Unspecified"),
            Priority = fct_explicit_na(Priority, na_level = "Unspecified"),
            Status = fct_explicit_na(Status, na_level = "Unspecified"),
            
            `Date Reported` = date(`Date Reported`),
            `Date Resolved` = date(`Date Resolved`),
            
            All = factor("all")) %>%
        
        dplyr::select(Due_Date = `Due Date`,
                      Task_Type,  # = `Task Type`,
                      Task_Subtype,  # = `Task Subtype`,
                      Task_Source,  # = `Task Source`,
                      Priority,
                      Status,
                      `Date Reported`,
                      `Date Resolved`,
                      `Time To Resolve In Days`,
                      `Time To Resolve In Hours`,
                      Maintained_by = `Maintained By`,
                      Owned_by = `Owned by`,
                      `Custom Identifier`,
                      `Primary Route`,
                      `Secondary Route`,
                      Created_on = `Created on`,
                      Created_by = `Created by`,
                      Modified_on = `Modified on`,
                      Modified_by = `Modified by`,
                      Latitude,
                      Longitude,
                      SignalID,
                      Zone,
                      Zone_Group,
                      Corridor,
                      All) %>%
        
        filter(!is.na(`Date Reported`),
               !is.na(Corridor),
               !(Zone_Group == "Zone 7" & `Date Reported` < "2018-05-01"),
               `Date Reported` < today())
    
    if (replicate) {
        # Replicate RTOP1 and RTOP2 as "All RTOP" and add to tasks
        # Replicate Zones so that Zone_Group == Zone
        # Replicate Zone 7m, 7d tasks as Zone 7
        all_tasks <- all_tasks %>% 
            bind_rows(all_tasks %>% 
                          filter(Zone_Group == "RTOP1") %>% 
                          mutate(Zone_Group = "All RTOP"), 
                      all_tasks %>% 
                          filter(Zone_Group == "RTOP2") %>% 
                          mutate(Zone_Group = "All RTOP"),
                      all_tasks %>%
                          filter(grepl("^Zone", Zone)) %>%
                          mutate(Zone_Group = Zone),
                      all_tasks %>%
                          filter(Zone %in% c("Zone 7m", "Zone 7d")) %>%
                          mutate(Zone_Group = "Zone 7")) %>%
            mutate(Zone_Group = factor(Zone_Group))
    }
    all_tasks
}


get_teams_tasks_from_s3 <- function(
    bucket, teams_locations_key, archived_tasks_prefix, current_tasks_key, replicate = TRUE) {
    
    # Teams Locations
    if (!file.exists(basename(teams_locations_key))) {
        aws.s3::save_object(teams_locations_key, bucket = bucket)  # "gdot-spm")
    }
    teams_locations <- read_feather(basename(teams_locations_key))
    
    
    # Keys for past years' tasks. Not the one actively updated.
    teams_keys <- aws.s3::get_bucket_df(
        bucket = bucket,  # "gdot-spm", 
        prefix = archived_tasks_prefix  # "mark/teams/tasks20"
    ) %>% 
        select(Key) %>%
        unlist(as.list(.))
    
    col_spec <- cols(
        .default = col_character(),
        `Due Date` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        `Date Reported` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        `Date Resolved` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        `Created on` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        `Modified on` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        Latitude = col_double(),
        Longitude = col_double()
    )
    
    archived_tasks <- lapply(teams_keys, 
                     function(key) {
                         s3read_using(
                             function(x) read_delim(
                                 x, 
                                 delim = ",", 
                                 col_types = col_spec, 
                                 escape_double = FALSE),
                             bucket = bucket,  # "gdot-spm", 
                             object = key)
                     }) %>% 
        bind_rows() %>% 
        select(-X1) 
    
    
    # Read the file with current teams tasks
    current_tasks <- s3read_using(
        function(x) read_delim(
            x, 
            delim = ",", 
            col_types = col_spec, 
            escape_double = FALSE), 
        bucket = bucket,  # "gdot-spm", 
        object = current_tasks_key)  # "mark/teams/tasks.csv.zip")
    
    bind_rows(archived_tasks, current_tasks) %>% 
        distinct() %>%
        tidy_teams_tasks(teams_locations, replicate)
}



get_daily_tasks_status <- function(daily_tasks_status, groupings) {
    
    #' Get number of reported and resolved tasks for each date
    #' plus cumulative reported and resolved, and tasks outstanding as of the date
    #' of a reported or resolved task, grouped by 'groupings'
    #' based on `Reported on` and `Resolved on` date fields.
    #' 
    #' @param daily_tasks_status Number reported and resolved by date.
    #'     [Zone_Group|Zone|Corridor|SignalID|Date|n_reported|n_resolved]
    #' @param groupings One of: SignalID, Zone, Zone_Group.
    #' @return A data frame [(grouping)|Date|n_reported|n_resolved].
    #' @examples
    #' get_daily_tasks_status(daily_tasks_status, "SignalID")
    
    groupings <- sapply(groupings, as.name)
    daily_tasks_status %>% 
        group_by(!!!groupings, Date) %>%
        arrange(!!!groupings, Date) %>%
        summarize(
            Reported = sum(n_reported),
            Resolved = sum(n_resolved)) %>%
        ungroup() %>%
        group_by(!!!groupings) %>%
        mutate(
            cum_Reported = cumsum(Reported),
            cum_Resolved = cumsum(Resolved),
            Outstanding = cum_Reported - cum_Resolved) %>%
        ungroup()
}


### Need to expand Zone_Group, Corridor, !!task_param_, seq(...months...) and 
### fill reported and resolved with 0, and outstanding with fill_down
### because for some of these types, there may not be any in a month

get_outstanding_tasks_by_month <- function(df, task_param_) {
    
    #' Aggregate daily outstanding tasks by month by taking the last row
    #' in each month (cumulative numbers for that month)
    #' and assigning it to the first of the month
    #' 
    #' @param df data frame of daily data.
    #' @return A data frame [Zone_Group, Corridor|Month|...].
    #' @examples
    #' get_outstanding_tasks_by_month(cor_daily_outstanding_tasks)
    
    df %>%
        group_by(
            Zone_Group, Corridor, !!task_param_, Month = floor_date(Date, unit = "month")
        ) %>%
        summarize(
            Reported = sum(Reported), 
            Resolved = sum(Resolved), 
            cum_Reported = cum_Reported[which.max(Date)],
            cum_Resolved = cum_Resolved[which.max(Date)],
            Outstanding = Outstanding[which.max(Date)]) %>%
        ungroup() %>%
        
        # Complete because not all Zone Group, Corridor, Month, param combinations
        # may be represented and we need to fill in zeros for reported and resolved
        # and the cumulative counts filled in
        complete(nesting(Zone_Group, Corridor), !!task_param_, Month, 
                 fill = list(Reported = 0, Resolved = 0)) %>%
        arrange(Corridor, Zone_Group, Month) %>%
        group_by(Zone_Group, Corridor, !!task_param_) %>%
        mutate(cum_Reported = runner::fill_run(cum_Reported, 
                                               run_for_first = TRUE, 
                                               only_within = FALSE)) %>%
        
        # Fill_run will leave NAs where the Zone Group, Corridor, Month, param combination has all NAs. 
        # Need to replace all those with zeros.
        replace_na(list(cum_Reported = 0, cum_Resolved = 0, Outstanding = 0)) %>%
        
        # Calculate deltas from month to month within 
        # Zone Group, Corridor, Month, param combinations
        group_by(Corridor, Zone_Group, !!task_param_) %>%
        mutate(
            delta.rep = (Reported - lag(Reported))/lag(Reported),
            delta.res = (Resolved - lag(Resolved))/lag(Resolved),
            delta.out = (Outstanding - lag(Outstanding))/lag(Outstanding)
        ) %>%
        ungroup()
}

# get_cor_monthly_reported <- function(teams, groupings, task_param) {
#     
#     #' Aggregate number of tasks reported by Corridor and grouping
#     #' 
#     #' @param teams TEAMS data set where tasks are replicated for All RTOP, etc.
#     #' @param groupings One or more of: SignalID, Corridor, Zone, Zone_Group.
#     #' @param task_param One of: Priority, Type, Subtype, ...
#     #' @return A data frame [Zone_Group|Zone|Corridor|Month|grouping|n]
#     #' @examples 
#     #' get_cor_monthly_reported(teams, "Priority")
#     
#     groupings <- sapply(groupings, as.name)
#     task_param = as.name(task_param)
#     teams %>% 
#         group_by(
#             !!!groupings, #Zone_Group, Zone, Corridor,
#             Month = floor_date(`Date Reported`, unit = "1 month"),
#             !!task_param
#         ) %>% 
#         count() %>%
#         arrange(!!!groupings, !!task_param, Month) %>%
#         ungroup()
# }
# 
# 
# get_cor_monthly_reported_by_task_param <- function(teams, task_param) {
#     
#     #' Get Monthly number of reported tasks by task_param
#     #' 
#     #' @param teams TEAMS data set where tasks are replicated for All RTOP, etc.
#     #' @param task_param One of: Priority, Type, Subtype, ...
#     #' @return A data frame
#     
#     by_corridor <- get_cor_monthly_reported(
#         teams, c("Zone_Group", "Zone", "Corridor"), task_param) %>%
#         select(-Zone)
#     by_zone_group <- get_cor_monthly_reported(
#         teams, "Zone_Group", task_param) %>%
#         mutate(Corridor = Zone_Group)
#     
#     bind_rows(by_corridor, by_zone_group) %>%
#         mutate(Zone_Group = factor(Zone_Group),
#                Corridor = factor(Corridor))
# }

get_sig_from_monthly_tasks <- function(monthly_data, all_corridors) {
    
    #' Filter for Zone_Group = Corridor for all Corridors
    #' for use in sig$mo$tasks_...
    #' 
    #' @param monthly_data cor_monthly data, e.g., cor_monthly_priority
    #' @param all_corridors All corridors data frame from S3
    #' @return A data frame, monthly data by corridor only, not zone or zone_group
    
    # This data frame has duplicates for each corridor for RTOP1, All RTOP, etc.
    # Take only one (in this case, the minimum as.character(Zone_Group),
    # but it doesn't matter because we're overwriting the Zone_Group anyway
    
    monthly_data %>% 
        group_by(Corridor) %>% 
        filter(as.character(Zone_Group) == min(as.character(Zone_Group))) %>%
        mutate(Zone_Group = Corridor) %>%
        filter(Corridor %in% all_corridors$Corridor) %>%
        ungroup()
}


get_outstanding_tasks_by_param <- function(teams, task_param, report_start_date) {
    
    #' Get all reported, resolved, cumulative reported, cumulative resolved
    #' and outstanding TEAMS tasks by day, month, corridor/zone group (cor)
    #' and corridor only (sig) by task parameter
    #' 
    #' @param teams TEAMS data set where tasks are replicated for All RTOP, etc.
    #' @param task_param One of: Priority, Type, Subtype, ...
    #' @param report_start_date the report start date as a date type
    #' @return A list of four data frames: cor_daily, sig_daily, cor_monthly, sig_monthly
    #' @examples
    #' get_outstanding_tasks_by_param(teams, "Priority", ymd("2018-07-01"))
    
    task_param_ <- as.name(task_param)
    # -----------------------------------------------------------------------------
    # Tasks Reported by Day, by SignalID
    tasks_reported_by_day <- teams %>% 
        group_by(Zone_Group, Zone, Corridor, SignalID, !!task_param_, Date = `Date Reported`) %>% 
        count() %>%
        ungroup() %>% 
        arrange(Zone_Group, Zone, Corridor, SignalID, Date)
    
    # Tasks Resolved by Day, by SignalID
    tasks_resolved_by_day <- teams %>% 
        filter(!is.na(`Date Resolved`)) %>%
        group_by(Zone_Group, Zone, Corridor, SignalID, !!task_param_, Date = `Date Resolved`) %>% 
        count() %>% 
        ungroup() %>% 
        arrange(Zone_Group, Zone, Corridor, SignalID, Date) 
    
    # -----------------------------------------------------------------------------
    # Tasks Reported and Resolved by Day, by SignalID
    daily_tasks_status <- full_join(
        tasks_reported_by_day, 
        tasks_resolved_by_day, 
        by = c("Zone_Group", "Zone", "Corridor", "SignalID", task_param, "Date"), 
        suffix = c("_reported", "_resolved")
    ) %>%
        ungroup() %>%
        replace_na(list(n_reported = 0, n_resolved = 0))
    
    # Tasks Reported and Resolved by Day, by Corridor
    cor_daily_tasks_status <- get_daily_tasks_status(
        daily_tasks_status, 
        groupings = c("Zone_Group", "Zone", "Corridor", task_param)) %>%
        filter(!Zone_Group %in% c("All RTOP", "RTOP1", "RTOP2", "Zone 7")) %>%
        select(-Zone)
    
    # Tasks Reported and Resolved by Day, by Zone Group
    zone_group_daily_tasks_status <- daily_tasks_status %>%
        get_daily_tasks_status(groupings = c("Zone_Group", task_param)) %>%
        mutate(Corridor = Zone_Group)
    

    cor_daily_outstanding_tasks <- bind_rows(
        cor_daily_tasks_status,
        zone_group_daily_tasks_status) %>%
        mutate(Zone_Group = factor(Zone_Group),
               Corridor = factor(Corridor)) %>%
        filter(Date >= report_start_date,
               Date <= today())
    
    sig_daily_outstanding_tasks <- get_sig_from_monthly_tasks(
        cor_daily_tasks_status, all_corridors) %>%
        filter(Date >= report_start_date,
               Date <= today())
    
    cor_monthly_outstanding_tasks <- get_outstanding_tasks_by_month(cor_daily_outstanding_tasks, task_param_)
    sig_monthly_outstanding_tasks <- get_outstanding_tasks_by_month(sig_daily_outstanding_tasks, task_param_)
    
    # Return Value
    list("cor_daily" = cor_daily_outstanding_tasks, 
         "sig_daily" = sig_daily_outstanding_tasks, 
         "cor_monthly" = cor_monthly_outstanding_tasks, 
         "sig_monthly" = sig_monthly_outstanding_tasks)
}





get_outstanding_tasks_by_day_range <- function(teams, report_start_date, first_of_month) {

    #' Get number of tasks that have been outstanding for
    #' 0-45 days, 45-90
    #' 
    #' @param teams TEAMS data set where tasks are replicated for All RTOP, etc.
    #' @param report_start_date ...
    #' @param first_of_month date for the month as the first of the month. for past
    #' months, all resolved dates after the end of this month are considered NA
    #' for that month's oustanding tasks, as a date object
    #' @return A data frame [Zone_Group|Corridor|Month|`0-45`|`45-90`|over90|mttr]
    #' @examples
    #' get_outstanding_tasks_by_day_range(ymd("2018-07-01"), teams)
    
    last_day <- first_of_month + months(1) - days(1)
    
    mttr <- teams %>%
        filter(`Date Reported` <= last_day,
               `Date Resolved` < last_day,
               `Date Reported` > report_start_date,
               !is.na(`Date Resolved`)) %>% 
        mutate(
            ttr = as.numeric(`Date Resolved` - `Date Reported`, units = "days"),
            Month = first_of_month) %>%
        group_by(Zone_Group, Zone, Corridor, Month) %>% 
        summarize(
            mttr = mean(ttr),
            num_resolved = n()) %>%
        ungroup()
    
    over45 <- teams %>% 
        filter(`Date Reported` < last_day - days(45),
               (is.na(`Date Resolved`) | `Date Resolved` > last_day)) %>% 
        group_by(Zone_Group, Zone, Corridor) %>% 
        count() %>% 
        rename(over45 = n) %>% 
        ungroup()
    
    over90 <- teams %>% 
        filter(`Date Reported` < last_day - days(90),
               (is.na(`Date Resolved`) | `Date Resolved` > last_day)) %>% 
        group_by(Zone_Group, Zone, Corridor) %>% 
        count() %>% 
        rename(over90 = n) %>% 
        ungroup()
    
    all <- teams %>% 
        filter((is.na(`Date Resolved`) | `Date Resolved` > last_day),
               `Date Reported` <= last_day) %>% 
        group_by(Zone_Group, Zone, Corridor) %>% 
        count() %>% 
        rename(all_outstanding = n) %>% 
        ungroup()
    
    outst <- list(over45, over90, all, mttr) %>% 
        reduce(left_join, by = c("Zone_Group", "Zone", "Corridor")) %>% 
        replace_na(list(over45 = 0, 
                        over90 = 0, 
                        all_outstanding = 0)) %>% 
        transmute(
            Zone_Group, Corridor, 
            Month = first_of_month,
            `0-45` = all_outstanding - over45, 
            `45-90` = over45 - over90, 
            over45,
            over90,
            num_outstanding = all_outstanding,
            num_resolved,
            mttr)
    
    bind_rows(
        outst %>%
            filter(!Zone_Group %in% c("All RTOP", "RTOP1", "RTOP2", "Zone 7")),
        outst %>% 
            mutate(Corridor = Zone_Group) %>%
            group_by(Zone_Group, Corridor, Month) %>% 
            summarize(
                `0-45` = sum(`0-45`),
                `45-90` = sum(`45-90`),
                over45 = sum(over45),
                over90 = sum(over90),
                mttr = weighted.mean(mttr, num_resolved, na.rm = TRUE),
                num_outstanding = sum(num_outstanding),
                num_resolved = sum(num_resolved))
    ) %>%
        mutate(Zone_Group = factor(Zone_Group),
               Corridor = factor(Corridor))
}















get_outstanding_events <- function(teams, group_var, spatial_grouping="Zone_Group") {
    
    # group_var is either All, Type, Subtype, ...
    # spatial grouping is either Zone_Group or Corridor
    rep <- teams %>% 
        filter(!is.na(`Date Reported`)) %>%
        mutate(Month = floor_date(`Date Reported`, "1 month")) %>%
        arrange(Month) %>%
        group_by_(group_var, spatial_grouping, quote(Month)) %>%
        summarize(Rep = n()) %>% 
        group_by_(spatial_grouping, group_var) %>%
        mutate(cumRep = cumsum(Rep))
    
    res <- teams %>% 
        filter(!is.na(`Date Resolved`)) %>%
        mutate(Month = `Date Resolved` - days(day(`Date Resolved`) -1)) %>%
        arrange(Month) %>%
        group_by_(group_var, spatial_grouping, quote(Month)) %>%
        summarize(Res = n()) %>% 
        group_by_(spatial_grouping, group_var) %>%
        mutate(cumRes = cumsum(Res))
    
    left_join(rep, res) %>% 
        fill(cumRes, .direction = "down") %>% 
        replace_na(list(Rep = 0, cumRep = 0, 
                        Res = 0, cumRes = 0)) %>% 
        group_by_(spatial_grouping, group_var) %>%
        mutate(outstanding = cumRep - cumRes) %>%
        ungroup()
}

readRDS_multiple <- function(pattern) {
    lf <- list.files(pattern = pattern)
    lf <- lf[grepl(".rds", lf)]
    bind_rows(lapply(lf, readRDS))
}


# db_build_data_for_signal_dashboard_ec2 <- function(month_abbrs, corridors, pth = '.', 
#                                                    upload_to_s3 = FALSE) {
#     
#     sc <- get_spark_context()
#     
#     insert_to_db <- function(table_name, month_abbr, signalids) {
#         result <- tryCatch({
#             keys <- glue("s3a://gdot-spm/mark/{table_name}/date={month_abbr}*")
#             df <- spark_read_parquet(sc, name = table_name, path = keys) %>% dplyr::select(-starts_with("__"))
#             
#             print(table_name)
# 
#             lapply(signalids, function(sid) {
#                 fn <- file.path(pth, month_abbr, glue("{sid}.db"))
#                 print(c(table_name, fn))
#                 conn <- dbConnect(RSQLite::SQLite(), fn)
#                 dbWriteTable(conn, table_name, filter(df, SignalID == sid)  %>% collect(), append = TRUE)
#                 dbDisconnect(conn)
#             }) #, mc.cores = parallel::detectCores() - 1)
#             
#         }, error = function(e) {
#             print(e)
#             print(glue("No data for {prefix}{month_abbr}"))
#         })
#     }
#     
#     signalids <- levels(corridors$SignalID)
#     
#     
#     lapply(month_abbrs, function(month_abbr) {
#         
# 
#         print(month_abbr)
#         
#         if (!dir.exists(file.path(pth, month_abbr))) {
#             dir.create(file.path(pth, month_abbr))
#         }
#         
#         mclapply(signalids, function(sid) { 
#             file.remove(file.path(pth, month_abbr, glue("{sid}.db"))) 
#         })
#         
#         
#         insert_to_db(table_name = "counts_1hr", month_abbr, signalids)
#         insert_to_db(table_name = "filtered_counts_1hr", month_abbr, signalids)
#         insert_to_db(table_name = "adjusted_counts_1hr", month_abbr, signalids)
#         insert_to_db(table_name = "detector_uptime_pd", month_abbr, signalids)
#         insert_to_db(table_name = "comm_uptime", month_abbr, signalids)
#         insert_to_db(table_name = "vehicles_pd", month_abbr, signalids)
#         insert_to_db(table_name = "vehicles_ph", month_abbr, signalids)
#         insert_to_db(table_name = "throughput", month_abbr, signalids)
#         insert_to_db(table_name = "arrivals_on_green", month_abbr, signalids)
#         insert_to_db(table_name = "split_failures", month_abbr, signalids)
#         insert_to_db(table_name = "queue_spillback", month_abbr, signalids)
#         
#         # -- copy this into the codebase. then update plotting function to match the new table names
#         
#         fns <- list.files(file.path(pth, month_abbr))
#         lapply(fns, function(fn) {
#             print(fn)
#             aws.s3::put_object(file = fn,
#                                object = glue("signal_dashboards/{month_abbr}/{basename(fn)}"),
#                                bucket = "gdot-spm")
#         })
#         
#         lapply(signalids, function(sid) {
#             print(sid)
#             aws.s3::put_object(file = file.path(pth, month_abbr, glue("{sid}.db")),
#                                object = glue("signal_dashboards/{month_abbr}/{sid}.db"),
#                                bucket = "gdot-spm")
#         })
#     })
# }

# db_build_data_for_signal_dashboard <- function(month_abbrs, corridors, pth = '.', 
#                                                upload_to_s3 = FALSE) {
#     
#     usable_cores <- get_usable_cores()
#     
#     insert_to_db <- function(prefix, month_abbr, dbtable, signalids, daily) {
#         result <- tryCatch({
#             df <- f(prefix, month_abbr, daily = daily)
#             
#             mclapply(signalids, function(sid) {
#                 fn <- file.path(pth, glue("{sid}.db"))
#                 print(c(prefix, fn))
#                 conn <- dbConnect(RSQLite::SQLite(), fn)
#                 dbWriteTable(conn, dbtable, filter(df, SignalID == sid), append = TRUE)
#                 dbDisconnect(conn)
#             }, usable_cores) #mc.cores = ceiling(parallel::detectCores()*1/2))
#             
#         }, error = function(e) {
#             print(e)
#             print(glue("No data for {prefix}{month_abbr}"))
#         })
#     }
#     
#     signalids <- levels(corridors$SignalID)
#     
#     mclapply(signalids, function(sid) { 
#         file.remove(file.path(pth, glue("{sid}.db"))) 
#     })
#     
#     lapply(month_abbrs, function(month_abbr) {
#         
#         print(month_abbr)
#         
#         insert_to_db(prefix = "counts_1hr_", month_abbr, dbtable = "rc", signalids, daily = TRUE)
#         insert_to_db(prefix = "filtered_counts_1hr_", month_abbr, dbtable = "fc", signalids, daily = FALSE)
#         insert_to_db(prefix = "ddu_", month_abbr, dbtable = "ddu", signalids, daily = FALSE)
#         insert_to_db(prefix = "cu_", month_abbr, dbtable = "cu", signalids, daily = FALSE)
#         insert_to_db(prefix = "vpd_", month_abbr, dbtable = "vpd", signalids, daily = FALSE)
#         insert_to_db(prefix = "vph_", month_abbr, dbtable = "vph", signalids, daily = FALSE)
#         insert_to_db(prefix = "tp_", month_abbr, dbtable = "tp", signalids, daily = FALSE)
#         insert_to_db(prefix = "aog_", month_abbr, dbtable = "aog", signalids, daily = FALSE)
#         insert_to_db(prefix = "sf_", month_abbr, dbtable = "sf", signalids, daily = FALSE)
#         insert_to_db(prefix = "qs_", month_abbr, dbtable = "qs", signalids, daily = FALSE)
#     })
#     
#     mclapply(signalids, function(sid) {
#         aws.s3::put_object(file = file.path(pth, glue("{sid}.db")),
#                            object = glue("signal_dashboards/{sid}.db"),
#                            bucket = "gdot-devices")
#     })
# }

# build_data_for_signal_dashboard <- function(month_abbrs, 
#                                             corridors, 
#                                             pth = ".", 
#                                             upload_to_s3 = FALSE) {
#     
#     write_signal_data <- function(df, data_name) {
#         
#         sid <- as.character(df$SignalID[1])
#         fn <- file.path(pth, glue("{sid}.rds"))
#         if (file.exists(fn)) {
#             data <- readRDS(fn)
#         } else {
#             data <- list()
#         }
#         if (!data_name %in% names(data)) {
#             data[[data_name]] <- data.frame()
#             print(glue("{sid} {data_name} is new"))
#         }
#         data[[data_name]] <- rbind(data[[data_name]], df)
#         saveRDS(data, fn)
#         return(head(df,1))
#     }
#     
#     
#     
#     lapply(month_abbrs, function(month_abbr) { 
#         
#         print(month_abbr)
#         
#         result <- tryCatch({
#             rc <- f("counts_1hr_", month_abbr, daily = TRUE) %>% 
#                 filter(SignalID %in% levels(corridors$SignalID))
#             rc %>% group_by(SignalID) %>% do(write_signal_data(., "rc"))
#             rm(rc); gc()
#         }, error = function(e) {
#             print(e)
#             print(glue("No data for raw counts for {month_abbr}"))
#         }, finally = {
#         })
#         
#         result <- tryCatch({
#             fc <- f("filtered_counts_1hr_", month_abbr)
#             fc %>% group_by(SignalID) %>% do(write_signal_data(., "fc"))
#             rm(fc); gc()
#         }, error = function(e) {
#             print(e)
#             print(glue("No data for filtered counts for {month_abbr}"))
#         })
#         
#         result <- tryCatch({
#             ddu <- f("ddu_", month_abbr)
#             ddu %>% group_by(SignalID) %>% do(write_signal_data(., "ddu"))
#             rm(ddu); gc()
#         }, error = function(e) {
#             print(glue("No data for detector uptime for {month_abbr}"))
#         })
#         
#         result <- tryCatch({
#             cu <- f("cu_", month_abbr)
#             cu %>% group_by(SignalID) %>% do(write_signal_data(., "cu"))
#             rm(cu); gc()
#         }, error = function(e) {
#             print(glue("No data for comm uptime for {month_abbr}"))
#         })
#         
#         result <- tryCatch({
#             vpd <- f("vpd_", month_abbr)
#             vpd %>% group_by(SignalID) %>% do(write_signal_data(., "vpd"))
#             rm(vpd); gc()
#         }, error = function(e) {
#             print(glue("No data for raw_counts for {month_abbr}"))
#         })
#         
#         result <- tryCatch({
#             tp <- f("tp_", month_abbr)
#             tp %>% group_by(SignalID) %>% do(write_signal_data(., "tp"))
#             rm(tp); gc()
#         }, error = function(e) {
#             print(glue("No data for throughput for {month_abbr}"))
#         })
#         
#         result <- tryCatch({
#             aog <- f("aog_", month_abbr)
#             aog %>% group_by(SignalID) %>% do(write_signal_data(., "aog"))
#             rm(aog); gc()
#         }, error = function(e) {
#             print(glue("No data for arrivals on green for {month_abbr}"))
#         })
#         
#         result <- tryCatch({
#             sf <- f("sf_", month_abbr)
#             sf %>% group_by(SignalID) %>% do(write_signal_data(., "sf"))
#             rm(sf); gc()
#         }, error = function(e) {
#             print(glue("No data for split failures for {month_abbr}"))
#         })
#         
#         result <- tryCatch({
#             qs <- f("qs_", month_abbr)
#             qs %>% group_by(SignalID) %>% do(write_signal_data(., "qs"))
#             rm(qs); gc()
#         }, error = function(e) {
#             print(glue("No data for queue spillback for {month_abbr}"))
#         })
#     })
#     if (upload_to_s3 == TRUE) {
#         lapply(list.files(pth, pattern = "*.rds"), function(fn) {
#             aws.s3::put_object(file = file.path(pth, fn), 
#                                object = glue("signal_dashboards/{fn}"), 
#                                bucket = "gdot-devices")
#         })
#     }
# }










## Gui's improved function
# Function to add Probabilities
get_pau <- function(df, corridors) {
    
    begin_date <- min(df$Date)  #ymd("2018-08-01")
    
    corrs <- corridors %>% group_by(SignalID) %>% summarize(Asof = min(Asof))
    
    # ped_config <- lapply(unique(df$Date), function(d) {
    #     get_ped_config(d) %>% 
    #         mutate(Date = d) %>%
    #         filter(SignalID %in% df$SignalID)
    # }) %>% 
    #     bind_rows() %>%
    #     mutate(SignalID = factor(SignalID),
    #            Detector = factor(Detector),
    #            CallPhase = factor(CallPhase))
    doParallel::registerDoParallel(cores = usable_cores)
    
    ped_config <- foreach(d = unique(df$Date), .combine = rbind) %dopar% {
        
        get_ped_config(d) %>% 
            mutate(Date = d) %>%
            filter(SignalID %in% df$SignalID)
    } %>% 
        mutate(SignalID = factor(SignalID),
               Detector = factor(Detector),
               CallPhase = factor(CallPhase)) 
    
    papd <- df %>% 
        
        full_join(ped_config, by = c("SignalID", "Detector", "CallPhase", "Date")) %>%
        transmute(SignalID = factor(SignalID),
                  Date = Date,
                  CallPhase = factor(CallPhase),
                  Detector = factor(Detector),
                  Week = week(Date),
                  DOW = wday(Date),
                  papd = papd) %>%
        filter(CallPhase != 0) %>%
        replace_na(list(papd = 0)) %>%
        arrange(SignalID, Detector, CallPhase, Date) %>%
        group_by(SignalID, Detector, CallPhase) %>%
        mutate(s0 = runner::streak_run(papd), 
               s0 = if_else(papd > 0, as.integer(0), s0), #--------------this is new
               ## New code to prevent old report months from updating based on new report month
               ## Fills max streak up to the last day of the month
               ms = ifelse(s0 >= lead(s0) | Date == rollback(Date), s0, NA)) %>% 
        mutate(ms = if_else(Date == max(Date), s0, ms)) %>%
        fill(ms, .direction = "up") %>%
        ungroup() %>%
        mutate(Week = week(Date),
               DOW = wday(Date)) %>%
        left_join(corrs, by = c("SignalID")) %>%
        replace_na(list(Asof = begin_date)) %>%
        filter(Date >= Asof) %>%
        dplyr::select(-Asof) %>%
        mutate(SignalID = factor(SignalID))
    
    modres <- papd %>% 
        group_by(SignalID, Detector, CallPhase) %>% 
        mutate(cnt = n()) %>% 
        ungroup() %>% 
        filter(cnt > 2) %>%
        nest(-c(SignalID, Detector, CallPhase)) %>% 
        mutate(model = purrr::map(data, function(x) {fitdist(x$papd, "exp", method = "mme")}))
    
    pz <- modres %>% 
        mutate(lambda = purrr::map(model, function(x) {x$estimate})) %>% 
        unnest(lambda) %>%
        dplyr::select(-(data:model)) %>%
        mutate(mean = lambda ** -1, # property of exponential distribution
               var = lambda ** -2) # property of exponential distribution
    
    pau <- left_join(papd, pz, by = c("SignalID", "Detector", "CallPhase")) %>%
        ungroup() %>%
        mutate(
            SignalID = as.character(SignalID),
            Detector = as.character(Detector),
            CallPhase = as.character(CallPhase),
            probbad = if_else(papd == 0,  1 - exp(-lambda * ms), 0)
        ) %>%
        transmute(
            SignalID = factor(SignalID),
            Detector = factor(Detector),
            CallPhase = factor(CallPhase),
            Date = Date,
            DOW = DOW,
            Week = Week,
            probbad = probbad,
            uptime = if_else(probbad > 0.99, 0, 1),
            all = 1
        )
    
    pau
}
dbUpdateTable <- function(conn, table_name, df, asof = NULL) {
    # per is Month|Date|Hour|Quarter
    if ("Date" %in% names(df)) {
        per = "Date"
    } else if ("Hour" %in% names(df)) {
        per = "Hour"
    } else if ("Month" %in% names(df)) {
        per = "Month"
    } else if ("Quarter" %in% names(df)) {
        per = "Quarter"
    }
    
    if (!is.null(asof)) {
        df <- df[df[[per]] >= asof,]
    }
    df <- df[!is.na(df[[per]]),]
    
    min_per <- min(df[[per]])
    max_per <- max(df[[per]])
    
    count_query <- sql(glue("SELECT COUNT(*) FROM {table_name} WHERE {per} >= '{min_per}'"))
    num_del <- dbGetQuery(conn, count_query)[1,1]
    
    delete_query <- sql(glue("DELETE FROM {table_name} WHERE {per} >= '{min_per}'"))
    print(delete_query)
    print(glue("{num_del} records being deleted."))
    
    dbSendQuery(conn, delete_query)
    RMySQL::dbWriteTable(conn, table_name, df, append = TRUE, row.names = FALSE)
    print(glue("{nrow(df)} records uploaded."))
}

