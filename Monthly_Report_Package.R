
# Monthly_Report_Package.R

source("Monthly_Report_Package_init.R")


# # WATCHDOG ###########################################################

print(glue("{Sys.time()} watchdog alerts [1 of 23]"))

tryCatch({
    # -- Alerts: detector downtime --
    
    bad_det <- dbGetQuery(conn, sql(glue(paste(
        "select signalid, detector, date",
        "from {conf$athena$database}.bad_detectors",
        "where date >='{today() - days(100)}'")))
    ) %>%
        transmute(
            SignalID = factor(signalid),
            Detector = factor(detector),
            Date = date(date)
        ) %>%
        as_tibble() 
    
    det_config <- mclapply(sort(unique(bad_det$Date)), mc.cores = usable_cores, function(date_) {
        get_det_config(date_) %>% 
            transmute(
                SignalID, 
                CallPhase, 
                Detector, 
                ApproachDesc, 
                LaneNumber, 
                Date = date_)
    }) %>% bind_rows() %>%
        mutate(
            SignalID = factor(SignalID),
            CallPhase = factor(CallPhase),
            Detector = factor(Detector))
    
    bad_det <- bad_det %>% 
        left_join(
            det_config, by = c("SignalID", "Detector", "Date")
        ) %>%
        left_join(
            dplyr::select(corridors, Zone_Group, Zone, Corridor, SignalID, Name), 
            by = c("SignalID")
        ) %>%
        filter(!is.na(Corridor)) %>%
        transmute(
            Zone_Group, 
            Zone, 
            Corridor,
            SignalID = factor(SignalID), 
            CallPhase = factor(CallPhase), 
            Detector = factor(Detector),
            Date, 
            Alert = factor("Bad Vehicle Detection"), 
            Name = factor(if_else(Corridor=="Ramp Meter", sub("@", "-", Name), Name)),
            ApproachDesc = if_else(is.na(ApproachDesc), "", as.character(glue("{trimws(ApproachDesc)} Lane {LaneNumber}")))
        )
    
    # Zone_Group | Zone | Corridor | SignalID/CameraID | CallPhase | DetectorID | Date | Alert | Name
    
    s3write_using(
        bad_det,
        FUN = write_fst, 
        object = "mark/watchdog/bad_detectors.fst",
        bucket = conf$bucket,
        opts = list(multipart = TRUE))
    rm(bad_det)
    rm(det_config)
    
    # -- Alerts: pedestrian detector downtime --
    
    bad_ped <- dbGetQuery(conn, sql(glue(paste(
        "select signalid, detector, date from {conf$athena$database}.bad_ped_detectors", 
        "where date >='{today() - days(100)}'")))) %>%
        transmute(
            SignalID = factor(signalid),
            Detector = factor(detector),
            Date = date(date)
        ) %>%
        distinct() %>%
        #filter(Date > today() - days(100)) %>%
        as_tibble() %>%
        left_join(
            dplyr::select(corridors, Zone_Group, Zone, Corridor, SignalID, Name), 
            by = c("SignalID")
        ) %>%
        transmute(Zone_Group,
                  Zone,
                  Corridor = factor(Corridor),
                  SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  Date,
                  Alert = factor("Bad Ped Detection"),
                  Name = factor(Name)
        )
    
    s3write_using(
        bad_ped,
        FUN = write_fst,
        object = "mark/watchdog/bad_ped_detectors.fst",
        bucket = conf$bucket)
    rm(bad_ped)
    
    # -- Alerts: CCTV downtime --
    
    bad_cam <- tbl(conn, sql(glue(paste(
        "select cameraid, date", 
        "from {conf$athena$database}.cctv_uptime",
        "where size = 0")))) %>%
        #filter(size == 0) %>%
        collect() %>%
        transmute(
            CameraID = factor(cameraid),
            Date = date(date)
        ) %>%
        filter(Date > today() - months(9)) %>%
        left_join(cam_config, by = c("CameraID")) %>%
        filter(Date > As_of_Date) %>%
        #left_join(distinct(all_corridors, Zone_Group, Zone, Corridor), by = c("Corridor")) %>%
        transmute(
            Zone_Group, 
            Zone,
            Corridor = factor(Corridor),
            SignalID = factor(CameraID), 
            CallPhase = factor(0), 
            Detector = factor(0),
            Date, Alert = factor("No Camera Image"), 
            Name = factor(Location)
        )
    
    s3write_using(
        bad_cam,
        FUN = write_fst,
        object = "mark/watchdog/bad_cameras.fst",
        bucket = conf$bucket)
    rm(bad_cam)
    
    # -- Watchdog Alerts --
    
    # Nothing to do here
    
    
    # -- --------------- --
    
    # Zone_Group | Zone | Corridor | SignalID/CameraID | CallPhase | DetectorID | Date | Alert | Name
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})




# # DETECTOR UPTIME ###########################################################

print(glue("{Sys.time()} Vehicle Detector Uptime [2 of 23]"))

tryCatch({
    cb <- function(x) {
        get_avg_daily_detector_uptime(x) %>%
            mutate(Date = date(Date))
    }
    
    avg_daily_detector_uptime <- s3_read_parquet_parallel(
        bucket = conf$bucket,
        table_name = "detector_uptime_pd",
        start_date = wk_calcs_start_date,
        end_date = report_end_date,
        signals_list = signals_list,
        callback = cb
    ) %>%
        mutate(
            SignalID = factor(SignalID)
        )
    
    plan(sequential)
    plan(multiprocess)
    
    cor_avg_daily_detector_uptime <- 
        get_cor_avg_daily_detector_uptime(avg_daily_detector_uptime, corridors)
    sub_avg_daily_detector_uptime <- 
        (get_cor_avg_daily_detector_uptime(avg_daily_detector_uptime, subcorridors) %>%
             filter(!is.na(Corridor)))
    
    weekly_detector_uptime <- 
        get_weekly_detector_uptime(avg_daily_detector_uptime)
    cor_weekly_detector_uptime <- 
        get_cor_weekly_detector_uptime(weekly_detector_uptime, corridors)
    sub_weekly_detector_uptime <- 
        (get_cor_weekly_detector_uptime(weekly_detector_uptime, subcorridors) %>%
             filter(!is.na(Corridor)))
    
    monthly_detector_uptime <- 
        get_monthly_detector_uptime(avg_daily_detector_uptime)
    cor_monthly_detector_uptime <- 
        get_cor_monthly_detector_uptime(avg_daily_detector_uptime, corridors)
    sub_monthly_detector_uptime <- 
        (get_cor_monthly_detector_uptime(avg_daily_detector_uptime, subcorridors) %>%
             filter(!is.na(Corridor)))
    
    addtoRDS(
        avg_daily_detector_uptime, "avg_daily_detector_uptime.rds", "uptime", 
        report_start_date, calcs_start_date)
    addtoRDS(
        weekly_detector_uptime, "weekly_detector_uptime.rds", "uptime", 
        report_start_date, wk_calcs_start_date)
    addtoRDS(
        monthly_detector_uptime, "monthly_detector_uptime.rds", "uptime", 
        report_start_date, calcs_start_date)
    
    addtoRDS(
        cor_avg_daily_detector_uptime, "cor_avg_daily_detector_uptime.rds", "uptime", 
        report_start_date, calcs_start_date)
    addtoRDS(
        cor_weekly_detector_uptime, "cor_weekly_detector_uptime.rds", "uptime", 
        report_start_date, wk_calcs_start_date)
    addtoRDS(
        cor_monthly_detector_uptime, "cor_monthly_detector_uptime.rds", "uptime", 
        report_start_date, calcs_start_date)
    
    addtoRDS(
        sub_avg_daily_detector_uptime, "sub_avg_daily_detector_uptime.rds", "uptime", 
        report_start_date, calcs_start_date)
    addtoRDS(
        sub_weekly_detector_uptime, "sub_weekly_detector_uptime.rds", "uptime", 
        report_start_date, wk_calcs_start_date)
    addtoRDS(
        sub_monthly_detector_uptime, "sub_monthly_detector_uptime.rds", "uptime", 
        report_start_date, calcs_start_date)
    
    # rm(ddu)
    # rm(daily_detector_uptime)
    rm(avg_daily_detector_uptime)
    rm(weekly_detector_uptime)
    rm(monthly_detector_uptime)
    rm(cor_avg_daily_detector_uptime)
    rm(cor_weekly_detector_uptime)
    rm(cor_monthly_detector_uptime)
    rm(sub_avg_daily_detector_uptime)
    rm(sub_weekly_detector_uptime)
    rm(sub_monthly_detector_uptime)
    # gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})

# DAILY PEDESTRIAN DETECTOR UPTIME ###############################################

print(glue("{Sys.time()} Ped Detector Uptime [3 of 23]"))

tryCatch({

    # papd <- s3_read_parquet_parallel(
    #     bucket = conf$bucket,
    #     table_name = "ped_actuations_pd",
    #     start_date = report_start_date, # We have to look at the entire report period for pau
    #     end_date = report_end_date,
    #     signals_list = signals_list
    # ) %>%
    #     replace_na(list(CallPhase = 0)) %>%
    #     mutate(
    #         SignalID = factor(SignalID),
    #         CallPhase = factor(CallPhase),
    #         Date = date(Date),
    #         papd = as.numeric(papd)
    #     )

    counts_ped_hourly <- s3_read_parquet_parallel(
        bucket = conf$bucket,
        table_name = "counts_ped_1hr",
        start_date = report_start_date, # We have to look at the entire report period for pau
        end_date = report_end_date,
        signals_list = signals_list
    ) %>%
        #replace_na(list(CallPhase = 0)) %>%
        filter(!is.na(CallPhase)) %>%    # Added 1/14/20 to perhaps exclude non-programmed ped detectors
        mutate(
            SignalID = factor(SignalID),
            Detector = factor(Detector),
            CallPhase = factor(CallPhase),
            Date = date(Date),
            DOW = wday(Date), 
            Week = week(Date),
            vol = as.numeric(vol)
        )
    
    counts_ped_daily <- counts_ped_hourly %>%
        group_by(SignalID, Date, DOW, Week, Detector, CallPhase) %>% 
        summarize(papd = sum(vol, na.rm = TRUE)) %>%
        ungroup()

    papd <- counts_ped_daily
    paph <- counts_ped_hourly %>% 
        filter(Date >= report_start_date) %>%   # TODO: Check this.
        rename(Hour = Timeperiod,
               paph = vol)
    rm(counts_ped_daily)
    rm(counts_ped_hourly)
    
    pau <- get_pau(papd, corridors)

    # We have do to this here rather than in Monthly_Report_Calcs
    # because we need the whole time series to calculate ped detector uptime
    # based on the exponential distribution method.
    get_bad_ped_detectors(pau) %>%
        filter(Date > ymd(report_end_date) - days(90)) %>%
    
        s3_upload_parquet_date_split(
            bucket = conf$bucket,
            prefix = "bad_ped_detectors",
            table_name = "bad_ped_detectors",
            athena_db = conf$athena$database)

    # Hack to make the aggregation functions work
    addtoRDS(
        pau, "pa_uptime.rds", "uptime",
        report_start_date, report_start_date)
    pau <- pau %>% 
        mutate(CallPhase = Detector)

        
    daily_pa_uptime <- get_daily_avg(pau, "uptime", peak_only = FALSE)
    weekly_pa_uptime <- get_weekly_avg_by_day(pau, "uptime", peak_only = FALSE)
    monthly_pa_uptime <- get_monthly_avg_by_day(pau, "uptime", "all", peak_only = FALSE)
    
    cor_daily_pa_uptime <- 
        get_cor_weekly_avg_by_day(daily_pa_uptime, corridors, "uptime")
    sub_daily_pa_uptime <- 
        get_cor_weekly_avg_by_day(daily_pa_uptime, subcorridors, "uptime") %>%
             filter(!is.na(Corridor))
    
    cor_weekly_pa_uptime <- 
        get_cor_weekly_avg_by_day(weekly_pa_uptime, corridors, "uptime")
    sub_weekly_pa_uptime <- 
        get_cor_weekly_avg_by_day(weekly_pa_uptime, subcorridors, "uptime") %>%
             filter(!is.na(Corridor))
    
    cor_monthly_pa_uptime <- 
        get_cor_monthly_avg_by_day(monthly_pa_uptime, corridors, "uptime")
    sub_monthly_pa_uptime <- 
        get_cor_monthly_avg_by_day(monthly_pa_uptime, subcorridors, "uptime") %>%
             filter(!is.na(Corridor))

    addtoRDS(
        daily_pa_uptime, "daily_pa_uptime.rds",  "uptime",
        report_start_date, report_start_date)
    addtoRDS(
        cor_daily_pa_uptime, "cor_daily_pa_uptime.rds",  "uptime",
        report_start_date, report_start_date)
    addtoRDS(
        sub_daily_pa_uptime, "sub_daily_pa_uptime.rds",  "uptime",
        report_start_date, report_start_date)
    
    addtoRDS(
        weekly_pa_uptime, "weekly_pa_uptime.rds",  "uptime",
        report_start_date, report_start_date)
    addtoRDS(
        cor_weekly_pa_uptime, "cor_weekly_pa_uptime.rds",  "uptime",
        report_start_date, report_start_date)
    addtoRDS(
        sub_weekly_pa_uptime, "sub_weekly_pa_uptime.rds",  "uptime",
        report_start_date, report_start_date)
    
    addtoRDS(
        monthly_pa_uptime, "monthly_pa_uptime.rds",  "uptime",
        report_start_date, report_start_date)
    addtoRDS(
        cor_monthly_pa_uptime, "cor_monthly_pa_uptime.rds",  "uptime",
        report_start_date, report_start_date)
    addtoRDS(
        sub_monthly_pa_uptime, "sub_monthly_pa_uptime.rds",  "uptime",
        report_start_date, report_start_date)
    
    # rm(papd)
    # rm(bad_ped_detectors)
    rm(pau)
    rm(daily_pa_uptime)
    rm(weekly_pa_uptime)
    rm(monthly_pa_uptime)
    rm(cor_daily_pa_uptime)
    rm(cor_weekly_pa_uptime)
    rm(cor_monthly_pa_uptime)
    rm(sub_daily_pa_uptime)
    rm(sub_weekly_pa_uptime)
    rm(sub_monthly_pa_uptime)
    # gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})


# DAILY PEDESTRIAN ACTIVATIONS ################################################

print(glue("{Sys.time()} Daily Pedestrian Activations [4 of 23]"))

tryCatch({
    
    weekly_papd <- get_weekly_papd(papd)
    
    # Group into corridors --------------------------------------------------------
    cor_weekly_papd <- get_cor_weekly_papd(weekly_papd, corridors)
    
    # Group into subcorridors --------------------------------------------------------
    sub_weekly_papd <- get_cor_weekly_papd(weekly_papd, subcorridors) %>%
        filter(!is.na(Corridor))
    
    # Monthly volumes for bar charts and % change ---------------------------------
    monthly_papd <- get_monthly_papd(papd)
    
    # Group into corridors
    cor_monthly_papd <- get_cor_monthly_papd(monthly_papd, corridors)
    
    # Group into subcorridors
    sub_monthly_papd <- get_cor_monthly_papd(monthly_papd, subcorridors) %>%
        filter(!is.na(Corridor))
    
    # Monthly % change from previous month by corridor ----------------------------
    addtoRDS(weekly_papd, "weekly_papd.rds", "papd", report_start_date, report_start_date)
    addtoRDS(monthly_papd, "monthly_papd.rds", "papd", report_start_date, report_start_date)
    addtoRDS(cor_weekly_papd, "cor_weekly_papd.rds", "papd", report_start_date, report_start_date)
    addtoRDS(cor_monthly_papd, "cor_monthly_papd.rds", "papd", report_start_date, report_start_date)
    addtoRDS(sub_weekly_papd, "sub_weekly_papd.rds", "papd", report_start_date, report_start_date)
    addtoRDS(sub_monthly_papd, "sub_monthly_papd.rds", "papd", report_start_date, report_start_date)
    
    rm(papd)
    rm(weekly_papd)
    rm(monthly_papd)
    rm(cor_weekly_papd)
    rm(cor_monthly_papd)
    rm(sub_weekly_papd)
    rm(sub_monthly_papd)
    # gc()
    
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})



# HOURLY PEDESTRIAN ACTIVATIONS ###############################################

print(glue("{Sys.time()} Hourly Pedestrian Activations [5 of 23]"))

tryCatch({
    
    # paph <- s3_read_parquet(
    #     bucket = conf$bucket,
    #     table_name = "ped_actuations_ph",
    #     start_date = calcs_start_date,
    #     end_date = report_end_date,
    #     signals_list = signals_list
    # ) %>%
    #     mutate(
    #         SignalID = factor(SignalID),
    #         Date = date(Date)
    #     )
    
    plan(sequential)
    plan(multiprocess)
    
    weekly_paph <- get_weekly_paph(paph)
    monthly_paph <- get_monthly_paph(paph)
    
    # Group into corridors --------------------------------------------------------
    cor_weekly_paph <- get_cor_weekly_paph(weekly_paph, corridors)
    sub_weekly_paph <- get_cor_weekly_paph(weekly_paph, subcorridors) %>%
        filter(!is.na(Corridor))
    
    # Hourly volumes by Corridor --------------------------------------------------
    cor_monthly_paph <- get_cor_monthly_paph(monthly_paph, corridors)
    sub_monthly_paph <- get_cor_monthly_paph(monthly_paph, subcorridors) %>%
        filter(!is.na(Corridor))
    
    addtoRDS(weekly_paph, "weekly_paph.rds", "paph", report_start_date, wk_calcs_start_date)
    addtoRDS(monthly_paph, "monthly_paph.rds", "paph", report_start_date, calcs_start_date)
    addtoRDS(cor_weekly_paph, "cor_weekly_paph.rds", "paph", report_start_date, wk_calcs_start_date)
    addtoRDS(cor_monthly_paph, "cor_monthly_paph.rds", "paph", report_start_date, calcs_start_date)
    addtoRDS(sub_weekly_paph, "sub_weekly_paph.rds", "paph", report_start_date, wk_calcs_start_date)
    addtoRDS(sub_monthly_paph, "sub_monthly_paph.rds", "paph", report_start_date, calcs_start_date)
    
    rm(paph)
    rm(weekly_paph)
    rm(monthly_paph)
    rm(cor_weekly_paph)
    rm(cor_monthly_paph)
    rm(sub_weekly_paph)
    rm(sub_monthly_paph)
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})



# GET PEDESTRIAN DELAY ###################################################

print(glue("{Sys.time()} Pedestrian Delay [6 of 23]"))

#tryCatch({
if (FALSE) {   # this is done in Python using the script Anthony wrote
    cb <- function(x) {
        filter(x,
               Duration < 300) %>%
            transmute(
                SignalID,
                CallPhase = EventParam,
                Date = date(Date),
                Date_Hour = Begin_Walk,
                DOW = wday(Date),
                Week = week(Date),
                Duration)
    }
    
    ped_delay <- s3_read_parquet(
        bucket = conf$bucket, 
        table_name = "ped_delay", 
        start_date = wk_calcs_start_date,
        end_date = report_end_date, 
        signals_list = signals_list,
        callback = cb
    ) %>%
        mutate(
            SignalID = factor(SignalID),
            CallPhase = factor(CallPhase))
    
    
    daily_pd <- get_daily_avg(ped_delay, "Duration")
    weekly_pd_by_day <- get_weekly_avg_by_day(ped_delay, "Duration", peak_only = FALSE)
    monthly_pd_by_day <- get_monthly_avg_by_day(ped_delay, "Duration", peak_only = FALSE)
    
    cor_weekly_pd_by_day <- get_cor_weekly_avg_by_day(weekly_pd_by_day, corridors, "Duration")
    cor_monthly_pd_by_day <- get_cor_monthly_avg_by_day(monthly_pd_by_day, corridors, "Duration")
    
    sub_weekly_pd_by_day <- get_cor_weekly_avg_by_day(weekly_pd_by_day, subcorridors, "Duration") %>%
        filter(!is.na(Corridor))
    sub_monthly_pd_by_day <- get_cor_monthly_avg_by_day(monthly_pd_by_day, subcorridors, "Duration") %>%
        filter(!is.na(Corridor))
    
    addtoRDS(weekly_pd_by_day, "weekly_pd_by_day.rds", "Duration", report_start_date, wk_calcs_start_date)
    addtoRDS(monthly_pd_by_day, "monthly_pd_by_day.rds", "Duration", report_start_date, calcs_start_date)
    addtoRDS(cor_weekly_pd_by_day, "cor_weekly_pd_by_day.rds", "Duration", report_start_date, wk_calcs_start_date)
    addtoRDS(cor_monthly_pd_by_day, "cor_monthly_pd_by_day.rds", "Duration", report_start_date, calcs_start_date)
    addtoRDS(sub_weekly_pd_by_day, "sub_weekly_pd_by_day.rds", "Duration", report_start_date, wk_calcs_start_date)
    addtoRDS(sub_monthly_pd_by_day, "sub_monthly_pd_by_day.rds", "Duration", report_start_date, calcs_start_date)
    
    rm(ped_delay)
    rm(daily_pd)
    rm(weekly_pd_by_day)
    rm(monthly_pd_by_day)
    rm(cor_weekly_pd_by_day)
    rm(cor_monthly_pd_by_day)
    rm(sub_weekly_pd_by_day)
    rm(sub_monthly_pd_by_day)
#}, error = function(e) {
#    print("ENCOUNTERED AN ERROR:")
#    print(e)
#})
}


# GET COMMUNICATIONS UPTIME ###################################################

print(glue("{Sys.time()} Communication Uptime [7 of 23]"))

tryCatch({
    cu <- s3_read_parquet_parallel(
        bucket = conf$bucket, 
        table_name = "comm_uptime", 
        start_date = wk_calcs_start_date, 
        end_date = report_end_date, 
        signals_list = signals_list
    ) %>%
        mutate(
            SignalID = factor(SignalID),
            CallPhase = factor(CallPhase),
            Date = date(Date)
        )
    
    plan(sequential)
    plan(multiprocess)
    
    daily_comm_uptime <- get_daily_avg(cu, "uptime", peak_only = FALSE)
    cor_daily_comm_uptime <- 
        get_cor_weekly_avg_by_day(daily_comm_uptime, corridors, "uptime")
    sub_daily_comm_uptime <- 
        (get_cor_weekly_avg_by_day(daily_comm_uptime, subcorridors, "uptime") %>%
             filter(!is.na(Corridor)))
    
    weekly_comm_uptime <- get_weekly_avg_by_day(cu, "uptime", peak_only = FALSE)
    cor_weekly_comm_uptime <- 
        get_cor_weekly_avg_by_day(weekly_comm_uptime, corridors, "uptime")
    sub_weekly_comm_uptime <- 
        (get_cor_weekly_avg_by_day(weekly_comm_uptime, subcorridors, "uptime") %>%
             filter(!is.na(Corridor)))
    
    monthly_comm_uptime <- get_monthly_avg_by_day(cu, "uptime", peak_only = FALSE)
    cor_monthly_comm_uptime <- 
        get_cor_monthly_avg_by_day(monthly_comm_uptime, corridors, "uptime")
    sub_monthly_comm_uptime <- 
        (get_cor_monthly_avg_by_day(monthly_comm_uptime, subcorridors, "uptime") %>%
             filter(!is.na(Corridor)))
    
    
    addtoRDS(daily_comm_uptime, "daily_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
    addtoRDS(cor_daily_comm_uptime, "cor_daily_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
    addtoRDS(sub_daily_comm_uptime, "sub_daily_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
    
    addtoRDS(weekly_comm_uptime, "weekly_comm_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)
    addtoRDS(cor_weekly_comm_uptime, "cor_weekly_comm_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)
    addtoRDS(sub_weekly_comm_uptime, "sub_weekly_comm_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)
    
    addtoRDS(monthly_comm_uptime, "monthly_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
    addtoRDS(cor_monthly_comm_uptime, "cor_monthly_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
    addtoRDS(sub_monthly_comm_uptime, "sub_monthly_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
    
    rm(cu)
    rm(daily_comm_uptime)
    rm(weekly_comm_uptime)
    rm(monthly_comm_uptime)
    rm(cor_daily_comm_uptime)
    rm(cor_weekly_comm_uptime)
    rm(cor_monthly_comm_uptime)
    rm(sub_daily_comm_uptime)
    rm(sub_weekly_comm_uptime)
    rm(sub_monthly_comm_uptime)
    # gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})



# DAILY VOLUMES ###############################################################

print(glue("{Sys.time()} Daily Volumes [8 of 23]"))

tryCatch({
    
    vpd <- s3_read_parquet_parallel(
        bucket = conf$bucket, 
        table_name = "vehicles_pd", 
        start_date = wk_calcs_start_date, 
        end_date = report_end_date, 
        signals_list = signals_list
    ) %>%
        mutate(
            SignalID = factor(SignalID),
            CallPhase = factor(CallPhase),
            Date = date(Date)
        )
    
    weekly_vpd <- get_weekly_vpd(vpd)
    
    plan(sequential)
    plan(multiprocess)
    
    # Group into corridors --------------------------------------------------------
    cor_weekly_vpd %<-% get_cor_weekly_vpd(weekly_vpd, corridors)
    # Subcorridors
    sub_weekly_vpd %<-% 
        (get_cor_weekly_vpd(weekly_vpd, subcorridors) %>%
             filter(!is.na(Corridor)))
    
    # Monthly volumes for bar charts and % change ---------------------------------
    monthly_vpd <- get_monthly_vpd(vpd)
    
    # Group into corridors
    cor_monthly_vpd %<-% get_cor_monthly_vpd(monthly_vpd, corridors)
    # Subcorridors
    sub_monthly_vpd %<-% 
        (get_cor_monthly_vpd(monthly_vpd, subcorridors) %>%
             filter(!is.na(Corridor)))
    
    # Monthly % change from previous month by corridor ----------------------------
    addtoRDS(weekly_vpd, "weekly_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
    addtoRDS(monthly_vpd, "monthly_vpd.rds", "vpd", report_start_date, calcs_start_date)
    addtoRDS(cor_weekly_vpd, "cor_weekly_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
    addtoRDS(cor_monthly_vpd, "cor_monthly_vpd.rds", "vpd", report_start_date, calcs_start_date)
    addtoRDS(sub_weekly_vpd, "sub_weekly_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
    addtoRDS(sub_monthly_vpd, "sub_monthly_vpd.rds", "vpd", report_start_date, calcs_start_date)
    
    rm(vpd)
    rm(weekly_vpd)
    rm(monthly_vpd)
    rm(cor_weekly_vpd)
    rm(cor_monthly_vpd)
    rm(sub_weekly_vpd)
    rm(sub_monthly_vpd)
    # gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})



# HOURLY VOLUMES ##############################################################

print(glue("{Sys.time()} Hourly Volumes [9 of 23]"))

tryCatch({
    
    vph <- s3_read_parquet_parallel(
        bucket = conf$bucket, 
        table_name = "vehicles_ph", 
        start_date = wk_calcs_start_date, 
        end_date = report_end_date, 
        signals_list = signals_list
    ) %>%
        mutate(
            SignalID = factor(SignalID),
            CallPhase = factor(2), # Hack because next function needs a CallPhase
            Date = date(Date)
        )
    
    weekly_vph <- get_weekly_vph(vph)
    weekly_vph_peak <- get_weekly_vph_peak(weekly_vph)
    
    # Group into corridors --------------------------------------------------------
    cor_weekly_vph <- get_cor_weekly_vph(weekly_vph, corridors)
    cor_weekly_vph_peak <- get_cor_weekly_vph_peak(cor_weekly_vph)
    
    # Group into Subcorridors --------------------------------------------------------
    sub_weekly_vph <- get_cor_weekly_vph(weekly_vph, subcorridors) %>%
        filter(!is.na(Corridor))
    sub_weekly_vph_peak <- get_cor_weekly_vph_peak(sub_weekly_vph) %>%
        map(~filter(., !is.na(Corridor)))
    
    monthly_vph <- get_monthly_vph(vph)
    monthly_vph_peak <- get_monthly_vph_peak(monthly_vph)
    
    # Hourly volumes by Corridor --------------------------------------------------
    cor_monthly_vph <- get_cor_monthly_vph(monthly_vph, corridors)
    cor_monthly_vph_peak <- get_cor_monthly_vph_peak(cor_monthly_vph)
    
    # Hourly volumes by Subcorridor --------------------------------------------------
    sub_monthly_vph <- get_cor_monthly_vph(monthly_vph, subcorridors) %>%
        filter(!is.na(Corridor))
    sub_monthly_vph_peak <- get_cor_monthly_vph_peak(sub_monthly_vph) %>%
        map(~filter(., !is.na(Corridor)))
    
    addtoRDS(weekly_vph, "weekly_vph.rds", "vph", report_start_date, wk_calcs_start_date)
    addtoRDS(monthly_vph, "monthly_vph.rds", "vph", report_start_date, calcs_start_date)
    addtoRDS(cor_weekly_vph, "cor_weekly_vph.rds", "vph", report_start_date, wk_calcs_start_date)
    addtoRDS(cor_monthly_vph, "cor_monthly_vph.rds", "vph", report_start_date, calcs_start_date)
    addtoRDS(sub_weekly_vph, "sub_weekly_vph.rds", "vph", report_start_date, wk_calcs_start_date)
    addtoRDS(sub_monthly_vph, "sub_monthly_vph.rds", "vph", report_start_date, calcs_start_date)
    
    addtoRDS(weekly_vph_peak, "weekly_vph_peak.rds", "vph", report_start_date, wk_calcs_start_date)
    addtoRDS(monthly_vph_peak, "monthly_vph_peak.rds", "vph", report_start_date, calcs_start_date)
    addtoRDS(cor_weekly_vph_peak, "cor_weekly_vph_peak.rds", "vph", report_start_date, wk_calcs_start_date)
    addtoRDS(cor_monthly_vph_peak, "cor_monthly_vph_peak.rds", "vph", report_start_date, calcs_start_date)
    addtoRDS(sub_weekly_vph_peak, "sub_weekly_vph_peak.rds", "vph", report_start_date, wk_calcs_start_date)
    addtoRDS(sub_monthly_vph_peak, "sub_monthly_vph_peak.rds", "vph", report_start_date, calcs_start_date)
    
    rm(vph)
    rm(weekly_vph)
    rm(monthly_vph)
    rm(cor_weekly_vph)
    rm(sub_weekly_vph)
    rm(cor_monthly_vph)
    rm(sub_monthly_vph)
    rm(weekly_vph_peak)
    rm(monthly_vph_peak)
    rm(cor_weekly_vph_peak)
    rm(cor_monthly_vph_peak)
    rm(sub_weekly_vph_peak)
    rm(sub_monthly_vph_peak)
    #gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})






# DAILY THROUGHPUT ############################################################

print(glue("{Sys.time()} Daily Throughput [10 of 23]"))

tryCatch({
    # throughput <- f("tp_", month_abbrs)
    
    throughput <- s3_read_parquet(
        bucket = conf$bucket, 
        table_name = "throughput", 
        start_date = wk_calcs_start_date, 
        end_date = report_end_date, 
        signals_list = signals_list
    ) %>%
        mutate(
            SignalID = factor(SignalID),
            CallPhase = factor(as.integer(CallPhase)),
            Date = date(Date)
        )
    
    plan(sequential)
    plan(multiprocess)
    
    weekly_throughput %<-% get_weekly_thruput(throughput)
    monthly_throughput %<-% get_monthly_thruput(throughput)
    
    # Weekly throughput - Group into corridors ---------------------------------
    cor_weekly_throughput <- get_cor_weekly_thruput(weekly_throughput, corridors)
    sub_weekly_throughput <- get_cor_weekly_thruput(weekly_throughput, subcorridors) %>%
        filter(!is.na(Corridor))
    
    # Monthly throughput - Group into corridors
    cor_monthly_throughput <- get_cor_monthly_thruput(monthly_throughput, corridors)
    sub_monthly_throughput <- get_cor_monthly_thruput(monthly_throughput, subcorridors) %>%
        filter(!is.na(Corridor))
    
    # Monthly % change from previous month by corridor -------------------------
    # cor_mo_pct_throughput <- get_cor_monthly_pct_change_thruput(cor_monthly_throughput)
    
    addtoRDS(weekly_throughput, "weekly_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
    addtoRDS(monthly_throughput, "monthly_throughput.rds", "vph", report_start_date, calcs_start_date)
    addtoRDS(cor_weekly_throughput, "cor_weekly_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
    addtoRDS(cor_monthly_throughput, "cor_monthly_throughput.rds", "vph", report_start_date, calcs_start_date)
    addtoRDS(sub_weekly_throughput, "sub_weekly_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
    addtoRDS(sub_monthly_throughput, "sub_monthly_throughput.rds", "vph", report_start_date, calcs_start_date)
    
    rm(throughput)
    rm(weekly_throughput)
    rm(monthly_throughput)
    rm(cor_weekly_throughput)
    rm(cor_monthly_throughput)
    rm(sub_weekly_throughput)
    rm(sub_monthly_throughput)
    # gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})




# DAILY ARRIVALS ON GREEN #####################################################

print(glue("{Sys.time()} Daily AOG [11 of 23]"))

tryCatch({
    aog <- s3_read_parquet(
        bucket = conf$bucket, 
        table_name = "arrivals_on_green", 
        start_date = wk_calcs_start_date, 
        end_date = report_end_date, 
        signals_list = signals_list
    ) %>%
        mutate(
            SignalID = factor(SignalID),
            CallPhase = factor(CallPhase),
            Date = date(Date),
            DOW = wday(Date),
            Week = week(Date)
        ) #%>% write_fst("aog.fst")
    
    daily_aog <- get_daily_aog(aog)
    weekly_aog_by_day <- get_weekly_aog_by_day(aog)
    monthly_aog_by_day <- get_monthly_aog_by_day(aog)
    
    cor_weekly_aog_by_day <- get_cor_weekly_aog_by_day(weekly_aog_by_day, corridors)
    cor_monthly_aog_by_day <- get_cor_monthly_aog_by_day(monthly_aog_by_day, corridors)
    
    sub_weekly_aog_by_day <- get_cor_weekly_aog_by_day(weekly_aog_by_day, subcorridors) %>%
        filter(!is.na(Corridor))
    sub_monthly_aog_by_day <- get_cor_monthly_aog_by_day(monthly_aog_by_day, subcorridors) %>%
        filter(!is.na(Corridor))
    
    addtoRDS(weekly_aog_by_day, "weekly_aog_by_day.rds", "aog", report_start_date, wk_calcs_start_date)
    addtoRDS(monthly_aog_by_day, "monthly_aog_by_day.rds", "aog", report_start_date, calcs_start_date)
    addtoRDS(cor_weekly_aog_by_day, "cor_weekly_aog_by_day.rds", "aog", report_start_date, wk_calcs_start_date)
    addtoRDS(cor_monthly_aog_by_day, "cor_monthly_aog_by_day.rds", "aog", report_start_date, calcs_start_date)
    addtoRDS(sub_weekly_aog_by_day, "sub_weekly_aog_by_day.rds", "aog", report_start_date, wk_calcs_start_date)
    addtoRDS(sub_monthly_aog_by_day, "sub_monthly_aog_by_day.rds", "aog", report_start_date, calcs_start_date)
    
    rm(daily_aog)
    rm(weekly_aog_by_day)
    rm(monthly_aog_by_day)
    rm(cor_weekly_aog_by_day)
    rm(cor_monthly_aog_by_day)
    rm(sub_weekly_aog_by_day)
    rm(sub_monthly_aog_by_day)
    #gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})



# HOURLY ARRIVALS ON GREEN ####################################################

print(glue("{Sys.time()} Hourly AOG [12 of 23]"))

tryCatch({
    aog_by_hr <- get_aog_by_hr(aog)
    monthly_aog_by_hr <- get_monthly_aog_by_hr(aog_by_hr)
    
    # Hourly volumes by Corridor --------------------------------------------------
    cor_monthly_aog_by_hr <- get_cor_monthly_aog_by_hr(monthly_aog_by_hr, corridors)
    sub_monthly_aog_by_hr <- get_cor_monthly_aog_by_hr(monthly_aog_by_hr, subcorridors) %>%
        filter(!is.na(Corridor))
    
    # cor_monthly_aog_peak <- get_cor_monthly_aog_peak(cor_monthly_aog_by_hr)
    
    addtoRDS(monthly_aog_by_hr, "monthly_aog_by_hr.rds", "aog", report_start_date, calcs_start_date)
    addtoRDS(cor_monthly_aog_by_hr, "cor_monthly_aog_by_hr.rds", "aog", report_start_date, calcs_start_date)
    addtoRDS(sub_monthly_aog_by_hr, "sub_monthly_aog_by_hr.rds", "aog", report_start_date, calcs_start_date)
    
    # rm(aog)
    rm(aog_by_hr)
    # rm(cor_monthly_aog_peak)
    rm(monthly_aog_by_hr)
    rm(cor_monthly_aog_by_hr)
    rm(sub_monthly_aog_by_hr)
    #gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})



# DAILY PROGRESSION RATIO #####################################################

print(glue("{Sys.time()} Daily Progression Ratio [13 of 23]"))

tryCatch({
    #daily_pr <- get_daily_pr(aog)
    weekly_pr_by_day <- get_weekly_pr_by_day(aog)
    monthly_pr_by_day <- get_monthly_pr_by_day(aog)
    
    cor_weekly_pr_by_day <- get_cor_weekly_pr_by_day(weekly_pr_by_day, corridors)
    cor_monthly_pr_by_day <- get_cor_monthly_pr_by_day(monthly_pr_by_day, corridors)
    
    sub_weekly_pr_by_day <- get_cor_weekly_pr_by_day(weekly_pr_by_day, subcorridors) %>%
        filter(!is.na(Corridor))
    sub_monthly_pr_by_day <- get_cor_monthly_pr_by_day(monthly_pr_by_day, subcorridors) %>%
        filter(!is.na(Corridor))
    
    addtoRDS(weekly_pr_by_day, "weekly_pr_by_day.rds", "pr", report_start_date, wk_calcs_start_date)
    addtoRDS(monthly_pr_by_day, "monthly_pr_by_day.rds", "pr", report_start_date, calcs_start_date)
    addtoRDS(cor_weekly_pr_by_day, "cor_weekly_pr_by_day.rds", "pr", report_start_date, wk_calcs_start_date)
    addtoRDS(cor_monthly_pr_by_day, "cor_monthly_pr_by_day.rds", "pr", report_start_date, calcs_start_date)
    addtoRDS(sub_weekly_pr_by_day, "sub_weekly_pr_by_day.rds", "pr", report_start_date, wk_calcs_start_date)
    addtoRDS(sub_monthly_pr_by_day, "sub_monthly_pr_by_day.rds", "pr", report_start_date, calcs_start_date)
    
    #rm(daily_pr)
    rm(weekly_pr_by_day)
    rm(monthly_pr_by_day)
    rm(cor_weekly_pr_by_day)
    rm(cor_monthly_pr_by_day)
    rm(sub_weekly_pr_by_day)
    rm(sub_monthly_pr_by_day)
    #gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})



# HOURLY PROGESSION RATIO ####################################################

print(glue("{Sys.time()} Hourly Progression Ratio [14 of 23]"))

tryCatch({
    pr_by_hr <- get_pr_by_hr(aog)
    monthly_pr_by_hr <- get_monthly_pr_by_hr(pr_by_hr)
    
    # Hourly volumes by Corridor --------------------------------------------------
    cor_monthly_pr_by_hr <- get_cor_monthly_pr_by_hr(monthly_pr_by_hr, corridors)
    sub_monthly_pr_by_hr <- get_cor_monthly_pr_by_hr(monthly_pr_by_hr, subcorridors) %>%
        filter(!is.na(Corridor))
    
    # cor_monthly_pr_peak <- get_cor_monthly_pr_peak(cor_monthly_pr_by_hr)
    
    addtoRDS(monthly_pr_by_hr, "monthly_pr_by_hr.rds", "pr", report_start_date, calcs_start_date)
    addtoRDS(cor_monthly_pr_by_hr, "cor_monthly_pr_by_hr.rds", "pr", report_start_date, calcs_start_date)
    addtoRDS(sub_monthly_pr_by_hr, "sub_monthly_pr_by_hr.rds", "pr", report_start_date, calcs_start_date)
    
    rm(aog)
    rm(pr_by_hr)
    # rm(cor_monthly_pr_peak)
    rm(monthly_pr_by_hr)
    rm(cor_monthly_pr_by_hr)
    rm(sub_monthly_pr_by_hr)
    #gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})





# DAILY SPLIT FAILURES #####################################################

tryCatch({
    print(glue("{Sys.time()} Daily Split Failures [15 of 23]"))
    
    sf <- s3_read_parquet_parallel(
        bucket = conf$bucket,
        table_name = "split_failures", 
        start_date = wk_calcs_start_date, 
        end_date = report_end_date, 
        signals_list = signals_list,
        callback = function(x) filter(x, CallPhase == 0)
    ) %>%
        mutate(
            SignalID = factor(SignalID),
            CallPhase = factor(CallPhase),
            Date = date(Date)
        )
    
    # Divide into peak/off-peak split failures
    # -------------------------------------------------------------------------
    sfo <- sf %>% filter(!hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))
    sfp <- sf %>% filter(hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))
    # ------------------------------------------------------------------------- 
    
    weekly_sf_by_day <- get_weekly_avg_by_day(
        sfp, "sf_freq", "cycles", peak_only = FALSE)
    weekly_sfo_by_day <- get_weekly_avg_by_day(
        sfo, "sf_freq", "cycles", peak_only = FALSE)
    
    cor_weekly_sf_by_day <- get_cor_weekly_sf_by_day(weekly_sf_by_day, corridors)
    cor_weekly_sfo_by_day <- get_cor_weekly_sf_by_day(weekly_sfo_by_day, corridors)
    
    sub_weekly_sf_by_day <- get_cor_weekly_sf_by_day(weekly_sf_by_day, subcorridors) %>%
        filter(!is.na(Corridor))
    sub_weekly_sfo_by_day <- get_cor_weekly_sf_by_day(weekly_sfo_by_day, subcorridors) %>%
        filter(!is.na(Corridor))
    
    monthly_sf_by_day <- get_monthly_avg_by_day(
        sfp, "sf_freq", "cycles", peak_only = FALSE)
    monthly_sfo_by_day <- get_monthly_avg_by_day(
        sfo, "sf_freq", "cycles", peak_only = FALSE)
    
    cor_monthly_sf_by_day <- get_cor_monthly_sf_by_day(monthly_sf_by_day, corridors)
    cor_monthly_sfo_by_day <- get_cor_monthly_sf_by_day(monthly_sfo_by_day, corridors)
    
    sub_monthly_sf_by_day <- get_cor_monthly_sf_by_day(monthly_sf_by_day, subcorridors) %>%
        filter(!is.na(Corridor))
    sub_monthly_sfo_by_day <- get_cor_monthly_sf_by_day(monthly_sfo_by_day, subcorridors) %>%
        filter(!is.na(Corridor))
    
    
    addtoRDS(weekly_sf_by_day, "wsf.rds", "sf_freq", report_start_date, wk_calcs_start_date)
    addtoRDS(monthly_sf_by_day, "monthly_sfd.rds", "sf_freq", report_start_date, calcs_start_date)
    addtoRDS(cor_weekly_sf_by_day, "cor_wsf.rds", "sf_freq", report_start_date, wk_calcs_start_date)
    addtoRDS(cor_monthly_sf_by_day, "cor_monthly_sfd.rds", "sf_freq", report_start_date, calcs_start_date)
    addtoRDS(sub_weekly_sf_by_day, "sub_wsf.rds", "sf_freq", report_start_date, wk_calcs_start_date)
    addtoRDS(sub_monthly_sf_by_day, "sub_monthly_sfd.rds", "sf_freq", report_start_date, calcs_start_date)
    
    addtoRDS(weekly_sfo_by_day, "wsfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
    addtoRDS(monthly_sfo_by_day, "monthly_sfo.rds", "sf_freq", report_start_date, calcs_start_date)
    addtoRDS(cor_weekly_sfo_by_day, "cor_wsfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
    addtoRDS(cor_monthly_sfo_by_day, "cor_monthly_sfo.rds", "sf_freq", report_start_date, calcs_start_date)
    addtoRDS(sub_weekly_sfo_by_day, "sub_wsfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
    addtoRDS(sub_monthly_sfo_by_day, "sub_monthly_sfo.rds", "sf_freq", report_start_date, calcs_start_date)
    
    rm(sfp)
    rm(sfo)
    rm(weekly_sf_by_day)
    rm(monthly_sf_by_day)
    rm(cor_weekly_sf_by_day)
    rm(cor_monthly_sf_by_day)
    rm(sub_weekly_sf_by_day)
    rm(sub_monthly_sf_by_day)
    
    rm(weekly_sfo_by_day)
    rm(monthly_sfo_by_day)
    rm(cor_weekly_sfo_by_day)
    rm(cor_monthly_sfo_by_day)
    rm(sub_weekly_sfo_by_day)
    rm(sub_monthly_sfo_by_day)
    
    #gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})

# HOURLY SPLIT FAILURES #######################################################

print(glue("{Sys.time()} Hourly Split Failures [16 of 23]"))

tryCatch({
    sfh <- get_sf_by_hr(sf)
    msfh <- get_monthly_sf_by_hr(sfh)
    
    # Hourly volumes by Corridor --------------------------------------------------
    cor_msfh <- get_cor_monthly_sf_by_hr(msfh, corridors)
    sub_msfh <- get_cor_monthly_sf_by_hr(msfh, subcorridors) %>%
        filter(!is.na(Corridor))
    
    addtoRDS(msfh, "msfh.rds", "sf_freq", report_start_date, calcs_start_date)
    addtoRDS(cor_msfh, "cor_msfh.rds", "sf_freq", report_start_date, calcs_start_date)
    addtoRDS(sub_msfh, "sub_msfh.rds", "sf_freq", report_start_date, calcs_start_date)
    
    
    # Peak/Off-Peak Split Failures
    
    # msfp <- msfh %>%
    #     summarize_by_peak("Hour") %>%
    #     map(~rename(., Month = Period))
    
    # wsfh <- get_weekly_avg_by_hr(sfh, "sf_freq", "cycles")
    # wsfp <- wsfh %>%
    #     summarize_by_peak("Hour") %>%
    #     map(~rename(., Date = Period))
    
    # cor_msfp <- msfp %>% 
    #     map(~get_cor_monthly_sf_by_day(., corridors))
    # cor_wsfp <- wsfp %>% 
    #     map(~get_cor_weekly_sf_by_day(., corridors))
    
    # sub_msfp <- msfp %>%
    #     map(~get_cor_monthly_sf_by_day(., subcorridors)) %>%
    #     map(~filter(., !is.na(Corridor)))
    # sub_wsfp <- wsfp %>% 
    #     map(~get_cor_weekly_sf_by_day(., subcorridors)) %>%
    #     map(~filter(., !is.na(Corridor)))
    
    
    # saveRDS(msfp, "msfp.rds")
    # saveRDS(wsfp, "wsfp.rds")
    # saveRDS(cor_msfp, "cor_monthly_sf_peak.rds")
    # saveRDS(cor_wsfp, "cor_weekly_sf_peak.rds")
    # 
    # saveRDS(sub_msfp, "sub_monthly_sf_peak.rds")
    # saveRDS(sub_wsfp, "sub_weekly_sf_peak.rds")
    
    rm(sf)    
    rm(sfh)
    rm(msfh)
    rm(cor_msfh)
    rm(sub_msfh)
    
    # rm(msfp)
    # rm(wsfp)
    # rm(cor_msfp)
    # rm(cor_wsfp)
    # rm(sub_msfp)
    # rm(sub_wsfp)
    
    #gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})



# DAILY QUEUE SPILLBACK #######################################################

print(glue("{Sys.time()} Daily Queue Spillback [17 of 23]"))

tryCatch({
    
    qs <- s3_read_parquet_parallel(
        bucket = conf$bucket, 
        table_name = "queue_spillback", 
        start_date = wk_calcs_start_date, 
        end_date = report_end_date, 
        signals_list = signals_list
    ) %>%
        mutate(
            SignalID = factor(SignalID),
            CallPhase = factor(CallPhase),
            Date = date(Date)
        )
    
    wqs <- get_weekly_qs_by_day(qs)
    cor_wqs <- get_cor_weekly_qs_by_day(wqs, corridors)
    sub_wqs <- get_cor_weekly_qs_by_day(wqs, subcorridors) %>%
        filter(!is.na(Corridor))
    
    monthly_qsd <- get_monthly_qs_by_day(qs)
    cor_monthly_qsd <- get_cor_monthly_qs_by_day(monthly_qsd, corridors)
    sub_monthly_qsd <- get_cor_monthly_qs_by_day(monthly_qsd, subcorridors) %>%
        filter(!is.na(Corridor))
    
    addtoRDS(wqs, "wqs.rds", "qs_freq", report_start_date, wk_calcs_start_date)
    addtoRDS(monthly_qsd, "monthly_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
    addtoRDS(cor_wqs, "cor_wqs.rds", "qs_freq", report_start_date, wk_calcs_start_date)
    addtoRDS(cor_monthly_qsd, "cor_monthly_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
    addtoRDS(sub_wqs, "sub_wqs.rds", "qs_freq", report_start_date, wk_calcs_start_date)
    addtoRDS(sub_monthly_qsd, "sub_monthly_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
    
    rm(wqs)
    rm(monthly_qsd)
    rm(cor_wqs)
    rm(cor_monthly_qsd)
    rm(sub_wqs)
    rm(sub_monthly_qsd)
    #gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})




# HOURLY QUEUE SPILLBACK ######################################################

print(glue("{Sys.time()} Hourly Queue Spillback [18 of 23]"))

tryCatch({
    qsh <- get_qs_by_hr(qs)
    mqsh <- get_monthly_qs_by_hr(qsh)
    
    # Hourly volumes by Corridor --------------------------------------------------
    cor_mqsh <- get_cor_monthly_qs_by_hr(mqsh, corridors)
    sub_mqsh <- get_cor_monthly_qs_by_hr(mqsh, subcorridors) %>%
        filter(!is.na(Corridor))
    
    addtoRDS(mqsh, "mqsh.rds", "qs_freq", report_start_date, calcs_start_date)
    addtoRDS(cor_mqsh, "cor_mqsh.rds", "qs_freq", report_start_date, calcs_start_date)
    addtoRDS(sub_mqsh, "sub_mqsh.rds", "qs_freq", report_start_date, calcs_start_date)
    
    rm(qs)
    rm(qsh)
    rm(mqsh)
    rm(cor_mqsh)
    rm(sub_mqsh)
    # gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})



# TRAVEL TIME AND BUFFER TIME INDEXES #########################################

print(glue("{Sys.time()} Travel Time Indexes [19 of 23]"))

tryCatch({
    
    # ------- Corridor Travel Time Metrics ------- #
    
    tt <- s3_read_parquet_parallel(
        bucket = conf$bucket,
        table_name = "cor_travel_time_metrics",
        start_date = wk_calcs_start_date, 
        end_date = report_end_date
    ) %>%
        mutate(
            Corridor = factor(Corridor)
        ) %>% 
        left_join(distinct(all_corridors, Zone_Group, Zone, Corridor)) %>%
        filter(!is.na(Zone_Group))
    
    tti <- tt %>%
        dplyr::select(-c(pti, bi))
    
    pti <- tt %>%
        dplyr::select(-c(tti, bi))
    
    bi <- tt %>%
        dplyr::select(-c(tti, pti))
    
    cor_monthly_vph <- readRDS("cor_monthly_vph.rds") %>% rename(Zone = Zone_Group) %>%
        left_join(distinct(corridors, Zone_Group, Zone), by=("Zone"))
    
    cor_monthly_tti_by_hr <- get_cor_monthly_ti_by_hr(tti, cor_monthly_vph, all_corridors)
    cor_monthly_pti_by_hr <- get_cor_monthly_ti_by_hr(pti, cor_monthly_vph, all_corridors)
    cor_monthly_bi_by_hr <- get_cor_monthly_ti_by_hr(bi, cor_monthly_vph, all_corridors)
    
    cor_monthly_tti <- get_cor_monthly_ti_by_day(tti, cor_monthly_vph, all_corridors)
    cor_monthly_pti <- get_cor_monthly_ti_by_day(pti, cor_monthly_vph, all_corridors)
    cor_monthly_bi <- get_cor_monthly_ti_by_day(bi, cor_monthly_vph, all_corridors)
    
    addtoRDS(cor_monthly_tti, "cor_monthly_tti.rds", "tti", report_start_date, calcs_start_date)
    addtoRDS(cor_monthly_tti_by_hr, "cor_monthly_tti_by_hr.rds", "tti", report_start_date, calcs_start_date)
    
    addtoRDS(cor_monthly_pti, "cor_monthly_pti.rds", "pti", report_start_date, calcs_start_date)
    addtoRDS(cor_monthly_pti_by_hr, "cor_monthly_pti_by_hr.rds", "pti", report_start_date, calcs_start_date)
    
    addtoRDS(cor_monthly_bi, "cor_monthly_bi.rds", "bi", report_start_date, calcs_start_date)
    addtoRDS(cor_monthly_bi_by_hr, "cor_monthly_bi_by_hr.rds", "bi", report_start_date, calcs_start_date)
    
    # ------- Subcorridor Travel Time Metrics ------- #
    
    tt <- s3_read_parquet_parallel(
        bucket = conf$bucket,
        table_name = "sub_travel_time_metrics",
        start_date = calcs_start_date, 
        end_date = report_end_date
        ) %>%
        mutate(
            Corridor = factor(Corridor),
            Subcorridor = factor(Subcorridor)
        ) %>%
        rename(Zone = Corridor,
               Corridor = Subcorridor) %>%
        left_join(distinct(subcorridors, Zone_Group, Zone))
    
    tti <- tt %>%
        dplyr::select(-c(pti, bi))
    
    pti <- tt %>%
        dplyr::select(-c(tti, bi))
    
    bi <- tt %>%
        dplyr::select(-c(tti, pti))
    
    sub_monthly_vph <- readRDS("sub_monthly_vph.rds") %>% rename(Zone = Zone_Group) %>%
        left_join(distinct(subcorridors, Zone_Group, Zone), by=("Zone"))
    
    sub_monthly_tti_by_hr <- get_cor_monthly_ti_by_hr(tti, sub_monthly_vph, subcorridors)
    sub_monthly_pti_by_hr <- get_cor_monthly_ti_by_hr(pti, sub_monthly_vph, subcorridors)
    sub_monthly_bi_by_hr <- get_cor_monthly_ti_by_hr(bi, sub_monthly_vph, subcorridors)
    
    sub_monthly_tti <- get_cor_monthly_ti_by_day(tti, sub_monthly_vph, subcorridors)
    sub_monthly_pti <- get_cor_monthly_ti_by_day(pti, sub_monthly_vph, subcorridors)
    sub_monthly_bi <- get_cor_monthly_ti_by_day(bi, sub_monthly_vph, subcorridors)
    
    addtoRDS(sub_monthly_tti, "sub_monthly_tti.rds", "tti", report_start_date, calcs_start_date)
    addtoRDS(sub_monthly_tti_by_hr, "sub_monthly_tti_by_hr.rds", "tti", report_start_date, calcs_start_date)
    
    addtoRDS(sub_monthly_pti, "sub_monthly_pti.rds", "pti", report_start_date, calcs_start_date)
    addtoRDS(sub_monthly_pti_by_hr, "sub_monthly_pti_by_hr.rds", "pti", report_start_date, calcs_start_date)
    
    addtoRDS(sub_monthly_bi, "sub_monthly_bi.rds", "bi", report_start_date, calcs_start_date)
    addtoRDS(sub_monthly_bi_by_hr, "sub_monthly_bi_by_hr.rds", "bi", report_start_date, calcs_start_date)
    
    
    rm(tt)
    rm(tti)
    rm(pti)
    rm(bi)
    rm(cor_monthly_vph)
    rm(cor_monthly_tti)
    rm(cor_monthly_tti_by_hr)
    rm(cor_monthly_pti)
    rm(cor_monthly_pti_by_hr)
    rm(cor_monthly_bi)
    rm(cor_monthly_bi_by_hr)
    
    rm(sub_monthly_tti)
    rm(sub_monthly_tti_by_hr)
    rm(sub_monthly_pti)
    rm(sub_monthly_pti_by_hr)
    rm(sub_monthly_bi)
    rm(sub_monthly_bi_by_hr)

    # gc()
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})



# DETECTOR UPTIME AS REPORTED BY FIELD ENGINEERS ##############################

print(glue("{Sys.time()} Uptimes [20 of 23]"))

if (TRUE==FALSE) {
    # # VEH, PED UPTIME - AS REPORTED BY FIELD ENGINEERS via EXCEL
    
    keys <- aws.s3::get_bucket_df(conf$bucket, prefix = "manual_veh_ped_uptime")$Key
    keys <- keys[endsWith(keys, ".xlsx")]
    
    months <- str_extract(basename(keys), "^\\S+ \\d{4}")
    #xl_uptime_fns <- basename(keys)[!is.na(months)]
    #xl_uptime_mos <- dmy(paste("1", months[!is.na(months)]))
    
    # xl_uptime_fns <- file.path(conf$xl_uptime$path, conf$xl_uptime$filenames)
    # xl_uptime_fns <- conf$xl_uptime$filenames
    # xl_uptime_mos <- conf$xl_uptime$months
    
    man_xl <- lapply(keys, 
                     function(k) {
                         get_det_uptime_from_manual_xl(
                             bucket = conf$bucket, 
                             key = k, 
                             corridors = all_corridors)
                     }) %>%
        bind_rows() %>%
        filter(!is.na(Zone_Group)) %>%
        mutate(Corridor = factor(Corridor))
    
    # man_xl <- purrr::map2(
    #     xl_uptime_fns,
    #     xl_uptime_mos,
    #     get_det_uptime_from_manual_xl
    # ) %>%
    #     bind_rows() %>%
    #     mutate(Zone_Group = factor(Zone_Group)) %>%
    #     filter(Month >= report_start_date)
    
    man_veh_xl <- man_xl %>% filter(Type == "Vehicle")
    man_ped_xl <- man_xl %>% filter(Type == "Pedestrian")
    
    
    cor_monthly_xl_veh_uptime <- man_veh_xl %>% # bind_rows(mrs_veh_xl, man_veh_xl) %>%
        dplyr::select(-c(Zone_Group, Type)) %>%
        get_cor_monthly_avg_by_day(all_corridors, "uptime", "num")
    
    
    cor_monthly_xl_ped_uptime <- man_ped_xl %>% # bind_rows(mrs_ped_xl, man_ped_xl) %>%
        dplyr::select(-c(Zone_Group, Type)) %>%
        get_cor_monthly_avg_by_day(all_corridors, "uptime", "num")
    
    saveRDS(cor_monthly_xl_veh_uptime, "cor_monthly_xl_veh_uptime.rds")
    saveRDS(cor_monthly_xl_ped_uptime, "cor_monthly_xl_ped_uptime.rds")
    
    
}#, error = function(e) {
#    print("ENCOUNTERED AN ERROR:")
#    print(e)
#})

# # CCTV UPTIME From 511 and Encoders

tryCatch({
    
    daily_cctv_uptime_511 <- get_daily_cctv_uptime("cctv_uptime", cam_config, report_start_date)
    daily_cctv_uptime_encoders <- get_daily_cctv_uptime("cctv_uptime_encoders", cam_config, report_start_date)
    
    # up:
    #   2-on 511 (dark brown)
    #   1-working at encoder but not on 511 (light brown)
    #   0-not working on either (gray)
    daily_cctv_uptime <- full_join(daily_cctv_uptime_511,
                                   daily_cctv_uptime_encoders,
                                   by = c("Zone_Group", "Zone", "Corridor", "Subcorridor", "CameraID", "Description", "Date"),
                                   suffix = c("_511", "_enc")
        ) %>%
        replace_na(list(up_enc = 0, num_enc = 0, uptime_enc = 0,
                        up_511 = 0, num_511 = 0, uptime_511 = 0)) %>%
        select(Zone_Group, Zone, Corridor, Subcorridor, CameraID, Description, Date, up_511, up_enc) %>%
        mutate(uptime = up_511, 
               num = 1,
               up = pmax(up_511 * 2, up_enc),
               Zone_Group = factor(Zone_Group),
               Zone = factor(Zone),
               Corridor = factor(Corridor),
               Subcorridor = factor(Subcorridor),
               CameraID = factor(CameraID),
               Description = factor(Description))
    
    
    # Find the days where uptime across the board is very low (close to 0)
    #  This is symptomatic of a problem with the acquisition rather than the cameras themselves
    bad_days <- daily_cctv_uptime %>%
        group_by(Date) %>%
        summarize(
            sup = sum(uptime),
            snum = sum(num),
            suptime = sum(uptime) / sum(num)
        ) %>%
        filter(suptime < 0.2)
    
    weekly_cctv_uptime <- get_weekly_avg_by_day_cctv(daily_cctv_uptime)

    monthly_cctv_uptime <- daily_cctv_uptime %>%
        group_by(
            Zone_Group, Zone, Corridor, Subcorridor, CameraID, Description, 
            Month = floor_date(Date, unit = "months")) %>%
        summarize(up = sum(uptime), uptime = weighted.mean(uptime, num), num = sum(num)) %>%
        ungroup()
    
    cor_daily_cctv_uptime <- get_cor_weekly_avg_by_day(
        daily_cctv_uptime, all_corridors, "uptime", "num")

    cor_weekly_cctv_uptime <- get_cor_weekly_avg_by_day(
        weekly_cctv_uptime, all_corridors, "uptime", "num")

    cor_monthly_cctv_uptime <- get_cor_monthly_avg_by_day(
        monthly_cctv_uptime, all_corridors, "uptime", "num")
    
    
    sub_daily_cctv_uptime <- daily_cctv_uptime %>% 
        select(-Zone_Group) %>%
        rename(Zone_Group = Zone,
               Zone = Corridor,
               Corridor = Subcorridor) %>%
        get_cor_weekly_avg_by_day(subcorridors, "uptime", "num")
    
    sub_weekly_cctv_uptime <- weekly_cctv_uptime %>%
        select(-Zone_Group) %>%
        rename(Zone_Group = Zone,
               Zone = Corridor,
               Corridor = Subcorridor) %>%
        get_cor_weekly_avg_by_day(subcorridors, "uptime", "num")
    
    sub_monthly_cctv_uptime <- monthly_cctv_uptime %>%
        select(-Zone_Group) %>%
        rename(Zone_Group = Zone,
               Zone = Corridor,
               Corridor = Subcorridor) %>%
        get_cor_monthly_avg_by_day(subcorridors, "uptime", "num")
    
    
    saveRDS(daily_cctv_uptime, "daily_cctv_uptime.rds")
    saveRDS(weekly_cctv_uptime, "weekly_cctv_uptime.rds")
    saveRDS(monthly_cctv_uptime, "monthly_cctv_uptime.rds")
    
    saveRDS(cor_daily_cctv_uptime, "cor_daily_cctv_uptime.rds")
    saveRDS(cor_weekly_cctv_uptime, "cor_weekly_cctv_uptime.rds")
    saveRDS(cor_monthly_cctv_uptime, "cor_monthly_cctv_uptime.rds")
    
    saveRDS(sub_daily_cctv_uptime, "sub_daily_cctv_uptime.rds")
    saveRDS(sub_weekly_cctv_uptime, "sub_weekly_cctv_uptime.rds")
    saveRDS(sub_monthly_cctv_uptime, "sub_monthly_cctv_uptime.rds")
    
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})

# # RSU UPTIME

tryCatch({
    
    daily_rsu_uptime <- get_rsu_uptime(report_start_date)
    cor_daily_rsu_uptime <- get_cor_weekly_avg_by_day(
        daily_rsu_uptime, corridors, "uptime")
    sub_daily_rsu_uptime <- get_cor_weekly_avg_by_day(
        daily_rsu_uptime, subcorridors, "uptime")
    
    weekly_rsu_uptime <- get_weekly_avg_by_day(
        mutate(daily_rsu_uptime, CallPhase = 0, Week = week(Date)), "uptime", peak_only = FALSE)
    cor_weekly_rsu_uptime <- get_cor_weekly_avg_by_day(
        weekly_rsu_uptime, corridors, "uptime")
    sub_weekly_rsu_uptime <- get_cor_weekly_avg_by_day(
        weekly_rsu_uptime, subcorridors, "uptime")
    
    monthly_rsu_uptime <- get_monthly_avg_by_day(
        mutate(daily_rsu_uptime, CallPhase = 0), "uptime", peak_only = FALSE)
    cor_monthly_rsu_uptime <- get_cor_monthly_avg_by_day(
        monthly_rsu_uptime, corridors, "uptime")
    sub_monthly_rsu_uptime <- get_cor_monthly_avg_by_day(
        monthly_rsu_uptime, subcorridors, "uptime")
    
    
    saveRDS(daily_rsu_uptime, "daily_rsu_uptime.rds")
    saveRDS(weekly_rsu_uptime, "weekly_rsu_uptime.rds")
    saveRDS(monthly_rsu_uptime, "monthly_rsu_uptime.rds")
    
    saveRDS(cor_daily_rsu_uptime, "cor_daily_rsu_uptime.rds")
    saveRDS(cor_weekly_rsu_uptime, "cor_weekly_rsu_uptime.rds")
    saveRDS(cor_monthly_rsu_uptime, "cor_monthly_rsu_uptime.rds")
    
    saveRDS(sub_daily_rsu_uptime, "sub_daily_rsu_uptime.rds")
    saveRDS(sub_weekly_rsu_uptime, "sub_weekly_rsu_uptime.rds")
    saveRDS(sub_monthly_rsu_uptime, "sub_monthly_rsu_uptime.rds")
    
    
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})









# ACTIVITIES ##############################

print(glue("{Sys.time()} TEAMS [21 of 23]"))

tryCatch({
    
    teams <- get_teams_tasks_from_s3(
        bucket = conf$bucket,
        teams_locations_key = "mark/teams/teams_locations.feather",
        archived_tasks_prefix = "mark/teams/tasks20",
        current_tasks_key = "mark/teams/tasks.csv.zip",
        replicate = TRUE
    )
    
    tasks_by_type <- get_outstanding_tasks_by_param(
        teams, "Task_Type", report_start_date)
    tasks_by_subtype <- get_outstanding_tasks_by_param(
        teams, "Task_Subtype", report_start_date)
    tasks_by_priority <- get_outstanding_tasks_by_param(
        teams, "Priority", report_start_date)
    tasks_by_source <- get_outstanding_tasks_by_param(
        teams, "Task_Source", report_start_date)
    tasks_all <- get_outstanding_tasks_by_param(
        teams, "All", report_start_date)
    
    cor_outstanding_tasks_by_day_range <- lapply(
        dates, function(x) get_outstanding_tasks_by_day_range(teams, report_start_date, x)
    ) %>%
        bind_rows() %>%
        mutate(Zone_Group = factor(Zone_Group),
               Corridor = factor(Corridor)) %>%
        
        arrange(Zone_Group, Corridor, Month) %>%
        group_by(Zone_Group, Corridor) %>% 
        mutate(
            delta.over45 = (over45 - lag(over45))/lag(over45),
            delta.mttr = (mttr - lag(mttr))/lag(mttr)
        ) %>%
        ungroup()
    
    sig_outstanding_tasks_by_day_range <- cor_outstanding_tasks_by_day_range %>% 
        group_by(Corridor) %>% 
        filter(as.character(Zone_Group) == min(as.character(Zone_Group))) %>%
        mutate(Zone_Group = Corridor) %>%
        filter(Corridor %in% all_corridors$Corridor) %>%
        ungroup()
    
    saveRDS(tasks_by_type, "tasks_by_type.rds") 
    saveRDS(tasks_by_subtype, "tasks_by_subtype.rds") 
    saveRDS(tasks_by_priority, "tasks_by_priority.rds") 
    saveRDS(tasks_by_source, "tasks_by_source.rds") 
    saveRDS(tasks_all, "tasks_all.rds") 
    saveRDS(cor_outstanding_tasks_by_day_range, "cor_tasks_by_date.rds")
    saveRDS(sig_outstanding_tasks_by_day_range, "sig_tasks_by_date.rds")

}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})




# USER DELAY COSTS   ##############################

print(glue("{Sys.time()} User Delay Costs [21.1 of 23]"))

tryCatch({
    
    months <- seq(ymd(report_start_date), ymd(report_end_date), by = "1 month")
    udc <- mclapply(months, mc.cores = usable_cores, FUN = function(yyyymmdd) {
        obj <- glue("mark/user_delay_costs/date={yyyymmdd}/user_delay_costs_{yyyymmdd}.parquet")
        if (nrow(aws.s3::get_bucket_df(conf$bucket, prefix = obj)) > 0) {
            s3read_using(
                read_parquet, 
                bucket = conf$bucket, 
                object = obj
            ) %>%
                #mutate(month = yyyymmdd) %>% 
                filter(!is.na(date)) %>% 
                transmute(
                    Zone = zone, 
                    Corridor = corridor, 
                    analysis_month = ymd(yyyymmdd),
                    month_hour = mdy_hms(date), # YYYY-mm-dd HH:MM:SS
                    month_hour = month_hour - days(day(month_hour)) + days(1), # YYYY-mm-01 HH:MM:SS
                    Month = date(floor_date(month_hour, "month")), # YYYY-mm-01  # year_month
                    delay_cost = combined.delay_cost)
            
        }
    }) %>% bind_rows()
    
    hourly_udc <- udc %>% 
        # transmute(
        #     Zone = zone, 
        #     Corridor = corridor, 
        #     month_hour = mdy_hms(date), # YYYY-mm-dd HH:MM:SS
        #     month_hour = month_hour - days(day(month_hour)) + days(1), # YYYY-mm-01 HH:MM:SS
        #     Month = date(floor_date(month_hour, "month")), # YYYY-mm-01  # year_month
        #     delay_cost = combined.delay_cost) %>% 
        group_by(Zone, Corridor, Month, month_hour) %>%  # year_month
        summarize(delay_cost = sum(delay_cost, na.rm = TRUE)) %>% 
        ungroup() 
    
    
    months <- unique(udc$analysis_month)
    udc_trend_table_list <- lapply(months[2:length(months)], function(current_month) {
        #current_month <- max(udc$month)
        last_month <- current_month - months(1)
        last_year <- current_month - years(1)
        
        current_month_col <- as.name(format(current_month, "%B %Y"))
        last_month_col <- as.name(format(last_month, "%B %Y"))
        last_year_col <- as.name(format(last_year, "%B %Y"))
        
        
        udc_trend_table_list <- udc %>% 
            filter(analysis_month <= current_month) %>%
            # transmute(
            #     Zone = zone, 
            #     Corridor = corridor, 
            #     date = date(mdy_hms(date)), 
            #     month = floor_date(date, "months"),
            #     delay_cost = combined.delay_cost) %>% 
            filter(
                Month %in% c(current_month, last_month, last_year)) %>%
            group_by(
                Zone, Corridor, Month) %>% 
            summarize(
                delay_cost = sum(delay_cost, na.rm = TRUE)) %>%
            ungroup() %>%
            mutate(
                Month = format(Month, "%B %Y")) %>%
            spread(
                Month, delay_cost) %>% 
            mutate(
                Month = current_month,
                `Month-over-Month` = (!!current_month_col - !!last_month_col) / !!last_month_col, 
                `Year-over-Year` = (!!current_month_col - !!last_year_col) / !!last_year_col) %>% 
            select(
                Zone, Corridor, Month,
                !!last_year_col, `Year-over-Year`, 
                !!last_month_col, `Month-over-Month`, 
                !!current_month_col)
    }) #%>% bind_rows()
    names(udc_trend_table_list) <- months[2:length(months)]
    
    
    saveRDS(hourly_udc, "hourly_udc.rds")
    saveRDS(udc_trend_table_list, "udc_trend_table_list.rds")
    

}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})





# Package up for Flexdashboard

print(glue("{Sys.time()} Package for Monthly Report [22 of 23]"))

sigify <- function(df, cor_df, corridors, identifier = "SignalID") {
    if (identifier == "SignalID") {
        df_ <- df %>%
            left_join(distinct(corridors, SignalID, Corridor, Name), by = c("SignalID")) %>%
            rename(Zone_Group = Corridor, Corridor = SignalID) %>%
            ungroup() %>%
            mutate(Corridor = factor(Corridor))
    } else if (identifier == "CameraID") {
        corridors <- rename(corridors, Name = Location)
        df_ <- df %>%
            select(-matches("Subcorridor"),
                   -matches("Zone_Group")) %>%
            left_join(distinct(corridors, CameraID, Corridor, Name), by = c("Corridor", "CameraID")) %>%
            rename(
                Zone_Group = Corridor, 
                Corridor = CameraID) %>%
            ungroup() %>%
            mutate(Corridor = factor(Corridor))
    } else {
        stop("bad identifier. Must be SignalID (default) or CameraID")
    }
    
    cor_df_ <- cor_df %>%
        filter(Corridor %in% unique(df_$Zone_Group)) %>%
        mutate(Zone_Group = Corridor) %>%
        select(-matches("Subcorridor"))

    br <- bind_rows(df_, cor_df_) %>%
        mutate(Corridor = factor(Corridor))
    
    if ("Zone_Group" %in% names(br)) {
        br <- br %>%
            mutate(Zone_Group = factor(Zone_Group))
    }
    
    if ("Month" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Month)
    } else if ("Hour" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Hour)
    } else if ("Date" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Date)
    }
}


# cor$mo$tasks$outstanding = readRDS("cor_monthly_outstanding_tasks.rds")
# sig$mo$tasks$outstanding = readRDS("sig_monthly_outstanding_tasks.rds")
# 
# cor$mo$tasks$priority = readRDS("cor_monthly_priority.rds")
# cor$mo$tasks$type = readRDS("cor_monthly_type.rds")
# cor$mo$tasks$subtype = readRDS("cor_monthly_subtype.rds")
# cor$mo$tasks$source = readRDS("cor_monthly_source.rds")
# cor$mo$tasks$all = readRDS("cor_monthly_all.rds")
# 
# sig$mo$tasks$priority = readRDS("sig_monthly_priority.rds")
# sig$mo$tasks$type = readRDS("sig_monthly_type.rds")
# sig$mo$tasks$subtype = readRDS("sig_monthly_subtype.rds")
# sig$mo$tasks$source = readRDS("sig_monthly_source.rds")
# sig$mo$tasks$all = readRDS("sig_monthly_all.rds")

tryCatch({
    cor <- list()
    cor$dy <- list(
        "du" = readRDS("cor_avg_daily_detector_uptime.rds"),
        "cu" = readRDS("cor_daily_comm_uptime.rds"),
        "pau" = readRDS("cor_daily_pa_uptime.rds"),
        "cctv" = readRDS("cor_daily_cctv_uptime.rds"),
        "ru" = readRDS("cor_daily_rsu_uptime.rds"),
        "ttyp" = readRDS("tasks_by_type.rds")$cor_daily,
        "tsub" = readRDS("tasks_by_subtype.rds")$cor_daily,
        "tpri" = readRDS("tasks_by_priority.rds")$cor_daily,
        "tsou" = readRDS("tasks_by_source.rds")$cor_daily,
        "tasks" = readRDS("tasks_all.rds")$cor_daily
    )
    cor$wk <- list(
        "vpd" = readRDS("cor_weekly_vpd.rds"),
        #"vph" = readRDS("cor_weekly_vph.rds"),
        "vphp" = readRDS("cor_weekly_vph_peak.rds"),
        "papd" = readRDS("cor_weekly_papd.rds"),
        #"paph" = readRDS("cor_weekly_paph.rds"),
        "pd" = readRDS("cor_weekly_pd_by_day.rds"),
        "tp" = readRDS("cor_weekly_throughput.rds"),
        "aog" = readRDS("cor_weekly_aog_by_day.rds"),
        "pr" = readRDS("cor_weekly_pr_by_day.rds"),
        "qs" = readRDS("cor_wqs.rds"),
        "sf" = readRDS("cor_wsf.rds"),
        "sfo" = readRDS("cor_wsfo.rds"),
        "du" = readRDS("cor_weekly_detector_uptime.rds"),
        "cu" = readRDS("cor_weekly_comm_uptime.rds"),
        "pau" = readRDS("cor_weekly_pa_uptime.rds"),
        "cctv" = readRDS("cor_weekly_cctv_uptime.rds"),
        "ru" = readRDS("cor_weekly_rsu_uptime.rds")
    )
    cor$mo <- list(
        "vpd" = readRDS("cor_monthly_vpd.rds"),
        #"vph" = readRDS("cor_monthly_vph.rds"),
        "vphp" = readRDS("cor_monthly_vph_peak.rds"),
        "papd" = readRDS("cor_monthly_papd.rds"),
        #"paph" = readRDS("cor_monthly_paph.rds"),
        "pd" = readRDS("cor_monthly_pd_by_day.rds"),
        "tp" = readRDS("cor_monthly_throughput.rds"),
        "aogd" = readRDS("cor_monthly_aog_by_day.rds"),
        "aogh" = readRDS("cor_monthly_aog_by_hr.rds"),
        "prd" = readRDS("cor_monthly_pr_by_day.rds"),
        "prh" = readRDS("cor_monthly_pr_by_hr.rds"),
        "qsd" = readRDS("cor_monthly_qsd.rds"),
        "qsh" = readRDS("cor_mqsh.rds"),
        "sfd" = readRDS("cor_monthly_sfd.rds"),
        "sfh" = readRDS("cor_msfh.rds"),
        "sfo" = readRDS("cor_monthly_sfo.rds"),
        "tti" = readRDS("cor_monthly_tti.rds"),
        "ttih" = readRDS("cor_monthly_tti_by_hr.rds"),
        "pti" = readRDS("cor_monthly_pti.rds"),
        "ptih" = readRDS("cor_monthly_pti_by_hr.rds"),
        "bi" = readRDS("cor_monthly_bi.rds"),
        "bih" = readRDS("cor_monthly_bi_by_hr.rds"),
        "du" = readRDS("cor_monthly_detector_uptime.rds"),
        "cu" = readRDS("cor_monthly_comm_uptime.rds"),
        "pau" = readRDS("cor_monthly_pa_uptime.rds"),
        #"veh" = readRDS("cor_monthly_xl_veh_uptime.rds"),
        #"ped" = readRDS("cor_monthly_xl_ped_uptime.rds"),
        "cctv" = readRDS("cor_monthly_cctv_uptime.rds"),
        "ru" = readRDS("cor_monthly_rsu_uptime.rds") %>% 
            complete(
                nesting(Corridor, Zone_Group), 
                Month = dates),
        #"events" = readRDS("cor_monthly_events.rds"),
        "ttyp" = readRDS("tasks_by_type.rds")$cor_monthly,
        "tsub" = readRDS("tasks_by_subtype.rds")$cor_monthly,
        "tpri" = readRDS("tasks_by_priority.rds")$cor_monthly,
        "tsou" = readRDS("tasks_by_source.rds")$cor_monthly,
        "tasks" = readRDS("tasks_all.rds")$cor_monthly,
        "over45" = readRDS("cor_tasks_by_date.rds") %>%
            transmute(Zone_Group, Corridor, Month, over45, delta = delta.over45),
        "mttr" = readRDS("cor_tasks_by_date.rds") %>%
            transmute(Zone_Group, Corridor, Month, mttr, delta = delta.mttr),
        "hourly_udc" = readRDS("hourly_udc.rds"),
        "udc_trend_table" = readRDS("udc_trend_table_list.rds")
    )
    cor$qu <- list(
        "vpd" = get_quarterly(cor$mo$vpd, "vpd"),
        #"vph" = data.frame(), # get_quarterly(cor$mo$vph, "vph"),
        "vphpa" = get_quarterly(cor$mo$vphp$am, "vph"),
        "vphpp" = get_quarterly(cor$mo$vphp$pm, "vph"),
        "papd" = get_quarterly(cor$mo$papd, "papd"),
        "pd" = get_quarterly(cor$mo$pd, "Duration"),
        "tp" = get_quarterly(cor$mo$tp, "vph"),
        "aogd" = get_quarterly(cor$mo$aogd, "aog", "vol"),
        "prd" = get_quarterly(cor$mo$prd, "pr", "vol"),
        "qsd" = get_quarterly(cor$mo$qsd, "qs_freq"),
        "sfd" = get_quarterly(cor$mo$sfd, "sf_freq"),
        "sfo" = get_quarterly(cor$mo$sfo, "sf_freq"),
        "tti" = get_quarterly(cor$mo$tti, "tti"),
        "pti" = get_quarterly(cor$mo$pti, "pti"),
        "bi" = get_quarterly(cor$mo$bi, "bi"),
        "du" = get_quarterly(cor$mo$du, "uptime"),
        "cu" = get_quarterly(cor$mo$cu, "uptime"),
        "pau" = get_quarterly(cor$mo$pau, "uptime"),
        #"veh" = get_quarterly(cor$mo$veh, "uptime", "num"),
        #"ped" = get_quarterly(cor$mo$ped, "uptime", "num"),
        "cctv" = get_quarterly(cor$mo$cctv, "uptime", "num"),
        "ru" = get_quarterly(cor$mo$ru, "uptime"),
        #"reported" = get_quarterly(cor$mo$events, "Reported"),
        #"resolved" = get_quarterly(cor$mo$events, "Resolved"),
        #"outstanding" = get_quarterly(cor$mo$events, "Outstanding", operation = "latest")
        "reported" = get_quarterly(cor$mo$tasks, "Reported"),
        "resolved" = get_quarterly(cor$mo$tasks, "Resolved"),
        "outstanding" = get_quarterly(cor$mo$tasks, "Outstanding", operation = "latest"),
        "over45" = get_quarterly(cor$mo$over45, "over45", operation = "latest"),
        "mttr" = get_quarterly(cor$mo$mttr, "mttr", operation = "latest")
    )
    
    cor$summary_data <- get_corridor_summary_data(cor)
    
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})


tryCatch({
    sub <- list()
    sub$dy <- list(
        "du" = readRDS("sub_avg_daily_detector_uptime.rds") %>%
            select(Zone_Group, Corridor, Date, uptime.sb, uptime.pr, uptime),
        "cu" = readRDS("sub_daily_comm_uptime.rds") %>%
            select(Zone_Group, Corridor, Date, uptime),
        "pau" = readRDS("sub_daily_pa_uptime.rds") %>%
            select(Zone_Group, Corridor, Date, uptime),
        "cctv" = readRDS("sub_daily_cctv_uptime.rds") %>%
            select(Zone_Group, Corridor, Date, uptime),
        
        # temp until we choose to group cctv by subcorridor
        #"cctv" = cor$dy$cctv %>% 
        #    filter(as.character(Zone_Group) != as.character(Corridor)) %>% 
        #    mutate(Zone_Group = Corridor) %>%
        #    select(Zone_Group, Corridor, Date, uptime),
        "ru" = readRDS("sub_daily_rsu_uptime.rds") %>%
            select(Zone_Group, Corridor, Date, uptime)
    )
    sub$wk <- list(
        "vpd" = readRDS("sub_weekly_vpd.rds") %>%
            select(Zone_Group, Corridor, Date, vpd),
        #"vph" = readRDS("sub_weekly_vph.rds"),
        "vphp" = readRDS("sub_weekly_vph_peak.rds") %>%
            map(~select(., Zone_Group, Corridor, Date, vph)),
        "papd" = readRDS("sub_weekly_papd.rds") %>%
            select(Zone_Group, Corridor, Date, papd),
        #"paph" = readRDS("sub_weekly_paph.rds"),
        "pd" = readRDS("sub_weekly_pd_by_day.rds") %>%
            select(Zone_Group, Corridor, Date, Duration),
        "tp" = readRDS("sub_weekly_throughput.rds") %>%
            select(Zone_Group, Corridor, Date, vph),
        "aog" = readRDS("sub_weekly_aog_by_day.rds") %>%
            select(Zone_Group, Corridor, Date, aog),
        "pr" = readRDS("sub_weekly_pr_by_day.rds") %>%
            select(Zone_Group, Corridor, Date, pr),
        "qs" = readRDS("sub_wqs.rds") %>%
            select(Zone_Group, Corridor, Date, qs_freq),
        "sf" = readRDS("sub_wsf.rds") %>%
            select(Zone_Group, Corridor, Date, sf_freq),
        "sfo" = readRDS("sub_wsfo.rds") %>%
            select(Zone_Group, Corridor, Date, sf_freq),
        "du" = readRDS("sub_weekly_detector_uptime.rds") %>%
            select(Zone_Group, Corridor, Date, uptime),
        "cu" = readRDS("sub_weekly_comm_uptime.rds") %>%
            select(Zone_Group, Corridor, Date, uptime),
        "pau" = readRDS("sub_weekly_pa_uptime.rds") %>%
            select(Zone_Group, Corridor, Date, uptime),
        "cctv" = readRDS("sub_weekly_cctv_uptime.rds") %>%
            select(Zone_Group, Corridor, Date, uptime),
        # temp until we choose to group cctv by subcorridor
        #"cctv" = cor$wk$cctv %>% 
        #    filter(as.character(Zone_Group) != as.character(Corridor)) %>% 
        #    mutate(Zone_Group = Corridor) %>%
        #    select(Zone_Group, Corridor, Date, uptime),
        "ru" = readRDS("sub_weekly_rsu_uptime.rds") %>%
            select(Zone_Group, Corridor, Date, uptime)
    )
    sub$mo <- list(
        "vpd" = readRDS("sub_monthly_vpd.rds"),
        #"vph" = readRDS("sub_monthly_vph.rds"),
        "vphp" = readRDS("sub_monthly_vph_peak.rds"),
        "papd" = readRDS("sub_monthly_papd.rds"),
        #"paph" = readRDS("sub_monthly_paph.rds"),
        "pd" = readRDS("sub_monthly_pd_by_day.rds"),
        "tp" = readRDS("sub_monthly_throughput.rds"),
        "aogd" = readRDS("sub_monthly_aog_by_day.rds"),
        "aogh" = readRDS("sub_monthly_aog_by_hr.rds"),
        "prd" = readRDS("sub_monthly_pr_by_day.rds"),
        "prh" = readRDS("sub_monthly_pr_by_hr.rds"),
        "qsd" = readRDS("sub_monthly_qsd.rds"),
        "qsh" = readRDS("sub_mqsh.rds"),
        "sfd" = readRDS("sub_monthly_sfd.rds"),
        "sfo" = readRDS("sub_monthly_sfo.rds"),
        "sfh" = readRDS("sub_msfh.rds"),
        "tti" = readRDS("sub_monthly_tti.rds"),
        "ttih" = readRDS("sub_monthly_tti_by_hr.rds"),
        "pti" = readRDS("sub_monthly_pti.rds"),
        "ptih" = readRDS("sub_monthly_pti_by_hr.rds"),
        "bi" = readRDS("sub_monthly_bi.rds"),
        "bih" = readRDS("sub_monthly_bi_by_hr.rds"),
        "du" = readRDS("sub_monthly_detector_uptime.rds"),
        "cu" = readRDS("sub_monthly_comm_uptime.rds"),
        "pau" = readRDS("sub_monthly_pa_uptime.rds"),
        "cctv" = readRDS("sub_monthly_cctv_uptime.rds"),
        # temp until we choose to group cctv by subcorridor
        #"cctv" = cor$mo$cctv %>% 
        #    filter(as.character(Zone_Group) != as.character(Corridor)) %>% 
        #    mutate(Zone_Group = Corridor),
        "ru" = readRDS("sub_monthly_rsu_uptime.rds") %>% 
            complete(
                nesting(Corridor, Zone_Group), 
                Month = dates)
    )
    sub$qu <- list(
        "vpd" = get_quarterly(sub$mo$vpd, "vpd"),
        #"vph" = get_quarterly(sub$mo$vph, "vph"),
        "vphpa" = get_quarterly(sub$mo$vphp$am, "vph"),
        "vphpp" = get_quarterly(sub$mo$vphp$pm, "vph"),
        "tp" = get_quarterly(sub$mo$tp, "vph"),
        "aogd" = get_quarterly(sub$mo$aogd, "aog", "vol"),
        "prd" = get_quarterly(sub$mo$prd, "pr", "vol"),
        "qsd" = get_quarterly(sub$mo$qsd, "qs_freq"),
        "sfd" = get_quarterly(sub$mo$sfd, "sf_freq"),
        "sfo" = get_quarterly(sub$mo$sfo, "sf_freq"),
        "du" = get_quarterly(sub$mo$du, "uptime"),
        "cu" = get_quarterly(sub$mo$cu, "uptime"),
        "pau" = get_quarterly(sub$mo$pau, "uptime"),
        "cctv" = get_quarterly(sub$mo$cctv, "uptime"),
        "ru" = get_quarterly(sub$mo$ru, "uptime")
    )
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})



tryCatch({
    sig <- list()
    sig$dy <- list(
        "du" = sigify(readRDS("avg_daily_detector_uptime.rds"), cor$dy$du, corridors) %>%
            select(Zone_Group, Corridor, Date, uptime, uptime.sb, uptime.pr),
        "cu" = sigify(readRDS("daily_comm_uptime.rds"), cor$dy$cu, corridors) %>%
            select(Zone_Group, Corridor, Date, uptime),
        "pau" = sigify(readRDS("daily_pa_uptime.rds"), cor$dy$pau, corridors) %>%
            select(Zone_Group, Corridor, Date, uptime),
        "cctv" = sigify(readRDS("daily_cctv_uptime.rds"), cor$dy$cctv, cam_config, identifier = "CameraID") %>%
            select(Zone_Group, Corridor, Description, Date, uptime, up) %>%
            mutate(Description = ifelse(is.na(Description), as.character(Corridor), as.character(Description)),
                   Description = factor(Description)),
        "ru" = sigify(readRDS("daily_rsu_uptime.rds"), cor$dy$ru, corridors),
        "ttyp" = readRDS("tasks_by_type.rds")$sig_daily,
        "tsub" = readRDS("tasks_by_subtype.rds")$sig_daily,
        "tpri" = readRDS("tasks_by_priority.rds")$sig_daily,
        "tsou" = readRDS("tasks_by_source.rds")$sig_daily,
        "tasks" = readRDS("tasks_all.rds")$sig_daily
    )
    sig$wk <- list(
        "vpd" = sigify(readRDS("weekly_vpd.rds"), cor$wk$vpd, corridors) %>%
            select(Zone_Group, Corridor, Date, vpd),
        #"vph" = sigify(readRDS("weekly_vph.rds"), cor$wk$vph, corridors),
        "vphp" = purrr::map2(
            readRDS("weekly_vph_peak.rds"), cor$wk$vphp,
            function(x, y) {
                sigify(x, y, corridors) %>%
                    select(Zone_Group, Corridor, Date, vph)
            }
        ),
        "papd" = sigify(readRDS("weekly_papd.rds"), cor$wk$papd, corridors) %>%
            select(Zone_Group, Corridor, Date, papd),
        #"paph" = sigify(readRDS("weekly_paph.rds"), cor$wk$paph, corridors),
        "pd" = sigify(readRDS("weekly_pd_by_day.rds"), cor$wk$pd, corridors) %>%
            select(Zone_Group, Corridor, Date, Duration),
        "tp" = sigify(readRDS("weekly_throughput.rds"), cor$wk$tp, corridors) %>%
            select(Zone_Group, Corridor, Date, vph),
        "aog" = sigify(readRDS("weekly_aog_by_day.rds"), cor$wk$aog, corridors) %>%
            select(Zone_Group, Corridor, Date, aog),
        "pr" = sigify(readRDS("weekly_pr_by_day.rds"), cor$wk$pr, corridors) %>%
            select(Zone_Group, Corridor, Date, pr),
        "qs" = sigify(readRDS("wqs.rds"), cor$wk$qs, corridors) %>%
            select(Zone_Group, Corridor, Date, qs_freq),
        "sf" = sigify(readRDS("wsf.rds"), cor$wk$sf, corridors) %>%
            select(Zone_Group, Corridor, Date, sf_freq),
        "sfo" = sigify(readRDS("wsfo.rds"), cor$wk$sfo, corridors) %>%
            select(Zone_Group, Corridor, Date, sf_freq),
        "cu" = sigify(readRDS("weekly_comm_uptime.rds"), cor$wk$cu, corridors) %>%
            select(Zone_Group, Corridor, Date, uptime),
        "pau" = sigify(readRDS("weekly_pa_uptime.rds"), cor$wk$pau, corridors) %>%
            select(Zone_Group, Corridor, Date, uptime),
        "cctv" = sigify(readRDS("weekly_cctv_uptime.rds"), cor$wk$cctv, cam_config, identifier = "CameraID") %>%
            select(Zone_Group, Corridor, Description, Date, uptime) %>%
            mutate(Description = ifelse(is.na(Description), as.character(Corridor), as.character(Description)),
                   Description = factor(Description)),
        "ru" = sigify(readRDS("weekly_rsu_uptime.rds"), cor$wk$ru, corridors)
    )
    sig$mo <- list(
        "vpd" = sigify(readRDS("monthly_vpd.rds"), cor$mo$vpd, corridors) %>%
            select(-c(Name, ones)),
        #"vph" = sigify(readRDS("monthly_vph.rds"), cor$mo$vph, corridors) %>%
        #    select(-c(Name, ones)),
        "vphp" = purrr::map2(
            readRDS("monthly_vph_peak.rds"), cor$mo$vphp,
            function(x, y) {
                sigify(x, y, corridors) %>%
                    select(-c(Name, ones, Zone))
            }
        ),
        "papd" = sigify(readRDS("monthly_papd.rds"), cor$mo$papd, corridors) %>%
            select(-c(Name, ones)),
        #"paph" = sigify(readRDS("monthly_paph.rds"), cor$mo$paph, corridors) %>%
        #    select(-c(Name, ones)),
        "pd" = sigify(readRDS("monthly_pd_by_day.rds"), cor$mo$pd, corridors) %>%
            select(-c(Name, ones)),
        "tp" = sigify(readRDS("monthly_throughput.rds"), cor$mo$tp, corridors) %>%
            select(-c(Name, ones)),
        "aogd" = sigify(readRDS("monthly_aog_by_day.rds"), cor$mo$aogd, corridors) %>%
            select(-c(Name, vol)),
        "aogh" = sigify(readRDS("monthly_aog_by_hr.rds"), cor$mo$aogh, corridors) %>%
            select(-c(Name, vol)),
        "prd" = sigify(readRDS("monthly_pr_by_day.rds"), cor$mo$prd, corridors) %>%
            select(-c(Name, vol)),
        "prh" = sigify(readRDS("monthly_pr_by_hr.rds"), cor$mo$prh, corridors) %>%
            select(-c(Name, vol)),
        "qsd" = sigify(readRDS("monthly_qsd.rds"), cor$mo$qsd, corridors) %>%
            select(-c(Name, cycles)),
        "qsh" = sigify(readRDS("mqsh.rds"), cor$mo$qsh, corridors) %>%
            select(-c(Name, cycles)),
        "sfd" = sigify(readRDS("monthly_sfd.rds"), cor$mo$sfd, corridors) %>%
            select(-c(Name, cycles)),
        "sfh" = sigify(readRDS("msfh.rds"), cor$mo$sfh, corridors) %>%
            select(-c(Name, cycles)),
        "sfo" = sigify(readRDS("monthly_sfo.rds"), cor$mo$sfo, corridors) %>%
            select(-c(Name, cycles)),
        "tti" = data.frame(),
        "pti" = data.frame(),
        "bi" = data.frame(),
        "du" = sigify(readRDS("monthly_detector_uptime.rds"), cor$mo$du, corridors) %>%
            select(Zone_Group, Corridor, Month, uptime, uptime.sb, uptime.pr, delta),
        "cu" = sigify(readRDS("monthly_comm_uptime.rds"), cor$mo$cu, corridors) %>%
            select(Zone_Group, Corridor, Month, uptime, delta),
        "pau" = sigify(readRDS("monthly_pa_uptime.rds"), cor$mo$pau, corridors) %>%
            select(Zone_Group, Corridor, Month, uptime, delta),
        "cctv" = sigify(readRDS("monthly_cctv_uptime.rds"), cor$mo$cctv, cam_config, identifier = "CameraID") %>%
            select(Zone_Group, Corridor, Description, Month, uptime, delta) %>%
            mutate(Description = ifelse(is.na(Description), as.character(Corridor), as.character(Description)),
                   Description = factor(Description)),
        "ru" = sigify(readRDS("monthly_rsu_uptime.rds"), cor$mo$ru, corridors),
        "ttyp" = readRDS("tasks_by_type.rds")$sig_monthly,
        "tsub" = readRDS("tasks_by_subtype.rds")$sig_monthly,
        "tpri" = readRDS("tasks_by_priority.rds")$sig_monthly,
        "tsou" = readRDS("tasks_by_source.rds")$sig_monthly,
        "tasks" = readRDS("tasks_all.rds")$sig_monthly,
        "over45" = readRDS("sig_tasks_by_date.rds") %>%
            transmute(Zone_Group, Corridor, Month, over45, delta = delta.over45),
        "mttr" = readRDS("sig_tasks_by_date.rds") %>%
            transmute(Zone_Group, Corridor, Month, mttr, delta = delta.mttr)
    )
}, error = function(e) {
    print("ENCOUNTERED AN ERROR:")
    print(e)
})

# Assign Descriptions for hover text

descs <- corridors %>% 
    select(SignalID, Corridor, Description) %>%
    group_by(SignalID, Corridor) %>% 
    filter(Description == first(Description)) %>% 
    ungroup()

for (tab in c("vpd","papd","pd",
              "tp","aog","aogd","aogh","pr","prd","prh","qs","qsd","qsh","sf","sfd","sfh","sfo",
              "du","cu","pau","cctv","ru")) {
    print(tab)
    if (tab %in% names(sig$mo) & tab != "cctv") {
        sig$mo[[tab]] <- sig$mo[[tab]] %>% 
            left_join(descs, by = c("Corridor" = "SignalID", "Zone_Group" = "Corridor")) %>%
            mutate(
                Description = coalesce(Description, Corridor),
                Corridor = factor(Corridor),
                Description = factor(Description))
    }
    if (tab %in% names(sub$mo)) {
        sub$mo[[tab]] <- sub$mo[[tab]] %>% mutate(Description = Corridor)
    }
    if (tab %in% names(cor$mo)) {
        cor$mo[[tab]] <- cor$mo[[tab]] %>% mutate(Description = Corridor)
    }
    
    if (tab %in% names(sig$wk) & tab != "cctv") {
        sig$wk[[tab]] <- sig$wk[[tab]] %>% 
            left_join(descs, by = c("Corridor" = "SignalID", "Zone_Group" = "Corridor")) %>%
            mutate(Description = coalesce(Description, Corridor),
                   Corridor = factor(Corridor),
                   Description = factor(Description))
    }
    if (tab %in% names(sub$wk)) {
        sub$wk[[tab]] <- sub$wk[[tab]] %>% mutate(Description = Corridor)
    }
    if (tab %in% names(cor$wk)) {
        cor$wk[[tab]] <- cor$wk[[tab]] %>% mutate(Description = Corridor)
    }
}

for (tab in c("du", "cu", "ru", "pau")) {
    print(tab)
    sig$dy[[tab]] <- sig$dy[[tab]] %>% 
        left_join(descs, by = c("Corridor" = "SignalID", "Zone_Group" = "Corridor")) %>%
        mutate(
            Description = coalesce(Description, Corridor),
            Corridor = factor(Corridor),
            Description = factor(Description))
}



saveRDS(cor, "cor.rds")
saveRDS(sig, "sig.rds")
saveRDS(sub, "sub.rds")

print(glue("{Sys.time()} Upload to AWS [23 of 23]"))

aws.s3::put_object(
    file = "cor.rds",
    object = "cor_ec2.rds",
    bucket = conf$bucket,
    multipart = TRUE
)
aws.s3::put_object(
    file = "sig.rds",
    object = "sig_ec2.rds",
    bucket = conf$bucket,
    multipart = TRUE
)
aws.s3::put_object(
    file = "sub.rds",
    object = "sub_ec2.rds",
    bucket = conf$bucket,
    multipart = TRUE
)


qsave(cor, "cor.qs")
qsave(sig, "sig.qs")
qsave(sub, "sub.qs")



aws.s3::put_object(
    file = "cor.qs",
    object = "cor_ec2.qs",
    bucket = conf$bucket,
    multipart = TRUE
)
aws.s3::put_object(
    file = "sig.qs",
    object = "sig_ec2.qs",
    bucket = conf$bucket,
    multipart = TRUE
)
aws.s3::put_object(
    file = "sub.qs",
    object = "sub_ec2.qs",
    bucket = conf$bucket,
    multipart = TRUE
)


#------------------------------------------
conf_mode <- "production"
source("Monthly_Report_UI_Functions.R")
#------------------------------------------
perf_plots <- list()

perf_plots$tp_trend <- perf_plot_beta_(
    filter(cor$mo$tp, Corridor=="All RTOP"), 
    "vph", "", RED2, format_func = as_int)
perf_plots$aog_trend <- perf_plot_beta_(
    filter(cor$mo$aogd, Corridor=="All RTOP"), 
    "aog", "", RED2, format_func = as_pct, hoverformat = ".1%")
perf_plots$pr_trend <- perf_plot_beta_(
    filter(cor$mo$prd, Corridor=="All RTOP"),
    "pr", "", RED2, format_func = as_2dec, hoverformat = ".2f")
perf_plots$qs_trend <- perf_plot_beta_(
    filter(cor$mo$qsd, Corridor=="All RTOP"), 
    "qs_freq", "", RED2, format_func = as_pct, hoverformat = ".1%")
perf_plots$sf_trend <- perf_plot_beta_(
    filter(cor$mo$sfd, Corridor=="All RTOP"),
    "sf_freq", "", RED2, format_func = as_pct, hoverformat = ".1%")
perf_plots$sfo_trend <- perf_plot_beta_(
    filter(cor$mo$sfo, Corridor=="All RTOP"), 
    "sf_freq", "", RED2, format_func = as_pct, hoverformat = ".1%")
perf_plots$tti_trend <- perf_plot_beta_(
    filter(cor$mo$tti, Corridor=="All RTOP"), 
    "tti", "", RED2, format_func = as_2dec, hoverformat = ".2f")
perf_plots$pti_trend <- perf_plot_beta_(
    filter(cor$mo$pti, Corridor=="All RTOP"), 
    "pti", "", RED2, format_func = as_2dec, hoverformat = ".2f")
perf_plots$vpd_trend <- perf_plot_beta_(
    filter(cor$mo$vpd, Corridor=="All RTOP"), 
    "vpd", "", GDOT_BLUE, format_func = as_int)
perf_plots$vpha_trend <- perf_plot_beta_(
    filter(cor$mo$vphp$am, Corridor=="All RTOP"),
    "vph", "", GDOT_BLUE, format_func = as_int)
perf_plots$vphp_trend <- perf_plot_beta_(
    filter(cor$mo$vphp$pm, Corridor=="All RTOP"),
    "vph", "", GDOT_BLUE, format_func = as_int)
perf_plots$du_trend <- perf_plot_beta_(
    filter(cor$mo$du, Corridor=="All RTOP"),
    "uptime", "", ORANGE, format_func = as_pct, hoverformat = ".1%")
perf_plots$pau_trend <- perf_plot_beta_(
    filter(cor$mo$pau, Corridor=="All RTOP"),
    "uptime", "", ORANGE, format_func = as_pct, hoverformat = ".1%")
perf_plots$cctv_trend <- perf_plot_beta_(
    filter(cor$mo$cctv, Corridor=="All RTOP"),
    "uptime", "", ORANGE, format_func = as_pct, hoverformat = ".1%")
perf_plots$cu_trend <- perf_plot_beta_(
    filter(cor$mo$cu, Corridor=="All RTOP"),
    "uptime", "", ORANGE, format_func = as_pct, hoverformat = ".1%")
perf_plots$ru_trend <- perf_plot_beta_(
    filter(cor$mo$ru, Corridor=="All RTOP"),
    "uptime", "", ORANGE, format_func = as_pct, hoverformat = ".1%")

qsave(perf_plots, "perf_plots.qs")

aws.s3::put_object(
    file = "perf_plots.qs",
    object = "perf_plots.qs",
    bucket = conf$bucket,
    multipart = TRUE)
