library(aws.s3)
library(dplyr)
library(tidyr)
library(lubridate)
library(purrr)
library(readr)


get_summary_data <- function(df, current_month = NULL) {
    #' Converts sub or cor data set to a single data frame for the current_month
    #' for use in get_subcorridor_summary_table function
    #'
    #' @param df cor or sub data
    #' @param current_month Current month in date format
    #' @return A data frame, monthly data of all metrics by Zone and Corridor
    # current_month <- months[order(months)][match(current_month, months.formatted)]
    data <- list(
        rename(df$mo$du, du = uptime, du.delta = delta),
        rename(df$mo$pau, pau = uptime, pau.delta = delta),
        #rename(df$mo$cctv, cctv = uptime, cctv.delta = delta),
        rename(df$mo$ru, rsu = uptime, rsu.delta = delta),
        rename(df$mo$cu, cu = uptime, cu.delta = delta),
        rename(df$mo$tp, tp.delta = delta),
        rename(df$mo$aogd, aog.delta = delta),
        rename(df$mo$qsd, qs.delta = delta),
        rename(df$mo$sfd, sf.delta = delta),
        rename(df$mo$tti, tti.delta = delta),
        rename(df$mo$pti, pti.delta = delta)
    ) %>%
        reduce(full_join, by = c("Zone_Group", "Corridor", "Month")) %>%
        filter(
            Zone_Group != Corridor
        ) %>%
        select(
            -uptime.sb,
            -uptime.pr,
            #-num,
            -starts_with("Description"),
            -starts_with("ones"),
            -starts_with("cycles"),
            -starts_with("pct"),
            -starts_with("vol")
        )
    if ("mttr" %in% names(df$mo)) { # cor, not sub
        data <- data %>% 
            arrange(Month, Zone_Group, Corridor)
    } else {
        data <- data %>% 
            rename(
                Subcorridor = Corridor,
                Corridor = Zone_Group
            ) %>%
            arrange(Month, Corridor, Subcorridor)
    }
    data <- data[, c(2, 1, 3:length(data))] # reorder Subcorridor, Corridor
    if (!is.null(current_month)) {
        data <- data %>% filter(Month == ymd(current_month))
    }
    return(data)
}


sub <- s3readRDS(bucket = "gdot-spm", object = "sub_ec2.rds")
cor <- s3readRDS(bucket = "gdot-spm", object = "cor_ec2.rds")

current_month <- ymd("2020-04-01")

csd <- get_summary_data(sub)
cmd <- get_summary_data(cor)


lapply(cor$mo, names)
lapply(cor$wk, names)
lapply(cor$dy, names)

lapply(sig$mo, names)
lapply(sig$wk, names)
lapply(sig$dy, names)

lapply(sub$mo, names)
lapply(sub$wk, names)
lapply(sub$dy, names)


                                    
write_csv(csd, "health_metrics_data_2020-05-18.csv")
