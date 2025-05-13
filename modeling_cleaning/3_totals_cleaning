############TOTALS EXAMPLE
library(arrow)
library(dplyr)
library(lubridate)
df_totals = read_parquet("2025-01-27_totals.parquet")

devices = c("19", "20", "21", "22", "23", "25", "26", "27", "28", "29", "30", "32", "33", "34", "35",
            "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "50", "51",
            "52", "53", "55", "56", "57", "58", "60", "61", "62", "64", "65", "66", "94", "96", "97")

df_totals = df_totals %>% filter(device_id %in% devices)

df_totals$device_id = as.factor(df_totals$device_id)

######NEW For V7 This adds a row for 12-31-2023 so we can get Jan 24 numbers
temp = df_totals %>% arrange(device_id, gmt) %>% group_by(device_id) %>% filter(row_number()==1) %>% filter(gmt < as.Date("2024-1-31"))
temp$gmt = as.POSIXct("2023-12-31 10:00:02")

df_totals = rbind(df_totals,temp)
rm(temp)
######NEW For V7

df_totals_d = df_totals %>% arrange(device_id, gmt) %>%
  group_by(device_id) %>%
  mutate(power_on_diff = power_on - lag(power_on),
         b1_defrost_diff = b1_defrost - lag(b1_defrost),
         b2_defrost_diff = b2_defrost - lag(b2_defrost),
         b3_defrost_diff = b3_defrost - lag(b3_defrost),
         b4_defrost_diff = b4_defrost - lag(b4_defrost),
         b1_syrup_out_diff = b1_syrup_out - lag(b1_syrup_out),
         b2_syrup_out_diff = b2_syrup_out - lag(b2_syrup_out),
         b3_syrup_out_diff = b3_syrup_out - lag(b3_syrup_out),
         b4_syrup_out_diff = b4_syrup_out - lag(b4_syrup_out),
         b1_syrup_fill_diff = b1_syrup_fill - lag(b1_syrup_fill),
         b2_syrup_fill_diff = b2_syrup_fill - lag(b2_syrup_fill),
         b3_syrup_fill_diff = b3_syrup_fill - lag(b3_syrup_fill),
         b4_syrup_fill_diff = b4_syrup_fill - lag(b4_syrup_fill),
         power_on_diff = power_on - lag(power_on),
         time_diff = as.numeric(gmt - lag(gmt)))

df_totals_d$month = month(df_totals_d$gmt)
df_totals_d$year = year(df_totals_d$gmt)

#this will turn on resulting columns as sci not
options(scipen = 999)

totals_month2 = df_totals_d %>% group_by(device_id,year,month) %>% 
  summarise(count = n(),
            total_time_h = sum(time_diff)/60/60,
            power_h = sum(power_on_diff)/60/60,
            b1_defrost_total_h = sum(b1_defrost_diff)/60/60,
            b2_defrost_total_h = sum(b2_defrost_diff)/60/60,
            b3_defrost_total_h = sum(b3_defrost_diff)/60/60,
            b4_defrost_total_h = sum(b4_defrost_diff)/60/60,
            b1_syrup_out_total_h = sum(b1_syrup_out_diff)/60/60,
            b2_syrup_out_total_h = sum(b2_syrup_out_diff)/60/60,
            b3_syrup_out_total_h = sum(b3_syrup_out_diff)/60/60,
            b4_syrup_out_total_h = sum(b4_syrup_out_diff)/60/60,
            b1_syrup_fill_total_h = sum(b1_syrup_fill_diff)/60/60,
            b2_syrup_fill_total_h = sum(b2_syrup_fill_diff)/60/60,
            b3_syrup_fill_total_h = sum(b3_syrup_fill_diff)/60/60,
            b4_syrup_fill_total_h = sum(b4_syrup_fill_diff)/60/60,
            percent_power = power_h/total_time_h)

# Sum each barrel to get device total for defrost and syrup out
totals_month2 <- totals_month2 %>%
  mutate(device_total_defrost_hr= b1_defrost_total_h + b2_defrost_total_h + b3_defrost_total_h + b4_defrost_total_h)

totals_month2 <- totals_month2 %>%
  mutate(device_total_syrupout_hr= b1_syrup_out_total_h + b2_syrup_out_total_h + b3_syrup_out_total_h + b4_syrup_out_total_h)

totals_month2 <- totals_month2 %>%
  mutate(device_total_syrupfill_hr = b1_syrup_fill_total_h + b2_syrup_fill_total_h + b3_syrup_fill_total_h + b4_syrup_fill_total_h)

# Complete list with year/month combos
full_months = expand.grid(
  device_id = unique(totals_month2$device_id),
  year = c(2024, 2025),
  month = 1:12
) %>% filter(!(year == 2025 & month > 1)) %>%
  arrange(device_id, year, month)

# Populate totals_month as totals_filled
totals_month2 = full_months %>%
  left_join(totals_month2, by = c("device_id", "year", "month")) %>%
  arrange(device_id, year, month)

#break the data into three mutually exclusive df to deal with issue
total_time_wrong = totals_month2 %>% filter(percent_power>=1.5)
total_time_right = totals_month2 %>% filter(percent_power<1.5)
total_time_na = totals_month2 %>% filter(is.na(percent_power))

###correct the total_time_h value and the other columns previously using the wrong value.
total_time_wrong$total_time_h = total_time_wrong$total_time_h*6
total_time_wrong$percent_power = total_time_wrong$power_h/total_time_wrong$total_time_h

#combine the dataframes back togehter
totals_month2 = rbind(total_time_wrong, total_time_right)
totals_month2 = rbind(totals_month2, total_time_na)

rm(list = c("total_time_na", "total_time_right", "total_time_wrong"))

df_config = read_parquet("2025-01-27_configuration.parquet")
df_config$device_id = as.factor(df_config$device_id)
totals_month2 = totals_month2 %>% left_join(df_config)

#sort and calculate percents
totals_month2 = totals_month2 %>% arrange(device_id, year, month)
totals_month2$percent_defrost = ((totals_month2$b1_defrost_total_h+totals_month2$b2_defrost_total_h+totals_month2$b3_defrost_total_h+totals_month2$b4_defrost_total_h)/totals_month2$brl_cnt)/totals_month2$total_time_h
totals_month2$percent_syrup_out = ((totals_month2$b1_syrup_out_total_h+totals_month2$b2_syrup_out_total_h+totals_month2$b3_syrup_out_total_h+totals_month2$b4_syrup_out_total_h)/totals_month2$brl_cnt)/totals_month2$total_time_h
totals_month2$percent_current = (totals_month2$percent_power-totals_month2$percent_defrost-totals_month2$percent_defrost)
totals_month2$percent_occupancy = ((totals_month2$percent_power-totals_month2$percent_current)/totals_month2$percent_current)

device_avg_defrost_month <- totals_month2 %>% group_by(device_id) %>% 
  summarize(avg_defrost_hr= mean(device_total_defrost_hr, na.rm = TRUE),
            avg_syrupout_hr= mean(device_total_syrupout_hr, na.rm = TRUE),
            avg_occupancy_pct= mean(percent_occupancy, na.rm = TRUE))

# Extrapolate
extrapolate <- function(df) {
  n <- nrow(df)
  i <- 1
  while (i <= n) {
    if (is.na(df$power_h[i])) {
      start_idx <- i
      while (i <= n && is.na(df$power_h[i])) {
        i <- i + 1
      }
      end_idx <- i
      total_months <- end_idx - start_idx + 1
      
      if (end_idx <= n) {
        # Distribute/divide for sum columns
        sum_cols <- c("power_h", "b1_defrost_total_h", "b2_defrost_total_h", "b3_defrost_total_h", "b4_defrost_total_h", "total_time_h",
                      "b1_syrup_out_total_h", "b2_syrup_out_total_h", "b3_syrup_out_total_h", "b4_syrup_out_total_h")
        for (col in sum_cols) {
          if (col %in% names(df) && !is.na(df[[col]][end_idx])) {
            avg_val <- df[[col]][end_idx] / total_months
            df[start_idx:end_idx, col] <- avg_val
          }
        }
        
        # Copy end index value for percent columns
        rate_cols <- c("percent_power", "percent_defrost", "percent_syrup_out", "percent_current", "percent_occupancy")
        for (col in rate_cols) {
          if (col %in% names(df) && !is.na(df[[col]][end_idx])) {
            df[start_idx:end_idx, col] <- df[[col]][end_idx]
          }
        }
      }
    } else {
      i <- i + 1
    }
  }
  return(df)
}

totals_month2 <- totals_month2 %>%
  group_by(device_id) %>%
  group_modify(~ extrapolate(.x))


totals_month2$newb1_syrup_out_total_h=0
totals_month2$newb2_syrup_out_total_h=0
totals_month2$newb3_syrup_out_total_h=0
totals_month2$newb4_syrup_out_total_h=0

sorted_syrup <- t(apply(totals_month2[, c("b1_syrup_out_total_h", "b2_syrup_out_total_h", "b3_syrup_out_total_h", "b4_syrup_out_total_h")], 1, function(x) sort(x, decreasing = TRUE, na.last = TRUE)))

totals_month2$newb1_syrup_out_total_h <- sorted_syrup[,1]
totals_month2$newb2_syrup_out_total_h <- sorted_syrup[,2]
totals_month2$newb3_syrup_out_total_h <- sorted_syrup[,3]
totals_month2$newb4_syrup_out_total_h <- sorted_syrup[,4]

write.csv(totals_month2, "Marmon_totals_analysis_v6.csv", row.names = FALSE)
write.csv(device_avg_defrost_month, "Marmon_device_defrost_avgs", row.names = FALSE)

