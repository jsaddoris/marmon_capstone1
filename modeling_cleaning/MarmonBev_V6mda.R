#install.packages("arrow")
rm(list = ls())
library(arrow)
library(dplyr)
library(lubridate)
df_msgs = read_parquet("2025-01-27_messages.parquet")
df_totals = read_parquet("2025-01-27_totals.parquet")
df_msgs$device_id = as.factor(df_msgs$device_id)
df_msgs$element = as.factor(df_msgs$element)
df_msgs$state = as.factor(df_msgs$state)

msgs = c("7001", "7005", "7009", "7024", "7028", "7032", "7040", "7044", "7050", "7055")

devices = c("19", "20", "21", "22", "23", "25", "26", "27", "28", "29", "30", "32", "33", "34", "35",
"36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "50", "51",
"52", "53", "55", "56", "57", "58", "60", "61", "62", "64", "65", "66", "94", "96", "97")

df_msgs = df_msgs %>% filter(device_id %in% devices)
df_msgs = df_msgs %>% filter(msg_id %in% msgs)


df_msgs = df_msgs %>% arrange(device_id, element,msg_id, gmt)


library(tidyr)
df_msgs_p = df_msgs %>% group_by(device_id, element,msg_id, gmt, state ) %>%
  summarise(count = n())

df_msgs_p = df_msgs_p %>% arrange(device_id, element,msg_id, gmt)



df_msgs_p2 = df_msgs_p %>%
  filter(state == 2)
df_msgs_p2$starttime = df_msgs_p2$gmt
df_msgs_p2$resolvetime = as.POSIXct(0)

df_msgs_p5 = df_msgs_p %>% 
  filter(state == 5)
df_msgs_p5$starttime = as.POSIXct(0)
df_msgs_p5$resolvetime = df_msgs_p5$gmt

df_msgs_c = df_msgs_p2 %>% rbind(df_msgs_p5)

df_msgs_c = df_msgs_c %>% arrange(device_id, element,msg_id, gmt,state)

df_msgs_c = df_msgs_c %>% group_by(device_id, element,msg_id) %>%
  mutate(resolvetime_c = lead(resolvetime))

df_msgs_c$resolvetime = NULL

df_msgs_c = df_msgs_c %>% filter(state == 2)


df_msgs_c$s_diff = df_msgs_c$resolvetime_c - df_msgs_c$starttime 

df_msgs_c$s_diff = as.numeric(df_msgs_c$s_diff)
df_msgs_c = df_msgs_c %>% filter(s_diff > 0)
df_msgs_c$m_diff = df_msgs_c$s_diff/60
df_msgs_c$h_diff = df_msgs_c$m_diff/60
df_msgs_c$d_diff = df_msgs_c$h_diff/24

df_msgs_p_f = df_msgs_c %>% filter(m_diff > 1)

rm(list = c("df_msgs_p2","df_msgs_p5", "df_msgs_c","df_msgs_p"))
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
         b1_syrup_diff = b1_syrup_fill - lag(b1_syrup_fill),
         b2_syrup_diff = b2_syrup_fill - lag(b2_syrup_fill),
         b3_syrup_diff = b3_syrup_fill - lag(b3_syrup_fill),
         b4_syrup_diff = b4_syrup_fill - lag(b4_syrup_fill),
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
            percent_power = power_h/total_time_h)
            #we will get to these later
            #percent_defrost = avg_defrost_total_h/total_time_h,
            #percent_syrup_out = avg_syrup_out_total_h/total_time_h)

# Sum each barrel to get device total for defrost and syrup out
totals_month2 <- totals_month2 %>%
  mutate(device_total_defrost_hr= b1_defrost_total_h + b2_defrost_total_h + b3_defrost_total_h + b4_defrost_total_h)

totals_month2 <- totals_month2 %>%
  mutate(device_total_syrupout_hr= b1_syrup_out_total_h + b2_syrup_out_total_h + b3_syrup_out_total_h + b4_syrup_out_total_h)

device_avg_defrost_month <- totals_month2 %>% group_by(device_id) %>% 
                              summarize(avg_defrost_hr= mean(device_total_defrost_month, na.rm = TRUE),
                              avg_syrupout_hr= mean(device_total_syrupout_hr, na.rm = TRUE))

# How to deal with power_on being higher than total_time ???

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
totals_month2$newb1_syrup_out_total_h=0
totals_month2$newb2_syrup_out_total_h=0
totals_month2$newb3_syrup_out_total_h=0
totals_month2$newb4_syrup_out_total_h=0

sorted_syrup <- t(apply(totals_month2[, c("b1_syrup_out_total_h", "b2_syrup_out_total_h", "b3_syrup_out_total_h", "b4_syrup_out_total_h")], 1, function(x) sort(x, decreasing = TRUE, na.last = TRUE)))

totals_month2$newb1_syrup_out_total_h <- sorted_syrup[,1]
totals_month2$newb2_syrup_out_total_h <- sorted_syrup[,2]
totals_month2$newb3_syrup_out_total_h <- sorted_syrup[,3]
totals_month2$newb4_syrup_out_total_h <- sorted_syrup[,4]


write.csv(totals_month2, "Marmon_totals_analysis_v2.csv", row.names = FALSE)
