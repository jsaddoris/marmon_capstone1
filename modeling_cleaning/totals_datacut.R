library(readxl)
library(dplyr)
library(openxlsx)

file_path <- "all_files_excel.xlsx"


totals_data <- read_excel(file_path, sheet = "totals")


device_ids_to_keep <- c(1, 2, 3, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 32, 33, 34, 35, 36, 37, 38, 39, 40, 
                        41, 42, 43, 44, 45, 46, 47, 48, 50, 51, 52, 53, 55, 56, 57, 58, 59, 60, 61, 62, 64, 65, 66, 
                        94, 96, 97)

cleaned_data <- filter(totals_data, device_id %in% device_ids_to_keep)

write.xlsx(cleaned_data, file = "totals_datacut_excel.xlsx")