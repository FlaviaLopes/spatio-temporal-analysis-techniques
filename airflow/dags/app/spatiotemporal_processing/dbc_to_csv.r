# /usr/local/lib Rscript
install.packages('read.dbc')
install.packages('list')
library(read.dbc)

args <- commandArgs(trailingOnly = TRUE)
input_path <- args[1]
output_path <- args[2]

files = list.files(input_path)
files <- files[1:length(files)]
for (i in 1:length(files)){
    df <- read.dbc(paste(input_path, '/', files[i], sep=''))
    write.csv(df, paste(output_path, '/', unlist(strsplit(files[i], '\\.'))[1], '.csv', sep=''))
}
