library(dplyr)
library(dbplyr) # DB connectivity for dplyr
library(DBI)    # Useful for working with DBs
library(lubridate) # The best date package anywhere
library(stringr) # Just used to pad with zeros
library(ggplot2)
library(scales)
library(tidyr)

#Connect to database
db.location <- "/Users/Marty/Documents/BMKT670/Wedge/SQLOutput/"
db.name <- "Wedge.db"
con <- dbConnect(RSQLite::SQLite(),
                 dbname = paste0(db.location,db.name))

#create table from connection
cardno_db <- tbl(con, "sales_cardno_department")

library(reshape2)
#collect the data into a dataframe
d <- cardno_db %>% collect() 

#For this we want data from January, since 2017 only features January data
pottestd <- d[d$year == 2017,]
pottraind <- d[d$month == "01" & d$year != 2017,]

#Next we need to summarize the data
pottraind1 <- pottraind %>% 
  #group by card number and department
  group_by(cardno, department_name) %>% 
  #create columns for spend and number of years
  #We need these to create average spend from 2010 - 2016
  summarize(spend = sum(spend), years.count=n()) %>% 
  #Add avg. spend column
  mutate(spent = spend/years.count) %>% 
  #select card number, department, and avg spent
  select(cardno, department_name, spent)

pottestd1 <- pottestd %>% 
  #Only need one column because we'll add to the previous df
  group_by(cardno, department_name) %>% 
  summarize(spent = sum(spend))

#Change cardno to char to avoid problems with ints
pottestd1$cardno <- as.character(pottestd1$cardno)
pottraind1$cardno <- as.character(pottraind1$cardno)

#Left join on training, as we need tests that exist in training set
test1 <- left_join(pottraind1, pottestd1, by = c("cardno", "department_name"))

#first set training
train1 <- test1 %>% 
  select(cardno, department_name, spent.x)

test1 <- test1 %>% 
  select(cardno, department_name, spent.y)

#spread the data out so we have department spend per column per cardno
train_wide <- train1 %>% 
  gather(variable, value, -(cardno:department_name)) %>%
  unite(temp, department_name, variable) %>%
  spread(temp, value)

#repeat for test
test_wide <- test1 %>% 
  gather(variable, value, -(cardno:department_name)) %>%
  unite(temp, department_name, variable) %>%
  spread(temp, value)

#Need to set NA values to 0
test_wide[is.na(test_wide)] <- 0
train_wide[is.na(train_wide)] <- 0

#Rejoin wide train and test sets
final_file <- full_join(test_wide, train_wide, by = "cardno")

#randomly shuffle data to avoid cardno patterns
final_file <- final_file[sample(nrow(final_file)),]

#Create sums column for outputs
final_file$outputs <- rowSums(final_file[,2:19])


#delete cardno -- unecessary in NN
final_file <- final_file[, -(1:19)]

#Finally split the data into the four dataframes we need to make the correct files
#First split into train and test
test_final <- final_file[1:1250, ]
train_final <- final_file[1251:19645, ]

#Then into Xs and Ys
test_output <- test_final[, 19]
test_input <- test_final[, 1:18]

train_output <- train_final[, 19]
train_input <- train_final[, 1:18]

#Write the file to disk
write.csv(test_input, file = "test_inputs.csv")
write.csv(test_output, file = "test_outputs.csv")
write.csv(train_input, file = "train_inputs.csv")
write.csv(train_output, file = "train_outputs.csv")
