###use to split a field in a data frame into two
#test data file location:     Z:\Shared\R code\Real Time



#load packages
library(tidyr)
library(dplyr)

#load data using import dataset button (see data location above)
head(test_env_file)
test<- (test_env_file)

#separate "Tag" column into two columns one with us and one with the ID
test2<-test%>%
  separate(Tag, c("us", "TagID"), sep="-")
head(test2)

#examine warnings
test2[6800, ] #it appears that the warnings appear in rows where the TagID="error" should drop these lines from the dataset

#remove all records with "error" in the tag ID field
test2<- filter(test2, !grepl("error", TagID))
test3<- filter(test2, grepl("error", Tag)) #this will create a data frame with the data that was filtered out in the previous line of code

#drop unnecessary columns at the end of othe data frame
test2<- test2[ , 1:3]

#rename columns using Myfanwy's suggestion, and rename function from the dplyr package
test2<- rename(test2, datetime=Timestamp..GMT.8.)
head(test2)
  