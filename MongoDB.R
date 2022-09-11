setwd("C:\\Users\\ninas\\OneDrive\\Desktop\\MSc Business Analytics\\2nd Quarter\\Big Data Systems & Architectures\\Assignment 1\\BIKES")

.libPaths('C:\\Users\\ninas\\R\\RPackages')

#install.packages('stringr')
library('jsonlite')
library("mongolite")
library("stringr")
library(dplyr)

#establish a connection with the Mongo database
mongo_Conn <- mongo(collection = "BikesColl",  db = "BikesDB", url = "mongodb://localhost")
#delete all data in the BikesColl collection in the BikesDB Database 
mongo_Conn$remove('{}')

test <- fromJSON("C:\\Users\\ninas\\OneDrive\\Desktop\\MSc Business Analytics\\2nd Quarter\\Big Data Systems & Architectures\\Assignment 1\\BIKES\\10\\00\\06\\10000682.json")

#read all json files from file list
file_list <- read.table("files_list2.txt",sep="\n", stringsAsFactors = FALSE)
file_list[1:10,]

#file_list2 <- read.table("files_list2.txt",sep="\n", stringsAsFactors = FALSE)
#function that trims blanks at the end and the start of a string
trim <- function (x) gsub("^\\s+|\\s+$", "", x)
veh_title <- c()
for (i in 1:nrow(file_list)) {
  x <- fromJSON(readLines(file_list[i,], warn=FALSE, encoding="UTF-8"))
  x <- x$title
  veh_title <- rbind(veh_title,x)
}
#no NAs for vehicle names, though there are some random variables such as "????????   '16" , they might be replaced
sum(is.na(veh_title))
sum(is.null(veh_title))
veh_title <- trim(veh_title)
veh_title <- sort(veh_title)
unique(veh_title)[1:100]


Price <- c()
for (i in 1:nrow(file_list)) {
  x <- fromJSON(readLines(file_list[i,], warn=FALSE, encoding="UTF-8"))
  x <- x$ad_data$Price
  Price <- rbind(Price,x)
}
#all prices start with ??? symbol. It is removed. Also, some prices have a value 'Askforprice', which is wrong
#so it is updated to NA. Finally it is read as character type, it is updated to numeric
unique(sort(Price, decreasing = TRUE))
Price <- gsub('???','',Price)
Price <- gsub('\\.','',Price)
Price[which(Price=='Askforprice')] <- NA
Price <- as.numeric(Price)
#prices that are lower than 250 dollars are also set as NA, because they are not reasonable and will distort calculations
Price[which(Price < 250 | Price > 70000)] <- NA
unique(sort(Price, decreasing = TRUE))



Category <- c()
for (i in 1:nrow(file_list)) {
  x <- fromJSON(readLines(file_list[i,], warn=FALSE, encoding="UTF-8"))
  x <- x$ad_data$Category
  Category <- rbind(Category,x)
}
#no NAs in the Category variable
sum(is.na(Category) == TRUE)
sum(is.null(Category) == TRUE)
#all categories seem to be ok
unique(sort(Category, decreasing = TRUE))


Registration <- c()
for (i in 1:nrow(file_list)) {
  x <- fromJSON(readLines(file_list[i,], warn=FALSE, encoding="UTF-8"))
  x <- x$ad_data$Registration
  Registration <- rbind(Registration,x)
}
#no NAs in the Category variable
sum(is.na(Registration) == TRUE)
#all categories seem to be ok
unique(sort(Registration, decreasing = TRUE))

#create a function to keep n characters from the right end of a string
substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}
#keep only the year of registration
Registration <- substrRight(Registration, 4)
unique(sort(Registration, decreasing = TRUE))
#it is noticed that there is at least one ad with years prior to 1930, it is set to NA
Registration[which(Registration < '1930')] <- NA 
unique(sort(Registration, decreasing = TRUE))


Mileage <- c()
for (i in 1:nrow(file_list)) {
  x <- fromJSON(readLines(file_list[i,], warn=FALSE, encoding="UTF-8"))
  x <- x$ad_data$Mileage
  Mileage <- rbind(Mileage,x)
}
#no NAs in the Mileage variable
sum(is.null(Mileage) == TRUE)
#all Mileage value end with 'km', so it must be removed, also the ',' is replaced with blank, and the mileage is
#updated to numeric from a character variable
unique(sort(Mileage))
Mileage <- gsub(' km','',Mileage)
Mileage <- gsub(',','',Mileage)
Mileage <- as.numeric(Mileage)
#ads for vehicles that have travelled more than 1mil km are set to NA, they seem unreasonable
Mileage[which(Mileage >=999999)] <- NA
unique(sort(Mileage, decreasing = TRUE))


Color <- c()
for (i in 1:nrow(file_list)) {
  x <- fromJSON(readLines(file_list[i,], warn=FALSE, encoding="UTF-8"))
  x <- x$ad_data$Color
  Color <- rbind(Color,x)
}
#no NAs in the color variable
sum(is.na(Color) == TRUE)
#it is noticed that half of the recorded colors have a '(Metallic)' description, it is removed
unique(sort(Color))
Color <- gsub('\\(Metallic)','', Color)
Color <- trim(Color)
unique(sort(Color))



typeof <- c()
for (i in 1:nrow(file_list)) {
  x <- fromJSON(readLines(file_list[i,], warn=FALSE, encoding="UTF-8"))
  x <- x$metadata$type
  typeof <- c(typeof,x)
}
#no NAs in the type variable
sum(is.na(typeof) == TRUE)
#all observations have 'Bikes' value in type, no cleaning is needed
unique(sort(typeof))


brand <- c()
for (i in 1:nrow(file_list)) {
  x <- fromJSON(readLines(file_list[i,], warn=FALSE, encoding="UTF-8"))
  x <- x$metadata$brand
  brand <- c(brand,x)
}
#no NAs in the brand variable
sum(is.na(brand) == TRUE)
#all brands look fine, no cleaning is needed
unique(sort(brand))


veh_model <- c()
for (i in 1:nrow(file_list)) {
  x <- fromJSON(readLines(file_list[i,], warn=FALSE, encoding="UTF-8"))
  x <- x$metadata$model
  veh_model <- c(veh_model,x)
}
#no NAs in the model variable
sum(is.na(veh_model) == TRUE)
unique(sort(veh_model))
#1348 ads have 'Negotiable', 114 have 'Ask for price' in their model description description
length(veh_model[str_detect(veh_model, regex("Negotiable", ignore_case = TRUE))])
length(veh_model[str_detect(veh_model, regex("ask for", ignore_case = TRUE))])


prev_own <- c()
for (i in 1:nrow(file_list)) {
  x <- fromJSON(readLines(file_list[i,], warn=FALSE, encoding="UTF-8"))
  x <- x$ad_data$`Previous owners`
  prev_own <- c(prev_own,x)
}
#unique values of number of previous owners seems fine, but they are in string form, they must be updated to numeric
unique(prev_own)



Cleaningfunc <- function(x) {
#Price cleaning
#if price is 'Askforprice' update it to NA
if (x$ad_data$Price == 'Askforprice'){
  x$ad_data$Price <- NULL
} else {
#remove symbols and update price to numeric
  x$ad_data$Price <- gsub('???','',x$ad_data$Price)
  x$ad_data$Price <- gsub('\\.','',x$ad_data$Price)
  x$ad_data$Price <- as.numeric(x$ad_data$Price)
#if price is <250 or > 70000 update to null
  if (x$ad_data$Price < 250 | x$ad_data$Price > 70000){
    x$ad_data$Price <- NULL
  }
}
    
# Find if a price is negotiable
if (str_detect(x$metadata$model, regex("Negotiable", ignore_case = TRUE))) {
  x$metadata$Negotiable = 1
}
else {
  x$metadata$Negotiable = 0
}
x$metadata$Negotiable <- as.numeric(x$metadata$Negotiable)
#Cleaning process of registration date
#create a function to keep n characters from the right end of a string
substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}

#keep only the year of registration
x$ad_data$Registration <- substrRight(x$ad_data$Registration, 4)
x$ad_data$Registration <- as.numeric(x$ad_data$Registration)
#set the observations with registration year prior to 1930 to NA
if (x$ad_data$Registration < '1930'){
  x$ad_data$Registration <- NULL
}

#cleaning of mileage
#remove redundant symbols and characters from mileage, and update it to numeric
if (is.null(x$ad_data$Mileage) == FALSE){
  x$ad_data$Mileage <- gsub(' km', '', x$ad_data$Mileage)
  x$ad_data$Mileage <- gsub(',', '', x$ad_data$Mileage)
  x$ad_data$Mileage <- as.numeric(x$ad_data$Mileage)
  #ads for vehicles that have travelled more than 1mil km are set to NA
  if (x$ad_data$Mileage  >= 999999){
    x$ad_data$Mileage <- NULL
  }
}

#Cleaning of colors - removal of (Metallic) at the end of the string
x$ad_data$Color <- gsub('\\(Metallic)','', x$ad_data$Color)
x$ad_data$Color <- trim(x$ad_data$Color)

#updating number of previous owners to numeric
if (is.null(x$ad_data$`Previous owners`) == FALSE){
  x$ad_data$`Previous owners` <- as.numeric(x$ad_data$`Previous owners`)
}

#return clean document
return(x)
}


#create a 'for' iteration that goes through all the json files one by one, cleans them through the 'Cleaningfunc' function
#and finally transforms them back to json format in order to insert them in the Mongo Database
data <- c()
for (i in 1:nrow(file_list)) {
  x <- fromJSON(readLines(file_list[i,], warn=FALSE, encoding="UTF-8"))
  x <- Cleaningfunc(x)
  j <- toJSON(x, auto_unbox = TRUE)
  data <- c(data, j)
}

#insert data to mongo
mongo_Conn$insert(data)
mongo_Conn$count()
