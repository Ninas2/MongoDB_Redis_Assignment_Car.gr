setwd("C:\\Users\\ninas\\R\\RPackages")

.libPaths('C:\\Users\\ninas\\R\\RPackages')

library("redux")
#read the 'emails_sent' file
emails_sent <- read.csv("C:/Users/ninas/OneDrive/Desktop/MSc Business Analytics/2nd Quarter/Big Data Systems & Architectures/Assignment 1/RECORDED_ACTIONS/emails_sent.csv")
emails_sent

#read the 'modified_listing' file
modified_listings <- read.csv("C:/Users/ninas/OneDrive/Desktop/MSc Business Analytics/2nd Quarter/Big Data Systems & Architectures/Assignment 1/RECORDED_ACTIONS/modified_listings.csv")
modified_listings
length(unique(modified_listings$UserID))
#create a connection to the Redis server
redis <- redux::hiredis(
  redux::redis_config(
    host = "127.0.0.1", 
    port = "6379"))

#clear all contents in Redis server
redis$FLUSHALL()
#Question 1.1 - How many salesmen did modify their listing in January
#keep only the observations that refer to January from the modified listings variable
modified_listings_JAN <- modified_listings[which(modified_listings$MonthID == 1),]
#extract the unique IDs of the salesmen that modified their list in January
Jan_positions <- which(modified_listings_JAN$ModifiedListing==1)
Jan_positions <- modified_listings_JAN[Jan_positions,]
Jan_positions <- unique(Jan_positions[,1])

#set the value for the salesmen that update their list in January to 1 using a Redis bitmap
for (i in (Jan_positions)){
  redis$SETBIT('ModificationsJanuary',i, 1)
}
#count of salesmen that modified their list in January - 9969
redis$BITCOUNT('ModificationsJanuary')


#Question 1.2 - How many salesmen did not modify their listing in January
#use bitop-not to locate the salesmen that did not modify their list in January
redis$BITOP('NOT','NoModificationsJanuary','ModificationsJanuary')
#Numbers of modified and non-modified don't add up to the total amount of salesmen - 9969 + 10031 = 20000
redis$BITCOUNT('NoModificationsJanuary')



#Question 1.3 - How many salesmen received at least one mail in each month
#create a new dataframe that is similar to the emails_sent df, but without the email ID
emails_sent_new <- emails_sent[,-1]
#extract the unique IDs of the salesmen that received at least one mail in January
emails_sent_Jan <- unique(emails_sent_new[which(emails_sent_new$MonthID==1),])
Jan_index <- unique(emails_sent_Jan$UserID)

#extract the unique IDs of the salesmen that received at least one mail in February
emails_sent_Feb <- unique(emails_sent_new[which(emails_sent_new$MonthID==2),])
Feb_index <- unique(emails_sent_Feb$UserID)

#extract the unique IDs of the salesmen that received at least one mail in March
emails_sent_Mar <- unique(emails_sent_new[which(emails_sent_new$MonthID==3),])
Mar_index <- unique(emails_sent_Mar$UserID)

#create 3 bitmaps (one for each month) and assign the value 1 to salesmen that 
#received at least one email in each month and count how many they are for each month
for (j in Jan_index){
  redis$SETBIT('EmailsJanuary', j, 1)
}
redis$BITCOUNT('EmailsJanuary') # 9617 salesmen received at least one mail in January

for (f in Feb_index){
  redis$SETBIT('EmailsFebruary', f, 1)
}
redis$BITCOUNT('EmailsFebruary') # 9666 salesmen received at least one mail in February

for (m in Mar_index){
  redis$SETBIT('EmailsMarch', m, 1) 
}
redis$BITCOUNT('EmailsMarch') # 9520 salesmen received at least one mail in March

#use bitop-and to locate the salesmen that received at least one mail in all month examined 
redis$BITOP('AND', 'EmailsAllMonths',c('EmailsJanuary', 'EmailsFebruary', 'EmailsMarch'))

#2668 salesmen received at least one mail in all 3 months
redis$BITCOUNT('EmailsAllMonths')

 
#Question 1.4 - How many salesmen received a mail in January and March but not in February
#use bitop-not to locate the salesmen that did not receive any mail in February
redis$BITOP('NOT', 'NoEmailsFebruary', 'EmailsFebruary')
#use bitop-and to locate the salesmen that received at least one mail in January and March, but not in February
redis$BITOP('AND', 'EmailsJanMarNoFeb', c('EmailsJanuary', 'NoEmailsFebruary', 'EmailsMarch'))

#2417 salesmen received at least one mail in January and March but not in February
redis$BITCOUNT('EmailsJanMarNoFeb')

#Question 1.5 - How many received an email in January, did not open it and updated their listing
#find the salesmen that received and opened at least one mail in January
opened_Jan <- which(emails_sent_Jan$EmailOpened==1)
opened_Jan <- emails_sent_Jan[opened_Jan,]
opened_Jan <- unique(opened_Jan$UserID)

#create a bitmap for the salesmen that opened at least one mail in January
for (j in opened_Jan){
  redis$SETBIT('OpenedJan', j, 1)
}
redis$BITCOUNT('OpenedJan') #5645 salesmen received and opened at least one mail in January
#use bitop-not to locate all the salesmen that did not open any mail in January 
redis$BITOP('NOT', 'AllNotOpenedJanuary', 'OpenedJan')
redis$BITCOUNT('AllNotOpenedJanuary') 
#use bitop-and to find all users that did not open any mail and received at least one in January
redis$BITOP('AND','JanNotOpened',c('AllNotOpenedJanuary', 'EmailsJanuary'))
redis$BITCOUNT('JanNotOpened') #3972 salesmen received at least one mail, and did not open it in January

#use bitop-and to locate the salesmen that received at least one mail in January, did not open any of them 
#but updated their list anyway
redis$BITOP('AND', 'UpdateAndNotOpenedJAN', c('JanNotOpened', 'ModificationsJanuary'))
redis$BITCOUNT('UpdateAndNotOpenedJAN') # 1961 salesmen


#Question 1.6 - How many received an email, did not open it and updated their listing in January, February or March
#keep only the observations that refer to February from the modified listings variable
modified_listings_FEB <- modified_listings[which(modified_listings$MonthID == 2),]
#extract the IDs of the salesmen that modified their list in February
Feb_positions <- which(modified_listings_FEB$ModifiedListing==1)
Feb_positions <- modified_listings_FEB[Feb_positions,]
Feb_positions <- unique(Feb_positions[,1])

#set the value for the salesmen that update their list in February to 1 using a Redis bitmap
for (i in (Feb_positions)){
  redis$SETBIT('ModificationsFebruary',i, 1)
}
#count of salesmen that modified their list in February - 10007
redis$BITCOUNT('ModificationsFebruary')

#keep only the observations that refer to March from the modified listings variable
modified_listings_MAR <- modified_listings[which(modified_listings$MonthID == 3),]
#extract the IDs of the salesmen that modified their list in March
Mar_positions <- which(modified_listings_MAR$ModifiedListing==1)
Mar_positions <- modified_listings_MAR[Mar_positions,]
Mar_positions <- unique(Mar_positions[,1])

#set the value for the salesmen that update their list in March to 1 using a bitmap
for (i in (Mar_positions)){
  redis$SETBIT('ModificationsMarch',i, 1)
}
#count of salesmen that modified their list in March - 9991
redis$BITCOUNT('ModificationsMarch')


#find the salesmen that received and opened at least one mail in February
opened_Feb <- which(emails_sent_Feb$EmailOpened==1)
opened_Feb <- emails_sent_Feb[opened_Feb,]
opened_Feb <- unique(opened_Feb$UserID)

#create a bitmap for the salesmen that opened at least one mail in February
for (f in opened_Feb){
  redis$SETBIT('OpenedFebruary', f, 1)
}
redis$BITCOUNT('OpenedFebruary') #5721 salesmen
#use bitop-not to locate all the salesmen that did not open any mail in February
redis$BITOP('NOT','AllNotOpenedFebruary', 'OpenedFebruary')
redis$BITCOUNT('AllNotOpenedFebruary')
#use bitop-and to locate all the salesmen that received at least one mail in February, but did not open any of them
redis$BITOP('AND','FebNotOpened',c('AllNotOpenedFebruary', 'EmailsFebruary'))
redis$BITCOUNT('FebNotOpened') #3945 salesmen

#use bitop-and to locate the salesmen that received at least one mail in February, did not open any of them
#but updated their list anyway
redis$BITOP('AND', 'UpdateAndNotOpenedFEB', c('FebNotOpened', 'ModificationsFebruary'))
redis$BITCOUNT('UpdateAndNotOpenedFEB') #1971 total salesmen


#find the salesmen that received and opened at least one mail in March
opened_Mar <- which(emails_sent_Mar$EmailOpened==1)
opened_Mar <- emails_sent_Mar[opened_Mar,]
opened_Mar <- unique(opened_Mar$UserID)

#create a bitmap for the salesmen that opened at least one mail in March
for (m in opened_Mar){
  redis$SETBIT('OpenedMarch', m, 1)
}
redis$BITCOUNT('OpenedMarch') #5572 salesmen
#use bitop-not to locate all the salesmen that did not open any mail in March
redis$BITOP('NOT','AllNotOpenedMarch', 'OpenedMarch')
redis$BITCOUNT('AllNotOpenedMarch')
#use bitop-and to locate all the salesmen that received at least one mail in March, but did not open any of them
redis$BITOP('AND','MarNotOpened',c('AllNotOpenedMarch', 'EmailsMarch'))
redis$BITCOUNT('MarNotOpened') #3948 salesmen

#use bitop-and to locate the salesmen that received at least one mail in March, did not open any of them
#but updated their list anyway
redis$BITOP('AND', 'UpdateAndNotOpenedMAR', c('MarNotOpened', 'ModificationsMarch'))
redis$BITCOUNT('UpdateAndNotOpenedMAR') #1966 salesmen

#use bitop-or to locate the salesmen that received at least one mail, did not open any of them
#but updated their list anyway in January, February or March 
redis$BITOP('OR', 'UpdateAndNotOpenAllMonths',c('UpdateAndNotOpenedJAN', 'UpdateAndNotOpenedFEB', 'UpdateAndNotOpenedMAR'))
redis$BITCOUNT('UpdateAndNotOpenAllMonths') #5249 salesmen
 

#Question 1.7 - Evaluate the firm's strategy
#for each month, three different percentiles are calculated using Redis bitmaps and divisions through R
#,and they are inputted in a print function
#1)the opened mail per sent mail for the salesmen
#2)the ratio of salesmen that modified their listing that received a mail without opening it to the total mails sent
#3)the ratio of salesmen that modified their listing that received and opened at least one mail to the total mails sent
redis$BITOP('AND','JanModandRead', c('OpenedJan','ModificationsJanuary'))
redis$BITCOUNT('JanModandRead')
redis$BITOP('AND','JanModandSent', c('EmailsJanuary','ModificationsJanuary'))
redis$BITCOUNT('JanModandSent')
January_open_pc <- redis$BITCOUNT('OpenedJan')/redis$BITCOUNT('EmailsJanuary') 
January_sentMod_pc <- redis$BITCOUNT('JanModandSent')/redis$BITCOUNT('EmailsJanuary')
January_update_pc <- redis$BITCOUNT('JanModandRead')/redis$BITCOUNT('EmailsJanuary')


print(cat(paste('January: Mail opening percent -',round(January_open_pc,2), '%'
            , paste('(',redis$BITCOUNT('OpenedJan'),'/',redis$BITCOUNT('EmailsJanuary') ,')',sep='')
            , ', List update-per-mail-sent -',round(January_sentMod_pc,2), '%'
            ,paste('(',redis$BITCOUNT('JanModandSent'),'/',redis$BITCOUNT('EmailsJanuary') ,')',sep=''))
            
      ,paste(', List update-per-mail-opened -',round(January_update_pc,2), '%'
            ,paste('(',redis$BITCOUNT('JanModandRead'),'/',redis$BITCOUNT('EmailsJanuary') ,')',sep='')),sep="\n"))


redis$BITOP('AND','FebModandRead', c('OpenedFebruary','ModificationsFebruary'))
redis$BITCOUNT('FebModandRead')
redis$BITOP('AND','FebModandSent', c('EmailsFebruary','ModificationsFebruary'))
redis$BITCOUNT('FebModandSent')
February_open_pc <-  redis$BITCOUNT('OpenedFebruary')/redis$BITCOUNT('EmailsFebruary') 
February_sentMod_pc <- redis$BITCOUNT('FebModandSent')/redis$BITCOUNT('EmailsFebruary')
February_update_pc <- redis$BITCOUNT('FebModandRead')/redis$BITCOUNT('EmailsFebruary')

print(cat(paste('February: Mail opening percent -',round(February_open_pc,2), '%'
                , paste('(',redis$BITCOUNT('OpenedFebruary'),'/',redis$BITCOUNT('EmailsFebruary') ,')',sep='')
                , ', List update-per-mail-sent -',round(February_sentMod_pc,2), '%'
                ,paste('(',redis$BITCOUNT('FebModandSent'),'/',redis$BITCOUNT('EmailsFebruary') ,')',sep=''))
          
          ,paste(', List update-per-mail-opened -',round(February_update_pc,2), '%'
                 ,paste('(',redis$BITCOUNT('FebModandRead'),'/',redis$BITCOUNT('EmailsFebruary') ,')',sep='')),sep="\n")) 


redis$BITOP('AND','MarModandRead', c('OpenedMarch','ModificationsMarch'))
redis$BITCOUNT('MarModandRead')
redis$BITOP('AND','MarModandSent', c('EmailsMarch','ModificationsMarch'))
redis$BITCOUNT('MarModandSent')
March_open_pc <- redis$BITCOUNT('OpenedMarch')/redis$BITCOUNT('EmailsMarch') 
March_sentMod_pc <- redis$BITCOUNT('MarModandSent')/redis$BITCOUNT('EmailsMarch')
March_update_pc <- redis$BITCOUNT('MarModandRead')/redis$BITCOUNT('EmailsMarch')


print(cat(paste('March: Mail opening percent -',round(March_open_pc,2), '%'
                , paste('(',redis$BITCOUNT('OpenedMarch'),'/',redis$BITCOUNT('EmailsMarch') ,')',sep='')
                , ', List update-per-mail-sent -',round(March_sentMod_pc,2), '%'
                ,paste('(',redis$BITCOUNT('MarModandSent'),'/',redis$BITCOUNT('EmailsMarch') ,')',sep=''))
          
          ,paste(', List update-per-mail-opened -',round(March_update_pc,2), '%'
                 ,paste('(',redis$BITCOUNT('MarModandRead'),'/',redis$BITCOUNT('EmailsMarch') ,')',sep='')),sep="\n"))  
