library(mongolite)
library(lubridate)
#Connecting to transaction created database on CA
deals_con <- mongo(collection = "loan_transactions",db = "videx-production",url = "mongodb://prod-read:X9RPfWVhBk3ihl4d@prd-credavenue-mongo-shard-00-01.a8fyh.mongodb.net:27017/admin?authSource=admin&replicaSet=atlas-10wzyk-shard-0&readPreference=primary&ssl=true")
#choosing only those deals where specific columns are filled
deals<- cbind(deals_con$find(fields = '{"_id":true}'),deals_con$find('{}'))
#lot of columns are nested so massaging them to clean the data frame
#cleaning the interest rate details on table first
investor_names_clean<-read.csv("C:/Users/robin.tyagi_credaven/Desktop/Client Tier Classification/Code Mappers/Investor.Mapping.csv")
transaction_names_clean<-read.csv("C:/Users/robin.tyagi_credaven/Desktop/Client Tier Classification/Code Mappers/Transaction Mapper.csv")
rating_pricing_matrix<-read.csv("C:/Users/robin.tyagi_credaven/Desktop/Client Tier Classification/Code Mappers/Rating.Pricing.Matrix.csv")
print("####################")
print(paste("Cleaning deal tenor nested table now"))
deals<-deals[,1:100]
deals_interest<-deals[,3]
deals_rating<-as.data.frame(deals[,24])
colnames(deals_rating)<-"Rating"
deals<-cbind(deals,deals_interest$value,deals_interest$type,deals_rating)
deals<-deals[,-3]
rm(deals_interest)
rm(deals_rating)
print("Cleaning of deal tenor  and rating field is done and relevant values are extracted")
print("####################")
#removing dms_enabled column from the dataframe
deals<-deals[,-6]

##############################################
print("DMS filed which was not required for analysis is now dropt")
print("Dropping nested client preferences column now")
deals<-deals[,-16]
deals<-deals[,-c(16:21)]
deals<-deals[,-c(19:24)]
deals<-deals[,-c(19:85)]
print("All unnecessary variables dropt from dataframe")
print("####################")
##############################################

###### Cleaning final data frame######
colnames(deals)[1]<-"ID"
deals$Deal.Amount.In.Crores<-deals$facility_limit/10^7
deals<-deals[,-c(2,4)]
colnames(deals)[4]<-"Transaction.Type"
deals<-deals[,-6]
deals<-deals[,-9]
deals<-deals[,-10]
colnames(deals)[12]<-"Deal.Tenor.In.Months"
colnames(deals)[c(14,15)]<-c("Interest.Rate","Interest.Type")
##Connecting to entities database to extract CINs from customer ID
entities_transaction_con <- mongo(collection = "entities",db = "videx-production",url = "mongodb://prod-read:X9RPfWVhBk3ihl4d@prd-credavenue-mongo-shard-00-01.a8fyh.mongodb.net:27017/admin?authSource=admin&replicaSet=atlas-10wzyk-shard-0&readPreference=primary&ssl=true")
entities    <- cbind(entities_transaction_con$find(fields = '{"_id":true}'),entities_transaction_con$find('{}'))
entity_list<-entities[,c(1:4,10,11,14,16,17,18,20)]
colnames(entity_list)[1]<-"ID"
#entity_list<-entity_list[-which(entity_list$customer==FALSE),]
#entity_list<-entity_list[-which(is.na(entity_list$customer)),]
deals$CIN<-entity_list$CIN[match(deals$customer_id,entity_list$ID)]
#connection to recommendation model outputs- will use as backup to check zero recp accurac
recommendations_con<-mongo(collection = "investor_recommendation",db = "reports",url = "mongodb://prod-read:X9RPfWVhBk3ihl4d@prd-credavenue-mongo-shard-00-01.a8fyh.mongodb.net:27017/admin?authSource=admin&replicaSet=atlas-10wzyk-shard-0&readPreference=primary&ssl=true")
recommendations<-cbind(recommendations_con$find(fields='{"_id":true}'),recommendations_con$find('{}'))
#Pulling CIN from entity DB from entity_id
# CIN will be useful to link to financial ratio and MCA
deals$CIN<-entity_list$CIN[match(deals$customer_id,entity_list$ID)]
#Creating a new dataframe for all entities which have created transactions
#this data frame will eventually have all scores and compare against list of clients
deals_details<-data.frame(matrix("",nrow=length(unique(deals$customer_id)),ncol=5))
#naming columns for the data frame as per our requirement
colnames(deals_details)<-c("Entity_ID","CIN","Entity_Name","Rating","Rating.Agency")
unique_customers<-data.frame(unique(deals$customer_id))
unique_customers<-na.omit(unique_customers)
#making empty data frames to store results
reco_information<-data.frame()
reco_investor<-data.frame()
reco_info_final<-data.frame()
#fetching values for the deal details dataframe
#getting the list of recommendations from DS model to check coverage
#scoring coverage by each investor category using HHI formula
for (i in 1:nrow(unique_customers)){
  deals_details[i,1]<- unique_customers[i,1]
  deals_details[i,2]<-deals$CIN[match(unique_customers[i,1],deals$customer_id)]
  deals_details[i,3]<-deals$client_name[match(unique_customers[i,1],deals$customer_id)]
  deals_details[i,4]<-deals$Rating[match(unique_customers[i,1],deals$customer_id)]
  deals_details[i,5]<-deals$rating_agency[match(unique_customers[i,1],deals$customer_id)]
  reco_information<-deals[deals$customer_id==unique_customers[i,1],]
  reco_information<-reco_information[-which(is.na(reco_information$ID)),]
  reco_information$updated_at<-as.Date(reco_information$updated_at)
  reco_information$released_on<-as.Date(reco_information$released_on)
  for (j in 1:nrow(reco_information)){
    reco_investor<-recommendations[recommendations$deal_id==reco_information$ID[j],]
    reco_investor<-reco_investor[reco_investor$created_at==max(reco_investor$created_at),]
    reco_results<-as.data.frame(reco_investor[1,6])
    reco_results$investor_names<-entity_list$company_name[match(reco_results$investor_id,entity_list$ID)]
    reco_results$investor_names_mapped<-investor_names_clean$Mapped.Name[match(reco_results$investor_names,investor_names_clean$investor_names)]
    reco_results$institution_type<-investor_names_clean$Institution.Type[match(reco_results$investor_names,investor_names_clean$investor_names)]
    reco_information$no.of.recommended.investors[j]<-length(which(reco_results$total_score>0))
    reco_information$high.probability.investors[j]<-length(which(reco_results$total_score>80))
    reco_information$private.bank.coverage[j]<-length(which(reco_results$institution_type =="Private Bank" & reco_results$total_score>0))/length(which(reco_results$institution_type =="Private Bank"))
    reco_information$PSU.bank.coverage[j]<-length(which(reco_results$institution_type =="Public Sector Bank" & reco_results$total_score>0))/length(which(reco_results$institution_type =="Public Sector Bank"))
    reco_information$foreign.bank.coverage[j]<-length(which(reco_results$institution_type =="Foreign Bank" & reco_results$total_score>0))/length(which(reco_results$institution_type =="Foreign Bank"))
    reco_information$NBFC.coverage[j]<-length(which(reco_results$institution_type =="NBFC" & reco_results$total_score>0))/length(which(reco_results$institution_type =="NBFC"))
  }
  reco_info_final<-rbind(reco_info_final,reco_information)
  print (paste(i,"number of companies out of ",nrow(unique_customers),"companies completed",sep=" "))
}
#removing matured & finalized transactions since they aren't of much interest
reco_info_final<-reco_info_final[-which(reco_info_final$transaction_state=="matured"),]
reco_info_final<-reco_info_final[-which(reco_info_final$transaction_state=="finalized"),]
reco_info_final$transaction_clean<-transaction_names_clean$Mapped.Value[match(reco_info_final$Transaction.Type,transaction_names_clean$Value)]
reco_info_final$transaction_class<-transaction_names_clean$Type[match(reco_info_final$Transaction.Type,transaction_names_clean$Value)]
reco_info_final<-reco_info_final[,-c(4,6,8)]
#making blank ratings as unrated
reco_info_final$Rating[which(is.na(reco_info_final$Rating))]<-"Unrated"
#calculating investor coverage score by using HHI formula
reco_info_final$coverage.score<-(reco_info_final$private.bank.coverage^2+reco_info_final$PSU.bank.coverage^2+reco_info_final$foreign.bank.coverage^2+reco_info_final$NBFC.coverage^2)^1/2
#making all blank security field as unsecured- this is dangerous but quality of deal is poor
reco_info_final$security_type[which(is.na(reco_info_final$security_type))]<-"Unsecured"
#scoring 0.8 for secured deals and 0.2 for unsecured deals
for (k in 1:nrow(reco_info_final)){
  if (reco_info_final$security_type[k]=="secured"){
    reco_info_final$security.score[k]<-0.8
  }else{
    reco_info_final$security.score[k]<-0.2
  }
}
reco_info_final$coverage.score[is.nan(reco_info_final$coverage.score)]<-0
#normalizing investor coverage score between 0 and 1
for (l in 1:nrow(reco_info_final)){
  reco_info_final$normalized.coverage.score[l]<-pnorm(reco_info_final$coverage.score[l]*2)
}
#adjustment to make deals with 0 matches as 0 coverage score
reco_info_final$normalized.coverage.score[which(reco_info_final$normalized.coverage.score==0.5)]<-0
#reading corporate trade data from FIMMDA website and renaming tenor column to months
colnames(rating_pricing_matrix)<- c("Rating",6,12,24,36,48,60,72,84,96,108,120,180)
#score adjustment to make coverage score close to 1
reco_info_final$normalized.coverage.score<- reco_info_final$normalized.coverage.score*1.19
reco_info_final$Interest.Rate[which(is.na(reco_info_final$Interest.Rate))]<-""
#getting relevant price from FIMMDA spreads
#comparing with price ask and scoring
#unsecured pricing is assumed as 1% higher till A category and 2% thereon
#it is assumed that we will not be place deals in BB category in terms of pricing
browser()
for (m in 1:nrow(reco_info_final)){
  a<-which.min(abs(reco_info_final$Deal.Tenor.In.Months[m]-as.numeric(colnames(rating_pricing_matrix)[2:13])))+1
  if (reco_info_final$Rating[m]=="Unrated"){
    b<- match("BB",rating_pricing_matrix$Rating)
  } else {
    b<- match(reco_info_final$Rating[m],rating_pricing_matrix$Rating)
  }
  
  if(rating_pricing_matrix[b,a]=="Not.Possible"){
    comparable_price<- rating_pricing_matrix[b,a]
  } else if (reco_info_final$security_type=="unsecured" & b<=7){
    comparable_price<-as.numeric(rating_pricing_matrix[b,a])+1
  } else if (reco_info_final$security_type=="unsecured"){
    comparable_price<-as.numeric(rating_pricing_matrix[b,a])+2
  } else {
    comparable_price<-as.numeric(rating_pricing_matrix[b,a])
  }
  
  if (reco_info_final$transaction_class[m]=="Non.Fund.Based" & b<=4){
    comparable_price<-0.5
  } else if (reco_info_final$transaction_class[m]=="Non.Fund.Based" & b>4 & b<=7){
    comparable_price<-1
  } else if (reco_info_final$transaction_class[m]=="Non.Fund.Based" & b>7){
    comparable_price<-"Not.Possible"
  }
  price_ask<-as.numeric((reco_info_final$Interest.Rate[m]))
  if (comparable_price=="Not.Possible"){
    comparable_price=comparable_price
  } else {
    comparable_price<-as.numeric(comparable_price)
    pricingdiff<-min(2/exp((comparable_price-price_ask)),1)
  }
  
  if (reco_info_final$Interest.Rate[m]==""){
    reco_info_final$pricing.score[m]<-0.5
  } else if (comparable_price=="Not.Possible"){
    reco_info_final$pricing.score[m]<-0.15
  } else  if (pricingdiff <= 0) {
    reco_info_final$pricing.score[m]<-1
  }else{
    reco_info_final$pricing.score[m]<-round(pricingdiff,digits = 2)
  }
}
ei_con  <- mongo(collection = "customer_interest_relations",db = "credit_service_prod",url = "mongodb://prod-read:X9RPfWVhBk3ihl4d@prd-credavenue-mongo-shard-00-01.a8fyh.mongodb.net:27017/admin?authSource=admin&replicaSet=atlas-10wzyk-shard-0&readPreference=primary&ssl=true")
ei      <- cbind(ei_con$find(fields = '{"_id":true}'),ei_con$find('{}'))
client_con  <- mongo(collection = "customers",db = "credit_service_prod",url = "mongodb://prod-read:X9RPfWVhBk3ihl4d@prd-credavenue-mongo-shard-00-01.a8fyh.mongodb.net:27017/admin?authSource=admin&replicaSet=atlas-10wzyk-shard-0&readPreference=primary&ssl=true")
client_info <- cbind(client_con$find(fields = '{"_id":true}'),client_con$find('{}'))
deal_con     <- mongo(collection = "deals",db = "credit_service_prod",url = "mongodb://prod-read:X9RPfWVhBk3ihl4d@prd-credavenue-mongo-shard-00-01.a8fyh.mongodb.net:27017/admin?authSource=admin&replicaSet=atlas-10wzyk-shard-0&readPreference=primary&ssl=true")
deals_new    <- cbind(deal_con$find(fields = '{"_id":true}'),deal_con$find('{}'))
en_con       <- mongo(collection = "cra_entities",db = "credit_service_prod",url = "mongodb://prod-read:X9RPfWVhBk3ihl4d@prd-credavenue-mongo-shard-00-01.a8fyh.mongodb.net:27017/admin?authSource=admin&replicaSet=atlas-10wzyk-shard-0&readPreference=primary&ssl=true")
entities_revised    <- cbind(en_con$find(fields = '{"_id":true}'),en_con$find('{}'))
