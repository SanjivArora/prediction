
# E16_Count<-read.csv(paste(a,sort(list.files(a,pattern = "^RNZ_E16_Count"),decreasing = TRUE)[1],sep=""),header = TRUE, na.strings=c("","NA"))
# E16_Jam<-read.csv(paste(a,sort(list.files(a,pattern = "^RNZ_E16_Jam"),decreasing = TRUE)[1],sep=""),header = TRUE, na.strings=c("","NA"))
# E16_PMCount<-read.csv(paste(a,sort(list.files(a,pattern = "^RNZ_E16_PMCount"),decreasing = TRUE)[1],sep=""),header = TRUE, na.strings=c("","NA"))
# E16_RomVer<-read.csv(paste(a,sort(list.files(a,pattern = "^RNZ_E16_RomVer"),decreasing = TRUE)[1],sep=""),header = TRUE, na.strings=c("","NA"))
# E16_SC<-read.csv(paste(a,sort(list.files(a,pattern = "^RNZ_E16_SC"),decreasing = TRUE)[1],sep=""),header = TRUE, na.strings=c("","NA"))
# E16_TonerStat<-read.csv(),header = TRUE, na.strings=c("","NA"))

# merge_1<-merge(E16_Count,E16_Jam,by.x = "Serial",by.y = "Serial")
# merge_2<-
  
library(purrr)

default_sources=c("Count", "PMC")#, "SC")

base_path="H:/R - @remote data/Data/"

get_name <- function (prefix, files=list.files(base_path,pattern = paste("^", prefix, sep=""))) {
  return(sort(files,decreasing = TRUE))#[[1]])
}

prefixes_for_model = function(model, sources=default_sources) {
  return (
    map(sources, function (source) paste(model, source, sep="_"))
  )
}

files_for_model <- function (model) {
  #files <- list.files(base_path,pattern = paste("^", model, sep=""))
  return (map(prefixes_for_model(model), function(x) get_name(x)))
}


substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}

matching_files <- function(files_list){
  length(unique(map(files_list,function (x) substrRight(x,12))))==1
}

files_upload <- function (list_of_files) {
  if(matching_files(list_of_files)==TRUE){
    return (map(list_of_files, function (name) read.csv(file.path(a,name) ,header = TRUE, na.strings=c("","NA"))))
    } else {print("Files dates do not match")}
}



files_for_model("RNZ_E16")

rm(default_prefixes)


files_list<-files_for_model("RNZ_E16")

uploaded_files<-files_upload(files_list)

combined_files<-uploaded_files[[1]]

for(x in uploaded_files[-1]){
  
  combined_files<-merge(combined_files,x,by.x="Serial",by.y="Serial")
}

mapply(list, files_list[[1]], files_list[[2]])

write.csv(combined_files,"H:/Combined_Files_E16.CSV")

nums <- unlist(lapply(combined_files, is.numeric))


combined_files_2<-combined_files[,nums]

combined_files_3<-merge(combined_files,E16_SC,by.x = "Serial",by.y = "Serial")

u = umap(combined_files_2)
x=combined_files_2[1:5,]
View(x)
combined_files_3<-combined_files_2[,nums]

combined_files_3[is.na(combined_files_3)==TRUE]<-0

unique(combined_files_3$SCHistory_SC_CD_1)

as.Date(combined_files_2$SCHistory_OCCUR_DATE_1)>as.Date("2018-07-20")

combined_files_2[is.na(combined_files_2)==TRUE]<-0

plot(u$layout[as.Date(combined_files_3$SCHistory_OCCUR_DATE_1)<=as.Date("2018-07-13"),1],u$layout[as.Date(combined_files_3$SCHistory_OCCUR_DATE_1)<=as.Date("2018-07-13"),2])#, col=unique(combined_files_3$SCHistory_SC_CD_1))
points(u$layout[as.Date(combined_files_3$SCHistory_OCCUR_DATE_1)>as.Date("2018-07-13"),1],u$layout[as.Date(combined_files_3$SCHistory_OCCUR_DATE_1)>as.Date("2018-07-13"),2], col=unique(combined_files_3$SCHistory_SC_CD_1))
# points

plot(u$layout[,1],u$layout[,2],col = sort(unique(combined_files_3$SCHistory_SC_CD_1)))

plot(u2$layout[,1],u2$layout[,2],col = sort(unique(combined_files_3$SCHistory_SC_CD_1)))

standardized_files<-(combined_files_2- colMeans(combined_files_2))/ifelse(colSds(as.matrix(combined_files_2))==0,1,colSds(as.matrix(combined_files_2)))

View(standardized_files[1:5,])

is.infinite(standardized_files)

u2 = umap(standardized_files)

plot(u2$layout[as.Date(combined_files_2$SCHistory_OCCUR_DATE_1)<=as.Date("2018-07-13"),1],u2$layout[as.Date(combined_files_2$SCHistory_OCCUR_DATE_1)<=as.Date("2018-07-13"),2])#, col=unique(combined_files_3$SCHistory_SC_CD_1))
points(u2$layout[as.Date(combined_files_2$SCHistory_OCCUR_DATE_1)>as.Date("2018-07-13"),1],u2$layout[as.Date(combined_files_2$SCHistory_OCCUR_DATE_1)>as.Date("2018-07-13"),2], col=unique(combined_files_3$SCHistory_SC_CD_1))

plot(u2$layout[as.Date(combined_files_2$SCHistory_OCCUR_DATE_1)>as.Date("2018-07-13"),1],u2$layout[as.Date(combined_files_2$SCHistory_OCCUR_DATE_1)>as.Date("2018-07-13"),2], col=unique(combined_files_3$SCHistory_SC_CD_1))

# combined_files_3[,combined_files_3$SCHistory_SC_CD_1<100]


View(paste(substr(E16_SC$SCHistory_OCCUR_DATE_1,1,4),substr(E16_SC$SCHistory_OCCUR_DATE_1,6,7),substr(E16_SC$SCHistory_OCCUR_DATE_1,9,10),sep=""))


EndDate<-E16_SC$SCHistory_OCCUR_DATE_1
StartDate<-as.Date(E16_SC$SCHistory_OCCUR_DATE_1)-days(8)

paste(year(EndDate),ifelse(nchar(month(EndDate))==1,paste(0,month(EndDate),sep=""),month(EndDate)),ifelse(nchar(day(EndDate))==1,paste(0,day(EndDate),sep=""),day(EndDate)),sep="")
paste(year(StartDate),ifelse(nchar(month(StartDate))==1,paste(0,month(StartDate),sep=""),month(StartDate)),ifelse(nchar(day(StartDate))==1,paste(0,day(StartDate),sep=""),day(StartDate)),sep="")


# dts<-chron(dates.=paste(substr(E16_SC$SCHistory_OCCUR_DATE_1,9,10),substr(E16_SC$SCHistory_OCCUR_DATE_1,6,7),substr(E16_SC$SCHistory_OCCUR_DATE_1,1,4),sep="/"))

as.Date(E16_SC$SCHistory_OCCUR_DATE_1)-days(7)


Readings<-read_excel(path = "H:\\R - @remote data\\Data Shortlist.xlsx", sheet = "Sheet1")
Readings2<-make.names(Readings[[1]])


combined_files_reduced<-combined_files[,match(Readings2,names(combined_files))]


library(umap)


remove_na<-function(data,normalize = TRUE){
  data[is.na(data)==TRUE]<-0
  if(normalize ==TRUE) {
    data<-(data- colMeans(data))/ifelse(colSds(as.matrix(data))==0,1,colSds(as.matrix(data)))
  }
  return(data)
}


# combined_files_reduced[is.na(combined_files_reduced)==TRUE]<-0

combined_files_cleaned<-remove_na(combined_files_reduced)#,normalize = TRUE)

u3<-umap(combined_files_cleaned)

plot(u3$layout[,1],u3$layout[,2],col = combined_files_3$SCHistory_SC_CD_1)

plot(u3$layout[as.Date(combined_files_3$SCHistory_OCCUR_DATE_1)<=as.Date("2018-07-13"),1],u3$layout[as.Date(combined_files_3$SCHistory_OCCUR_DATE_1)>as.Date("2018-07-13"),2])#, col=unique(combined_files_3$SCHistory_SC_CD_1))
points(u3$layout[as.Date(combined_files_3$SCHistory_OCCUR_DATE_1)>as.Date("2018-07-13"),1],u3$layout[as.Date(combined_files_3$SCHistory_OCCUR_DATE_1)>as.Date("2018-07-13"),2], col=combined_files_3$SCHistory_SC_CD_1)





plot(u3$layout[,1],u3$layout[,2],xlim=c(-5,8))
points(u3$layout[as.Date(combined_files_3$SCHistory_OCCUR_DATE_1)>as.Date("2018-07-13"),1],u3$layout[as.Date(combined_files_3$SCHistory_OCCUR_DATE_1)>as.Date("2018-07-13"),2], col="red")

x <- u3$layout[as.Date(combined_files_3$SCHistory_OCCUR_DATE_1)>as.Date("2018-07-13") & ((combined_files_3$SCHistory_SC_CD_1 >= 200 & combined_files_3$SCHistory_SC_CD_1 < 300) | combined_files_3$SCHistory_SC_CD_1>= 800 ),]

plot(u3$layout[,1],u3$layout[,2])#,xlim=c(-5,8))
points(x[,1],x[,2], col="red")

plot(u3$layout[,1],u3$layout[,2])




cbind(unlist(mapply(list, files_list[[1]])),unlist(mapply(list, files_list[[2]])))
# Load file above in function before loop. Load and merge data sets in function, store in list and return list at the end

Upload_multiple_Datasets<-function(list_of_file_namess){
  n1<-length(list_of_file_namess)
  list_of_files<-unlist(mapply(list, files_list[[1]][1]))
  for(i in 2:n1){
    list_of_files<-cbind(unlist(list_of_files,unlist(mapply(list, files_list[[i]][1]))))
  }
  n2<-unlist(mapply(list, list_of_files[[1]]))
  Temporary_dataset<-files_upload(list_of_files[[1]][1])
  Datasets_history <- list()
  for(y in 1:n1){
    for(x in 2:n2){
      Temporary_dataset<-merge(Temporary_dataset,files_upload(list_of_files[[x]][y]),by.x = "Serial",by.y = "Serial")
    }
    Datasets_history[[y]]<-Temporary_dataset
    Temporary_dataset<-files_upload(list_of_files[[x]][y+1])
  }
  return(Temporary_dataset)
}










