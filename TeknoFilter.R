####################################################################################################################################
#                                                                                                                                  #
#                         Tag Filter for Teknologic Receiver Files converted from CBR description                                  #
#                         Written by: Gabe Singer, Damien Caillaud     On: 05/16/2017                                              #
#                         Last Updated: 09/29/2017  Gabriel Singer                                                                 #
#                         Included in last update: Added cleaning loop for Lotek receiver files                                    #
#                         Version 2.2 from Semi-rugged toughbook 11/16/2017 GS/CH. Copied by MP.                                   #
####################################################################################################################################


###Install Load Function
install.load <- function(package.name)
{
  if (!require(package.name, character.only=T)) install.packages(package.name)
  library(package.name, character.only=T)
}

install.load('tidyverse')
install.load('readxl')
install.packages("lubridate")


###counter
counter <- 1:12

###mode
mode <- function(x, i){
  ta <- table(x)
  tam <- max(ta)
  if (all(ta == tam))
    mod <- NA
  else
    if(is.numeric(x))
      mod <- as.numeric(names(ta)[ta == tam])
  else
    mod <- names(ta)[ta == tam]
  #print(i)
  return(mod)
}

###load taglist
tags<- read.csv("./taglist/FrianttaglistUCDtags(noBeacon).csv", header = T) #list of known Tag IDs
tags$Tag.ID..hex.<- as.character(tags$Tag.ID..hex.)       #make sure that class(idHex) is character

###magicFunction
magicFunc <- function(dat, tagHex, counter){
  dat5 <- dat
  tagid <- dat5[dat5$Hex==tagHex,]##use 2892 as an example
  logTable <- data.frame(hitRowNb=numeric(0), initialHitRowNb=numeric(0), isAccepted=logical(0), nbAcceptedHitsForThisInitialHit =logical(0))
  for(i in 1:nrow(tagid)){
    hits<- tagid[tagid$dtf>=tagid$dtf[i] & tagid$dtf<=tagid$winmax[i], ] #hits within window
    #head(tagid[tagid$dtf>=tagid$dtf[i], ], 100)
    #if nrow(hits>=4) then... (need to write code for this qualifier, if_else???)
    if(nrow(hits)>1){
      retained<- numeric(0) #make empty df to hold detections retained
      # indices<- numeric(0) #make empty df to hold detections retained
      
      for(j in 2:nrow(hits)){
        candidates<- round(as.numeric(hits$dtf[j]-hits$dtf[1])/counter, digits = 2) #subsubtract time of each det in window from initial and divide by 1:12
        candidates<- candidates[candidates>= tagid$nPRI[i]*0.651 & candidates<= tagid$nPRI[i]*1.3] #constrain values
        retained<- c(retained, candidates)#bind each batch of retained candiate PRIs to a single list
        # indices <- c(indices, rep(j, length(candidates)))
      }
      if(length(retained)==0){
        logTable <- rbind(logTable, data.frame(hitRowNb=NA, initialHitRowNb=i, isAccepted=FALSE, nbAcceptedHitsForThisInitialHit=0))
      } else {
        #plot(table(retained))
        ePRI<- min(mode(retained, i))
        if(is.na(ePRI)) ePRI <- min(retained)
        
        nbHits <- 1
        for(j in 2:nrow(hits)){
          ii <- round(as.numeric(hits$dtf[j]-hits$dtf[1])/ePRI)
          uppb <- ii*ePRI+hits$dtf[1]+0.006+ii*0.006
          lowb <- ii*ePRI+hits$dtf[1]-(0.006+ii*0.006)
          nbHits <- nbHits+(hits$dtf[j]>= lowb & hits$dtf[j]<= uppb)
          logTable <- rbind(logTable, data.frame(hitRowNb=i+j-1, initialHitRowNb=i, isAccepted=(hits$dtf[j]>= lowb & hits$dtf[j]<= uppb), nbAcceptedHitsForThisInitialHit=NA))
        }
        logTable$nbAcceptedHitsForThisInitialHit[logTable$initialHitRowNb ==i] <- nbHits
      }
    } else {
      logTable <- rbind(logTable, data.frame(hitRowNb=NA, initialHitRowNb=i, isAccepted=FALSE, nbAcceptedHitsForThisInitialHit=0))
    }
    #print(paste("row", i,"done"))
    #readline("next\n")
  }
  return(logTable)
}

###dataFilter
dataFilter <- function(dat, filterthresh, counter){
  res <- dat[numeric(0),]
  timer <- 0
  for(i in unique(dat$Hex)){
    ans <- magicFunc(dat, tagHex=i, counter=1:12)
    ans[!is.na(ans$hitRowNb),]
    ans2 <- ans[ans$nbAcceptedHitsForThisInitialHit>= filterthresh,]
    keep <- c(ans2$hitRowNb[ans2$isAccepted], ans2$initialHitRowNb[ans2$isAccepted])
    keep <- keep[!duplicated(keep)]
    ans3 <- dat[dat$Hex==i,][keep,]
    res <- rbind(res, ans3)
    timer <- timer+1
    print(timer/length(unique(dat$Hex)))
  }
  return(res)
}

###Cleaning Loop for .jst files
for(i in list.files("./jst")){
  dat <- read.csv(paste0("./jst/", i), header=F)        #read in each file
  names(dat)<- c("Filename", "RecSN", "DT", "FracSec", "Hex", "CRC", "validFlag", "TagAmp", "NBW") #rename columns
  tags <- read.csv("./taglist/FriantTaglist.csv")
  dat<- dat[dat$Hex %in% tags$Tag.ID..hex., ]
  dat$nPRI<- 5   # set nPRI (Nominal PRI) for the tag
  #combine the DT and FracSec columns into a single time column and convert to POSIXct
  dat$dtf<- paste0(dat$DT, substring(dat$FracSec,2)) #paste the fractional seconds to the end of the DT in a new column
  dat$dtf<- as.POSIXct(dat$dtf, format = "%m/%d/%Y %H:%M:%OS", tz="Etc/GMT-8") #convert to POSIXct beware this may change value of 0.0000X
  #head(strftime(dat2$dtf, format = "%m/%d/%Y %H:%M:%OS5")) #verify that although the fractional seconds don't print, they are indeed there
  dat2<- as.tbl(dat)
  dat2$Hex <- as.character(dat2$Hex)
  dat2<- arrange(dat2, Hex, dtf)#sort by TagID and then dtf, Frac Second
  #calculate tdiff, then remove multipath
  dat3 <- data.frame(dat2, tdiff=c(NA, difftime(dat2$dtf[-1], dat2$dtf[-nrow(dat2)])), winmax=dat2$dtf+((dat2$nPRI*1.3*max(counter))+1))
  dat4 <- data.frame(dat3, crazy=c(NA,dat3$Hex[-nrow(dat3)]==dat3$Hex[-1]))
  dat4$tdiff[dat4$crazy==0] <- NA
  dat5 <- dat4[,-14]
  dat5 <- dat5[dat5$tdiff>0.2 | is.na(dat5$tdiff),]
  dput(dat5, file = paste0("./cleaned/", dat5$RecSN[1], "_cleaned.txt"))
}###the above cleaning and saving loop is good, next step incorporate it into functioning loop

###Cleaning loop for .SUM files
timer2 <- 0
for(i in list.files("./raw")){
  #read in each file
  dat <- read.csv(paste0("./raw/", i), skip = 8, header=T)
  #rename columns to match db
  names(dat)<- c("Detection", "RecSN", "dtf", "Hex", "Tilt", "Volt", "Temp", "Pres", "Amp",
                 "Freq", "Thresh", "nbw", "snr", "Valid")
  
  #change object format to tbl
  dat <- as.tbl(dat)
  #drop the barker code and the CRC from tagid field
  dat$Hex <- substr(dat$Hex, 4, 7)
  #name and print new tbl w/ bad detect lines removed
  (dat <- dat %>%
      filter(Detection != "      -"))
  #filter receiver file by known taglist
  dat<- dat[dat$Hex %in% tags$Tag.ID..hex., ]
  #if there aren't any tags from the tag list in the receiver files, move on to the next file
  if(!(nrow(dat)))next
  #set nPRI (Nominal PRI) for the tag
  dat$nPRI<- 5
  #convert to POSIXct note: fractional secons will no longer print, but they are there
  dat$dtf<- as.POSIXct(dat$dtf, format = "%m/%d/%Y %H:%M:%OS", tz="Etc/GMT-8")
  #run the next line to verify that you haven't lost your frac seconds
  #(strftime(dat$dtf, format = "%m/%d/%Y %H:%M:%OS6"))
  dat2 <- dat
  dat2<- arrange(dat2, Hex, dtf)#sort by TagID and then dtf, Frac Second
  #calculate tdiff, then remove multipath
  dat3 <- data.frame(dat2, tdiff=c(NA, difftime(dat2$dtf[-1], dat2$dtf[-nrow(dat2)])),
                     winmax=dat2$dtf+((dat2$nPRI*1.3*max(counter))+1))
  dat4 <- data.frame(dat3, crazy=c(NA,dat3$Hex[-nrow(dat3)]==dat3$Hex[-1]))
  dat4$tdiff[dat4$crazy==0] <- NA
  dat5 <- dat4[,-18]
  dat5 <- dat5[dat5$tdiff>0.2 | is.na(dat5$tdiff),]
  timer2 <- timer2+1
  dput(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(", timer2, ")",  "_cleaned.txt"))
}     #need to test this loop with multiple files,
#loop ran, now feed files back into R and see if they look right
# SN6003 <- dget("./cleaned/15-6003_cleaned.txt")
# SN6034 <- dget("./cleaned/15-6034_cleaned.txt")
# SN6034JST <- dget("./cleaned/2015-6034_cleaned.txt") #looks good to me

###Cleaning loop for ATS receiver files
timer2 <- 0
for(i in list.files("./raw/")){
  dat <- read_excel(paste0("./raw/", i))                    #read in each file
  SN <- as.numeric(gsub("Serial Number: ", "", (dat[2, 1]))) #extract serial number of the receiver
  if(is.na(SN) == TRUE) {
    SN <- as.numeric(gsub("Serial Number: ", "", colnames(dat[0,1])))
  }
  print(SN)
  find.na <- as.numeric(which(is.na(dat[ , 1])))             #find the NA's in Column 1
  start <- max(find.na[find.na <= 100])                      #the value 100 can be set to anything, 
  #it just needs to be less that the total
  #number of detections in the file
  dat <- dat[(start + 7):nrow(dat), ]                        #ditch garbage at beginning of the file
  
  headers <- c("Filename", "SiteName", "SiteName2", "SiteName3", "dtf", "Hex", "Tilt", "VBatt", "Temp", "Pres", "SigStr",
               "BitPeriod", "Thresh", "Detection")                        #make vector of new headers
  names(dat) <- headers 
                                                            #rename with the right headers
  extracols <- c("Amp", "Freq", "nbw", "snr","Valid", "RKM", "GenRKM", "LAT", "LON")
  mat <- as.data.frame(matrix(rep(NA, nrow(dat)*length(extracols)), nrow(dat), length(extracols)))
  names(mat) <- extracols
  dat <- cbind(dat, mat)
  dat$Hex <- substr(dat$Hex, 5, 8)                           #deal with the TagCode situation
  dat$RecSN <- rep(SN, nrow(dat))                            #add SN column 
  dat <- dat[ ,5:(ncol(dat))]                                  #drop the filename and site name columns
  print(dat)
  dat <- as.tbl(dat)                                         #change object format to tbl 
  dat<- dat[dat$Hex %in% tags$Tag.ID..hex., ]                #filter receiver file by known taglist
  dat$nPRI<- 5                                               #set nPRI (Nominal PRI) for the tag (this will have 
  #to be set to something different for tags with a PRI other than 5)
  dat$dtf<- as.POSIXct(dat$dtf, format = "%m/%d/%Y %H:%M:%OS", 
                       tz="Etc/GMT-8")                       #convert to POSIXct note: fractional seconds will no longer print, but 
  #they are there. Run the next line to verify that you haven't lost your 
  #frac seconds
  #(strftime(dat$dtf, format = "%m/%d/%Y %H:%M:%OS6"))
  dat2 <- dat
  dat2<- arrange(dat2, Hex, dtf)                             #sort by TagID and then dtf, Frac Second
  dat3 <- data.frame(dat2, tdiff=c(NA, difftime(dat2$dtf[-1], dat2$dtf[-nrow(dat2)], units = "secs" )), 
                     winmax=dat2$dtf+((dat2$nPRI*1.3*max(counter))+1))   #calculate tdiff, then remove multipath
  dat4 <- data.frame(dat3, crazy=c(NA,dat3$Hex[-nrow(dat3)]==dat3$Hex[-1]))
  dat4$tdiff[dat4$crazy==0] <- NA
  dat5 <- dat4[,-14]
  dat5 <- dat5[dat5$tdiff>0.2 | is.na(dat5$tdiff),]
  timer2 <- timer2+1
  dput(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(", timer2, ")",  "_cleaned.txt"))
}

###Cleaning loop for Lotek Files 
timer2 <- 0
for(i in list.files("./raw/")){
  dat <- read.table(paste0("./raw/", i), header = F, sep = ",")   #read in each file
  SN <- as.numeric(regmatches(i,regexpr("[0-9].*[0-9]", i)))               #extract serial number of the receiver
  headers <- c("datetime", "FracSec", "Dec", "Hex", "SigStr")     #make vector of new headers
  names(dat) <- headers                                           #rename with the right headers
  dat$RecSN <- rep(SN, nrow(dat))                                 #add SN column 
  dat$datetime <- as.POSIXct(round(dat$datetime*60*60*24), 
                             origin = "1899-12-30", tz = "GMT")
  dat$Hex <- as.character(dat$Hex)
  dat$Hex <- substr(dat$Hex, 2, nchar(dat$Hex))
  
  dat<- dat[dat$Hex %in% tags$Tag.ID..hex., ]               #filter receiver file by known taglist (already done in Lotek
  # software should have the same number of dets)
  dat$nPRI<- 5                                               #set nPRI (Nominal PRI) for the tag (this will have 
  dat <- as.tbl(dat)                                         #change object format to tbl 
  dat$dtf <- paste0(dat$datetime, substring(dat$FracSec,2))  #paste the fractional seconds to the end of the DT in a new column
  dat$dtf<- ymd_hms(dat$dtf) #convert to POSIXct beware this may change value of 0.0000X
  #to be set to something different for tags with a PRI other than 5)
  #convert to POSIXct note: fractional seconds will no longer print, but 
  #they are there. Run the next line to verify that you haven't lost your 
  #frac seconds
  #(strftime(dat$dtf, format = "%m/%d/%Y %H:%M:%OS6"))
  dat2 <- dat
  dat2<- arrange(dat2, Hex, dtf)                             #sort by TagID and then dtf, Frac Second
  dat3 <- data.frame(dat2, tdiff=c(NA, difftime(dat2$dtf[-1], dat2$dtf[-nrow(dat2)], units = "secs" )), 
                     winmax=dat2$dtf+((dat2$nPRI*1.3*max(counter))+1))   #calculate tdiff, then remove multipath
  dat4 <- data.frame(dat3, crazy=c(NA,dat3$Hex[-nrow(dat3)]==dat3$Hex[-1]))
  dat4$tdiff[dat4$crazy==0] <- NA
  dat5 <- dat4[,-11]
  dat5 <- dat5[dat5$tdiff>0.2 | is.na(dat5$tdiff),]
  timer2 <- timer2+1
  dput(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(", timer2, ")",  "_cleaned.txt"))
}

###Filtering Loop
for(i in list.files("./cleaned")){
  datos <- dget(paste0("./cleaned/", i))        #read in each file
  myResults <- dataFilter(dat=datos, filterthresh=3, counter=1:12)
  rejecteds <- datos[!paste(strftime(datos$dtf, format = "%m/%d/%Y %H:%M:%OS6"), datos$Hex) %in%
                       paste(strftime(myResults$dtf, format = "%m/%d/%Y %H:%M:%OS6"), myResults$Hex),]
  write.csv(myResults, paste0("./accepted/", substr(i, 1, nchar(i)-12), "_accepted.csv"), row.names=F)
  write.csv(rejecteds, paste0("./rejected/", substr(i, 1, nchar(i)-12), "_rejected.csv"), row.names=F)
}


###############################################################
#both filtered files end up with same data, too
sum <- read.csv("./accepted/15-6034_accepted.csv", header = T)
jst <- read.csv("./accepted/2015-6034_accepted.csv", header = T)
###############################################################

