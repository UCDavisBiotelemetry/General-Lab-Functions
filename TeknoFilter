####################################################################################################################################
#                                                                                                                                  #
#                         Tag Filter for Teknologic Receiver Files converted from CBR description                                  #
#                           Written by: Gabe Singer, Damien Caillaud     On: 05/16/2017                                            #
#                                            Last Updated: 06/29/2017                                                              #
#                                                                                                                                  #
####################################################################################################################################


###Install Load Function
install.load <- function(package.name)
{
  if (!require(package.name, character.only=T)) install.packages(package.name)
  library(package.name, character.only=T)
}


install.load('tidyverse')

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
tags<- read.csv("./taglist/FriantTaglist.csv", header = T) #list of known Tag IDs


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
  dput(dat5, file = paste0("./cleaned/", dat5$RecSN[1], "_cleaned.txt"))
}     #need to test this loop with multiple files,
#loop ran, now feed files back into R and see if they look right
# SN6003 <- dget("./cleaned/15-6003_cleaned.txt")
# SN6034 <- dget("./cleaned/15-6034_cleaned.txt")
# SN6034JST <- dget("./cleaned/2015-6034_cleaned.txt") #looks good to me

###Filtering Loop
for(i in list.files("./cleaned")){
  datos <- dget(paste0("./cleaned/", i))        #read in each file
  myResults <- dataFilter(dat=datos, filterthresh=4, counter=1:12)
  rejecteds <- datos[!paste(strftime(datos$dtf, format = "%m/%d/%Y %H:%M:%OS6"), datos$Hex) %in% 
                      paste(strftime(myResults$dtf, format = "%m/%d/%Y %H:%M:%OS6"), myResults$Hex),]
  write.csv(myResults, paste0("./accepted/", myResults$RecSN[1], "_accepted.csv"), row.names=F)
  write.csv(rejecteds, paste0("./rejected/", rejecteds$RecSN[1], "_rejected.csv"), row.names=F)
}



###############################################################
#both filtered files end up with same data, too
sum <- read.csv("./accepted/15-6034_accepted.csv", header = T)
jst <- read.csv("./accepted/2015-6034_accepted.csv", header = T)
