####################################################################################################################################
#                                                                                                                                  #
#                         Tag Filter for Teknologic Receiver Files converted from CBR description                                  #
#                           Written by: Gabe Singer, Damien Caillaud     On: 05/16/2017                                            #
#                                   Last Updated: 11/21/2017 by Matt Pagel                                                         #
#                                                                                                                                  #
####################################################################################################################################
setwd("Z:/Shared/Projects/JSATS/DSP_Spring-Run Salmon/Pat Brandes Filter Data/Matt")
#setwd("C:/Users/chause/Desktop/Pats Filter Data/SJReceieverFilterData")
TAGFILENAME = "./taglist/Brandes.csv"
DoCleanJST = FALSE
DoCleanSUM = TRUE
DoCleanATS = FALSE
DoCleanLotek = FALSE
DoSaveIntermediate = TRUE # (DoCleanJST || DoCleanSUM || DoCleanATS || DoCleanLotek)
DoFilterFromSavedCleanedData = TRUE || !DoSaveIntermediate # if you're not saving the intermediate, you should do direct processing
FILTERTHRESH = 3
counter <- 1:12

###Install Load Function
install.load <- function(package.name)
{
  if (!require(package.name, character.only=T)) install.packages(package.name)
  library(package.name, character.only=T)
}

install.load('tidyverse')
install.load('readxl')
install.load('lubridate')
install.load('data.table')

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

getmode <- function(v) {
  uniqv <- sort(unique(v))
  print(uniqv)
  print(match(v,uniqv))
  print(tabulate(match(v,uniqv)))
  uniqv[which.max(tabulate(match(v, uniqv)))]
  return(uniqv)
}

data.table.parse<-function (file = "", n = NULL, text = NULL, prompt = "?", keep.source = getOption("keep.source"), 
                            srcfile = NULL, encoding = "unknown") 
{
  keep.source <- isTRUE(keep.source)
  if (!is.null(text)) {
    if (length(text) == 0L) 
      return(expression())
    if (missing(srcfile)) {
      srcfile <- "<text>"
      if (keep.source) 
        srcfile <- srcfilecopy(srcfile, text)
    }
    file <- stdin()
  }
  else {
    if (is.character(file)) {
      if (file == "") {
        file <- stdin()
        if (missing(srcfile)) 
          srcfile <- "<stdin>"
      }
      else {
        filename <- file
        file <- file(filename, "r")
        if (missing(srcfile)) 
          srcfile <- filename
        if (keep.source) {
          text <- readLines(file, warn = FALSE)
          if (!length(text)) 
            text <- ""
          close(file)
          file <- stdin()
          srcfile <- srcfilecopy(filename, text, file.mtime(filename), 
                                 isFile = TRUE)
        }
        else {
          text <- readLines(file, warn = FALSE)
          if (!length(text)) {
            text <- ""
          } else {
            text <- gsub("(, .internal.selfref = <pointer: 0x[0-9A-Fa-f]+>)","",text,perl=TRUE)
          }
          on.exit(close(file))
        }
      }
    }
  }
  #  text <- gsub("(, .internal.selfref = <pointer: 0x[0-9A-F]+>)","",text)
  .Internal(parse(file, n, text, prompt, srcfile, encoding))
}
data.table.get <- function(file, keep.source = FALSE)
  eval(data.table.parse(file = file, keep.source = keep.source))
dtget <- data.table.get

magicFunc <- function(dat, tagHex, counter, filterthresh){
  dat2 <-copy(dat)
  setkey(dat2,Hex)
  tagdet <- dat2[Hex==tagHex]
  setkey(tagdet,dtf)
  countermax <- max(counter)
  tagdet[,temporary:=as.POSIXct(dtf, format = "%m/%d/%Y %H:%M:%OS", tz="Etc/GMT-8")]
  if (is.na(tagdet[,.(temporary)][1])) {
    tagdet[,dtf:=as.POSIXct(dtf, format = "%Y-%m-%dT%H:%M:%S.%OSZ", tz="UTC")]
  } else {
    tagdet[,dtf:=temporary]
  }
  tagdet[,temporary:=NULL]
  tagdet[,winmax:=dtf+((nPRI*1.3*countermax)+1)]#[,dup:=dtf] ##use 2892 as an example
  #  rm(dat5)
  setkey(tagdet,dtf)
  shiftz = 1:(filterthresh-1)
  shiftzcols = paste("l",shiftz,sep="")
  tagdet[,(shiftzcols):=shift(dtf,n=shiftz,fill=NA,type="lead")]
#  tagdet[,(shiftzcols):=lapply(tagdet[,(shiftzcols),with=FALSE],function (x) as.POSIXct(x,format="%m/%d/%Y %H:%M:%OS", tz="Etc/GMT-8"))]
  aclist<-as.data.table(as.POSIXct(unique(unlist(tagdet[difftime(get(shiftzcols[max(shiftz)]),winmax)<=0,c("dtf",(shiftzcols[1:(filterthresh-1)])),with=F],use.names=FALSE)),origin="1970-01-01",tz="Etc/GMT-8"))
  if (aclist[,.N]>0) {
    setkey(aclist,x)
    res <- tagdet[aclist]
    res[,(shiftzcols[2:max(shiftz)]):=NULL]
    res[,twind:=l1-dtf]
  #  retained <- unique(c(res[,dtf],res[,dup]))
    #  res[,ID=.I]
    itr <- as.data.table(merge(x=counter,res))
    #  rm(res)
    itr[,icalc:=round(twind/x,2)]
    ll <- itr[icalc>=nPRI*0.651 & icalc<=nPRI*1.3]
  #  rm(itr)
    setkey(ll,dtf)
    abbrev = ll[,.(dtf,twind,icalc,x,nPRI)]
    setkey(abbrev,icalc)
#  modes<-abbrev[,.(icalc,tot=.N),by=icalc][,.(icalc,tot=.N),keyby=.(dtf,icalc)])[,icalc[sapply(.SD,which.max)],by=dtf,.SDcols="tot"][,.(dtf,ePRI=V1)]
    cmod<-abbrev[,dist:=abs(nPRI-icalc)][,.(tot=.N),by=.(icalc,dist)][order(-tot,dist,-icalc)][1]
    ePRI<-cmod$icalc
    if (is.na(ePRI)) ePRI<-5
    flopintervals <-as.data.table(0:countermax)
#  flopintervals <- as.data.table(counter)
#  flopintervals[,flop:=(counter+1)*.006][,.(x=counter,flopmin=counter*ePRI-flop,flopmax=counter*ePRI+flop)]
    flopintervals[,x:=V1][,V1:=NULL][,flop:=(x+1)*.006][,flopmin:=x*ePRI-flop][,flopmax:=x*ePRI+flop][,flop:=NULL]
    maxflop <- flopintervals[x==12,flopmax]
    windowz <- unique(abbrev[,.(dtf,ewinmax=dtf+maxflop)])
    dett <-windowz[,.(dup=dtf,dd=dtf)]
    setkey(dett,dup,dd)
    setkey(windowz,dtf,ewinmax)
    fomega <- foverlaps(dett,windowz,maxgap=0,type="within")[,dd:=NULL][,dif:=(dup-dtf)*1000][,dif2:=(dup-dtf)*1000]
    flopintervals[,newmin:=flopmin*1000][,newmax:=flopmax*1000]
    setkey(flopintervals,newmin,newmax)
    if (fomega[,.N]>0) {
      setkey(fomega,dif,dif2)
    # Hope to correct for: Error in if (any(y[[yintervals[2L]]] - y[[yintervals[1L]]] < 0L)) stop("All entries in column ",  : missing value where TRUE/FALSE needed
      tryCatch(
        {
          windHits<-foverlaps(fomega,flopintervals,type="within")[,.(firstHit=dtf,windowEnd=ewinmax,hit=dup,intervals=x)]},
        warning = function(w) {
          print("warning encountered when trying to find matching time entries")
          print(paste(fomega[,.N],"was the size of array"))
          windHits<-fomega[1==0][,.(firstHit=dtf,windowEnd=ewinmax,hit=dup,intervals=NA)]
          print(ePRI)
          print(tagHex)
          print(w)
        },
        error = function(e) {
          print("error encountered when trying to find matching time entries")
          print(paste(fomega[,.N],"was the size of array"))
          windHits<-fomega[1==0][,.(firstHit=dtf,windowEnd=ewinmax,hit=dup,intervals=NA)]
          print(ePRI)
          print(tagHex)
          print(e)
        },
        finally = { }
      )
  #  windHits<-foverlaps(fomega,flopintervals,type="within",nomatch=0)[,.(firstHit=dtf,windowEnd=ewinmax,hit=dup,intervals=x)][,c:=.N,keyby=firstHit][c>1]  logTable <- data.table(hitRowNb=numeric(0), initialHitRowNb=numeric(0), isAccepted=logical(0), nbAcceptedHitsForThisInitialHit =logical(0))
      NAs<-windHits[is.na(intervals)]
      noNAs<-windHits[!is.na(intervals)][,c:=.N,keyby="firstHit"] # do I need to check for no lines before c code?
      noOnlyFirst<-noNAs[c>1]
      onlyFirst<-noNAs[c==1,]
      #      noOnlyFirst<-noNAs[c>=filterthresh] #should be noOnlyTooShort
#      onlyFirst<-noNAs[c>0&c<filterthresh,]
      noOnlyFirst[,isAccepted:=TRUE]
      NAs[,isAccepted:=FALSE][,c:=NA]
      onlyFirst[,isAccepted:=FALSE][,c:=0]
      LT<-rbind(noOnlyFirst,NAs,onlyFirst)
      setkey(LT,firstHit,hit)
      logTable<-LT[,.(hit=hit, initialHit=firstHit, isAccepted, nbAcceptedHitsForThisInitialHit=c)]
    } else {
      logTable<-data.table(hit=NA, initialHit=NA, isAccepted=FALSE, nbAcceptedHitsForThisInitialHit=0)
    }
  } else {
    logTable<-data.table(hit=NA, initialHit=NA, isAccepted=FALSE, nbAcceptedHitsForThisInitialHit=0)
  }
  return(logTable)
}

dataFilter <- function(dat, filterthresh, counter){
  res <- dat[1==0] # copies structure
  timer <- 0
#  u<-unique(datos$Hex)
  setkey(dat,Hex)
  titl<-dat[!is.na(RecSN)][1][,RecSN]
  u<-as.list(unique(dat[,.N,by = Hex][N>=filterthresh])[,1])$Hex
  if (length(u)>0) timerbar<-winProgressBar(title=titl, label="Tag", min=0, max=length(u), initial=0)
#    txtProgressBar(min = 0, max = length(u), initial = 0, char = "=", style = 3)
  for(i in u){
    ans <- magicFunc(dat, tagHex=i, counter=1:12, filterthresh)
#    ans[!is.na(hitRowNb)]
    setkey(ans,nbAcceptedHitsForThisInitialHit,isAccepted)
    ans2 <- ans[(nbAcceptedHitsForThisInitialHit >= filterthresh)&(isAccepted)]
    if (ans2[,.N]>0) {
#    keep <- c(ans2[hitRowNb[ans2[isAccepted]]], ans2[initialHitRowNb[ans2[isAccepted]]])
#    setkey(ans2,hit)
      keep<-as.data.table(unique(ans2[,hit]))
      setkey(dat,dtf)
      setkey(keep,x)
      res <- rbind(res, dat[keep])
    }
    timer <- timer+1
    setWinProgressBar(timerbar,timer,label=i)
#    print(timer/length(u))
  }
  if (length(u)>0) close(timerbar)
  return(res)
}

###Cleaning .jst files
cleanJST <- function(i, tags) {
  dat <- read.csv(paste0("./jst/", i), header=F)        #read in each file
  names(dat)<- c("Filename", "RecSN", "DT", "FracSec", "Hex", "CRC", "validFlag", "TagAmp", "NBW") #rename columns
  #  tags <- read.csv("./taglist/FriantTaglist.csv")
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
  dat3 <- data.frame(dat2, tdiff=c(NA, difftime(dat2$dtf[-1], dat2$dtf[-nrow(dat2)])))
  dat4 <- data.frame(dat3, crazy=c(NA,dat3$Hex[-nrow(dat3)]==dat3$Hex[-1]))
  dat4$tdiff[dat4$crazy==0] <- NA
  dat5 <- dat4[,-14]
  dat5 <- dat5[dat5$tdiff>0.2 | is.na(dat5$tdiff),]
#  dput(dat5, file = paste0("./cleaned/", dat5$RecSN[1], "_cleaned.dput"))
  if (DoSaveIntermediate) fwrite(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(", itercount, ")",  "_cleaned.fwri"))
  if (!DoFilterFromSavedCleanedData) {
    dat5<-as.data.table(dat5)
    filterData(dat5)
  }
#  fwrite(dat5, file = paste0("./cleaned/", dat5$RecSN[1], "_cleaned.fwri"))
}

###Cleaning .SUM files
cleanSUM <- function() { #set up enclosure
  itercount <- 0
  f<-function(i, tags, max_counter) {
    #read in each file
    dat <- fread(i, skip = 8, header=T)
    #rename columns to match db
    names(dat)<- c("Detection", "RecSN", "dtf", "Hex", "Tilt", "Volt", "Temp", "Pres", "Amp",
                   "Freq", "Thresh", "nbw", "snr", "Valid")
    #drop the barker code and the CRC from tagid field & convert to POSIXct note: fractional seconds will no longer print, but they are there
    dat[, Hex := substr(Hex, 4, 7)]
    dat[, dtf := as.POSIXct(dtf, format = "%m/%d/%Y %H:%M:%OS", tz="Etc/GMT-8")]
    dat[, nPRI := 5]
    #tbl w/ bad detect lines removed, filtered by known taglist
    dat <- dat[trimws(Detection) != "-" & Hex %in% as.character(unlist(tags[2]))]
    setkey(dat, Hex, dtf)
    noMP <- dat[, tdiff := as.numeric(difftime(dtf,shift(dtf, n=1, fill=NA)),units="secs"), by=Hex]
#    noMP[, winmax:=dtf+5*1.3*max_counter+1]
    #  while (noMP[tdiff<0.2, .N][1]) { # keep looping until multipath gone
    noMP <- noMP[tdiff > 0.2 | is.na(tdiff)][, tdiff := as.numeric(difftime(dtf,shift(dtf, n=1, fill=NA)),units="secs"), by=Hex]
    #  }
    dat5 <- noMP
    itercount<<- itercount+1 #cleanSUM's itercount
    # dput(dat5, file = paste0("./cleaned/", dat5[,RecSN][1],"(",itercount,")", "_cleaned.dput"))
    if (DoSaveIntermediate) dput(dat5, file = paste0("./cleaned/", dat5[,RecSN][1],"(",itercount,")", "_cleaned.dput")) #fwrite(dat5, file = paste0("./cleaned/", dat5[,RecSN][1],"(",itercount,")", "_cleaned.fwri"))
    if (!DoFilterFromSavedCleanedData) {
      # already data.table
      filterData(dat5)
    }
  }
  f
}

###Cleaning loop for ATS receiver files
cleanATS <- function() {
  itercount <- 0
  f <- function(i, tags, max_counter) {
    dat <- read_excel(i)                    #read in each file
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
    dat3 <- data.frame(dat2, tdiff=c(NA, difftime(dat2$dtf[-1], dat2$dtf[-nrow(dat2)], units = "secs" )))   #calculate tdiff, then remove multipath
    dat4 <- data.frame(dat3, crazy=c(NA,dat3$Hex[-nrow(dat3)]==dat3$Hex[-1]))
    dat4$tdiff[dat4$crazy==0] <- NA
    dat5 <- dat4[,-14]
    dat5 <- dat5[dat5$tdiff>0.2 | is.na(dat5$tdiff),]
    itercount <<- itercount+1
#    dput(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(", itercount, ")",  "_cleaned.dput"))
    if (DoSaveIntermediate) fwrite(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(", itercount, ")",  "_cleaned.fwri"))
    if (!DoFilterFromSavedCleanedData) {
      dat5<-as.data.table(dat5)
      filterData(dat5)
    }
  }
  f
}

###Cleaning loop for Lotek Files 
cleanLotek <-function() {
  itercount <- 0
  f <- function(i, tags, max_counter) {
    dat <- read.table(i, header = F, sep = ",")   #read in each file
    SN <- as.numeric(regmatches(i,regexpr("[0-9].*[0-9]", i)))      #extract serial number of the receiver
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
    dat3 <- data.frame(dat2, tdiff=c(NA, difftime(dat2$dtf[-1], dat2$dtf[-nrow(dat2)], units = "secs" )))   #calculate tdiff, then remove multipath
    dat4 <- data.frame(dat3, crazy=c(NA,dat3$Hex[-nrow(dat3)]==dat3$Hex[-1]))
    dat4$tdiff[dat4$crazy==0] <- NA
    dat5 <- dat4[,-11]
    dat5 <- dat5[dat5$tdiff>0.2 | is.na(dat5$tdiff),]
    itercount <<- itercount+1
#    dput(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(", itercount, ")",  "_cleaned.dput"))
#    fwrite(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(", itercount, ")",  "_cleaned.fwri"))
    if (DoSaveIntermediate) fwrite(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(", itercount, ")",  "_cleaned.fwri"))
    if (!DoFilterFromSavedCleanedData) {
      dat5<-as.data.table(dat5)
      filterData(dat5)
    }
    
  }
  f
}

##filter Loop
filterData <- function(incomingData=NULL) {
  j<-0
  loopFiles <- function() {
    for(i in list.files("./cleaned",full.names=TRUE)){
      if (as.integer(file.info(i)["isdir"])) next
      #  datos <- dtget(i)        #read in each file
      if (endsWith(i,'.dput') || endsWith(i,'.txt')) datos <-as.data.table(dtget(i))
      if (endsWith(i,'.fwri')) {
        datos <-as.data.table(fread(i))
        datos[,dtf:=as.POSIXct(dtf, format = "%Y-%m-%dT%H:%M:%S.%OSZ", tz="UTC")]
      }
      proces(dat=datos)
    }
  }
  proces <- function(dat) {
      myResults <- dataFilter(dat=dat, filterthresh=FILTERTHRESH, counter=1:12)
      
      #  rejecteds <- datos[!paste(strftime(datos$dtf, format = "%m/%d/%Y %H:%M:%OS6"), datos$Hex) %in% 
      #                      paste(strftime(myResults$dtf, format = "%m/%d/%Y %H:%M:%OS6"), myResults$Hex),]
      setkey(dat,dtf)
      setkey(myResults,dtf)
      rejecteds <- dat[!myResults]
      j<<- j+1
      if (myResults[,.N]>0) {
        recsn <- myResults[!is.na(RecSN)][1][,RecSN]
      } else {
        recsn <- rejecteds[!is.na(RecSN)][1][,RecSN]
      }
      write.csv(rejecteds, paste0("./rejected/", j, "_", recsn, "_rejected.csv"), row.names=F)
      write.csv(myResults, paste0("./accepted/", j, "_", recsn, "_accepted.csv"), row.names=F)
  }
  if (is.null(incomingData)) loopFiles()
  else proces(dat=incomingData)
}
###load taglist
tags<- read.csv(TAGFILENAME, header=T, colClasses="character") #list of known Tag IDs
# tags$Tag.ID..hex.<- as.character(tags$Tag.ID..hex.)

if (DoCleanJST) for(i in list.files("./jst")) {
  if (file.info(i)["isdir"]) next 
  cleanJST(i, tags)
}

if (DoCleanSUM) {
  fn<-cleanSUM()
  for(i in list.files("./raw/",pattern="*.SUM", full.names=TRUE)){
    if (as.integer(file.info(i)["isdir"])) next
    fn(i, tags, max(counter))
  }
}

if (DoCleanATS) {
  fn<-cleanATS()
  for(i in list.files("./raw/",pattern="*.XLS*", full.names=TRUE)){
    if (as.integer(file.info(i)["isdir"])) next
    fn(i, tags, max(counter)) 
  }
}

if (DoCleanLotek) {
  fn<-cleanLotek()
  for(i in list.files("./raw/",pattern="*.TXT", full.names=TRUE)){
    if (as.integer(file.info(i)["isdir"])) next
    fn(i, tags, max(counter))
  }
}

#loop ran, now feed files back into R and see if they look right
# SN6003 <- dget("./cleaned/15-6003_cleaned.txt")
# SN6034 <- dget("./cleaned/15-6034_cleaned.txt")
# SN6034JST <- dget("./cleaned/2015-6034_cleaned.txt") #looks good to me

###Filtering Loop
rm(tags)
if (DoFilterFromSavedCleanedData) {
  filterData()
}

###############################################################
#both filtered files end up with same data, too
# sum <- read.csv("./accepted/15-6034_accepted.csv", header = T)
# jst <- read.csv("./accepted/2015-6034_accepted.csv", header = T)
