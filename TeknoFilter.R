###################################################################################################################
#
#                         Tag Filter for Teknologic Receiver Files converted from CBR description
#                           Written by: Gabe Singer, Damien Caillaud     On: 05/16/2017
#                                   Last Updated: 03/13/2018 by Matt Pagel
#                                           "Version" 2.5.1.2
#
#                             Special Note from http://www.twinsun.com/tz/tz-link.htm:
#        Numeric time zone abbreviations typically count hours east of UTC, e.g., +09 for Japan and -10 for Hawaii.
#                      However, the POSIX TZ environment variable uses the opposite convention.
#              For example, one might use TZ="JST-9" and TZ="HST10" for Japan and Hawaii, respectively.
###################################################################################################################

# TODO 20180313: File type cleaner consolidation...should just need a header row specified, everything else should be the same.
# TODO 20180313: re-verify non-preprocessed RT data
# TODO 20180313: check timestamps for SUM JST XLS TXT files
# TODO 20180313: do mode on a per-detection cluster basis, eliminate mode/getmode as outmoded, document max drift between clusters (known as version 2.6)
# TODO 20180313: process unknown tags too. Figure out if their PRI is near-integer seconds, less than 1hr, 1m (tester/beacon)
# TODO 20180313: for RT files, ignore incoming file name...just read them all in to a big array pre-clean.
# See also TODOs in-line
setwd("Z:/LimitedAccess/tek_realtime_sqs/data/preprocess/")
# TAGFILENAME = "taglist/t2018TagInventory.csv" # superseeded by vTAGFILENAME, which has element for default PRI
vTAGFILENAME = cbind(TagFilename=c("taglist/t2018TagInventory.csv","taglist/NOAATaglist20178.csv"),PRI=c(5,10))
DoCleanJST = FALSE
DoCleanRT = TRUE
DoCleanSUM = FALSE
DoCleanATS = FALSE
DoCleanLotek = FALSE
DoSaveIntermediate = TRUE # (DoCleanJST || DoCleanSUM || DoCleanATS || DoCleanLotek)
DoFilterFromSavedCleanedData = TRUE || !DoSaveIntermediate # if you're not saving the intermediate, you should do direct processing
FILTERTHRESH = 2 # PNNL spec: 4. Arnold: 2 for ATS&Tekno, 4 for Lotek
FLOPFACTOR = 0.155 # PNNL spec: 0.006. Arnold: .04*5 = 0.2
MULTIPATHWINDOW = 0.2 # PNNL spec: 0.156. Arnold: 0.2
COUNTERMAX <- 12 # PNNL spec: 12
NON_RT_Dir = "raw2/"
RT_Dir = "Z:/LimitedAccess/tek_realtime_sqs/data/preprocess/"
RT_File_PATTERN = "jsats_2016901[38]_TEK_JSATS_*|jsats_2017900[34]_JSATS_*"

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

mode <- function(x, i) {
  ta <- table(x)
  tam <- max(ta)
  if (all(ta == tam)) mod <- NA
  else if(is.numeric(x)) mod <- as.numeric(names(ta)[ta == tam])
  else mod <- names(ta)[ta == tam]
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

# if version < 3.4.0, install hasName - commented out as it's not currently needed
# bVersionGood = FALSE
# vMaj = as.numeric(R.version["major"])
# vMin = as.numeric(R.version["minor"])
# if ( (vMaj > 3) +  ((vMaj == 3) * (vMin>=4.0)) > 0) bVersionGood=TRUE
# if (!bVersionGood) hasName <- function(x, name) match(names(x), name, nomatch = 0L) > 0L

data.table.parse<-function (file = "", n = NULL, text = NULL, prompt = "?", keep.source = getOption("keep.source"), 
                            srcfile = NULL, encoding = "unknown") { # needed for dput data.tables (rather than data.frames)
  keep.source <- isTRUE(keep.source)
  if (!is.null(text)) {
    if (length(text) == 0L) return(expression())
    if (missing(srcfile)) {
      srcfile <- "<text>"
      if (keep.source)srcfile <- srcfilecopy(srcfile, text)
    }
    file <- stdin()
  }
  else {
    if (is.character(file)) {
      if (file == "") {
        file <- stdin()
        if (missing(srcfile)) srcfile <- "<stdin>"
      }
      else {
        filename <- file
        file <- file(filename, "r")
        if (missing(srcfile)) srcfile <- filename
        if (keep.source) {
          text <- readLines(file, warn = FALSE)
          if (!length(text)) text <- ""
          close(file)
          file <- stdin()
          srcfile <- srcfilecopy(filename, text, file.mtime(filename), isFile = TRUE)
        }
        else {
          text <- readLines(file, warn = FALSE)
          if (!length(text)) text <- ""
          else text <- gsub("(, .internal.selfref = <pointer: 0x[0-9A-Fa-f]+>)","",text,perl=TRUE)
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

list.files.size <- function(path = getwd(), full.names=TRUE, nodotdot = TRUE, ignore.case = TRUE, include.dirs = FALSE, ...) { 
  filelist <- data.table(filename=list.files(path=path, full.names=full.names, no.. = nodotdot, ignore.case = ignore.case, include.dirs = include.dirs, ...))
  filelist[,size:=file.size(filename)][,tot:=sum.file.sizes(filelist)][,perc:=size/tot]
  return(filelist)
}

sum.file.sizes <- function(DT) {
  return(unlist(DT[,.(x=sum(size))],use.names=F)[1])
}

magicFunc <- function(dat, tagHex, countermax, filterthresh){
  setkey(dat,Hex)
  tagdet <- dat[Hex==tagHex]
  setkey(tagdet,dtf)
  # countermax <- max(counter)
  tagdet[,temporary:=as.POSIXct(dtf, format = "%m/%d/%Y %H:%M:%OS", tz="Etc/GMT+8")] # dput file stores datestamp in this basic format
  if (is.na(tagdet[,.(temporary)][1])) tagdet[,dtf:=as.POSIXct(dtf, format = "%Y-%m-%dT%H:%M:%S.%OSZ", tz="UTC")] # fwri file stores as UTC in this format ...was in this doc as %S.%OSZ
  else tagdet[,dtf:=temporary]
  tagdet[,temporary:=NULL]
  tagdet[,winmax:=dtf+((nPRI*1.3*countermax)+1)]
  #  rm(dat5)
  setkey(tagdet,dtf)
  shiftz = 1:(filterthresh-1)
  shiftzcols = paste("l",shiftz,sep="")
  tagdet[,(shiftzcols):=shift(dtf,n=shiftz,fill=NA,type="lead")]
  aclist<-as.data.table(as.POSIXct(unique(unlist(tagdet[difftime(get(shiftzcols[max(shiftz)]),winmax)<=0,c("dtf",(shiftzcols[1:(filterthresh-1)])),with=F],use.names=FALSE)),origin="1970-01-01",tz="Etc/GMT+8"))
  if (aclist[,.N]>0) {
    setkey(aclist,x)
    res <- tagdet[aclist]
    res[,twind:=l1-dtf]
    itr <- as.data.table(merge(x=1:countermax,res))
    itr[,icalc:=round(twind/x,2)]
    ll <- itr[(icalc>=nPRI*0.651 & icalc<=nPRI*1.3)]
    setkey(ll,dtf)
    abbrev = ll[,.(dtf,twind,icalc,x,nPRI)]
    setkey(abbrev,icalc)
    cmod<-abbrev[,dist:=abs(nPRI-icalc)][,.(tot=.N),by=.(icalc,dist)][order(-tot,dist,-icalc)][1]
    ePRI<-cmod$icalc
    if (is.na(ePRI)) ePRI<-5
    flopintervals <-as.data.table(0:countermax)
    flopintervals[,x:=V1][,V1:=NULL][,flop:=(x+1)*FLOPFACTOR][,flopmin:=x*ePRI-flop][,flopmax:=x*ePRI+flop][,flop:=NULL]
    maxflop <- flopintervals[x==12,flopmax]
    windowz <- unique(abbrev[,.(dtf,ewinmax=dtf+maxflop)])
    dett <-res[,.(dup=dtf,dd=dtf)]
    setkey(dett,dup,dd)
    setkey(windowz,dtf,ewinmax)
    fomega <- foverlaps(dett,windowz,maxgap=0,type="within",nomatch=0)[,dd:=NULL][,dif:=(dup-dtf)*1000][,dif2:=(dup-dtf)*1000]
    flopintervals[,newmin:=flopmin*1000][,newmax:=flopmax*1000]
    setkey(flopintervals,newmin,newmax)
    if (fomega[,.N]>0) {
      # if (fomega[,.N]>250) { Sys.sleep(1)}
      setkey(fomega,dif,dif2)
      # if (fomega[,.N]>250) { Sys.sleep(1)}
      windHits<-foverlaps(fomega,flopintervals,maxgap=0,type="within",nomatch=0)[,.(firstHit=dtf,windowEnd=ewinmax,hit=dup,intervals=x)]
      NAs<-windHits[is.na(intervals)]
      noNAs<-windHits[!is.na(intervals)][,c:=.N,keyby="firstHit"] # do I need to check for no lines before c code?
      noOnlyFirst<-noNAs[c>1]
      onlyFirst<-noNAs[c==1,]
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

dataFilter <- function(dat, filterthresh, countermax){
  res <- dat[1==0] # copies structure
  timer <- 0
  setkey(dat,Hex)
  titl<-dat[!is.na(RecSN)][1][,RecSN]
  u<-as.list(unique(dat[,.N,by = Hex][N>=filterthresh])[,1])$Hex
  if (length(u)>0) {
    timerbar<-winProgressBar(title=titl, label="Tag", min=0, max=length(u), initial=0)
    for(i in u){
      setWinProgressBar(timerbar,timer,label=i)
      ans <- magicFunc(dat, tagHex=i, countermax=countermax, filterthresh)
      setkey(ans,nbAcceptedHitsForThisInitialHit,isAccepted)
      ans2 <- ans[(nbAcceptedHitsForThisInitialHit >= filterthresh)&(isAccepted)]
      if (ans2[,.N]>0) {
        keep<-as.data.table(unique(ans2[,hit]))
        setkey(dat,dtf)
        setkey(keep,x)
        res <- rbind(res, dat[keep])
      }
      timer <- timer+1
    }
    close(timerbar)
  }
  return(res)
}

###Cleaning .jst files
cleanJST <- function(i, tags) {
  dat <- read.csv(paste0("jst/", i), header=F)        #read in each file
  names(dat)<- c("Filename", "RecSN", "DT", "FracSec", "Hex", "CRC", "validFlag", "TagAmp", "NBW") #rename columns
  dat<- dat[dat$Hex %in% tags$TagID_Hex., ]
  dat$nPRI<- 5   # set nPRI (Nominal PRI) for the tag 
  # combine the DT and FracSec columns into a single time column and convert to POSIXct
  dat$dtf<- paste0(dat$DT, substring(dat$FracSec,2)) #paste the fractional seconds to the end of the DT in a new column
  dat$dtf<- as.POSIXct(dat$dtf, format = "%m/%d/%Y %H:%M:%OS", tz="Etc/GMT+8") #convert to POSIXct beware this may change value of 0.0000X
  # head(strftime(dat2$dtf, format = "%m/%d/%Y %H:%M:%OS5")) #verify that although the fractional seconds don't print, they are indeed there
  dat2<- as.tbl(dat)
  dat2$Hex <- as.character(dat2$Hex)
  dat2<- arrange(dat2, Hex, dtf)#sort by TagID and then dtf, Frac Second
  # calculate tdiff, then remove multipath
  dat3 <- data.frame(dat2, tdiff=c(NA, difftime(dat2$dtf[-1], dat2$dtf[-nrow(dat2)])))
  dat4 <- data.frame(dat3, crazy=c(NA,dat3$Hex[-nrow(dat3)]==dat3$Hex[-1]))
  dat4$tdiff[dat4$crazy==0] <- NA
  dat5 <- dat4[,-14]
  dat5 <- dat5[dat5$tdiff>MULTIPATHWINDOW | is.na(dat5$tdiff),]
  if (DoSaveIntermediate) fwrite(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(", itercount, ")",  "_cleaned.fwri"))
  if (!DoFilterFromSavedCleanedData) {
    dat5<-as.data.table(dat5)
    filterData(dat5)
  }
}

###Clean realtime csv files
cleanRT_prepreprocess <- function(...) {
  itercount <- 0
  function(i, tags) {
    dathead <- read.csv(i, header=F, nrows=10)
    classes<-sapply(dathead, class)
    classes[names(unlist(list(classes[which(classes %in% c("factor","numeric"))],classes[names(classes) %in% c("time","date","dtf")])))] <- "character"
    dat <- read.csv(i, header=F, colClasses=classes)
    names(dat)
    names(dat)<- c("SQSQueue","SQSMessageID","RecSN","DLOrder","DateTime","microsecs","Hex","TxAmplitude","TxOffset","TxNBW","TxCRC") # DetectionDate to dtf, TagID to Hex, ReceiverID to RecSN
    # names(dat)<- c("Filename", "RecSN", "DT", "FracSec", "Hex", "CRC", "validFlag", "TagAmp", "NBW") #rename columns
    # tags <- read.csv("./taglist/FriantTaglist.csv")
    dat<- dat[dat$Hex %in% tags$TagID_Hex, ]
    dat$nPRI<- 5   # set nPRI (Nominal PRI) for the tag 
    #combine the DT and FracSec columns into a single time column and convert to POSIXct
    dat$dtf <- paste0(dat$DateTime, dat$microsecs)
    dat$dtf<- as.POSIXct(dat$dtf, format = "%Y-%m-%d %H:%M:%OS") #tz="UTC" for realtime data. #convert to POSIXct beware this may change value of 0.0000X
    #head(strftime(dat2$dtf, format = "%m/%d/%Y %H:%M:%OS5")) #verify that although the fractional seconds don't print, they are indeed there
    dat2<- as.tbl(dat)
    dat2$Hex <- as.character(dat2$Hex)
    dat2<- arrange(dat2, Hex, dtf) #sort by TagID and then dtf, Frac Second
    #calculate tdiff, then remove multipath
    dat3 <- data.frame(dat2, tdiff=c(NA, difftime(dat2$dtf[-1], dat2$dtf[-nrow(dat2)])))
    dat4 <- data.frame(dat3, crazy=c(NA,dat3$Hex[-nrow(dat3)]==dat3$Hex[-1]))
    dat4$tdiff[dat4$crazy==0] <- NA
    dat5 <- dat4[,!(names(dat4) %in% c("crazy"))]
    dat5 <- dat5[dat5$tdiff>MULTIPATHWINDOW | is.na(dat5$tdiff),]
    if (DoSaveIntermediate) {
      itercount <<- itercount + 1
      dput(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(",itercount,")", "_cleaned.dput")) 
      # fwrite(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(", itercount, ")",  "_cleaned.fwri"))
    } 
    if (!DoFilterFromSavedCleanedData) {
      dat5<-as.data.table(dat5)
      filterData(dat5)
    }
  }
  # f(...)
}

# if using read.csv rather than fread, you'll want to 
#   1. read a bit from each column first
#   2. set the data type accordingly and 
#   3. re-set to character if it's picky
# dathead <- read.csv(i, header=T, nrows=10)
# classes<-sapply(dathead, class)
# classes[names(unlist(list(classes[which(classes %in% c("factor","numeric"))],classes[names(classes) %in% c("time","date","dtf")])))] <- "character"
# dat <- read.csv(i, header=T, colClasses=classes)
# names(dat) # SQSQueue,SQSMessageID,ReceiverID,DLOrder,DetectionDate,TagID,TxAmplitude,TxOffset,TxNBW
# names(dat) <- c("SQSQueue","SQSMessageID","RecSN","DLOrder","dtf","Hex","TxAmplitude","TxOffset","TxNBW")

cleanRT <- function(...) { # timestamps in stream are UTC, not PST
  itercount <- 0
  function(i, tags) {
    dat <- fread(i, header=T) # fread (unlike read.csv) should read dates as character automatically
    setnames(dat,old=c("ReceiverID","DetectionDate","TagID"),new=c("RecSN","dtf","Hex")) 
    setDT(dat, key=c("Hex")) # convert to data.table # key=c("RecSN","Hex","dtf"))
    dat[,dtf:=as.POSIXct(dtf, format = "%Y-%m-%d %H:%M:%OS", tz="GMT")] # comes in as UTC/GMT
    setkey(tags,TagID_Hex)
    dat2<-dat[tags,nomatch=0] # bring in the nominal PRI (nPRI)
    # combine the DT and FracSec columns into a single time column and convert to POSIXct
    setkey(dat2, RecSN, Hex, dtf)
    dat2[,tlag:=shift(.SD,n=1L,fill=NA,type="lag"), by=.(Hex,RecSN),.SDcols="dtf"]
    dat3<-na.omit(dat2,cols=c("tlag")) # TODO 20180313 need to verify this doesn't drop the first for a tag
    dat3[,c("SQSQueue","SQSMessageID","DLOrder","TxAmplitude","TxOffset","TxNBW"):=NULL]
    setkey(dat3,RecSN,Hex,dtf)
    # calculate tdiff, then remove multipath
    dat4 <- dat3[,tdiff:=difftime(dtf,tlag)][tdiff>MULTIPATHWINDOW | tdiff==0 | is.na(tdiff)]
    # dat4[dtf==tlag,tlag:=NA] # if we want to set the first lag dif to NA rather than 0 for the first detection of a tag
    rm(dat3)
    setkey(dat4,RecSN,Hex,dtf)
    setkey(dat ,RecSN,Hex,dtf)
    keepcols = unlist(list(colnames(dat),"nPRI")) # use initial datafile columns plus the nPRI column from the taglist file
    dat5 <- unique(dat[dat4,keepcols,with=FALSE]) # too slow?  try dat[dat4] then dat5[,(colnames(dat5)-keepcols):=NULL,with=FALSE]
    rm(dat4)
    rm(dat2)
    # I think "crazy" in Damien's original was just a check to see if matches previous tag, if not discard result of subtraction
    # Shouldn't be needed for data.table.
    SNs<-unique(dat5[,RecSN])
    for(sn in SNs) { # don't trust the initial file to have only a single receiver in it
      itercount <<- itercount + 1
      # fwri format stores timestamps as UTC-based (yyyy-mm-ddTHH:MM:SS.microsZ)
      if (DoSaveIntermediate) fwrite(dat5[RecSN==sn], file = paste0("./cleaned/RT", itercount, "_", sn,  "_cleaned.fwri"))
      if (!DoFilterFromSavedCleanedData) {
        # dat5<-as.data.table(dat5)
        filterData(dat5)
      }
    }
    rm(dat5)
  }
  # f(...) # leave out for enclosure.  I believe the unnamed function above is returned by cleanRT, keeping a static value for itercount
}

###Cleaning .SUM files
cleanSUM <- function() { #set up enclosure
  itercount <- 0
  function(i, tags) {
    # read in each file
    dat <- fread(i, skip = 8, header=T)
    # rename columns to match db
    names(dat)<- c("Detection", "RecSN", "dtf", "Hex", "Tilt", "Volt", "Temp", "Pres", "Amp",
                   "Freq", "Thresh", "nbw", "snr", "Valid")
    # drop the barker code and the CRC from tagid field & convert to POSIXct note: fractional seconds will no longer print, but they are there
    dat[, Hex := substr(Hex, 4, 7)]
    dat[, dtf := as.POSIXct(dtf, format = "%m/%d/%Y %H:%M:%OS", tz="Etc/GMT+8")]
    dat[, nPRI := 5]
    sn<-unlist(dat[!is.na(RecSN),.(x=RecSN)],use.names=F)
    if (length(sn)>0) {sn<-sn[1]}
    else {
      fg<-strsplit(i,'/',perl=TRUE)
      sn<-fg[length(fg)]
    }
    # tbl w/ bad detect lines removed, filtered by known taglist
    dat <- dat[trimws(Detection) != "-" & Hex %in% as.character(unlist(tags[2]))]
    setkey(dat, Hex, dtf)
    noMP <- dat[, tdiff := as.numeric(difftime(dtf,shift(dtf, n=1, fill=NA)),units="secs"), by=Hex]
    noMP <- noMP[tdiff > MULTIPATHWINDOW | is.na(tdiff)][, tdiff := as.numeric(difftime(dtf,shift(dtf, n=1, fill=NA)),units="secs"), by=Hex]
    dat5 <- noMP
    itercount<<- itercount+1 #cleanSUM's itercount
    if (DoSaveIntermediate) { 
      dput(dat5, file = paste0("./cleaned/", sn,"(",itercount,")", "_cleaned.dput")) 
      # fwrite(dat5, file = paste0("./cleaned/", dat5[,RecSN][1],"(",itercount,")", "_cleaned.fwri"))
    }
    if (!DoFilterFromSavedCleanedData) {
      # already data.table
      filterData(dat5)
    }
  }
  # f
}

###Cleaning loop for ATS receiver files
cleanATS <- function() {
  itercount <- 0
  function(i, tags) {
    dat <- read_excel(i)                    #read in each file
    SN <- as.numeric(gsub("Serial Number: ", "", (dat[2, 1]))) #extract serial number of the receiver
    if(is.na(SN) == TRUE) {
      SN <- as.numeric(gsub("Serial Number: ", "", colnames(dat[0,1])))
    }
    print(SN)
    find.na <- as.numeric(which(is.na(dat[ , 1])))             #find the NA's in Column 1
    # the value 100 below can be set to anything, it just needs to be less that the total number of detections in the file
    start <- max(find.na[find.na <= 100])                      
    dat <- dat[(start + 7):nrow(dat), ]                        #ditch garbage at beginning of the file
    
    headers <- c("Filename", "SiteName", "SiteName2", "SiteName3", "dtf", "Hex", "Tilt", "VBatt", "Temp", "Pres", "SigStr",
                 "BitPeriod", "Thresh", "Detection")                        #make vector of new headers
    names(dat) <- headers 
    # rename with the right headers
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
    # to be set to something different for tags with a PRI other than 5)
    dat$dtf<- as.POSIXct(dat$dtf, format = "%m/%d/%Y %H:%M:%OS", 
                         tz="Etc/GMT+8")                       #convert to POSIXct note: fractional seconds will no longer print, but 
    # they are there. Run the next line to verify that you haven't lost your 
    # frac seconds
    # (strftime(dat$dtf, format = "%m/%d/%Y %H:%M:%OS6"))
    dat2 <- dat
    dat2<- arrange(dat2, Hex, dtf)                             #sort by TagID and then dtf, Frac Second
    dat3 <- data.frame(dat2, tdiff=c(NA, difftime(dat2$dtf[-1], dat2$dtf[-nrow(dat2)], units = "secs" )))   #calculate tdiff, then remove multipath
    dat4 <- data.frame(dat3, crazy=c(NA,dat3$Hex[-nrow(dat3)]==dat3$Hex[-1]))
    dat4$tdiff[dat4$crazy==0] <- NA
    dat5 <- dat4[,-14]
    dat5 <- dat5[dat5$tdiff>0.2 | is.na(dat5$tdiff),]
    itercount <<- itercount+1
    if (DoSaveIntermediate) fwrite(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(", itercount, ")",  "_cleaned.fwri"))
    if (!DoFilterFromSavedCleanedData) {
      dat5<-as.data.table(dat5)
      filterData(dat5)
    }
  }
  # f
}

###Cleaning loop for Lotek Files 
cleanLotek <-function() {
  itercount <- 0
  function(i, tags) {
    dat <- read.table(i, header = F, sep = ",")   #read in each file
    SN <- as.numeric(regmatches(i,regexpr("[0-9].*[0-9]", i)))      #extract serial number of the receiver
    headers <- c("datetime", "FracSec", "Dec", "Hex", "SigStr")     #make vector of new headers
    names(dat) <- headers                                           #rename with the right headers
    dat$RecSN <- rep(SN, nrow(dat))                                 #add SN column 
    dat$datetime <- as.POSIXct(round(dat$datetime*60*60*24), 
                               origin = "1899-12-30", tz = "GMT")
    dat$Hex <- as.character(dat$Hex)
    dat$Hex <- substr(dat$Hex, 2, nchar(dat$Hex))
    
    dat<- dat[dat$Hex %in% tags$Tag.ID..hex., ]   # filter receiver file by known taglist (already done in Lotek
    # software should have the same number of dets)
    dat$nPRI<- 5                                               # set nPRI (Nominal PRI) for the tag (this will have 
    dat <- as.tbl(dat)                                         # change object format to tbl 
    dat$dtf <- paste0(dat$datetime, substring(dat$FracSec,2))  # paste the fractional seconds to the end of the DT in a new column
    dat$dtf<- ymd_hms(dat$dtf) # convert to POSIXct beware this may change value of 0.0000X
    # to be set to something different for tags with a PRI other than 5)
    # convert to POSIXct note: fractional seconds will no longer print, but 
    # they are there. Run the next line to verify that you haven't lost your 
    # frac seconds
    # (strftime(dat$dtf, format = "%m/%d/%Y %H:%M:%OS6"))
    dat2 <- dat
    dat2<- arrange(dat2, Hex, dtf)                             #sort by TagID and then dtf, Frac Second
    dat3 <- data.frame(dat2, tdiff=c(NA, difftime(dat2$dtf[-1], dat2$dtf[-nrow(dat2)], units = "secs" )))   #calculate tdiff, then remove multipath
    dat4 <- data.frame(dat3, crazy=c(NA,dat3$Hex[-nrow(dat3)]==dat3$Hex[-1]))
    dat4$tdiff[dat4$crazy==0] <- NA
    dat5 <- dat4[,-11]
    dat5 <- dat5[dat5$tdiff>0.2 | is.na(dat5$tdiff),]
    itercount <<- itercount+1
    if (DoSaveIntermediate) fwrite(dat5, file = paste0("./cleaned/", dat5$RecSN[1],"(", itercount, ")",  "_cleaned.fwri"))
    if (!DoFilterFromSavedCleanedData) {
      dat5<-as.data.table(dat5)
      filterData(dat5)
    }
  }
  # f
}

##filter Loop
filterData <- function(incomingData=NULL) {
  j <- 0
  loopFiles <- function() {
    for(i in list.files("./cleaned",full.names=TRUE)){
      if (as.integer(file.info(i)["isdir"])) next
      # if it's a dput file, read it back in, but make sure there's no funky memory addresses that were saved by data.table
      if (endsWith(i,'.dput') || endsWith(i,'.txt')) datos <-as.data.table(dtget(i))
      if (endsWith(i,'.fwri')) { # if it was written to disk with fwrite, use the faster fread, but make sure to set the datetime stamps
        datos <-as.data.table(fread(i))
        datos[,dtf:=as.POSIXct(dtf, format = "%Y-%m-%dT%H:%M:%S.%OSZ", tz="UTC")] # %OS6Z doesn't seem to work correctly
      }
      proces(dat=datos)
    }
  }
  proces <- function(dat) {
    myResults <- dataFilter(dat=dat, filterthresh=FILTERTHRESH, countermax=COUNTERMAX)
    setkey(dat,dtf) # TODO 20180313: we should probably put TagID_Hex and RecSN in the key also
    setkey(myResults,dtf)
    rejecteds <- dat[!myResults]
    j <<- j+1
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

# TODO 20180313: directly in data.table
readTags <- function(vTagFileNames=vTAGFILENAME, priColName=c('PRI_nominal','nPRI','PRI_estimate','ePRI','Period','PRI'),
                     TagColName=c('TagID_Hex','TagIDHex','TagID','TagCode_Hex','TagCode','CodeID','CodeHex','CodeID_Hex','CodeIDHex','Tag','Code','TagSN')) {
  ret <- data.frame(TagID_Hex=character(),nPRI=numeric())
  for (i in 1:nrow(vTagFileNames)) {
    fn = vTagFileNames[i,"TagFilename"]
    pv = vTagFileNames[i, "PRI"]
    tags<- read.csv(fn, header=T, stringsAsFactors=FALSE) #list of known Tag IDs #colClasses="character", 
    heads = names(tags)
    tcn = TagColName[which(TagColName %in% heads)[1]] # prioritize the first in priority list
    pcn = priColName[which(priColName %in% heads)[1]] # prioritize the first in priority list
    thiset = setnames(tags[c(tcn,pcn)],c(tcn,pcn),c("TagID_Hex","nPRI"))
    transform(thiset,nPRI=as.numeric(nPRI))
    thiset[is.na(thiset)] <- as.numeric(pv)
    ret <- rbindlist(list(ret, thiset),use.names=TRUE)
  }
  return(as.data.table(ret))
}

cleanWrapper <- function(functionCall, tags, precleanDir, filePattern, wpbTitle=NULL) {
  lfs<-list.files.size(precleanDir, pattern=filePattern, full.names=TRUE, include.dirs=FALSE)
  # lf<-list.files(precleanDir, pattern=filePattern, full.names=TRUE, include.dirs = FALSE)
  tf<-length(lfs[,filename]) # total files
  if (tf==0) return(F)
  tfs<-lfs[,tot][1] # total file sizes
  if (tfs==0) return(F)
  pb<-winProgressBar(title=wpbTitle, label="Setting up file cleaning...", min=0, max=tfs, initial=0)
  rt<-0 # running size total
  for(i in 1:tf){
    ts <- lfs[i,size]
    if (ts==0) next
    fn <- lfs[i,filename]
    rt <- rt+ts
    if (as.integer(file.info(fn)["isdir"])) next
    lab<-paste0(basename(fn),"\n",i,"/",tf," (",((10000*rt)%/%tfs)/100,"% by size)\n")
    setWinProgressBar(pb,rt,label=lab)
    functionCall(fn, tags)
  }
  close(pb)
  return(T)
}

###load taglist
# tags<- read.csv(TAGFILENAME, header=T, colClasses="character") # single tag list file. Superseeded.
tags<-readTags(vTAGFILENAME)

# Clean all technologies (filter by known TagID, remove multipath)
if (DoCleanJST) {
  fn<-cleanJST() # do the call this way for enclosure, otherwise itercount resets to 0 each time
  cleanWrapper(fn, tags, precleanDir = paste0(NON_RT_Dir,"jst/"), filePattern = "*.*", wpbTitle = "Cleaning Tekno JST files")
}

if (DoCleanRT) {
  fn<-cleanRT()
  cleanWrapper(fn, tags, precleanDir = RT_Dir, filePattern = RT_File_PATTERN, wpbTitle = "Cleaning Preprocessed Realtime Data")
}

if (DoCleanSUM) {
  fn<-cleanSUM()
  cleanWrapper(fn, tags, precleanDir = NON_RT_Dir, filePattern = "*.SUM", wpbTitle = "Cleaning SUM Files")
}

if (DoCleanATS) {
  fn<-cleanATS()
  cleanWrapper(fn, tags, precleanDir = NON_RT_Dir, filePattern = "*.XLS", wpbTitle = "Cleaning ATS XLS files")
}

if (DoCleanLotek) {
  fn<-cleanLotek()
  cleanWrapper(fn, tags, precleanDir = NON_RT_Dir, filePattern = "*.TXT", wpbTitle = "Cleaning LoTek TXT files")
}

###Filtering Loop
rm(tags)
if (DoFilterFromSavedCleanedData) {
  filterData()
}
