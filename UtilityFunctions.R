### Set up library directories; Needed if run from the command line via RScript.
goodLibraryPath <- function() {
  dox <- normalizePath(readRegistry("Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\Shell Folders","HCU",maxdepth=1)$Personal,winslash="/",mustWork=F) 
  # Windows doesn't approve of using this registry key folder, but they're not exactly going to take it away any time soon, as many programs depend on values here.
  # The alternate would be to hook into some .NET function calls, which wouldn't be simple to code and would also require .NET framework installed on target systems.
  # Another alternative would just assume that this is located at c:\users\<currentUserName>\Documents\, which is not true for older Windows Systems nor customized systems like mine;)

  if (!is.null(dox)) { dox <- paste0(dox,"/R/win-library/x.y") }
  options(repos=structure(c(CRAN="https://cran.cnr.berkeley.edu/")))

  lps <- c(dox, paste0(path.expand("~"),"/R/win-library/x.y"), tempdir(), getwd(), .libPaths())
  lps <- lapply(lps, normalizePath, winslash="/",mustWork=F)
  # message("Initially LIBPATH=",paste(lps,collapse=";"))
  lps2 <- lapply(lps, grep, pattern="/x.y", value=TRUE) # grab the paths with generic R version filler only
  rv <- paste(unlist(strsplit(toString(getRversion()),".",fixed=TRUE)[[1]])[1:2],sep=".",collapse=".")
  message("You are running R version: ",rv)
  # substr(as.character(getRversion()),1,3) will not work if subversion exceeds 9
  lps2 <- lapply(lps2, function(x) { sub("/x.y",paste0("/",rv),x,fixed=TRUE) }) # change x.y to e.g. 3.5
  lps <- Filter(file.exists,unlist(c(lps2,lps)))
  # lps <- Filter(file.exists,unlist(lps))
  .libPaths(new=lps)
  message("Library path set. LIBPATH=",paste(.libPaths(),collapse=";"))
}

install.load <- function(package.name) {
  if (!require(package.name, character.only=T)) {
    warn("Package ", package.name, " not found in ",
         paste0("LIBPATH=",paste(.libPaths(),collapse=";")),
         " we will attempt download and installation from cran-R.")
    install.packages(package.name)
    library(package.name, character.only=T)
  }
}

make_function_nomask <- function(args=alist(), body=quote({}), name=NULL, env = parent.frame()) {
  # Author Matt Pagel (github.com/MPagel)
  # First (online) function version 2019-01-04
  # Some core code lifted from https://stackoverflow.com/questions/12982528/how-to-create-an-r-function-programmatically
  
  # TODO: make documentation and example styling consistent with standards
  # Usage examples
  # -------------
  # # *Quoted name parameters*
  # rm(functionA) # remove functionA if it exists
  # make_function_nomask(body=quote(print("hello")),name="functionA")
  # #  creates a function functionA with no arguments and print("hello") for the body
  # make_function_nomask(body=quote(print("world")),name="functionA")
  # #  does not alter above-created function

  # # *Unquoted name parameter is also possible* (mixing quotes between calls is also okay)
  # rm(functionA) # remove functionA from previous example
  # make_function_nomask(body=quote(print("hello")),name=functionA)
  # #  creates a function functionA with no arguments and print("hello") for the body
  # make_function_nomask(body=quote(print("world")),name=functionA)
  # #  does not alter above-created function
  
  # # *Demonstration of behavior when also assigning result of function call*
  # rm(functionA) # remove functionA from previous example
  # functionA <- make_function_nomask(args=alist(x=1),body=quote(print(x)),name=functionA)
  # #  creates a function functionA with x as an argument with default value of 1 and print(x) for the body
  # functionA <- make_function_nomask(body=quote(print("world")),name=functionA)
  # #  does not alter above-created function
  # functionB <- make_function_nomask(body=quote(print("world")),name=functionB)
  # #  creates functionB with no arguments and print("world") for the body
  # functionC <- make_function_nomask(body=quote(print("!!!!!")),name=functionD)
  # #  creates functions functionD and functionC both with zero arguments and print("!!!!!") for the bodies
  # functionE <- make_function_nomask(body=quote(print("this behavior is intended")),name=functionD)
  # #  copies functionD as functionE: `print("!!!!!")`
  
  # # *Demonstration of all parameters, plus multi-line bodies*
  # functionZ <- make_function_nomask(
  #                 args=alist(lue=42, mice="pandimensional beings"),
  #                 body=quote({pv<-paste("the mice, which are",mice);pv<-paste(pv,", say the answer is",lue); # comment here
  #                             print(pv)
  #                           }),
  #                 name=functionZ,
  #                 env=parent.frame(n=3)
  #              ) # end make_function_nomask call
  # # creates a function functionZ, 2(??) levels above the calling frame and also assigns that same function to the same name in the calling frame
  # # WARNING: from https://www.rdocumentation.org/packages/base/versions/3.5.2/topics/sys.parent
  # #  Beware of the effect of lazy evaluation: these two functions look at the call stack at the time they are evaluated,
  # #  not at the time they are called. Passing calls to them as function arguments is unlikely to be a good idea.
  
  # Possible mistakes/side effects
  # ------------------------------
  # rm(functionA,functionB,functionC,functionD,functionE,functionZ) # remove functions A-Z from previous example
  # make_function_nomask(body=quote(print("hello")),name="functionA")
  # #  Nothing wrong here: creates a function functionA with no arguments and print("hello") for the body
  # functionA <- make_function_nomask(body=quote(print("world")),name=functionB)
  # #  Creates functionB with no arguments and print("world") for the body AND also assigns this same function content to functionA, overwriting existing content
    
  rv<-NULL
  # check if the right side of name=x is defined. Use case would be if a name is not quoted and does not exist
  tryCatch(is.null(name),error = function(e) { 
    # BUG: The following line depends on specific R error text. 
    #      May not be future-proof nor applicable to non-English locales.
    #      Should work for en-US for R versions 3.5.0-3.5.2
      characterize <- sub("(^object ')([^']*)(' not found$)","\\2",e$message) 
      if (length(characterize)>0 && nchar(characterize[1])<nchar(e$message)) {
        characterize<-characterize[1]
      } else { # No match
        message("error was:",e$message,"\t characterization:",characterize)
        characterize<-NULL
      }
      assign("name",characterize,inherits=TRUE) # assign to scope of make_function_nomask
  })
  # TODO: Many of the following conditionals can be rearranged to improve efficiency
  if (is.null(name)) { 
    args <- as.pairlist(args)
    rv<-eval(call("function", args, body)) # WAS: rv<-eval(call("function", args, body), env), but that should be in assign instead
  } else if (is.character(name)) {
    if (exists(name, mode="function")) rv<-get0(name) else { # TODO: should probably apply to more than just functions
      args <- as.pairlist(args)
      rv<-eval(call("function", args, body)) # WAS: ..., env), but that should be in assign instead
    }
  } else if (typeof(name)=="closure") { # existing non-quoted x for name=x parameter
    rv<-name
    if (is.null(rv)) {
      args <- as.pairlist(args)
      rv<-eval(call("function", args, body)) # WAS: ..., env), but that should be in assign instead
    }
  }
  if (is.character(name) && length(name)>0) {
    assign(name, rv, env) # WAS: pos=1), but that would always assign to the parent scope
  }
  return(invisible(rv))
}
