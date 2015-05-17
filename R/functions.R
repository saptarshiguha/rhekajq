

#' Will return environments that refer to heka messages
#' @param conn can be a filename or a raw array
#' @param streaming defaults to FALSE and can be used as an iterator
#' @return depends on the streaming argument
#' @details when given a raw array (e.g. you read the heka file into
#' memory) then this function returns an environment and attributes
#' \code{nread} and \code{mbytes} which are the number of messages
#' read and total messages bytes read. When the \code{conn} is
#' filename and \code{streaming} is \code{FALSE}, it will return the
#' same environment with an additional attribute \code{growth} which
#' is the number of times the memory buffer was resized (not important
#' ). When \code{streaming} is \code{TRUE} a function is
#' returned. This function takes a single argument \code{n} which is
#' the number of messages to return(default is 1). You call this
#' function which will return environments like above and null when
#' nothing can be read anymore. Also the attributes to the environment
#' correspond to cumulative bytes and messages read. The additional
#' attribute is \code{n} which corresponds to the number of messages
#' read in the invocation.
#'
#' @examples
#' \dontrun{
#' f <- "./20150511234003.859_ip-172-31-14-40" # a file with heka frames
#' r1 <- processTelemetryV4(conn=f)
#'
#' r1 <- processTelemetryV4(conn=f,streaming=TRUE)
#' while(TRUE){
#'  frames <- r1()
#'  if(is.null(frames)) break
#' }
#'
#' # 2nd method , reading in directly
#' w <- readBin(normalizePath(f), what='raw',n=as.numeric(file.info(normalizePath(f))['size']))
#' r2 <- processTelemetryV4(conn=w)
#' 
#' }
#' 
#' @export
processTelemetryV4 <- function(conn,streaming=FALSE,json=FALSE,...){
    if(is.raw(conn)){
        return(
            .Call("parseFramesFromArray", conn,as.logical(json))
            )
    }else if(is.character(conn)){
        v <- .Call("initializeWithFile",normalizePath(conn),NULL)
        l <- new.env()
        i <- 1
        if(streaming==FALSE){
            while(TRUE){
                r <- .Call("getOneObjectFromFile", v,as.logical(json))
                if(is.null(r)) {
                    l <- structure(l, growth=.Call("getGrowth",v),nread=.Call("getNRead",v),bytes=.Call("getMbytes",v))
                    rm(v)
                    break
                }
                l[[ as.character(i) ]] <- r
                i <- i+1
            }
        }else{
            hasleft <- TRUE
            return( function(n=1){
                       if(!hasleft) return(NULL)
                       l <- new.env();i <- 1
                       while(TRUE){
                           r <- .Call("getOneObjectFromFile", as.logical(v))
                           if(is.null(r)){
                               hasleft <<- FALSE
                               rm(v)
                               break
                           }else{
                               l[[ as.character(i) ]] <- r
                               i <- i+1
                           }
                       }
                       l <- structure(l,n=i,nread=.Call("getNRead",v),bytes=.Call("getMbytes",v))
                       return(l)
                   })
        return(l)
        }
    }
    stop(sprintf("Invalid conn class: %s",class(conn)))
}


#' Initializes the JQ system
#' the handle is stored under the name \code{jqhandle} in options
#' @return the handle itself
#' @export
jqinit <- function(){
    v <- .Call("rjqinit",NULL)
    options(jqhandle = v)
    v
}

#' Stores a query in the jq system, so that it doesn't have to be compiled repeatedly
#' @param f is the query string
#' @param jq is the handle
#' @return an object you pass to \code{jqparse}
#' @details see \href{http://stedolan.github.io/jq/tutorial/} for details of the query
#' @seealso \code{\link{jqparse}}
#' @export
jqquery <- function(f,jq=options("jqhandle")[[1]]){
    if(is.null(jq)) stop("jq not initialized? call jqinit")
    v <- .Call("rjqprogargs",jq, as.character(f))
    class(v) <- "jqquery"
    return(v)
}

#' Will apply a query to a JSON object and return a json
#' @param f is a JSON string or NULL (see details)
#' @param query is either a string (which will be compiled via \code{jqquery}) or a an object returned by \code{jqquery} 
#' @param jq is the handle
#' @return a json string with the results of the query
#' @seealso \code{\link{jqquery}}
#' @details In a RHIPE mapreduce job, you can leave \code{f} null,
#' provided you called \code{rhStringMapValues} before. The function \code{rhStringMapValues} will create a big json array out of \code{map.values} and store it internally for JQ to process. If \code{f} is NULL, JQ will apply the \code{query} to this internal JSON array version of \code{map.values}
#' @export
jqparse <- function(f,query,jq=options("jqhandle")[[1]]){
    if(is.null(jq)) stop("jq not initialized? call jqinit")
    if(is.character(query)) jqquery(query, jq) else if(!is(query,"jqquery")) stop("must be a jqquery class object, called from jqquery")
    .Call("rjqparse",jq, as.character(f))
}

#' pretty print a JSON
#' @param s the json
#' @param print to print ot not?
#' @param color use color text (wont work in Rstudio), terminal only.
#' @param ordered sort the keys?
#' @return the prettified json
#' @export
jqpp <- function(s,print=TRUE,color=TRUE,ordered=TRUE,jq=options("jqhandle")[[1]]){
    if(is.null(jq)) stop("jq not initialized? call jqinit")
    v <- .Call("rjpretty",jq,s,as.logical(color),as.logical(ordered))
    if(!is.character(s)) stop("s must be a string") 
    if(print) cat(v)
    invisible(v)
}

#' makes a json array version of a map.values (of FHR objects)
#' @param f is a list of JSON strings (e.g. map.values when reading from FHR data)
#' @details use this if you'd like to apply a JQ query to the JSON array of map.values FHR JSON objects. If you do, call \code{jqparse} with it's first argument as NULL.
#' @seealso \code{\link{jqquery}},\code{\link{jqparse}}
#' @export
rhStringMapValues <- function(f,jq=options("jqhandle")[[1]]){
    .Call("mapvalueConcat",jq, f)
}
