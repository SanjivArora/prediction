
moving.average <- function(data,q,pad=TRUE){
  n <- length(data)
  if(q<=0||q>=n) stop("invalid q")
  
  if(q%%2!=0){
    if(!pad){
      return(as.vector(filter(data,rep(1/q,q))))
    } else {
      datapad <- c(rep(data[1],(q-1)/2),data,rep(data[n],(q-1)/2))
      return(as.vector(na.omit(filter(datapad,rep(1/q,q)))))
    }
  } else {
    if(!pad){
      return(as.vector(filter(data,c(0.5/q,rep(1/q,q-1),0.5/q))))
    } else {
      datapad <- c(rep(data[1],q/2),data,rep(data[n],q/2))
      return(as.vector(na.omit(filter(datapad,c(0.5/q,rep(1/q,q-1),0.5/q)))))
    }
  }
}

classic.decomp <- function(data,d,type,order.p=NULL,times=NULL){
  N <- length(data)
  if(is.null(times)) times <- 1:N  
  if((type!="additive")&&(type!="multiplicative")) stop("type argument must be either 'additive' or 'multiplicative'")
  
  m.tilde <- moving.average(data,d)
  
  if(d%%2==0) q <- d
  else q <- d-1
  
  w <- c()
  periods <- seq(0,N-1,d)
  indlist <- list()
  for(k in 1:d){
    p.temp <- periods+k
    indices <- p.temp[p.temp<=(N-q/2)]
    indices <- indices[indices>(q/2)]
    indlist[[k]] <- indices
    if(type=="additive") w[k] <- mean((data-m.tilde)[indices])
    else w[k] <- mean((data/m.tilde)[indices])
  }
  
  if(type=="additive"){
    s.hat <- rep(w-mean(w),ceiling(N/d))[1:N]
    delta.hat <- data-s.hat
  } else {
    s.hat <- rep(w/mean(w),ceiling(N/d))[1:N]
    delta.hat <- data/s.hat
  } 
  
  if(!is.null(order.p)){
    fmstring <- "delta.hat~1"
    for(i in 1:order.p) fmstring <- paste(fmstring,"+I(times^",i,")",sep="")
    fm <- as.formula(fmstring)
    m.hat <- lm(fm)
    if(type=="additive") Y.hat <- data-m.hat$fitted.values-s.hat
    else Y.hat <- (data/s.hat)/m.hat$fitted.values
  } else {
    m.hat <- moving.average(delta.hat,d)
    if(type=="additive") Y.hat <- data-m.hat-s.hat
    else Y.hat <- (data/s.hat)/m.hat
  }
  
  result <- list(m.tilde=m.tilde,s.hat=s.hat,m.hat=m.hat,delta.hat=delta.hat,Y.hat=Y.hat,times=times,data=data,d=d,type=type)
  class(result) <- "CD"
  return(result)
}

plot.CD <- function(object){
  par(mfrow=c(5,1))
  plot(object$times,object$data,type="o",xlab="time",ylab="x",main="Raw data")
  plot(object$times,object$data,type="n",xlab="time",ylab="m.tilde",main="Moving average trend")
  lines(object$times,object$m.tilde,lwd=2,col=2)
  plot(object$times,object$s.hat,type="o",xlab="time",ylab="s.hat",main="Seasonal component")
  if(object$type=="additive") abline(h=0,lty=2)
  else abline(h=1,lty=2)
  plot(object$times,object$delta.hat,type="o",xlab="time",ylab="delta.hat and m.hat",main="Deseasonalised data and refined trend")
  if(class(object$m.hat)=="lm") lines(object$times,object$m.hat$fitted.values,col=2,lwd=2)
  else lines(object$times,object$m.hat,col=2,lwd=2)
  plot(object$times,object$Y.hat,type="o",xlab="time",ylab="Y.hat",main="Recovered residual process")
  par(mfrow=c(1,1))
}

matmin <- function(mat){
  rmin <- apply(mat,1,min)
  cmin <- apply(mat,2,min)
  return(c(which(rmin==min(rmin)),which(cmin==min(cmin))))
}

ar.orderselect <- function(X,include.mean,order.max=20){
  if(order.max<0) stop("order.max must be at least zero")
  avec <- c()
  if(order.max>0) pb <- txtProgressBar(min=0,max=order.max+1,style=3)
  for(i in 0:order.max){
    if(order.max>0) setTxtProgressBar(pb,i)
    
    af <- try(ar.fit(X,i,include.mean=include.mean)$aic,silent=T)
    if(class(af)=="try-error"){
      avec[i+1] <- Inf  
    } else {
      avec[i+1] <- af
    }
  }
  if(order.max>0) close(pb)
  result <- c(which(avec==min(avec))-1,min(avec))
  names(result) <- c("p","aic")
  if(result[1]==order.max) warning("selected order is equal to limit: suggest increasing order.max")
  return(result)
}

ma.orderselect <- function(X,include.mean,order.max=20){
  if(order.max<0) stop("order.max must be at least zero")
  avec <- c()
  if(order.max>0) pb <- txtProgressBar(min=0,max=order.max+1,style=3)
  for(i in 0:order.max){
    if(order.max>0) setTxtProgressBar(pb,i)
    
    mf <- try(ma.fit(X,i,include.mean=include.mean)$aic,silent=T)
    if(class(mf)=="try-error"){
      avec[i+1] <- Inf  
    } else {
      avec[i+1] <- mf
    }
  }
  if(order.max>0) close(pb)
  result <- c(which(avec==min(avec))-1,min(avec))
  names(result) <- c("q","aic")
  if(result[1]==order.max) warning("selected order is equal to limit: suggest increasing order.max")
  return(result)
}

arma.orderselect <- function(X,include.mean,order.max){
  if(order.max<0) stop("order.max must be at least zero")
  counter <- 0
  amat <- matrix(NA,order.max+1,order.max+1)
  if(order.max>0) pb <- txtProgressBar(min=0,max=(order.max+1)^2,style=3)
  for(p in 0:order.max){
    for(q in 0:order.max){
      counter <- counter+1
      af <- try(suppressWarnings(arma.fit(X,p=p,q=q,include.mean=include.mean)$aic),silent=T)
      if(order.max>0) setTxtProgressBar(pb,counter)
      if(class(af)=="try-error"){
        amat[p+1,q+1] <- Inf  
      } else {
        amat[p+1,q+1] <- af  
      }
    }
  }
  if(order.max>0) close(pb)
  result <- c(matmin(amat)-1,min(amat))
  names(result) <- c("p","q","aic")
  if(result[1]==order.max||result[2]==order.max) warning("selected order is equal to limit: suggest increasing order.max")
  return(result)
}

arima.orderselect <- function(X,d,order.max){
	if(d<=0) stop("d must be greater than zero in order to use this function")
	Xdiff <- diff(X,d=d)
	result <- arma.orderselect(Xdiff,include.mean=FALSE,order.max=order.max)
	res <- rep(NA,4)
	res[1] <- result[1]
	res[2] <- d
	res[3] <- result[2]
	res[4] <- result[3]
	names(res) <- c("p","(user) d","q","aic")
	return(res)	
}

ma.fit <- function(X,q,...){
  result <- suppressWarnings(arima(ts(X,1,length(X)),order=c(0,0,q),...))
  return(result)
}

ar.fit <- function(X,p,...){
  result <- suppressWarnings(arima(ts(X,1,length(X)),order=c(p,0,0),...))
  return(result)
}

arma.fit <- function(X,p,q,...){
  result <- suppressWarnings(arima(ts(X,1,length(X)),order=c(p,0,q),...))
  return(result)
}

arima.fit <- function(X,p,d,q,...){
  result <- suppressWarnings(arima(ts(X,1,length(X)),order=c(p,d,q),...))
  return(result)
}

sarima.fit <- function(X,p,d,q,P,D,Q,s,...){
  result <- suppressWarnings(arima(ts(X,1,length(X)),order=c(p,d,q),seasonal=list(order=c(P,D,Q),period=s),...))
  return(result)
}

extend.trends <- function(object,n.ahead){
  if(class(object)!="CD") stop("object must be a result of calling classic.decomp")
  if(class(object$m.hat)[1]!="lm") stop("refined trend is not parametric... cannot extrapolate")
  
  N <- length(object$data)
  d <- object$d
  
  ext.times <- N+1:n.ahead
  mhat.pred <- predict(object$m.hat,newdata=data.frame(times=ext.times),se.fit=T)
    
  rem <- N%%d
  shat.super <- rep(object$s.hat[(1+rem):(d+rem)],ceiling(n.ahead/d))
  shat.pred <- shat.super[1:n.ahead]
    
  return(list(m.hat.ext=as.vector(mhat.pred$fit),s.hat.ext=shat.pred,m.hat.se=as.vector(mhat.pred$se.fit),ext.times=ext.times))
}

sarima.acf <- function(data,d,D,s){
  
  if(D<1) datasdiff <- data
  else datasdiff <- diff(data,d=D,lag=s)
  
  if(d<1) datadd <- datasdiff
  else datadd <- diff(datasdiff,d=d,lag=1)
  
  N <- length(datadd)
  ddacf <- acf(datadd,lag.max=N-1,plot=F)
  seasvals <- as.vector(ddacf$acf)[seq(1,N,s)]
  slgm <- floor((N/s)/1)
  
  par(mfrow=c(1,2))
  acf(datadd,lag.max=(s-1),main="ACF for lags < s")
  plot(1,1,type="n",xlim=c(0,slgm),ylim=range(seasvals),xaxt="n",xlab="Lag",ylab="ACF",main="ACF for lags s, 2s, 3s, ...")
  axis(1,at=0:slgm,labels=s*(0:slgm))
  abline(h=0)
  abline(h=c(-2/sqrt(N),2/sqrt(N)),lty=2,col=4)
  segments(0:slgm,0,0:slgm,seasvals[1:(slgm+1)])
  par(mfrow=c(1,1))
}


garch.fit <- function(V,p,q){
  if((p+q)==0) stop("invalid order (p,q)")
  return(list(fit=suppressWarnings(garch(V,c(q,p),grad="numerical",trace=F)),p=p,q=q))
}

garch.sim <- function(gfit,nvals,nreps=100){
  if(nreps<1||nvals<1) stop("nreps and nvals must be > 0")
  
  ndrop <- max(gfit$p,gfit$q)
  if(nvals<=ndrop) stop("nvals must be > max(p,q)")
  
  eval.str <- "gcoef[1]"
  if(gfit$p>0){
    for(i in 1:gfit$p) eval.str <- paste(eval.str,"+gcoef[",1+i,"]*A[j-",i,"]^2",sep="")
  }
  if(gfit$q>0){
    for(i in 1:gfit$q) eval.str <- paste(eval.str,"+gcoef[",1+gfit$p+i,"]*h[j-",i,"]",sep="")
  }
  
  if(class(gfit$fit)=="garch") gcoef <- coef(gfit$fit)
  else gcoef <- gfit$fit
  
  #pb <- txtProgressBar(min=0,max=nreps,style=3)
  nvals <- nvals+ndrop
  matrep <- matrix(NA,nvals,nreps)
  for(i in 1:nreps){
    #setTxtProgressBar(pb,i)
    w <- rnorm(nvals)
    A <- h <- rep(0,nvals)
    for(j in (ndrop+1):nvals){
      h[j] <- eval(parse(text=eval.str))
      A[j] <- w[j]*sqrt(h[j])
    }
    matrep[,i] <- A
  }
  #close(pb)
  matrep <- matrep[-(1:ndrop),]
  return(matrep)
}

options(repos='http://cran.ms.unimelb.edu.au/')
necessary <- 'tseries'
installed <- necessary %in% installed.packages()[,'Package']
if(length(necessary[!installed])>=1) install.packages(necessary[!installed])
library(tseries)


