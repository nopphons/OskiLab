# the setwd function path should be set to the directory this file is in
# install.packages("RTextTools","tm","topicmodels","Rmpfr")
library(RTextTools)
library(tm)
library(topicmodels)
library(Rmpfr)
data <- read.csv("drug.csv", header = TRUE, stringsAsFactors=FALSE)
sampleAmount <- 100
unsampledRows <- vector()
sampledRows <- sample(nrow(data), sampleAmount)
for (i in 1:nrow(data)) {if (!(i %in% sampledRows)) {unsampledRows <- c(unsampledRows, i)}}
addStopwords <- c("will", "said", "norml", "pierre", "allen", "use", "according", "please", "reported", "states", "may", "contact", "information", "full", "read", "paul", "armentano", "httpwwworgindexcfmgroupid", "director",
 "available", "text", "paulorg", "appear", "also", "online", "keith", "stroup", "executive", "foundation",
  "either", "call", "say")
normlStopwords <- c(stopwords("english"), addStopwords)
normlTitles <- Corpus(VectorSource(data$Title[sampledRows], encoding = "UTF-8"), readerControl = list(language = "english"))
normlTitles <- tm_map(normlTitles, tolower)
normlTitles <- tm_map(normlTitles, removeWords, normlStopwords)
normlMain <- Corpus(VectorSource(data$Text[sampledRows], encoding = "UTF-8"), readerControl = list(language = "english"))
normlMain <- tm_map(normlMain, tolower)
normlMain <- tm_map(normlMain, removeWords, normlStopwords)
matrix <- create_matrix(cbind(as.vector(normlTitles),as.vector(normlMain)), language="english", removeNumbers=TRUE, stemWords=TRUE, weighting=weightTf)
#----------do not run this section, takes a long time---------- 
#calculates optimal number of topics for model
#See: http://epub.wu.ac.at/3558/1/main.pdf Section 4.3.3
#Result is 22 topics
harmonicMean <- function(loglikely) {
	
	as.double(median(loglikely) - log(mean(exp(-mpfr(loglikely, prec = 2000L) + median(loglikely)))))	
}
topicMax <- 100
topicRange <- seq(2,topicMax,1)
topicNumbers <- lapply(topicRange, function(k) LDA(matrix, k = k, method = "Gibbs", 
						control = list(iter = 1000, burnin = 1000, keep = 50)))
logLikelyhoods <- lapply(topicNumbers,function(topic) topic@logLiks[c(1:20)])
harmonicmeans <- sapply(logLikelyhoods, function(logvals) harmonicMean(logvals))
plot(topicRange, harmonicmeans, type = "l")
k <- topicRange[which.max(harmonicmeans)]
#--------------------------------------------------------------
k <- 22
lda <- LDA(matrix, k, method = "Gibbs", control = list(iter = 1000, burnin = 1000))
Terms <- terms(lda, 20)
write.table(Terms, file = "topicTerms.csv", sep = ",", quote = FALSE, row.names = FALSE)

attributes(lda)
slotNames(lda)

gamma<-slot(lda, "gamma")
write.table(gamma, file = "38Gibbs_gamma.txt", row.names = T, col.names =T)


# these are for CTM only (I think)

nu<-slot(lda, "nusquared") # Object of class "matrix"; variance of the variational distribution on the parameter mu
write.table(nu, file = "38Gibbs_nusq.txt", row.names = T, col.names =T)

sigma<-slot(lda, "Sigma")
write.table(sigma, file = "38Gibbs_sigma.txt", row.names = T, col.names =T)

