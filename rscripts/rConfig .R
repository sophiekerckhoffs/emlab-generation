user<-sophiekerckhoffs
emlabGenerationFolder<-paste("/home/sophie/emlab-generation/", sep="")
rscriptFolder<-paste(emlabGenerationFolder,"rscripts/", sep="")
headlessFolder<-paste(emlabGenerationFolder,"shellscripts/headlessScripts", sep="")
resultFolder<-paste("/home/sophie/Desktop/emlabGen/output/", sep="")
analysisFolder<-paste("/home/sophie/Desktop/emlabGen/analysis/", sep="")
queryFile<-paste(emlabGenerationFolder,"emlab-generation/","queries-Ranalysis.properties", sep="")
agentSpringReader<-paste(rscriptFolder,"AgentSpringHeadlessReader.R")
#agentSpringReader<-paste(rscriptFolder,"AgentSpringHeadlessReaderPureR.R")

setwd(rscriptFolder)
