###
### Comparison RevoScaleR and ColumnStore and In-Memory OLTP
###

setwd("C:/Program Files/Microsoft SQL Server/130/R_SERVER/library/RevoScaleR/SampleData")

library(RevoScaleR)

rxOptions(sampleDataDir = "C:/Program Files/Microsoft SQL Server/130/R_SERVER/library/RevoScaleR/SampleData")

inFile <- file.path(rxGetOption("sampleDataDir"), "airsample.csv")

outFile <- rxDataStep(inData = inFile, outFile = "C:/Program Files/Microsoft SQL Server/130/R_SERVER/library/RevoScaleR/SampleData/airline20170428.xdf",  stringsAsFactors = T, overwrite = TRUE)

rxGetVarInfo(outFile)

outFile2 <- rxDataStep(inData = inFile, outFile = "C:/Program Files/Microsoft SQL Server/130/R_SERVER/library/RevoScaleR/SampleData/airline20170428_2.xdf", 
            transformVars = c("ArrDelay", "CRSDepTime","DayOfWeek")
           ,transforms = list(ArrDelay = as.integer(ArrDelay), CRSDepTime = as.numeric(CRSDepTime), DayOfWeek = as.character(DayOfWeek))
           ,overwrite = TRUE
           ,maxRowsByCols = 10000000)


of2 <- data.frame(rxReadXdf(file=outFile2, varsToKeep=c("ArrDelay", "CRSDepTime","DayOfWeek")))

#get average delay per days of the week

#convert the DayOfWeek into Factor
rxGetVarInfo(outFile2)

transforms <- expression(list(
  DayOfWeek = factor(DayOfWeek, levels=c("Monday", "Tuesday", "Wednesday", "Thursday","Friday", "Saturday", "Sunday"))
  ,ArrDelay = as.numeric(ArrDelay)
  ))

myTab <- rxCrossTabs(ArrDelay~DayOfWeek
                     ,data = outFile2
                     ,transforms = transforms
                    )
summary(myTab, output = "means")


summary(rxCrossTabs(ArrDelay~DayOfWeek
            ,data = outFile2
            ,transforms = transforms
            ,blocksPerRead=300000), output="means")


summary(rxCrossTabs(ArrDelay~DayOfWeek
                    ,data = of2
                    ,transforms = transforms
                    ,blocksPerRead=300000), output="means")
# Getting times
system.time({ 
  summary(rxCrossTabs(ArrDelay~DayOfWeek
                      ,data = of2
                      ,transforms = transforms}
                      ,blocksPerRead=300000), output="means")
  })


## Comparison with  T-SQL

system.time({ 
LMResults <- rxLinMod(ArrDelay ~ DayOfWeek, data = outFile2, transforms = transforms)
LMResults$coefficients
})



## Graph comparison

# Linear regression

df_LR_comparison <- data.frame (
  method = c("T-SQL", "ColumnStore", "Memory Optimized", "RevoScaleR")
  ,CPUtime = c(3000,1625,2156,7689)
  ,ElapsedTime = c(14323,10851,10600,7760)
  )
library(ggplot2)

ggplot(df_LR_comparison, aes(method, fill=method)) + 
  geom_bar(aes(y=ElapsedTime), stat="identity") +
  geom_line(aes(y=CPUtime, group=1), color="white", size=3) +
  scale_colour_manual(" ", values=c("d1" = "blue", "d2" = "red"))+
  #scale_fill_manual("",values="red")+
  theme(legend.position="none")


