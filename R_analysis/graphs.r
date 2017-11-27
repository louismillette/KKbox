library(MASS)
library(loon)

dir = "C:\\Users\\Louis Millette\\Documents\\school\\STAT 441\\Project"
setwd(dir)
data <- read.csv("prows.csv")

par_hist <- function(){
  cols <- c('aquamarine', 'azure4', 'blue', 'blueviolet', 'brown4',
            'burlywood', 'burlywood4', 'chartreuse', 'chartreuse4',
            'darkgoldenrod1')
  aug_data <- data.frame(sl=data$song_length,
                         city=data$city,
                         BD=data$bd,
                         Gen=data$gender,
                         Reg=data$registered_via,
                         Lan = data$language,
                         tab=data$source_system_tab,
                         name = data$source_screen_name,
                         tagret=data$target)[c(sample(1:146014,1000,replace=F)),]
  labels <- names(aug_data)
  l_serialaxes(aug_data, linkingGroup = 'labels',
               showAxesLabels = FALSE,
               axesLayout = "parallel")
  l_hist(aug_data$tab, linkingGroup = "labels")
  l_hist(aug_data$city, linkingGroup = "labels")
  l_hist(aug_data$Lan, linkingGroup = "labels")
  l_hist(aug_data$BD, linkingGroup = "labels")
  l_hist(aug_data$tagret, linkingGroup = "labels")
}

par_hist()

pair_axis <- function(){
  aug_data <- data.frame(sl=data$song_length,
                         city=data$city,
                         BD=data$bd,
                         Gen=data$gender,
                         Reg=data$registered_via,
                         Lan = data$language,
                         tab=data$source_system_tab,
                         name = data$source_screen_name,
                         tagret=data$target)[c(sample(1:146014,1000,replace=F)),]
  l_hist(aug_data$tagret, linkingGroup = "labels")
  l_serialaxes(aug_data, linkingGroup = 'labels',
               showAxesLabels = FALSE,
               axesLayout = "parallel")
  l_pairs(aug_data,
          color="grey70",
          linkingGroup="labels", size=0.5)
  
}

pair_axis()


labels <- paste(olive[,"Region"], ":\n ", olive[, "Area"])
l_serialaxes(oliveAcids, linkingGroup = "olive",
             itemLabel = labels, showItemLabels = TRUE,
             showAxesLabels = FALSE,
             axesLayout = "parallel")
l_plot(oliveAcids, linkingGroup = "olive",
       itemLabel = labels, showItemLabels = TRUE)
areas <- as.numeric(olive[,"Area"])
l_hist(areas, linkingGroup = "olive")
