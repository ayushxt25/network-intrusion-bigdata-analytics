FROM rocker/r-ver:4.4.2
RUN R -e "install.packages(c('ggplot2','jsonlite'), repos='https://cloud.r-project.org')"
