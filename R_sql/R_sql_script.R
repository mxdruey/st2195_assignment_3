#install.packages("RSQLite")
library(DBI)
library(dplyr)

#if (file.exists("/home/max/LSE/DB_local_only/R_airlines2.db")) 
#  file.remove("/home/max/LSE/DB_local_only/R_airlines2.db")

conn <- dbConnect(RSQLite::SQLite(), "/home/max/LSE/DB_local_only/airlines2.db")

# list all tables
dbListTables(conn)
#read csv files to dataframe
ontime1 <- read.csv("/home/max/LSE/DB_local_only/dataverse_files/2000.csv", header = TRUE)
ontime2 <- read.csv("/home/max/LSE/DB_local_only/dataverse_files/2001.csv", header = TRUE)
ontime3 <- read.csv("/home/max/LSE/DB_local_only/dataverse_files/2002.csv", header = TRUE)
ontime4 <- read.csv("/home/max/LSE/DB_local_only/dataverse_files/2003.csv", header = TRUE)
ontime5 <- read.csv("/home/max/LSE/DB_local_only/dataverse_files/2004.csv", header = TRUE)
ontime6 <- read.csv("/home/max/LSE/DB_local_only/dataverse_files/2005.csv", header = TRUE)
airports <- read.csv("/home/max/LSE/DB_local_only/dataverse_files/airports.csv", header = TRUE)
carriers <- read.csv("/home/max/LSE/DB_local_only/dataverse_files/carriers.csv", header = TRUE)
planes <- read.csv("/home/max/LSE/DB_local_only/dataverse_files/plane-data.csv", header = TRUE)
#write dataframes to db
dbWriteTable(conn, "ontime", ontime1 )
dbWriteTable(conn, "ontime", ontime2, append = TRUE)
dbWriteTable(conn, "ontime", ontime3, append = TRUE)
dbWriteTable(conn, "ontime", ontime4, append = TRUE)
dbWriteTable(conn, "ontime", ontime5, append = TRUE)
dbWriteTable(conn, "ontime", ontime6, append = TRUE)
dbWriteTable(conn, "airports", airports, append = TRUE)
dbWriteTable(conn, "carriers", carriers, append = TRUE)
dbWriteTable(conn, "planes", planes, append = TRUE)

#dbListTables(conn)
##############SQL QUERRIES
q1 <- dbGetQuery(conn, 
                 "SELECT airports.city AS city, COUNT(*) AS total

FROM airports JOIN ontime ON ontime.dest = airports.iata

WHERE ontime.Cancelled = 0

GROUP BY airports.city

ORDER BY total DESC")
q1

q2 <- dbGetQuery(conn, 
                 "SELECT carriers.Description AS carrier, COUNT(*) AS total

FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code

WHERE ontime.Cancelled = 1

    AND carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')

GROUP BY carriers.Description

ORDER BY total DESC")
q2

q3 <- dbGetQuery(conn, 
                 "SELECT model AS model, AVG(ontime.DepDelay) AS avg_delay

FROM planes JOIN ontime USING(tailnum)

WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0

GROUP BY model

ORDER BY avg_delay")
q3

q4 <- dbGetQuery(conn, 
                 "SELECT

    q1.carrier AS carrier, (CAST(q1.numerator AS FLOAT)/ CAST(q2.denominator AS FLOAT)) AS ratio

FROM

(

    SELECT carriers.Description AS carrier, COUNT(*) AS numerator

    FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code

    WHERE ontime.Cancelled = 1 AND carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')

    GROUP BY carriers.Description

) AS q1 JOIN

(

    SELECT carriers.Description AS carrier, COUNT(*) AS denominator

    FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code

    WHERE carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')

    GROUP BY carriers.Description

) AS q2 USING(carrier)

ORDER BY ratio DESC
")
q4

################### DPLYR QUERRIES

ontime_db <- tbl(conn, "ontime")
airport_db <- tbl(conn, "airports")
carriers_db <- tbl(conn, "carriers")
planes_db <- tbl(conn, "planes")

#Which of the following cities has the highest number of inbound flights (excluding cancelled flights)?
q10 <- inner_join(airport_db, ontime_db, by = c("iata" = "Dest") ) %>% 
  filter(Cancelled == 0) %>% 
  group_by(city) %>%
  summarize(total = n()) %>%
  arrange(desc(total))
q10
write.csv(q10,"~/LSE/st2195_assignment_3/R_sql/question_1.csv", row.names = FALSE)

#Which of the following companies has the highest number of cancelled flights?
q11 <- inner_join(ontime_db, carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter (Cancelled == 1, Description %in% c('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')) %>%
  group_by(Description) %>%
  summarize(total = n()) %>%
  arrange(desc(total))
q11
write.csv(q11,"~/LSE/st2195_assignment_3/R_sql/question_2.csv", row.names = FALSE) 
#Which of the following airplanes has the lowest associated average departure delay (excluding cancelled and diverted flights)?

#TailNum and tailnum kept causing a ambiguity SQL Error 
planes_db <- tbl(conn, "planes") %>% rename(TailNum = tailnum) %>% rename(year_plane = year)

q12 <-  left_join(planes_db,ontime_db) %>%
  filter(Cancelled == 0 & Diverted == 0 & DepDelay > 0) %>% 
  group_by(model) %>%
  summarize(avg_delay = mean(DepDelay, na.rm=TRUE)) %>%
  arrange(desc(avg_delay))
q12

write.csv(q12,"~/LSE/st2195_assignment_3/R_sql/question_3.csv", row.names = FALSE)

#"Simple Solution" for question 4 (well not so simple but only one I could make work)
#Which of the following companies has the highest number of cancelled flights, relative to their number of total flights?

q_numerator <- inner_join(ontime_db, carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter (Cancelled == 1, Description %in% c('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')) %>%
  group_by(Description) %>%
  summarize(total_1 = n()) %>%
  arrange(desc(total_1))
q_numerator
q_denumerator <- inner_join(ontime_db, carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter (Description %in% c('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')) %>%
  group_by(Description) %>%
  summarize(total_2 = n()) %>%
  arrange(desc(total_2))
q_denumerator
simple_solution <- inner_join(q_numerator,q_denumerator) %>%
  group_by(Description) %>%
  summarise(ratio = (total_1) / (total_2)) %>%
  arrange(desc(ratio))

simple_solution

write.csv(simple_solution,"~/LSE/st2195_assignment_3/R_sql/Question_4_simple_solution.csv", row.names = FALSE)


dbDisconnect(conn)

