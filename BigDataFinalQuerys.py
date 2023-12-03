
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

fatalityData = spark.read.format("csv").option("header", "true").load('Fatal_Collisions.csv')
fatalityData.createOrReplaceTempView("fatalities")

# Determine what vehicle speed was present the most during crashes.
spark.sql("SELECT INVAGE AS Speed, COUNT(*) AS RecordCount FROM fatalities GROUP BY INVAGE ORDER BY RecordCount DESC LIMIT 1").show()

# Determine what district had the most fatal car crashes.
spark.sql("SELECT DISTRICT AS DISTRICT, COUNT(*) AS Fatalities FROM fatalities GROUP BY DISTRICT ORDER BY Fatalities DESC LIMIT 1").show()

# Determine how many fatal crashes occurred during the day as opposed to during night.
spark.sql("SELECT LIGHT as Light_Conditions, count(*) AS Fatalities FROM fatalities GROUP BY Light_Conditions ORDER BY Fatalities DESC").show()

# Determine how many fatal crashes occurred while roads were wet as opposed to dry.
spark.sql("SELECT RDSFCOND AS Road_Conditions, count(*) AS Fatalities FROM fatalities GROUP BY Road_Conditions ORDER BY Fatalities DESC").show()

# Determine what year had the most fatal car crashes
spark.sql("SELECT YEAR as Year, count(*) AS Fatalities FROM fatalities GROUP BY Year ORDER BY Year DESC LIMIT 1").show()

# Determine how many fatal collisions occurred while driver exceeding the speed limit.
spark.sql("""SELECT COALESCE(SPEEDING, 'No') AS Speeding, COUNT(*) AS Fatalities FROM fatalities GROUP BY Speeding ORDER BY Fatalities DESC """).show()

# Determine the count of fatal collisions that occurred while pedestrian crossing the right of way.
spark.sql("SELECT PEDACT AS Pedestrian_Action_Description, count(*) AS Fatalities FROM fatalities WHERE PEDACT='Crossing with right of way' GROUP BY Pedestrian_Action_Description").show()

# Determine the drivers' state of condition for the most fatal collisions
spark.sql("SELECT ALCOHOL AS Alcohol, count(*) AS Fatalities FROM fatalities WHERE ALCOHOL='Yes' GROUP BY Alcohol").show()
