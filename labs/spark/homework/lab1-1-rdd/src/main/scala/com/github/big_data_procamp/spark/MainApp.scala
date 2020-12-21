package com.procamp
import org.apache.spark.sql.SparkSession

object MainApp {
    def main(args: Array[String]) {
        val ss = SparkSession.builder.appName("most-pop-dest-airport-rdd").getOrCreate()
        val sc = ss.sparkContext

        // get flights, remove header
        val flightsFile = sc
          .textFile("/data/flight_delays/flights/flights.csv")
          .map(_.split(","))
          .map(x => (x(1) +"_"+ x(8), 1))

        val header = flightsFile.first
        val flights = flightsFile.filter(line => line != header)

        // calc flights grouped month + airport
        val flightsMaxMonthly = flights
          .reduceByKey(_ + _)
          .map(x => (x._1.split("_")(0), (x._1.split("_")(1), x._2)))
          .groupByKey()
          .map({case(x, y) =>
              val max = y.toList.sortBy(tup => tup._2).last
              (x, max)
          })

        // get airports, remove header

        val airportsWithHeader = sc
          .textFile("/data/flight_delays/airports/airports.csv")
          .map(_.split(","))
          .map(x => (x(0),x(1)))

        val airports_header = airportsWithHeader.first
        val airports = airportsWithHeader.filter(line => line != airports_header)

        // create output file header
        val file_header = sc.parallelize(Array("month\tairport\tarrivals"))

        // left join airports and save on disc
        file_header.union(
            flightsMaxMonthly
              .map(x => (x._2._1, (x._1, x._2._2)))
              .leftOuterJoin(airports)
              .sortBy(-_._2._1._1.toInt)
              .map(x => (x._2._1._1+"\t"+x._2._2 +"\t"+ x._2._1._2))
        )
          .coalesce(1, shuffle = true)
          .saveAsTextFile("/data/flight_delays/flights/monthly_most_pop_destinations")

        sc.stop()
    }
}
