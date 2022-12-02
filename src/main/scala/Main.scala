import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Data:
 * - for some reason, grabbing all teamAttributes from the 2015/2016 season yields ~30-40 fewer teams than
 *    there are in the Teams table. Querying to include 2013 & 2014 season entries and removing dupe team entries
 *    is also returning fewer team attributes than there are teams...
 *    - Team_Attributes.csv -> only 2015 entries
 *    - Team_Attributes_1.csv -> 2013, 2014 & 2015 szn entries
 *
 * - The Teams data does not include respective league ids. Furthermore, ID in the league table
 *    is actually the countryID in the db, which only appears in association to a team (or two) in
 *    the Matches table which is pretty inconvenient.
 *
 * - Team Attribute stats are stored as descriptions rn, but the db also has numeric values for some
 *    attributes. This may be easier to use for comparing, but the data is slightly inconsistent. I'm
 *    thinking of creating more custom classes to hold different attribute types (bu, cc, d) to better track
 *    compareTo metrics.
 */

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("distributed-fc").setMaster("local[4]")
    val sc = new SparkContext(conf);

    // Some classes to organize data

    //prolly don't need this
    case class Date(year: Int, month: Int) extends Ordered[Date] {
      import scala.math.Ordered.orderingToOrdered
      def compare(that: Date): Int = (this.year, this.month).compare(that.year, that.month);
    }

    case class Country(id: Int, name: String);

    case class League(countryID: Int, name: String);

    case class Matches(id: Int, leagueID: Int, homeID: Int, awayID: Int, homeGoals: Int, awayGoals: Int);

    case class Player(id: Int, name: String, height: Double, weight: Int);

    case class Teams(id: Int, name: String);

    //TeamAttributes(id, date, buSpeed, buDribbling, buPassing, buPositioning,
    //              ccPassing, ccCrossing, ccShooting, ccPositioning, dPressure, dAggresion, dWidth, dLine)
    case class TeamAttributes(id: Int, buSpeed: String, buDribbling: String, buPassing: String, buPositioning: String,
    ccPassing: String, ccCrossing: String, ccShooting: String, ccPositioning: String, dPressure: String, dAggresion: String, dWidth: String, dLine: String)


    val MatchData = sc.textFile("./data/Matches.csv").map{line => {
      var temp = line.split(",");
      Matches(temp(0).toInt, temp(1).toInt, temp(2).toInt, temp(3).toInt, temp(4).toInt, temp(5).toInt);
    }}

    val TeamData = sc.textFile("./data/Teams.csv").map(line => {
      Teams(line.split(",")(0).toInt, line.split(",")(1))
    })

    //262
    val teamAttributeData = sc.textFile("./data/Team_Attributes.csv").map(line => {
      var temp = line.split(",")
      var date = temp(1).replaceAll("\"", "").split("-")(0).toInt
      (temp(0).trim.toInt, TeamAttributes(temp(0).trim.toInt, temp(2), temp(3), temp(4), temp(5), temp(6),
                            temp(7), temp(8), temp(9), temp(10), temp(11), temp(12), temp(13)))
    }).groupByKey().map{case (_, v) => {
      v.toList(0)
    }}

    println(teamAttributeData.count());

    val leagueDate = sc.textFile("./data/Leagues.csv").map(line => {
      League(line.split(",")(0).toInt, line.split(",")(1))
    })

  }
}