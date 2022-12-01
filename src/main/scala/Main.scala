import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Data:
 *  Teams => (teamID, name)
 *  Matches -> (matchID, homeTeamID, awayTeamID, homeGoals, awayGoals)
 *
 *  TeamAttributes -> (id {fifa id}, date, buSpeed {class}, buDribbling, buPassing, buPositioning,
 *                  ccPassing, ccCrossing, ccShooting, ccPositioning, dPressure, dAggresion, dWidth, dLine)
 *
 */

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Lab6").setMaster("local[4]")
    val sc = new SparkContext(conf);

    // Some classes to organize data

    case class Date(year: Int, month: Int) extends Ordered[Date] {
      import scala.math.Ordered.orderingToOrdered
      def compare(that: Date): Int = (this.year, this.month).compare(that.year, that.month);
    }

    case class Country(id: Int, name: String);

    case class League(id: Int, countryID: Int, name: String);

    case class Match(id: Int, homeID: Int, awayID: Int, homeGoals: Int, awayGoals: Int);

    case class Player(id: Int, name: String, height: Double, weight: Int);

    case class Team(id: Int, name: String);

    //TeamAttributes(id {id}, date, buSpeed {class}, buDribbling, buPassing, buPositioning,
    //              ccPassing, ccCrossing, ccShooting, ccPositioning, dPressure, dAggresion, dWidth, dLine)
    // bu -> build up
    // cc -> chance creation
    // d -> defense
    case class TeamAttributes(id: Int, date: Date, buSpeed: String, buDribbling: String, buPassing: String, buPositioning: String,
    ccPassing: String, ccCrossing: String, ccShooting: String, ccPositioning: String, dPressure: String, dAggresion: String, dWidth: String, dLine: String)


    //Stores(storeID, (storeName, city))
    val MatchData = sc.textFile("./data/Matches.csv").map{line => {
      var temp = line.split(",");
      Match(temp(0).toInt, temp(1).toInt, temp(2).toInt, temp(3).toInt, temp(4).toInt);
    }}


  }
}