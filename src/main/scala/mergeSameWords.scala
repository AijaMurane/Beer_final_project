import java.sql.DriverManager
import scala.collection.mutable.ListBuffer

object mergeSameWords {
  def mergeSameWords(fName: String) = {
    val environmentVars = System.getenv()
    val sqlite_home = environmentVars.get("SQLITE_HOME").replace("\\", "/")

    val dbname = fName
    val url = s"jdbc:sqlite:$sqlite_home/db/$dbname"

    val conn = DriverManager.getConnection(url)

    //lets filter
    val sql =
      """
        |SELECT DISTINCT bt.word, SUM(bt.word_count) WordSum FROM beersTable bt
        |GROUP BY bt.word
        |ORDER BY bt.word_count DESC
        |LIMIT 100""".stripMargin

    val statement = conn.createStatement()
    val resultSet = statement.executeQuery(sql)

    val meta = resultSet.getMetaData
    var colSeq = ListBuffer[String]()
    for (i <- 1 to meta.getColumnCount) {
      val meta1 = meta.getColumnName(i) + " "
      print(meta1)
      colSeq += meta.getColumnName(i)
    }

    while ( resultSet.next() ) {
      println()
      colSeq.map(col => print(resultSet.getString(col) + " "))
    }

  }
}
