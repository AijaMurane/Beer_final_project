import java.sql.DriverManager

object writeToDatabase {
  def writeToDatabase(beerArray: Array[(String,Int)], fName: String) {
    val insertSql =
      """
        |INSERT INTO beersTable (word_id, word, word_count)
        |VALUES(?,?,?)
        |""".stripMargin

    val environmentVars = System.getenv()
    val sqlite_home = environmentVars.get("SQLITE_HOME").replace("\\", "/")
    val dbname = fName
    val url = s"jdbc:sqlite:$sqlite_home/db/$dbname"

    val conn = DriverManager.getConnection(url)

    val pstmt = conn.prepareStatement(insertSql)

    for (r <- beerArray) {
      val statement = conn.createStatement()

      val checkID =
        """
          |SELECT word_id FROM beersTable
          |ORDER BY word_id
          |DESC
          |LIMIT 1
          |""".stripMargin

      val resultSet = statement.executeQuery(checkID)

      val lastID = {
        try {
          resultSet.getInt(1)
        } catch {
          case e: Exception => 0
        }
      }

      val word_id = lastID + 1
      val word = r._1
      val word_count = r._2

      pstmt.setInt(1, word_id)
      pstmt.setString(2, word)
      pstmt.setInt(3, word_count)
      pstmt.execute()
    }
    pstmt.close()

  }
}
