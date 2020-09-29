import java.sql.DriverManager

object NewDatabase {
  def createNewDatabase() = {
    val environmentVars = System.getenv()

    val sqlite_home = environmentVars.get("SQLITE_HOME").replace("\\", "/")

    val dbname = "beersDB.db"
    val url = s"jdbc:sqlite:$sqlite_home/db/$dbname"

    val conn = DriverManager.getConnection(url)

    val sql =
      """
        |CREATE TABLE IF NOT EXISTS beersTable (
        |	word_id INTEGER PRIMARY KEY,
        |	word TEXT NOT NULL,
        |	word_count INTEGER NOT NULL);
        |""".stripMargin

    val statement = conn.createStatement()
    val resultSet = statement.execute(sql)
  }
}
