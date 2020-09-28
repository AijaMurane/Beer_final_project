import java.io.FileWriter
import java.sql.{Driver, DriverManager}
import java.util.Arrays

/** Extracts words from articles about one topic - beer. Filter the most common words.*/

object Beers extends App {
  val workingDir = System.getProperty("user.dir")

  val srcName1 = if(args.nonEmpty) args(0) else s"$workingDir\\resources\\Beer1.txt"
  val srcName2 = if(args.nonEmpty) args(1) else s"$workingDir\\resources\\Beer2.txt"
  val srcName3 = if(args.nonEmpty) args(2) else s"$workingDir\\resources\\Beer3.txt"

  /** Reads a text file from program arguments or a default source, if arguments are empty.
  The whole file is saved as a single string*/
  def openSource(fName: String): String = {
    println(s"Reading a file from source: $fName")
    val filePointer = scala.io.Source.fromFile(fName)
    val myString = filePointer.getLines.mkString
    filePointer.close()
    myString
  }

  /** The string is split into separate words and saved into array.
   Then the array is transferred to a sequence with only unique values along with number of occurrences of that word.*/
  def WordCount(myString: String): Array[(String,Int)] = {

    //getting rid of characters like dot and comma that we do not need and getting rid of words like "and", "or" etc. too
    val onlyWordsString = myString.replaceAll("[^a-zA-Z\\s]","").toLowerCase

    val wrongWords = Seq("with", "that", "from", "have", "which", "were", "this", "they", "their", "these", "there", "what", "other")

    val myArray = onlyWordsString.split("\\s+")
      .filter(x => x.length > 3 || x == "ale" || x == "ipa" || x == "dry")
      .filterNot(x => wrongWords.exists(y => x.contains(y)))

    //splitting text with white space word by word and getting rid of unnecessary words
   /** val myArray = onlyWordsString.split("\\s+").filterNot(i =>
        i.contains("of") ||
        i.contains("the") ||
        i.contains("and") ||
        i.contains("a") ||
        i.contains("of") ||
        i.contains("to") ||
        i.contains("in") ||
        i.contains("is") ||
        i.contains("with") ||
        i.contains("that") ||
        i.contains("as") ||
        i.contains("was") ||
        i.contains("are") ||
        i.contains("its") ||
        i.contains("but") ||
        i.contains("it") ||
        i.contains("for") ||
        i.contains("be") ||
        i.contains("can") ||
        i.contains("or") ||
        i.contains("by")) //FIXME add more unnecessary words and maybe create a function for this. I tried, but did not manage to mke it work*/

     //creating a tuple of a word and how many times it occurs in the text
    val arrayTuples = myArray.groupMapReduce(identity)(_ => 1)(_ + _)

    //filtering unique values and sorting them from most to least frequent words
    val uniqueTuples = arrayTuples.toSet.toArray.sortBy(_._2).reverse

    //the printing is for checking the result
    for (w <- uniqueTuples) {
      println(w)
    }

    uniqueTuples
    }

  def createNewDatabase() = {
    val environmentVars = System.getenv()

    //val properties = System.getProperties()
    val sqlite_home = environmentVars.get("SQLITE_HOME").replace("\\", "/")

    val dbname = "beersDB.db"
    println(s"Creating DB $dbname")
    val url = s"jdbc:sqlite:$sqlite_home/db/$dbname"

    val conn = DriverManager.getConnection(url)

    //lets make a table!
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

  val beerDB = createNewDatabase()

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

    /** TODO read files in a loop */


  val mySeq1 = openSource(srcName1)
  val mySeq2 = openSource(srcName2)
  val mySeq3 = openSource(srcName3)

  val beerArray = WordCount(mySeq1) ++ WordCount(mySeq2) ++ WordCount(mySeq3)
  println(beerArray)
  val writing = writeToDatabase(beerArray,"beersDB.db")

}
