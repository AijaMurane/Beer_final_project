import java.io.FileWriter
import java.sql.{Driver, DriverManager}
import java.util.Arrays

/** Extracts words from articles about one topic - beer. Filter the most common words.*/

object Beers extends App {
  val workingDir = System.getProperty("user.dir")
  val srcName = if(args.nonEmpty) args(0) else s"$workingDir\\resources\\Beer1.txt"

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

    //splitting text with white space word by word and getting rid of unnecessary words
    val myArray = onlyWordsString.split("\\s+").filterNot(i =>
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
        i.contains("by")) //FIXME add more unnecessary words and maybe create a function for this. I tried, but did not manage to mke it work

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
        |INSERT INTO beersTable (
        |word_id, word, word_count)
        |VALUES(?,?,?)
        |""".stripMargin

    val environmentVars = System.getenv()
    val sqlite_home = environmentVars.get("SQLITE_HOME").replace("\\", "/")
    val dbname = fName
    val url = s"jdbc:sqlite:$sqlite_home/db/$dbname"

    val conn = DriverManager.getConnection(url)

    val pstmt = conn.prepareStatement(insertSql)

    //FIXME get an integer. Something wrong with transfering the filtering to Scala language. Works in DBeaver
    for (r <- beerArray) {
      val lastID =
      """
      |SELECT word_id FROM beersTable
      |ORDER BY word_id
      |DESC LIMIT 1|""".stripMargin

      val word_id = lastID + 1
      val word = beerArray(i)._1 //FIXME don't know why it does not work
      val word_count = beerArray(i)._2 //FIXME don't know why it does not work


      pstmt.setInt(1, word_id)
      pstmt.setString(2, word)
      pstmt.setInt(3, word_count)
      pstmt.execute()
    }
    pstmt.close()

  }

  /** TODO Here should be a function that saves to a database. */

    /** TODO read files in a loop */
  val mySeq = openSource(srcName)
  val beerArray = WordCount(mySeq)
  println(beerArray)
  /** TODO Save to the database. */

  val writing = writeToDatabase(beerArray,"beersDB.db")

}
