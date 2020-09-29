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

  val mySeq1 = openSource(srcName1)
  val mySeq2 = openSource(srcName2)
  val mySeq3 = openSource(srcName3)

  val beerArray1 = WordCount.getWordCount(mySeq1)
  val beerArray2 = WordCount.getWordCount(mySeq2)
  val beerArray3 = WordCount.getWordCount(mySeq3)

  val beerArray = beerArray1 ++ beerArray2 ++ beerArray3

  println(beerArray)

  val beerDB = NewDatabase.createNewDatabase()

  val writing = writeToDatabase.writeToDatabase(beerArray,"beersDB.db")

  val mergedResult = mergeSameWords.mergeSameWords("beersDB.db")
}
