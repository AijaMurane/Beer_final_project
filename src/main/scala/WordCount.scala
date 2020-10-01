/**We get from a string to an array of tuples with words and how many times they appear.
 * We also get rid of words with no significant meaning */

object WordCount {
  def getWordCount(myString: String): Array[(String,Int)] = {

    //getting rid of characters like dot and comma that we do not need and getting rid of words like "and", "or" etc. too
    val onlyWordsString = myString.replaceAll("[^a-zA-Z\\s]","").toLowerCase

    //splitting text with white space word by word and getting rid of unnecessary words
    val wrongWords = Seq("with", "that", "from", "have", "which", "were", "this", "they", "their", "these", "there", "what", "other")
    val myArray = onlyWordsString.split("\\s+")
      .filter(x => x.length > 3 || x == "ale" || x == "ipa" || x == "dry")
      .filterNot(x => wrongWords.exists(y => x.contains(y)))

    //creating a tuple of a word and how many times it occurs in the text
    val arrayTuples = myArray.groupMapReduce(identity)(_ => 1)(_ + _)

    //filtering unique values and sorting them from most to least frequent words
    val uniqueTuples = arrayTuples.toSet.toArray.sortBy(_._2).reverse

    uniqueTuples
  }
}
