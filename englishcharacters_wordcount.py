from pyspark import SparkContext
import re

def removing_tags(inputtext):
	return re.sub(r"<[^>]+>", "", inputtext)

def english_character_words(word):
	return re.fullmatch(r"[A-Za-z]+", word) is not None

if __name__=="__main__":
	sc=SparkContext(appName="EnglishCharactersWordCount")
	inputdata= "/umbc/rs/is789sp25/common/data/enwiki-20240101-pages-articles-multistream1-part1.xml"
	output= "/home/xb56574/is789sp25_user/homework2/englishcharacters_wordcount_output"
	sentences=sc.textFile(inputdata).map(removing_tags)
	words=sentences.flatMap(lambda sentence:sentence.split())
	englishcharacter_words=words.filter(english_character_words)
	count=englishcharacter_words.map(lambda word:(word.lower(),1)).reduceByKey(lambda a, b:a+b)
	count.saveAsTextFile(output)
	sc.stop()
