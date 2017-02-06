from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
import re
import sys

def cleaner(line):
    return re.sub(r'[^\w]', ' ', line).lower().split(" ")

if len(sys.argv) != 2:
    sys.exit(1)

keyword = sys.argv[1].lower()

# Boilerplate Spark stuff:
conf = SparkConf().setMaster("local").setAppName("SparkTFIDF")
sc = SparkContext(conf = conf)

# Load documents (one per line).
rawData = sc.textFile("Berita.tsv")
fields = rawData.map(lambda x: x.split("\t"))
documents1 = fields.map(lambda x: x[2])
documents = documents1.map(cleaner)

#re.sub(r'[^\w]', ' ', x[2].lower().split(" "))
# Lower case

#string = documents.collect()
#for cur in string:
#    print str(cur)
##fields2 = string.map(lambda x: x.split(" "))


# Store the document names for later:
documentNames = fields.map(lambda x: x[1])

# Now hash the words in each document to their term frequencies:
hashingTF = HashingTF(100000)  #100K hash buckets just to save some memory
tf = hashingTF.transform(documents)

# At this point we have an RDD of sparse vectors representing each document,
# where each value maps to the term frequency of each unique hash value.

# Let's compute the TF*IDF of each term in each document:
tf.cache()
idf = IDF(minDocFreq=2).fit(tf)
tfidf = idf.transform(tf)

# Now we have an RDD of sparse vectors, where each value is the TFxIDF
# of each unique hash value for each document.

# I happen to know that the article for "Abraham Lincoln" is in our data
# set, so let's search for "Gettysburg" (Lincoln gave a famous speech there):

# First, let's figure out what hash value "Gettysburg" maps to by finding the
# index a sparse vector from HashingTF gives us back:
gettysburgTF = hashingTF.transform([keyword])
gettysburgHashValue = int(gettysburgTF.indices[0])

# Now we will extract the TF*IDF score for Gettsyburg's hash value into
# a new RDD for each document:
gettysburgRelevance = tfidf.map(lambda x: x[gettysburgHashValue])

# We'll zip in the document names so we can see which is which:
zippedResults = gettysburgRelevance.zip(documentNames).filter(lambda x : x[0] > 0)

print "Relevant documents for " + keyword + " is:"

zipped = zippedResults.collect()

for curr in zipped:
    curr = str(curr)
    curr = curr.replace('"', '')
    curr = curr.replace("'", "")
    strr=curr.split(', u')
    strr=strr[1].split(')')
    print strr[0]

# And, print the document with the maximum TF*IDF value:
print "Best document for "+ keyword + " is:"
try:
    curr = str(zippedResults.max())
    curr = curr.replace('"', '')
    curr = curr.replace("'", "")
    strr=curr.split(', u')
    strr=strr[1].split(')')
    print strr[0]
except:
    print "None"
