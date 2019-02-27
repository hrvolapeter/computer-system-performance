import glob
import re

files = glob.glob("./logs/*.log")


def GetDataFromFile(fileName):
    f = open(fileName, "r")
    text = f.read()
    return text


def analyzeData(text):
    # local\[(.{1,3})\]                               # group1
    # (spark.driver.memory')\, u'(.{1,6})\'\)\,    # group2
    # (Total time:) (\d{1,10}.\d{1,})              # group2

    coreCount = re.search(r'local\[(.{1,3})\]', text).group(1)
    memory = re.search(r"(spark.driver.memory')\, u'(.{1,6})\'\)\,", text).group(2)
    runtime = re.search(r'(Total time:) (\d{1,10}.\d{1,})', text).group(2)

    return (coreCount, memory, runtime)



runTimes = []

for currentFile in files:
    text = GetDataFromFile(currentFile)
    data = analyzeData(text)
    runTimes.append(data)


for potato in runTimes:
    print(str(potato), "\r")
    pass
