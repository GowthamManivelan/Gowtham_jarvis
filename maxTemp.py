#GOWTHAM MANIVELAN
#spark-submit --master local ~/maxTemp.py
import csv
import operator
temperature = []
longitude = []
rangeTemp = []
temp = 0
sample= open("parseoutput.txt", "r")
csv1 = csv.reader(sample,delimiter='\t')
print ("Sorting the text file")
sort= sorted(csv1, key=lambda x:int(operator.itemgetter(0)(x)))
print ("Mapping temperature and longitude")
for eachline in sort:
    if(eachline[2]!='+9999'):
       temperature.append(int(eachline[2]))
       longitude.append(int(eachline[0]))
print ("Calculating max temperature for longitude range 10...")
for i in range(len(longitude))[:]:
    if(longitude[i]-longitude[temp]<10):
       rangeTemp.append(temperature[i])
       a=i
    else:
       if rangeTemp:
          print (longitude[temp]-10,longitude[temp],max(rangeTemp))
       rangeTemp=[]
       temp = i