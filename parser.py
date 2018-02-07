import MySQLdb as my
db=my.connect(host="127.0.0.1",user="root",passwd="itmd521",db="itmd521")
cursor=db.cursor()
f = open("1950.txt","r") #opens file with name of "test.txt"
myList = []
print("Insertion in progress")
for line in f:
        nomention= line[0:4]
        weatherStation = line[4:10]
        WBAN= line[10:15]
        obRate=line[15:23]
        obHour=line[23:27]
        unknown=line[27]
        latitude=line[28:34]
        longitude=line[34:41]
        FM=line[41:46]
        elev=line[46:51]
        place1=line[51:56]
        place2=line[56:60]
        wd=line[60:63]
        qc=line[63]
        ph=line[64]
        ph2=line[65:69]
	 ph3=line[69]
        ph4=line[70]
        sk= line[70:75]
        qc= line[76]
        ph5= line[77]
        vd=line[78:84]
        qc2=line[84]
        ph6=line[85]
        ph7=line[86]
        temp=line[87:92]
        qc3=line[92]
        dp=line[93:98]
        qc4=line[99]
        ap=line[99:104]
        qc5=line[105]
        sql= "insert into record VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"% \
(nomention,weatherStation,WBAN,obRate,obHour,unknown,latitude,longitude,FM,elev,place1,place2,wd,qc,ph,ph2,ph3,ph4,sk,qc,ph5,vd,qc2,ph6,ph7,temp,qc3,dp,qc4,ap,qc5)
        number_of_rows=cursor.execute(sql)
        db.commit()
        #myList.append(FM)
        #myList.append(ph3)
        #myList.append(ph3)
f.close()
db.close()
print("Insertion Done")