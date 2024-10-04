import numpy as np
import math
import psycopg2
import cv2

def connectdatabase(databasename):
    conn = psycopg2.connect(dbname=databasename, user="postgres", password="123456")
    return conn

#create the adjacent matrix for the taxi data (example of New York City taxi data)
def adjacent_matrix_taxi(schemas,city,Oddata,year,start_time,end_time):
    dbname="postgis_Graduation"
    conn = connectdatabase(dbname)
    cur =conn.cursor()
    # i represents the day of week and j represents the season
    for i in range(0,7):
        for j in range(1,5):
            # get the number of the days
            sql="select date_trunc('day',start_time) from %s.%s%s%s%s where extract(dow from start_time) =%s  group by date_trunc('day',start_time) order by  date_trunc('day',start_time) "%(schemas,city,Oddata,year,j,i)
            cur.execute(sql)
            days=cur.fetchall()
            for day in days:
                daystamp=str(day[0].date())
                sql="select sid,eid,count(sid) from %s.%s%s%s%s where date_trunc('day',start_time) = %s and (extract(hour from start_time)>=%s and extract(hour from start_time)<=%s) group by (sid,eid) order by count(sid) desc"%(schemas,city,Oddata,year,j,"'"+daystamp+"'",start_time,end_time)
                cur.execute(sql)
                records=cur.fetchall()
                lenght=0
                for record in records:
                    lenght=lenght+record[2]
                #build the adjacent matrix
                admatrix=np.zeros((263,263),dtype=np.float32) # if the city is Chicago, the size of the matrix should be 77*77
                for record in records:
                    row=record[0]-1
                    col=record[1]-1
                    admatrix[row][col]=record[2]/lenght# normalization
                # max_min standardization
                admatrix=(admatrix-np.min(admatrix))/(np.max(admatrix)-np.min(admatrix))
                # output the admatrix as csv
                np.savetxt('Result/%s/Clustering_results/max_min/%s/%s/%s/%s.csv'%(city,Oddata,year,i,daystamp),admatrix,delimiter=',')
                # max_min gray log standardization
                log_img=(255/math.log(1+np.max(admatrix)+0.0000000000001))*np.log(1.0+admatrix)
                # #convert to uint8
                log_img=np.uint8(log_img)
                # output the log_admatrix as csv
                cv2.imwrite('Result/%s/Clustering_results/max_min_log/%s/%s/%s/%s.csv'%(city,Oddata,year,i,daystamp),log_img)

def adjacent_matrix_bike(schemas,city,Oddata,year,start_time,end_time):#创建按一定顺序排列的admatrix用于数据输入用作图的构建
    dbname="postgis_Graduation"
    conn = connectdatabase(dbname)
    cur =conn.cursor()
    # i represents the day of week and j represents the season
    for i in range(0,7):
        for j in range(1,5):
            sql="select date_trunc('day',start_time) from %s.%s%s%s%s where extract(dow from start_time) =%s  group by date_trunc('day',start_time) order by  date_trunc('day',start_time) "%(schemas,city,Oddata,year,j,i)
            cur.execute(sql)
            days=cur.fetchall()
            # get the number of the days
            for day in days:
                daystamp=str(day[0].date())
                sql="select s_zone_id,e_zone_id,count(s_zone_id) from %s.%s%s%s%s where date_trunc('day',start_time) = %s and (extract(hour from start_time)>=%s and extract(hour from start_time)<=%s)   group by (s_zone_id,e_zone_id) order by count(s_zone_id) desc"%(schemas,city,Oddata,year,j,"'"+daystamp+"'",start_time,end_time)
                cur.execute(sql)
                records=cur.fetchall()
                lenght=0
                for record in records:
                    lenght=lenght+record[2]
                #build the adjacent matrix
                admatrix=np.zeros((263,263),dtype=np.float32)
                for record in records:
                    # row=idmap[str(record[0])]
                    # col=idmap[str(record[1])]
                    admatrix[record[0]-1][record[1]-1]=record[2]/lenght# normalization
                # max_min standardization
                admatrix=(admatrix-np.min(admatrix))/(np.max(admatrix)-np.min(admatrix))
                # output the admatrix as csv
                np.savetxt('Result/%s/Clustering_results/max_min/%s/%s/%s/%s.csv'%(city,Oddata,year,i,daystamp),admatrix,delimiter=',')
                # max_min gray log standardization
                log_img=(255/math.log(1+np.max(admatrix)+0.0000000000001))*np.log(1.0+admatrix)
                # #convert to uint8
                log_img=np.uint8(log_img)
                # output the log_admatrix as csv
                cv2.imwrite('Result/%s/Clustering_results/max_min_log/%s/%s/%s/%s.csv'%(city,Oddata,year,i,daystamp),log_img)

if __name__ == '__main__':
    # adjacent_matrix_bike('public','nyc','bike','2016',0,23)
    adjacent_matrix_taxi('public','nyc','taxi','2016',0,23)