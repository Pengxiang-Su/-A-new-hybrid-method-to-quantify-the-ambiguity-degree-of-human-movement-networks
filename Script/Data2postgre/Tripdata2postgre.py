import psycopg2
import os
from Utils import bikePoint
import Utils
import csv
from multiprocessing import Pool
from datetime import datetime
import pandas as pd

# connect to the database. Database name is the database name you defined in PostgreSQL.
def connectdatabase(databasename):
    conn = psycopg2.connect(dbname=databasename, user="postgres", password="123456")
    return conn

# create the table for the bike OD data that without zone ids
# conn: the connection to the database
# startyear: the start year of the data
# endyear: the end year of the data
# schemas: the schema of the database. the name of the schema you defined in PostgreSQL.
# city: the city of the data, such as New York City, Chicago, etc.
# Oddata: the type of the data, such as bike, taxi, etc.
def create_bike_od_table_points(conn,startyear,endyear,schemas,city,Oddata):
    cur = conn.cursor()
    for i in range(startyear,endyear):#start and end depends on your data range
        for j in range(1,5):#sepreate the data into 4 parts corresponding to the 4 seasons
            #define the essential columns for our hybrid model (example of New York City bike data)
            sql = "CREATE TABLE if not exists %s.%s%s%s%s \
                ( Duration real,\
                usertype text,\
                Start_time timestamp,\
                edline geometry,\
                SID BIGINT,\
                s_zone_id integer,\
                End_time timestamp,\
                birth_year BIGINT,\
                gender integer,\
                EID BIGINT,\
                e_zone_id integer\
                );"%(schemas,city,Oddata,i,j) #you can define the other columns according to your data
            cur.execute(sql) 
            conn.commit()
    conn.close()

# create the table for the bike OD data that with zone ids
def create_bike_od_table(conn,startyear,endyear,schemas,city,Oddata):
    cur = conn.cursor()
    for i in range(startyear,endyear):#start and end depends on your data range
        for j in range(1,5):#sepreate the data into 4 parts corresponding to the 4 seasons
            #define the essential columns for our hybrid model (example of Chicago City bike data)
            sql = "CREATE TABLE if not exists %s.%s%s%s%s \
                (Duration real,\
                Start_time timestamp,\
                SID BIGINT,\
                s_zone_id integer,\
                End_time timestamp,\
                EID BIGINT,\
                e_zone_id integer\
                );"%(schemas,city,Oddata,i,j)
            cur.execute(sql) 
        conn.commit()
    conn.close()

# create the table for the taxi OD data
def create_taxi_od_table(conn,startyear,endyear,schemas,city,Oddata):
    cur = conn.cursor()
    for i in range(startyear,endyear):#start and end depends on your data range
        for j in range(1,5):
            #define the essential columns for our hybrid model (example of New York City taxi data)
            sql = "CREATE TABLE if not exists %s.%s%s%s%s \
                ( Duration real,\
                Distance real,\
                Vender_id varchar,\
                Start_time timestamp,\
                SID integer,\
                PassagerNumber integer,\
                End_time timestamp,\
                geom geometry,\
                startpoint geometry,\
                endpoint geometry,\
                EID integer\
                );"%(schemas,city,Oddata,i,j)
            cur.execute(sql) 
        conn.commit()
    conn.close()

# insert the data into the table with the given city, OD data type, and year.
# startmonth: the start month of the data
# endmonth: the end month of the data. Usually, the data is organized by month.
def putinoddata(city,schemas,Oddata,year,startmonth,endmonth):
    rawdatas =[]
    #read the data from the file
    for root,dirs,files in os.walk('Data/%s/%s/%s'%(city,Oddata,year)):
        for  i in range(startmonth,endmonth):
            filepath = os.path.join('Data/%s/%s/%s'%(city,Oddata,year),files[i])
            with open (filepath,'r') as f:
                reader = csv.reader(f)
                rawdatas += list(reader)[1:]
        break
    # denfine the number of the threads
    lenght = len(rawdatas)
    # the number of the threads are depended on the number of the cores of your computer
    poolnum=14
    arow = int(lenght/poolnum)
    p=Pool(poolnum=14)
    season= Utils.getseason(startmonth)
    # multiprocess the data
    for i in range(poolnum=14):
        if i==poolnum-1:
            p.map_async(write2databasebike,([rawdatas[i*arow:],schemas,city,Oddata,year,season],))
        else:
            p.map_async(write2databasebike,([rawdatas[i*arow:(i+1)*arow-1],schemas,city,Oddata,year,season],))
    p.close()
    p.join()

# insert the OD data with the startpoint and endpoint information (example of New York City bike data).
def write2databasebike_points(parameters):
    rawdatas = parameters[0]
    schemas = parameters[1]
    city = parameters[2]
    Oddata = parameters[3]
    year = parameters[4]
    season = parameters[5]
    print("thread %s begin"%(os.getpid()))
    dbname="postgis_Graduation"
    conn = connectdatabase(dbname)
    cur =conn.cursor()
    j=0
    start = datetime.now()
    for rawdata in rawdatas :
        try:
            #the jugdement of duration is optional, you can delete it if you don't need it.       
            duration = int(rawdata[0])
            # if the duration is more than 86400 seconds, we think it is a useless data.
            if duration >86400:
                continue
            start_time ="'"+str(datetime.strptime(rawdata[1], '%Y-%m-%d %H:%M:%S'))+"'"
            end_time = "'"+str(datetime.strptime(rawdata[2],'%Y-%m-%d %H:%M:%S'))+"'"
            Spoint = bikePoint(float(rawdata[6]),float(rawdata[5]))
            Epoint = bikePoint(float(rawdata[10]),float(rawdata[9]))
            edline =bikePoint.bikePoint2Line([Spoint,Epoint],4326)
            startpoint = bikePoint.bikePoint2Geometry(Spoint,4326)
            # map the startpoint to the zone id
            sql = 'select objectid from %s.NYC_zones where st_contains(geom,%s)'%(parameters[1],startpoint)
            cur.execute(sql)
            # filter the data without the zone id
            s_zoneid = cur.fetchone()
            if s_zoneid == None:
                continue
            endpoint =bikePoint.bikePoint2Geometry(Epoint,4326)
            # map the endpoint to the zone id
            sql = 'select objectid from %s.NYC_zones where st_contains(geom,%s)'%(parameters[1],endpoint)
            cur.execute(sql)
            e_zoneid = cur.fetchone()
            # filter the data without the zone id
            if e_zoneid == None:
                continue
            SID= int(rawdata[3])
            EID =int (rawdata[7])
            # the jugdement of brith_year is optional, you can delete it if you don't need it.
            if rawdata[13]=='':
                brith_year=0
            else:
                brith_year=int(rawdata[13])
            gender =int (rawdata[14])
            usertype = "'"+rawdata[12]+"'"
            # insert the data into the table
            sql =""" INSERT INTO %s.%s%s%s%s(Duration,usertype,start_time, edline, SID, s_zone_id , end_time , birth_year, gender ,EID, e_zone_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s);"""%(schemas,city,Oddata,year,season,duration,usertype,start_time,edline,SID,s_zoneid[0],end_time,brith_year,gender,EID,e_zoneid[0])
            cur.execute(sql)
        except Exception as e:
            j+=1
            continue
    end =datetime.now()
    print ('%s'%((end-start).total_seconds()))
    #print the useless data and the total data
    print('useless data: '+str(j)+'total data: '+str(len(rawdatas)))
    conn.commit()
    conn.close()

# insert the OD data without the startpoint and endpoint information (example of Chicago City bike data).
def write2databasebike(parameters):
    rawdatas = parameters[0]
    schemas = parameters[1]
    city = parameters[2]
    Oddata = parameters[3]
    year = parameters[4]
    season = parameters[5]
    print("thread %s begin"%(os.getpid()))
    dbname="postgis_Graduation"
    conn = connectdatabase(dbname)
    cur =conn.cursor()
    j=0
    start = datetime.now()
    for rawdata in rawdatas:
        try:       
            duration = int(rawdata[4])
            if duration >86400:
                continue
            start_time ="'"+str(datetime.strptime(rawdata[1], '%m/%d/%Y %H:%M:%S'))+"'"
            end_time = "'"+str(datetime.strptime(rawdata[2],'%m/%d/%Y %H:%M:%S'))+"'"
            SID= int(rawdata[5])
            EID =int (rawdata[7])
            sql = 'select zone_id from %s.cgc_bike_station_%s where id =%s'%(schemas,year,SID)
            cur.execute(sql)
            s_zoneid = cur.fetchone()
            sql = 'select zone_id from %s.cgc_bike_station_%s where id =%s'%(schemas,year,EID)
            cur.execute(sql)
            e_zoneid = cur.fetchone()
            if e_zoneid == None:
                continue
            sql =""" INSERT INTO %s.%s%s%s%s(Duration,start_time, SID, s_zone_id , end_time ,EID, e_zone_id) VALUES (%s, %s, %s, %s, %s, %s, %s);"""%(schemas,city,Oddata,year,season,duration,start_time,SID,s_zoneid[0],end_time,EID,e_zoneid[0])
            cur.execute(sql)
        except Exception as e:
            j+=1
            continue
    end =datetime.now()
    print ('%s'%((end-start).total_seconds()))
    print('useless data: '+str(j)+'total data: '+str(len(rawdatas)))
    conn.commit()
    conn.close()

# insert the OD data (example of New York City taxi data).
def write2databasetaxi(parameters):
    records = parameters[0]
    schemas = parameters[1]
    city = parameters[2]
    Oddata = parameters[3]
    year = parameters[4]
    season = parameters[5]
    print ("thread %s begin"%(str(os.getpid())))
    dbname="postgis_Graduation"
    conn = connectdatabase(dbname)
    cur =conn.cursor()
    j=0
    start =datetime.now()
    for record in records:
        try:
            if record[5] == ''or record[9] == '':
                j+=1
                continue
            Spoint = bikePoint(float(record[5]),float(record[6]))
            Epoint = bikePoint(float(record[9]),float(record[10]))
            geom = bikePoint.bikePoint2Line([Spoint,Epoint],4326)
            startpoint = bikePoint.bikePoint2Geometry(Spoint,4326)
            sql = 'select objectid from %s.NYC_zones where st_contains(geom,%s)'%(schemas,startpoint)
            cur.execute(sql)
            sid = cur.fetchone()
            if sid == None:
                continue
            endpoint =bikePoint.bikePoint2Geometry(Epoint,4326)
            sql = 'select objectid from %s.NYC_zones where st_contains(geom,%s)'%(schemas,endpoint)
            cur.execute(sql)
            eid = cur.fetchone()
            if eid == None:
                continue
            passagernumber=record[3]
            distance = record[4]
            Otime = record[1]
            Etime = record[2]
            duration = (datetime.strptime(Etime,'%Y-%m-%d %H:%M:%S')-datetime.strptime(Otime,'%Y-%m-%d %H:%M:%S')).total_seconds()
            vender_id = "'"+record[0]+"'"
            sql ="""
            INSERT INTO %s.%s%s%s%s(duration, distance, vender_id, start_time, passagernumber, end_time, geom,sid,eid,startpoint,endpoint)
            VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s);
            """ %(schemas,city,Oddata,season,duration,distance,vender_id,"'"+Otime+"'",passagernumber,"'"+Etime+"'",geom,sid[0],eid[0],startpoint,endpoint)
            cur.execute(sql)
        except Exception as e:
            print(str(e))
            print(record)
            continue
    end = datetime.now()
    print(str((end-start).total_seconds()))
    print('useless data: '+str(j)+'total data: '+str(len(records)))
    conn.commit()
    conn.close()

# create the index for the table
def create_index(schema,city,Oddata,startyear,endyear):
    dbname="postgis_Graduation"
    conn = connectdatabase(dbname)
    cur =conn.cursor()
    for i in range(startyear,endyear):
        for j in range(1,5):
            # create the index for the start_time.
            sql ="create index if not exists %s.%s%s%s%s_start_time_index on nyc.new_cgcbike%s%s(start_time) "%(schema,city,Oddata,i,j,i,j)
            cur.execute(sql)
            # create the index for the s_zone_id and e_zone_id.
            sql ="create index if not exists %s.%s%s%s%s_s_zone_id_e_index on nyc.new_cgcbike%s%s(s_zone_id,e_zone_id) "%(schema,city,Oddata,i,j,i,j)
            cur.execute(sql)
            # create the index for the SID and EID.
            sql ="create index if not exists %s.%s%s%s%s_sid_eid_index on nyc.new_cgcbike%s%s(sid,eid) "%(schema,city,Oddata,i,j,i,j)
            cur.execute(sql)
    conn.commit()
    conn.close()

# create the table for the bike station
def creat_bike_station(schema,city,startyear,endyear):
    dbname="postgis_Graduation"
    conn = connectdatabase(dbname)
    cur =conn.cursor()
    for i  in range(startyear,endyear):
        sql = "create table if not exists %s.%s_bike_station_%s (id bigint,geom geometry,zone_id integer)"%(schema,city,i)
        cur.execute(sql)
    conn.commit()
    
# map the station to the zone id
def station2zoneid(filepath,schema,city,year):
    dbname="postgis_Graduation"
    conn = connectdatabase(dbname)
    cur =conn.cursor()
    station_df=pd.read_csv(filepath)
    station_df=station_df[['id','latitude','longitude']]
    station_df['id']=station_df['id'].astype(int)
    #check the duplicate station and filter the station without the zone id
    station_df['latitude']=station_df['latitude'].astype(float)
    station_df['longitude']=station_df['longitude'].astype(float)
    stations=station_df.to_numpy()
    stations_without_duplicate={}
    for station in stations:
        if station[0] not in stations_without_duplicate.keys():
            #map the station to the zone id
            sql ="select area_numbe from %s.CGC_zones where st_contains(geom,ST_GeomFromText('POINT(%s %s)',4326))"%(schema,station[2],station[1])
            cur.execute(sql)
            queryresult = cur.fetchone()
            # the station without the zone id will be filtered
            if queryresult == None:
                continue
            else:
                station = list(station)
                station.append(queryresult[0])
                stations_without_duplicate[station[0]]=station
    #input the stations_without_duplicate into the database
    for key in stations_without_duplicate.keys():
        sql ="insert into %s.%s_bike_station_%s (id, geom,zone_id) values(%s,ST_GeomFromText('POINT(%s %s)',4326),%s)"%(schema,city,year,stations_without_duplicate[key][0],stations_without_duplicate[key][2],stations_without_duplicate[key][1],stations_without_duplicate[key][3])
        cur.execute(sql)
    conn.commit()
    conn.close()

if __name__ == '__main__':
    dbname="postgis_Graduation"# the database name you defined in PostgreSQL
    conn = connectdatabase(dbname)
    # create_bike_od_table_points(conn,2016,2019,'public','nyc','bike)
    # create_bike_od_table(conn,2016,2018,'public','cgc','bike')
    # create_taxi_od_table(conn,2016,2019,'public','nyc','taxi')
    # creat_bike_station('public','cgc',2016,2018)
    # station2zoneid('../../Data/CGC/Bike/2016/station_infor/Divvy_Stations_2016_Q1Q2Q3Q4.csv','public','cgc',2016)
    # putinoddata('nyc','public','bike',2016,1,4) the first season of the data in NYC 2016
    # create_index('public','nyc','bike',2016,2019)
