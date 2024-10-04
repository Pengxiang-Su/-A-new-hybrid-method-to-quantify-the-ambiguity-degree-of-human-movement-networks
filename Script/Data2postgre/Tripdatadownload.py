from azureml.opendatasets import NycTlcYellow
from azureml.opendatasets import PublicHolidays
import psycopg2
from dateutil import parser
import io

#connect to the database
def connectdatabase(databasename):
    conn = psycopg2.connect(dbname=databasename, user="postgres", password="123456")
    return conn

#get the data of NYCTlcYellow
def get_taxidata(schemas,city,Oddata,year,season):
    dbname="postgis_Graduation"
    conn = connectdatabase(dbname)
    cur=conn.cursor()
    # create the table to store the data
    sql='create table if not exists %s.%s%s%s%s (vendorID text, start_time timestamp,end_time timestamp, passengerCount integer,tripDistance real,sid integer,eid integer)'%(schemas,city,Oddata,year,season)
    cur.execute(sql)
    # conn.commit()
    #define the time period of the data
    start_dates=['2017-04-01']
    end_dates=['2017-07-01']
    for i in range(len(start_dates)):
        start_date = parser.parse(start_dates[i])
        end_date=parser.parse(end_dates[i])
        #define the columes of the table
        parameter = ['vendorID','lpepPickupDatetime','lpepDropoffDatetime','passengerCount','tripDistance','puLocationId','doLocationId']
        #get the data of NYCTlcYellow
        nyc_tlc = NycTlcYellow(start_date=start_date, end_date=end_date,cols=parameter)
        nyc_tlc_df = nyc_tlc.to_pandas_dataframe()
        nyc_tlc_df=nyc_tlc_df[parameter]
        #change the name of the columns
        nyc_tlc_df1=nyc_tlc_df.rename(columns={'lpepPickupDatetime':'start_time','lpepDropoffDatetime':'end_time','puLocationId':'sid','doLocationId':'eid','tripDistance':'distance'})
        output=io.StringIO()
        nyc_tlc_df1.to_csv(output,'\t',index=False,header=False)
        output1 = output.getvalue()
        #write the data into the database
        cur.copy_from(io.StringIO(output1),'%s.%s%s%s%s'%(schemas,city,Oddata,year,season))
        # conn.commit()
    conn.close()

#get the public holidays data
def get_holidaysdata(schemas,year):
    dbname="postgis_Graduation"
    conn = connectdatabase(dbname)
    cur=conn.cursor()
    #define the columes of the table
    parameter = ['countryOrRegion', 'holidayName', 'normalizeHolidayName', 'isPaidTimeOff', 'countryRegionCode', 'date']
    #create the table to store the data
    sql='create table if not exists %s.holidays_%s (countryOrRegion text, holidayName text, normalizeHolidayName text, isPaidTimeOff text, countryRegionCode text, date timestamp)'%(schemas,year)
    cur.execute(sql)
    conn.commit()
    start_dates=['2015-01-01']
    end_dates=['2015-12-31']
    start_date = parser.parse(start_dates[0])
    end_date=parser.parse(end_dates[0])
    hol = PublicHolidays(start_date=start_date, end_date=end_date)
    hol_df = hol.to_pandas_dataframe()
    hol_df=hol_df[parameter]
    output=io.StringIO()
    hol_df.to_csv(output,'\t',index=False,header=False)
    output1 = output.getvalue()
    #write the data into the database
    cur.copy_from(io.StringIO(output1),'%s.holidays_%s'%(schemas,year))
    conn.commit()
    conn.close()

if __name__ == '__main__':
    get_taxidata('public','nyc','taxi','2017','2')
    # get_holidaysdata('public','2015')
    