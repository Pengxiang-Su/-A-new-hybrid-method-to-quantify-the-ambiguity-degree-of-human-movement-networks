import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2


# define the name of the hoildays in America
hoildays_newyork=["New Year's Day","Martin Luther King Jr. Day","Presidents' Day","Memorial Day","Independence Day","Labor Day / May Day","Columbus Day","Veterans Day","Thanksgiving Day","Christmas Day"]
# define a colors list with different colors to store the colors of each hoilday
colors=['#FF0000','#04819E','#00CC00','#1924B1','#FF7F00','#FCBAD3','#AA96DA','#A8D8EA','#EBE76C','#F0B86E']

def connectdatabase(databasename):
    conn = psycopg2.connect(dbname=databasename, user="postgres", password="123456")
    return conn

#visualize the membership matrix
# start_date: the start date of the OD data
# city: the city of the OD data
# trange: the number of days you want to visualize
# Oddata: the type of the OD data, it can be 'taxi' or 'bike'    
def membership_plot(start_date,city,trange,Oddata):    
    start_date=pd.to_datetime(start_date)
    #get the year of the start_date
    year=start_date.year
    #read the membership data
    if Oddata =='taxi':
        membership=pd.read_csv('result/%s/Clustering_results/max_min_log/%s/%s/%s_membership_matrix.csv'%(city,Oddata,year,year))
    if Oddata =='bike':
        membership=pd.read_csv('Result/%s/Clustering_results/max_min_log/%s/%s/%s_membership_matrix.csv'%(city,Oddata,year,year))
    #get the first row of the membership
    membership=membership.values[0,1:]
    days_of_year=[start_date+pd.Timedelta(days=i) for i in range(trange)]
    #get the weekdays of the days_of_year
    weekdays=[day.weekday()+1 for day in days_of_year]
    #if the weekday is 7, then change it to 0
    weekdays=[0 if weekday==7 else weekday for weekday in weekdays]
    #concatenate the days_of_year and weekdays
    days_of_year=pd.DataFrame({'days_of_year':days_of_year,'weekdays':weekdays})
    #group by weekdays
    days_of_year=days_of_year.groupby('weekdays')
    #number of weekdays
    number_of_weekdays=[len(days_of_year.get_group(i)) for i in range(7)]
    #get the membership of each week and store them in the weekday_membership array
    weekday_membership=[]
    for number in number_of_weekdays:
        weekday_membership.append(membership[:number])
        membership=membership[number:]
    if city =='SHC':
        for i in range(len(weekday_membership)):
            if len (weekday_membership[i]) <5:
                    weekday_membership[i]=np.insert(weekday_membership[i],len(weekday_membership[i]),None)
    else:
        start_weekdays=start_date.weekday()+1
        for i in range(1,start_weekdays):
            #add 0 in the first element of the weekday_membership
            weekday_membership[i]=np.insert(weekday_membership[i],0,None)
        for i in range(start_weekdays+1,8):
            if i == 7:
                #add 0 in the first element of the weekday_membership
                if len (weekday_membership[0]) <53:
                    weekday_membership[0]=np.insert(weekday_membership[0],len(weekday_membership[0]),None)
            else:
                #add 0 in the last element of the weekday_membership
                if len (weekday_membership[i]) <53:
                    weekday_membership[i]=np.insert(weekday_membership[i],len(weekday_membership[i]),None)
    hoildays=get_hoildays(year)
    hoildays[0]=1
    plot_membership([weekday_membership[0],weekday_membership[6]],city,hoildays,'weekend')
    plot_membership(weekday_membership[1:-1],city,hoildays,'weekday')

def plot_membership(weekday_membership,city,hoildays,workrest):
    #set the size of the figure
    plt.figure(figsize=(15,8))
    #set the distance of the figure
    plt.subplots_adjust(left=0.1,right=0.95,top=0.86,bottom=0.15)
    #define a colors list with different colors to store the colors of each hoilday
    if workrest=='weekday':
        colors=['#1f77b4','#2ca02c','#9467bd','#17becf','#008080'] #['#e377c2','#2ca02c','#8C564B','#ff7f0e','#9467bd']
    if workrest=='weekend':
        colors=['#ff9896','#d62728'] #['#d62728','#1f77b4']
    if city=='SHC':
        x=[i for i in range(14,19)]
        #plot the membership of different weekdays
        for i in range(len(weekday_membership)):
            plt.plot(x,weekday_membership[i],c=colors[i],label='Weekday %s'%(i+1), linewidth=5, alpha=0.6)
        ax=plt.subplot()
        ax.tick_params(axis='y', direction='out', pad=10)
        ax.tick_params(axis='x', direction='out', pad=20)
        #set the y axis to show the number of the decimal places
        plt.ylim(0,1)
        #set the x axis to show the number of the decimal places
        plt.xlim(14,18)
        plt.xticks([14,15,16,17,18])
    else:
        x=[i for i in range(1,54)]
        #plot the membership of different weekdays
        for i in range(len(weekday_membership)):
            plt.plot(x,weekday_membership[i],c=colors[i],label='Weekday %s'%(i+1), linewidth=5, alpha=0.6)
        #plot the hoildays
        for hoilday in hoildays:
            plt.vlines(hoilday,0,1,colors='black',linestyles='--', linewidth=1.0)
        ax=plt.subplot()
        ax.tick_params(axis='y', direction='out', pad=10)
        ax.tick_params(axis='x', direction='out', pad=20)
        #set the y axis to show the number of the decimal places
        plt.ylim(0,1)
        #set the x axis to show the number of the decimal places
        plt.xlim(0,54)
    plt.xticks(fontsize=28,fontproperties='Time New Roman')
    plt.yticks(fontsize=28,fontname='Time New Roman')
    plt.xlabel('Number of week',fontsize=35,fontproperties='Time New Roman',fontweight='bold')
    plt.ylabel('Probability of weekends',fontsize=35,fontproperties='Time New Roman',fontweight='bold')
    plt.show()


#visualize the scatter plot of the clustering center
def scatter_plot(start_date,city,trange,Oddata):
    start_date=pd.to_datetime(start_date)
    #get the year of the start_date
    year=start_date.year
    #read the clustering center data
    if Oddata =='taxi':
        points=pd.read_csv('result/%s/Clustering_results/max_min_log/%s/%s/%s_clustering_centers.csv'%(city,Oddata,year,year))
    if Oddata =='bike':
        points=pd.read_csv('result/%s/Clustering_results/max_min_log/%s/%s/%s_clustering_centers.csv'%(city,Oddata,year,year))
    #get the first two rows of the points
    points=points.values[0:2,1:]
    days_of_year=[start_date+pd.Timedelta(days=i) for i in range(trange)]
    #get the weekdays of the days_of_year
    weekdays=[day.weekday()+1 for day in days_of_year]
    #if the weekday is 7, then change it to 0
    weekdays=[0 if weekday==7 else weekday for weekday in weekdays]
    #concatenate the days_of_year and weekdays
    days_of_year=pd.DataFrame({'days_of_year':days_of_year,'weekdays':weekdays})
    days_of_year=days_of_year.groupby('weekdays')
    number_of_weekdays=[len(days_of_year.get_group(i)) for i in range(7)]
    weekday_points=[]
    for number in number_of_weekdays:
        weekday_points.append(points[0:2,:number])
        points=points[0:2,number:]
    plot_scatter(weekday_points)

def plot_scatter(scatters):
    plt.figure(figsize=(15,12))
    plt.subplots_adjust(left=0.17,right=0.95,top=0.95,bottom=0.12)
    colors=['#ff9896','#1f77b4','#2ca02c','#9467bd','#17becf','#008080','#d62728']
    #plot the scatter of different weekdays
    for i in range(len(scatters)):
        if i == 0 or i ==6:
            plt.scatter(scatters[i][0,:],scatters[i][1,:], c=colors[i], label='Weekday %s'%(i+1), marker='o',alpha=0.6, s=200)
        else:
            plt.scatter(scatters[i][0,:],scatters[i][1,:], c=colors[i], label='Weekday %s'%(i+1), marker='o',alpha=0.6, s=200)
    ax=plt.subplot()
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: '%1.3f' % x))
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: '%1.3f' % x))
    plt.xticks(fontsize=28,fontproperties='Time New Roman')
    plt.yticks(fontsize=28,fontname='Time New Roman')
    ax.tick_params(axis='y', direction='out', pad=10)
    ax.tick_params(axis='x', direction='out', pad=20)
    plt.xlabel('Similarity of weekends',fontsize=35,fontproperties='Time New Roman',fontweight='bold')
    plt.ylabel('Similarity of weekdays',fontsize=35,fontproperties='Time New Roman',fontweight='bold')
    plt.show()


def get_hoildays(year):
    conn=connectdatabase('postgis_Graduation')
    cur=conn.cursor()
    #the example of select the hoildays data from the hoildays table you created in Tripdatadownload.py
    cur.execute("select date from nyc.holidays_%s where countryorregion=%s order by date"%(year,"'United States'"))
    hoildays=cur.fetchall()
    hoildays=[hoilday[0] for hoilday in hoildays]
    hoildays_days=[]
    for hoilday in hoildays:
        if year==2016 or year==2017:
            week=hoilday.isocalendar()[1]+1
        else:
            week=hoilday.isocalendar()[1]
        if week>53:
            week=1
        hoildays_days.append(week)
    return hoildays_days


if __name__ == '__main__':
    # example of how to use the function(plot the membership matrix)
    membership_plot('2016-01-01','NYC',366,'taxi')#Figure 3(1)
    membership_plot('2017-01-01','NYC',365,'taxi')#Figure 3(2)
    membership_plot('2018-01-01','NYC',365,'taxi')#Figure 3(3)
    membership_plot('2015-01-01','CGC',365,'taxi')#Figure 4(1)
    membership_plot('2016-01-01','CGC',366,'taxi')#Figure 4(2)
    membership_plot('2017-01-01','CGC',365,'taxi')#Figure 4(3)
    membership_plot('2018-01-01','CGC',365,'taxi')#Figure 4(4)
    membership_plot('2015-04-01','SHC',30,'taxi')#Figure 5
    membership_plot('2016-01-01','NYC',366,'bike')#Figure 6(1)
    membership_plot('2017-01-01','NYC',365,'bike')#Figure 6(2)
    membership_plot('2018-01-01','NYC',365,'bike')#Figure 6(3)
    membership_plot('2016-01-01','CGC',366,'bike')#Figure 7(1)
    membership_plot('2017-01-01','CGC',365,'bike')#Figure 7(2)
    # the Figure 8 is plot through the online tool https://www.chiplot.online/group_line_plot.html
    # example of how to use the function(plot the clustering centers)
    scatter_plot('2016-01-01','NYC',366,'taxi')#Figure 9(1)
    scatter_plot('2017-01-01','NYC',365,'taxi')#Figure 9(2)
    scatter_plot('2018-01-01','NYC',365,'taxi')#Figure 9(3)
    scatter_plot('2016-01-01','NYC',366,'bike')#Figure 9(4)
    scatter_plot('2017-01-01','NYC',365,'bike')#Figure 9(5)
    scatter_plot('2018-01-01','NYC',365,'bike')#Figure 9(6)
    scatter_plot('2015-01-01','CGC',365,'taxi')#Figure 10(1)
    scatter_plot('2016-01-01','CGC',366,'taxi')#Figure 10(2)
    scatter_plot('2017-01-01','CGC',365,'taxi')#Figure 10(3)
    scatter_plot('2018-01-01','CGC',365,'taxi')#Figure 10(4)
    scatter_plot('2016-01-01','CGC',366,'bike')#Figure 10(5)
    scatter_plot('2017-01-01','CGC',365,'bike')#Figure 10(6)
    scatter_plot('2015-04-01','SHC',30,'taxi')#Figure 10(7)