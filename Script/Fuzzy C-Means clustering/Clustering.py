from sklearn.metrics import accuracy_score
import numpy as np
import pandas as pd
import os
from skimage.metrics import structural_similarity as Ssim
import cv2
from datetime import datetime
import psycopg2
import skfuzzy as fuzz

def connectdatabase(databasename):
    conn = psycopg2.connect(dbname=databasename, user="postgres", password="123456")
    return conn

def getdata(datapath,labelpath,datatype,filter=False):
    imgs=[]
    for root,dirs,files in os.walk(datapath):
        for i in range(len(dirs)):
            if i >6:
                break
            for afile in os.listdir(os.path.join(datapath,dirs[i])):
                filepath = os.path.join('%s/%s'%(datapath,dirs[i]),afile)
                if datatype=='png':
                    img=cv2.imread(filepath,0)
                elif datatype=='csv':
                    img=np.loadtxt(filepath,delimiter=',')
                imgs.append(img)
        break
    if filter:
        labels=pd.DataFrame(pd.read_csv(labelpath),columns=['0','1','2'])
        labels=labels[labels['2']==0].to_numpy()[:,0:2]
    else:
        labels=pd.DataFrame(pd.read_csv(labelpath),columns=['0','1']).to_numpy()
    labels=labels.tolist()
    return imgs,labels
           
 
def graph_cmeans(city,stand_type,Oddata,year):
    start=datetime.now()
    #get the data and labels
    if stand_type=='max_min':
        datapath='Result/%s/Clustering_results/%s/%s/%s'%(city,stand_type,Oddata,year)
        labelpath='Result/%s/Clustering_results/%s/%s/%s/%slabels.csv'%(city,stand_type,Oddata,year,year)
        imgs,labels=getdata(datapath,labelpath,'csv')
    elif stand_type=='max_min_log':
        datapath='Result/%s/Clustering_results/%s/%s/%s'%(city,stand_type,Oddata,year)
        labelpath='Result/%s/Clustering_results/%s/%s/%s/%slabels.csv'%(city,stand_type,Oddata,year,year)
        imgs,labels=getdata(datapath,labelpath,'png')
    y_test_label=[x[1] for x in labels]
    down=np.array(y_test_label)
    up=np.array([1-x for x in y_test_label])
    initmembership=np.vstack((up,down))
    G_vectors=np.zeros((len(imgs),len(imgs)))
    for i in range(len(imgs)):
            for j in range (i,len(imgs)):
                mssim=Ssim(imgs[i],imgs[j],data_range=0.01,win_size=3)
                G_vectors[i,j]=mssim
                G_vectors[j,i]=G_vectors[i,j] 
    clusternumber=2
    cntr, u, u0, d, jm, p, fpc = fuzz.cluster.cmeans(
        G_vectors, clusternumber, 2, error=0.005, maxiter=10000,init=initmembership)
    #output the clustering centers
    pd.DataFrame(cntr).to_csv('Result/%s/Clustering_results/%s/%s/%s/%s_clustering_centers.csv'%(city,stand_type,Oddata,year,year))
    y_pred = np.argmax(u,axis=0)
    y_test_label=[x[1] for x in labels]
    #output the membership matrix
    pd.DataFrame(u).to_csv('Result/%s/Clustering_results/%s/%s/%s/%s_membership_matrix.csv'%(city,stand_type,Oddata,year,year))
    end=datetime.now()
    print(year,' ',fpc,' ',(accuracy_score(y_test_label, y_pred)*100),' ','时间%s'%(end-start).total_seconds())


if __name__ == '__main__':
    graph_cmeans('CGC','max_min_log','taxi',2015)
    #the following code is the example of how to use the function
    #graph_cmeans('CGC','max_min_log','taxi',2016)
    #graph_cmeans('CGC','max_min_log','bike',2016)
    #graph_cmeans('CGC','max_min_log','taxi',2017)
    #graph_cmeans('CGC','max_min_log','bike',2017)
    #graph_cmeans('CGC','max_min_log','taxi',2018)
    #graph_cmeans('CGC','max_min','taxi',2015)
    #graph_cmeans('CGC','max_min','taxi',2016)
    #graph_cmeans('CGC','max_min','bike',2016)
    #graph_cmeans('CGC','max_min','taxi',2017)
    #graph_cmeans('CGC','max_min','bike',2017)
    #graph_cmeans('CGC','max_min','taxi',2018)
    #For the clustering results of bike you have to add the missing column 
    #(correspond to the missing data) to keep the format consistent
    #graph_cmeans('CGC','max_min_log','bike',2016)
    #graph_cmeans('NYC','max_min_log','taxi',2016)
    #graph_cmeans('NYC','max_min_log','bike',2016)
    #graph_cmeans('NYC','max_min_log','taxi',2017)
    #graph_cmeans('NYC','max_min_log','bike',2017)
    #graph_cmeans('NYC','max_min_log','taxi',2018)
    #graph_cmeans('NYC','max_min','bike',2018)
    #graph_cmeans('NYC','max_min','taxi',2016)
    #graph_cmeans('NYC','max_min','bike',2016)
    #graph_cmeans('NYC','max_min','taxi',2017)
    #graph_cmeans('NYC','max_min','bike',2017)
    #graph_cmeans('NYC','max_min','taxi',2018)
    #graph_cmeans('NYC','max_min','bike',2018)

