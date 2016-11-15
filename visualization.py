from pandas import *
from numpy import *
import matplotlib.pyplot as plt
#from configuration import CONFIG
#data = read_csv('train_2011_2012_2013.csv', sep = ';')
#print(read_csv('train_2011_2012_2013.csv', sep = ';').shape)#labels = read_csv('train_2011_2012_2013.csv', sep = ';', nrows = 2)
#data.columns = labels.columns
#data = data.ix[:,CONFIG.useful_columns]
#data.to_csv('train_sample.csv',sep = ";")

#Moyenne cumulative au cours du temps

def row_iterator(path):
    for row in read_csv(path, sep=';', chunksize = 20000):
        yield row

def cumulative_mean():
    mean = [0]
    nb_date = 0
    k = 0
    it = row_iterator('train_2011_2012_2013.csv')
    for row in it:
        k += 1
        row_date = row.ix[:,["DATE","CSPL_RECEIVED_CALLS"]].groupby(["DATE"]).sum()
        mean_before = mean[-1]*nb_date
        nb_date += row.shape[0]    
        mean.append((mean_before + row_date['CSPL_RECEIVED_CALLS'].as_matrix().sum())/nb_date)
        
        print(k, mean[-1])
    
    plt.figure()
    plt.plot(mean)
    plt.legend()
    plt.show()
    
def MOTHERFUCKER():
    it = row_iterator('train_2011_2012_2013.csv')
    values=[]
    last_date = 0    
    for chunk in it:
        row_date = chunk.ix[:,["DATE","CSPL_RECEIVED_CALLS"]].groupby(['DATE']).sum().as_matrix()
        if last_date == row_date[0,0]:
            values[-1] += row_date[0,0]
        last_date = row_date[-1, 0]
        for row in row_date:
            values.append(row[0])
    plt.figure()
    plt.plot(values)
    plt.legend()
    plt.show()
    
MOTHERFUCKER()



    
    
    