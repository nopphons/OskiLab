import csv, re, math, time
from collections import Counter
import pandas as pd
import numpy as np
from datetime import date, timedelta
from nltk import PorterStemmer
from nltk.corpus import stopwords
from collections import defaultdict
from multiprocessing import Process, Manager
import sys
import os
from datetime import datetime
#syntax: python cleantxtparallel.py [csvin] [csvout] [num_chunks] [clean/collate/both]
#num_chunks = int(sys.argv[3])
#python -i cosine_strain_parallel.py 'matched_42_83.csv' 'output_matched_42.csv' 16
#WORD = re.compile(r'\w+')
stem = sys.argv[4]
stop =  stopwords.words('english')
WORD = re.compile('[a-z]+')
def text_to_vector(x):
    x = x.lower()
    words = WORD.findall(x)
    if(stem == 1):
        words = map(PorterStemmer().stem_word,WORD.findall(x))
        words = [w for w in words if not w in stop]
    return Counter(words)

def get_cosine(vec1, vec2):
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])
    sum1 = sum([vec1[x]**2 for x in vec1.keys()])
    sum2 = sum([vec2[x]**2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)
    if not denominator:
        return None
    else:
        return float(numerator) / denominator

input = sys.argv[1]  #Scrape50_53.csv'
data = pd.read_csv(input)
data = data.fillna(' ')
rows = len(data)
chunk = int(sys.argv[3])
nrow = rows/chunk + 1
output = sys.argv[2]

def strain_dyad(start, end):
    #df = pd.DataFrame(columns=["Scrape","State","Strain1","Strain2", "Cosine Similarity"])
    with open(output[:-4] + '_' + str(start) + '-'+ str(end-1) +'.csv', 'wb') as csvtarget:
        chunk_writer = csv.writer(csvtarget)
        chunk_writer.writerow(['strain1','strain2','acquired1','acquired2','strain_same','date_distance','cosine_sim','review_dist','type_dist','method_dist','jaccard_sim','jaccard_flavor'])
        if end > rows:
            end = rows
        for i in range(start, end):
            print i
            strain1 = data['strainname'][i]
            acquired1 = data['acquiredform'][i]
            date1 = data['date'][i]
            numstars1 = data['numberofstars'][i]
            review1 = data['reviewtest'][i]
            type1 = data['method1'][i]
            method1 = data['method2'][i]
            jaccard1 = set()
            for k in range(1,20):
                effect = data['effect'+str(k)][i]
                if effect != " ":
                    jaccard1.add(effect)
            jaccard_flavor1 = set()
            for k in range(1,6):
                flavor = data['flavor'+str(k)][i]
                if flavor != " ":
                    jaccard_flavor1.add(flavor)
            for j in range(i+1, rows):
                strain2 = data['strainname'][j]
                acquired2 = data['acquiredform'][j]
                date2 = data['date'][j]
                numstars2 = data['numberofstars'][j]
                review2 = data['reviewtest'][j]
                type2 = data['method1'][j]
                method2 = data['method2'][j]
                jaccard2 = set()
                for k in range(1,20):
                    effect = data['effect'+str(k)][j]
                    if effect != " ":
                        jaccard2.add(effect)  
                jaccard_flavor2 = set()
                for k in range(1,6):
                    flavor = data['flavor'+str(k)][i]
                    if flavor != " ":
                        jaccard_flavor2.add(flavor)
                strainsame = int(strain1 == strain2) 
                format = '%m/%d/%Y %I:%M %p'
                date_distance = abs((datetime.strptime(date2, format) - datetime.strptime(date1, format)).days)
                cosine = get_cosine(text_to_vector(review1), text_to_vector(review2))
                review_distance = abs(numstars1-numstars2)
                type_dist = int(type1 != type2)
                method_dist = int(method1 != method2)
                if len(jaccard2.union(jaccard1)) != 0:
                    jaccard_sim = float(len(jaccard2.intersection(jaccard1)))/len(jaccard2.union(jaccard1))
                else:
                    jaccard_sim = None
                if len(jaccard_flavor2.union(jaccard_flavor1)) != 0:
                    jaccard_flavor = float(len(jaccard_flavor2.intersection(jaccard_flavor1)))/len(jaccard_flavor2.union(jaccard_flavor1))
                else:
                    jaccard_flavor = None
                chunk_writer.writerow([strain1,strain2,acquired1,acquired2,strainsame,date_distance,cosine,review_distance,type_dist,method_dist,jaccard_sim,jaccard_flavor])
    return

processes = [Process(target=strain_dyad, args=(x*nrow, (x+1)*nrow)) for x in range(chunk)]
# Run processes
for p in processes:
    p.start()

# Exit the completed processes
for p in processes:
    p.join()
    #if p.is_alive():
    #    print "ALIVE"
    #   process.terminate()
print "Finished Finding Cosine Similarities for all groups"

import csv
import os.path
#output = 'output_matched.csv'
output = sys.argv[2] 
with open(output,'wb') as outfile:
    for fname in os.listdir('.'):
        if fname.endswith('.csv') and fname.startswith(output[:-4]+'_'):
            outfile.write(open(fname, 'r').read())







