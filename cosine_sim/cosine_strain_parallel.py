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
#syntax: python cleantxtparallel.py [csvin] [csvout] [num_chunks] [clean/collate/both]
#num_chunks = int(sys.argv[3])
#python -i cosine_strain_parallel.py 'matched_42_83.csv' 'output_matched_42.csv' 16
#WORD = re.compile(r'\w+')
stop =  stopwords.words('english')
WORD = re.compile('[a-z]+')
def text_to_vector(x):
    x = x.lower()
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

input = sys.argv[1] #'uniques82.csv'
data = pd.read_csv(input)
strain = data.loc[:,'strain'] #data.loc[:,'Strain']
counts = strain.value_counts()
index = [i for i in range(len(strain)) if counts[strain[i]] > 50 ]
strain50 = data.iloc[index, :];
#strain50 = data
strain50 = strain50.fillna(' ')
#strain_grouped = strain50.groupby(['Strain'])['ProductDescription'].apply(lambda x: ' '.join(x)) 
strain_grouped = pd.DataFrame(strain50.groupby(['med_rec','scrape','st','strain'])['prod_desc'].apply(lambda x: ' '.join(x)))
regroup = strain_grouped.reset_index().groupby(['med_rec','scrape','st'])
chunk = int(sys.argv[3])
nrow = len(regroup)/chunk + 1
#manager = Manager()
#cosine_strain_list =  manager.list()

output = sys.argv[2]
print "Number of Groups: " + str(len(regroup))

def cosine_state_scrape_strain(regroup, start, end):
    for index, group in enumerate(regroup):
        if (index >= start and index < end):
            med_rec = group[0][0]; scrape = group[0][1];state = group[0][2]
            print "Group" + str(index)
            cosine_strain(index, group[1], 0, len(group[1]), med_rec, scrape, state)


def cosine_strain(index, strain_grouped, start, end, med_rec, scrape, state):
    #df = pd.DataFrame(columns=["Scrape","State","Strain1","Strain2", "Cosine Similarity"])
    with open(output[:-4] + '_' + str(index) +'.csv', 'wb') as csvtarget:
        chunk_writer = csv.writer(csvtarget)
        chunk_writer.writerow(['med_rec','scrape', 'state', 'strain1','strain2','cosine'])
        num_strain = len(strain_grouped)
        strain_temp = strain_grouped.reset_index()
        if end > num_strain:
            end = num_strain
        for i in range(start, end):
            #print i
            strain1 = strain_temp['strain'][i]
            for j in range(i+1, num_strain):
                strain2 = strain_temp['strain'][j]
                cosine = get_cosine(text_to_vector(strain_temp['prod_desc'][i]), text_to_vector(strain_temp['prod_desc'][j]))
                #df2 = pd.DataFrame({'Strain1': [strain1], 'Strain2': [strain2],'Cosine Similarity':[cosine]})
                #cosine_strain_list.append([scrape, state, strain1,strain2,cosine])
                chunk_writer.writerow([med_rec, scrape, state, strain1,strain2,cosine])
    return

processes = [Process(target=cosine_state_scrape_strain, args=(regroup, x*nrow, (x+1)*nrow)) for x in range(chunk)]
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
#if(False):
#    output_df = pd.DataFrame(columns=["Scrape","State","Strain1","Strain2", "Cosine Similarity"]); row = 0
#    for i in range(len(cosine_strain_list)):
#        print i
#        output_df.loc[row, 'Scrape'] =cosine_strain_list[i][0]
#        output_df.loc[row, 'State'] =cosine_strain_list[i][1]
#        output_df.loc[row, 'Strain1'] =cosine_strain_list[i][2]
#        output_df.loc[row, 'Strain2'] =cosine_strain_list[i][3]
#        output_df.loc[row, 'Cosine Similarity'] =cosine_strain_list[i][4]
#        row += 1

#output = sys.argv[2] 
#output_df.to_csv(output,header = True, encoding='utf-8', index = False)

#same state + scrape
import csv
import os.path
output = 'output_matched.csv'
with open(output,'wb') as outfile:
    #outfile.truncate()
    for i in range(555):
        fname = output[:-4] + '_' + str(i) +'.csv'
        if os.path.isfile(fname):
            outfile.write(open(fname, 'r').read())
        #os.remove(fname)

