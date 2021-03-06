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

input = sys.argv[1]  #'Scrape50_53.csv'
data = pd.read_csv(input)
strain = data.loc[:,'strain'] #data.loc[:,'Strain']
counts = strain.value_counts()
#index = [i for i in range(len(strain)) if counts[strain[i]] > 50 ]
data = data.fillna(' ')
#index = [i for i in range(len(strain)) if data.iloc[i,]['prod_desc'] != ' ']
#strain50 = data.iloc[index, :];
strain50 = data
strain_grouped = strain50.groupby(['scrape'])
regroup = strain_grouped
#strain_grouped = strain50.groupby(['Strain'])['ProductDescription'].apply(lambda x: ' '.join(x)) 
#strain_grouped = pd.DataFrame(strain50.groupby(['med_rec','scrape','st','strain'])['prod_desc'].apply(lambda x: ' '.join(x)))
#regroup = strain_grouped.reset_index().groupby(['med_rec','scrape','st'])
chunk = int(sys.argv[3])
nrow = len(regroup)/chunk + 1
#manager = Manager()
#cosine_strain_list =  manager.list()

output = sys.argv[2]
print "Number of Groups: " + str(len(regroup))


def cosine_strain(index, strain_grouped, start, end, scrape):
    #df = pd.DataFrame(columns=["Scrape","State","Strain1","Strain2", "Cosine Similarity"])
    with open(output[:-4] + '_' + str(index)+ '_' + str(start) + '-'+ str(end-1) +'.csv', 'wb') as csvtarget:
        chunk_writer = csv.writer(csvtarget)
        chunk_writer.writerow(['scrape','med_rec1', 'med_rec2', 'state1', 'state2', 'imagekey1','imagekey2',
            'dispensaryname1','dispensaryname2','strain1' ,'strain2','med_rec_match','state_match','disp_match','strain_match','cosine'])
        num_strain = len(strain_grouped)
        strain_temp = strain_grouped.reset_index()
        if end > num_strain:
            end = num_strain
        for i in range(start, end):
            #print i
            med_rec1 = strain_temp['med_rec'][i]
            img1 = strain_temp['imagekey'][i]
            state1 = strain_temp['st'][i]
            dispensaryname1 = strain_temp['dispensaryname'][i]
            strain1 = strain_temp['strain'][i]
            for j in range(i+1, num_strain):
                med_rec2 = strain_temp['med_rec'][j]
                img2 = strain_temp['imagekey'][j]
                state2 = strain_temp['st'][j]
                dispensaryname2 = strain_temp['dispensaryname'][j]
                strain2 = strain_temp['strain'][j]
                cosine = get_cosine(text_to_vector(strain_temp['prod_desc'][i]), text_to_vector(strain_temp['prod_desc'][j]))
                #df2 = pd.DataFrame({'Strain1': [strain1], 'Strain2': [strain2],'Cosine Similarity':[cosine]})
                #cosine_strain_list.append([scrape, state, strain1,strain2,cosine])
                med_rec_match = int(med_rec1 == med_rec2)
                state_match = int(state1 == state2)
                disp_match = int(dispensaryname1 == dispensaryname2)
                strain_rec_match = int(strain1 == strain2)
                chunk_writer.writerow([scrape, med_rec1, med_rec2, state1, state2, img1,img2, dispensaryname1, dispensaryname2,
                 strain1,strain2, med_rec_match, state_match, disp_match, strain_rec_match, cosine])
    return


for index, group in enumerate(regroup):
        scrape = group[0]
        print "Scrape" + str(index)
        nrow = len(group[1])/chunk + 1
        processes = [Process(target=cosine_strain, args=(index, group[1], x*nrow, (x+1)*nrow,scrape)) for x in range(chunk)]
        # Run processes
        for p in processes:
            p.start()

        # Exit the completed processes
        for p in processes:
            p.join()
            

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
#output = 'output_matched.csv'
output = sys.argv[2] 
with open(output,'wb') as outfile:
    #outfile.truncate()
    for i in range(len(regroup)):
        fname = output[:-4] + '_' + str(i) +'.csv'
        if os.path.isfile(fname):
            outfile.write(open(fname, 'r').read())
        #os.remove(fname)

