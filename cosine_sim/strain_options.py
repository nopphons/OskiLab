import csv, re, math, time
from collections import Counter
import pandas as pd
from decimal import Decimal, getcontext
import numpy as np
from datetime import date, timedelta
from nltk import PorterStemmer
from nltk.corpus import stopwords
from collections import defaultdict
from multiprocessing import Process, Manager
import sys
import os
from dateutil import parser
#python -i cosine_strain_parallel.py 'matched_42_83.csv' 'output_matched_42.csv' 16
print('Type 1 for Yes, 0 for No')
chunk = int(raw_input('How many chunk/multiprocessors to use?: '))
option1 = int(raw_input('Option 1: Concatenate by strain-scrape-st-rec: '))
option2 = int(raw_input('Option 2: Compare observations that are in the same state: '))
option3 = int(raw_input('Option 3: Compare observations that are in the same legal regime (REC): '))
option4 = int(raw_input('Option 4: Do you want to stem English words?: '))
stop =  stopwords.words('english')
WORD = re.compile('[a-z]+')
getcontext().prec = 4
def text_to_vector(x):
    x = x.lower()
    words = WORD.findall(x)
    if(option4 == 1):
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
output = sys.argv[2]
data = pd.read_csv(input)
data = data.fillna(' ')
regroup = data; group_by = ['scrape']
if(option1 == 1):
    strain_grouped = pd.DataFrame(data.groupby(['med_rec','scrape','st','strain'])['prod_desc'].apply(lambda x: ' '.join(x)))
    regroup = strain_grouped.reset_index()
if(option2 == 1):
    group_by.append('st')
if(option3 == 1):
    group_by.append('med_rec')

strain_grouped = regroup.groupby(group_by)
nrow = len(regroup)/chunk + 1
print "Number of Groups: " + str(len(strain_grouped))


def cosine_strain(index, subgroup, start, end):
    if(option1 != 1):
        with open(output[:-4] + '_' + str(index)+ '_' + str(start) + '-'+ str(end-1) +'.csv', 'wb') as csvtarget:
            chunk_writer = csv.writer(csvtarget)
            chunk_writer.writerow(['scrape','med_rec1', 'med_rec2', 'state1', 'state2', 'imagekey1','imagekey2',
                'dispensaryname1','dispensaryname2','strain1' ,'strain2','med_rec_match','state_match','disp_match','strain_match','cosine',
                'price_dist', 'age_diff', 'pop_diff', 'form_dist', 'rating_dist','THC_diff','CBD_diff','CBN_diff','cat_match','vote_dist'])
            num_strain = len(subgroup)
            strain_temp = subgroup.reset_index()
            if end > num_strain:
                end = num_strain
            for i in range(start, end):
                #print i
                scrape = strain_temp['scrape'][i]
                med_rec1 = strain_temp['med_rec'][i]
                state1 = strain_temp['st'][i]
                img1 = strain_temp['image_key'][i]
                dispensaryname1 = strain_temp['dispensaryname'][i]
                strain1 = strain_temp['strain'][i]
                prod_desc1 = strain_temp['prod_desc'][i]
                price1 = strain_temp['eighth'][i]
                date1 = strain_temp['joindate'][i]
                totalview1 = strain_temp['totalviews'][i]
                deliv1 = strain_temp['deliv'][i]
                rating1 = strain_temp['rating'][i]
                THC1 = strain_temp['thc'][i]
                CBD1 = strain_temp['cbd'][i]
                CBN1 = strain_temp['cbn'][i]
                product1 = strain_temp['producttype'][i]
                vote1 = strain_temp['yesPer'][i]

                for j in range(i+1, num_strain):
                    med_rec2 = strain_temp['med_rec'][j]
                    state2 = strain_temp['st'][j]
                    img2 = strain_temp['image_key'][j]
                    dispensaryname2 = strain_temp['dispensaryname'][j]
                    strain2 = strain_temp['strain'][j]
                    prod_desc2 = strain_temp['prod_desc'][j]
                    price2 = strain_temp['eighth'][j]
                    date2 = strain_temp['joindate'][j]
                    totalview2 = strain_temp['totalviews'][j]
                    deliv2 = strain_temp['deliv'][j]
                    rating2 = strain_temp['rating'][j]
                    THC2 = strain_temp['thc'][j]
                    CBD2 = strain_temp['cbd'][j]
                    CBN2 = strain_temp['cbn'][j]
                    product2 = strain_temp['producttype'][j]
                    vote2 = strain_temp['yesPer'][j]

                    med_rec_match = int(med_rec1 == med_rec2)
                    state_match = int(state1 == state2)
                    disp_match = int(dispensaryname1 == dispensaryname2)
                    strain_match = int(strain1 == strain2)
                    cosine = get_cosine(text_to_vector(prod_desc1), text_to_vector(prod_desc2))
                    age_diff = abs((parser.parse(date1)- parser.parse(date2)).days)
                    pop_diff = int(abs(totalview1-totalview2))
                    form_dist = int(deliv1 == deliv2)
                    rating_dist = int(rating1 == rating2)
                    THC_diff = ' ' ; CBD_diff = ' '; CBN_diff = ' ' ; vote_dist = ' '; price_dist = ' '; rating_dist = ' '
                    if price1 != ' ' and price2 != ' ':
                        price_dist = abs(float(Decimal(price1)-Decimal(price2)))
                    if rating1 != ' ' and rating2 != ' ':
                        rating_dist = abs(float(Decimal(rating1)-Decimal(rating2)))
                    if THC1 != ' ' and THC2 != ' ':
                        THC_diff = abs(float(Decimal(THC1[:-1])-Decimal(THC2[:-1])))
                    if CBD1 != ' ' and CBD2 != ' ':
                        CBD_diff = abs(float(Decimal(CBD1[:-1])-Decimal(CBD2[:-1])))
                    if CBN1 != ' ' and CBN2 != ' ':
                        CBN_diff = abs(float(Decimal(CBN1[:-1])-Decimal(CBN2[:-1])))
                    category_match = int(product1 == product2)
                    if vote1 != ' ' and vote2 != ' ':
                        vote_dist = abs(float(Decimal(vote1[:-1])-Decimal(vote2[:-1])))

                    chunk_writer.writerow([scrape, med_rec1, med_rec2, state1, state2, img1,img2, dispensaryname1, dispensaryname2,
                     strain1,strain2, med_rec_match, state_match, disp_match, strain_match, cosine,price_dist,age_diff,pop_diff,
                     form_dist,rating_dist,THC_diff,CBD_diff,CBN_diff,category_match, vote_dist])
    else:
        with open(output[:-4] + '_' + str(index)+ '_' + str(start) + '-'+ str(end-1) +'.csv', 'wb') as csvtarget:
            chunk_writer = csv.writer(csvtarget)
            chunk_writer.writerow(['scrape','med_rec1', 'med_rec2', 'state1', 'state2','strain1' ,'strain2','med_rec_match','state_match','strain_match','cosine'])
            num_strain = len(subgroup)
            strain_temp = subgroup.reset_index()
            if end > num_strain:
                end = num_strain
            for i in range(start, end):
                scrape = strain_temp['scrape'][i]
                med_rec1 = strain_temp['med_rec'][i]
                state1 = strain_temp['st'][i]
                strain1 = strain_temp['strain'][i]
                prod_desc1 = strain_temp['prod_desc'][i]
                for j in range(i+1, num_strain):
                    med_rec2 = strain_temp['med_rec'][j]
                    state2 = strain_temp['st'][j]
                    strain2 = strain_temp['strain'][j]
                    prod_desc2 = strain_temp['prod_desc'][j]

                    med_rec_match = int(med_rec1 == med_rec2)
                    state_match = int(state1 == state2)
                    strain_match = int(strain1 == strain2)
                    cosine = get_cosine(text_to_vector(prod_desc1), text_to_vector(prod_desc2))
                    chunk_writer.writerow([scrape, med_rec1, med_rec2, state1, state2, strain1,strain2, med_rec_match, state_match, strain_match, cosine])
    return


for index, group in enumerate(strain_grouped):
        print "Group" + str(index)
        nrow = len(group[1])/chunk + 1
        processes = [Process(target=cosine_strain, args=(index, group[1], x*nrow, (x+1)*nrow)) for x in range(chunk)]
        # Run processes
        for p in processes:
            p.start()
        # Exit the completed processes
        for p in processes:
            p.join()
            

print "Finished Finding Cosine Similarities for all groups"

import csv
import os.path
#output = 'output_matched.csv'
output = sys.argv[2] 
with open(output,'wb') as outfile:
    chunk_writer = csv.writer(outfile)
    chunk_writer.writerow(['scrape','med_rec1', 'med_rec2', 'state1', 'state2', 'imagekey1','imagekey2',
            'dispensaryname1','dispensaryname2','strain1' ,'strain2','med_rec_match','state_match','disp_match','strain_match','cosine',
            'price_dist', 'age_diff', 'pop_diff', 'form_dist', 'rating_dist','THC_diff','CBD_diff','CBN_diff','cat_match','vote_dist'])
    for fname in os.listdir('.'):
        if fname.endswith('.csv') and fname.startswith(output[:-4]+'_'):
            towrite = open(fname, 'r').read()
            head = len(towrite.split('\r\n')[0])
            outfile.write(towrite[head+2:])
            os.remove(fname)


