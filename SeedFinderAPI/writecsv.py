import json
import csv
import pandas as pd
import datetime


with open('data.json') as outfile:
    data = json.load(outfile)

todays_date = datetime.datetime.now().date()
columns = ['date']; indexs = range(len(data))
df = pd.DataFrame(todays_date, index = indexs, columns=columns)
df =df.reindex(columns=['name','link','br_name','description','pic','days','auto','types','review_count','review_average','in_count',
	'in_stretch_from','in_stretch_to','in_stretch_percent','in_flowering_to','in_flowering_from','in_flowering_avg','in_yield'
	,'out_count','out_stretch_from','out_stretch_to','out_stretch_percent','out_flowering_to','out_flowering_from','out_flowering_avg','out_yield', 'strength']) 


for i in range(len(data)):
	print i
	df.loc[i,'name'] = data[i]['name'] if data[i]['name'] else ''
	if data[i]['links']:
		df.loc[i,'link'] = data[i]['links']['info'] if data[i]['links']['info'] else ''
	if data[i]['brinfo']:
		df.loc[i,'br_name'] = data[i]['brinfo']['name'] if data[i]['brinfo']['name'] else ''
		df.loc[i,'description'] = data[i]['brinfo']['descr'] if data[i]['brinfo']['descr'] else ''
		df.loc[i,'pic'] = data[i]['brinfo']['pic'] 
		if data[i]['brinfo']['flowering']:
			df.loc[i,'days'] = data[i]['brinfo']['flowering']['days'] if data[i]['brinfo']['flowering']['days'] else ''
			df.loc[i,'auto'] = data[i]['brinfo']['flowering']['auto'] if data[i]['brinfo']['flowering']['auto'] else ''
		df.loc[i,'types'] = data[i]['brinfo']['type'] if data[i]['brinfo']['type'] else ''

	if 'reviews' in data[i].keys() and data[i]['reviews']:
		df.loc[i,'review_count'] = data[i]['reviews']['count']['val'] if 'count' in data[i]['reviews'].keys() and data[i]['reviews']['count'] else ''
		df.loc[i,'review_average'] = data[i]['reviews']['average']['val'] if data[i]['reviews']['average'] else ''

		if 'indoor' in data[i]['reviews'].keys() and data[i]['reviews']['indoor']:
			df.loc[i,'in_count'] = data[i]['reviews']['indoor']['count']['val'] if 'count' in data[i]['reviews']['indoor'].keys() else ''
			if 'stretch' in data[i]['reviews']['indoor'].keys() and data[i]['reviews']['indoor']['stretch']:
				df.loc[i,'in_stretch_from'] = data[i]['reviews']['indoor']['stretch']['from'] if 'from' in data[i]['reviews']['indoor']['stretch'].keys() else ''
				df.loc[i,'in_stretch_to'] = data[i]['reviews']['indoor']['stretch']['to'] if 'to' in data[i]['reviews']['indoor']['stretch'].keys() else ''
				df.loc[i,'in_stretch_percent'] = data[i]['reviews']['indoor']['stretch']['percent'] if 'percent' in data[i]['reviews']['indoor']['stretch'].keys() else ''
			df.loc[i,'in_avg'] = data[i]['reviews']['indoor']['average']['val'] if data[i]['reviews']['indoor']['average'] else ''
			
			if 'flowering' in data[i]['reviews']['indoor'].keys() and data[i]['reviews']['indoor']['flowering']:
				df.loc[i,'in_flowering_to'] = data[i]['reviews']['indoor']['flowering']['to'] if 'to' in data[i]['reviews']['indoor']['flowering'].keys() else ''
				df.loc[i,'in_flowering_from'] = data[i]['reviews']['indoor']['flowering']['from']  if 'from' in data[i]['reviews']['indoor']['flowering'].keys() else ''
				df.loc[i,'in_flowering_avg'] = data[i]['reviews']['indoor']['flowering']['avg']  if 'avg' in data[i]['reviews']['indoor']['flowering'].keys() else ''
			df.loc[i,'in_yield'] = data[i]['reviews']['indoor']['yield']['val'] if data[i]['reviews']['indoor']['yield'] else ''
		
		if 'outdoor' in data[i]['reviews'].keys() and data[i]['reviews']['outdoor']:
			df.loc[i,'out_count'] = data[i]['reviews']['outdoor']['count']['val'] if 'count' in data[i]['reviews']['outdoor'].keys() else ''
			if 'stretch' in data[i]['reviews']['outdoor'].keys()  and data[i]['reviews']['outdoor']['stretch']:
				df.loc[i,'out_stretch_from'] = data[i]['reviews']['average']['val'] if data[i]['reviews']['outdoor']['stretch'] else ''
				df.loc[i,'out_stretch_to'] = data[i]['reviews']['average']['outfo'] if data[i]['reviews']['outdoor']['stretch'] else ''
				df.loc[i,'out_stretch_percent'] = data[i]['reviews']['count']['val'] if data[i]['reviews']['outdoor']['stretch'] else ''
			df.loc[i,'out_avg'] = data[i]['reviews']['outdoor']['average']['val'] if data[i]['reviews']['outdoor']['average'] else ''
			if 'flowering' in data[i]['reviews']['outdoor'].keys() and data[i]['reviews']['outdoor']['flowering']:
				df.loc[i,'out_flowering_to'] = data[i]['reviews']['average']['val'] if 'to' in data[i]['reviews']['outdoor']['flowering'].keys() else ''
				df.loc[i,'out_flowering_from'] = data[i]['reviews']['average']['val'] if 'from' in data[i]['reviews']['outdoor']['flowering'].keys() else ''
				df.loc[i,'out_flowering_avg'] = data[i]['reviews']['average']['val'] if 'average' in data[i]['reviews']['outdoor']['flowering'].keys() else ''
			df.loc[i,'out_yield'] = data[i]['reviews']['outdoor']['yield']['val'] if data[i]['reviews']['outdoor']['yield'] else ''
	
		if 'tasting' in data[i]['reviews'].keys() and data[i]['reviews']['tasting']:

			if 'strength' in data[i]['reviews']['tasting'].keys() and data[i]['reviews']['tasting']['strength']:
				df.loc[i, 'strength'] = data[i]['reviews']['tasting']['strength']['val']

			if 'taste' in data[i]['reviews']['tasting'].keys() and data[i]['reviews']['tasting']['taste']:
				ind = 0;
				for key in data[i]['reviews']['tasting']['taste']['cloud'].keys():
					ind += 1
					df.loc[i, 'taste_' + str(ind) +'_name'] = data[i]['reviews']['tasting']['taste']['cloud'][key]['title']
					df.loc[i, 'taste_' + str(ind) +'_size'] = data[i]['reviews']['tasting']['taste']['cloud'][key]['size']
					df.loc[i, 'taste_' + str(ind) +'_val'] = data[i]['reviews']['tasting']['taste']['cloud'][key]['val']

			if 'smell' in data[i]['reviews']['tasting'].keys() and data[i]['reviews']['tasting']['smell']:
				ind = 0;
				for key in data[i]['reviews']['tasting']['smell']['cloud'].keys():
					ind += 1
					df.loc[i, 'smell_' + str(ind) +'_name'] = data[i]['reviews']['tasting']['smell']['cloud'][key]['title']
					df.loc[i, 'smell_' + str(ind) +'_size'] = data[i]['reviews']['tasting']['smell']['cloud'][key]['size']
					df.loc[i, 'smell_' + str(ind) +'_val'] = data[i]['reviews']['tasting']['smell']['cloud'][key]['val']
			
			if 'effect' in data[i]['reviews']['tasting'].keys() and data[i]['reviews']['tasting']['effect']:
				ind = 0;
				for key in data[i]['reviews']['tasting']['effect']['cloud'].keys():
					ind += 1
					df.loc[i, 'effect_' + str(ind) +'_name'] = data[i]['reviews']['tasting']['effect']['cloud'][key]['title']
					df.loc[i, 'effect_' + str(ind) +'_size'] = data[i]['reviews']['tasting']['effect']['cloud'][key]['size']
					df.loc[i, 'effect_' + str(ind) +'_val'] = data[i]['reviews']['tasting']['effect']['cloud'][key]['val']

	if 'medical' in data[i].keys() and data[i]['medical']:
		ind = 0;
		for key in data[i]['medical'].keys():
			ind += 1
			df.loc[i, 'medical_' + str(ind) +'_name'] = data[i]['medical'][key]['name']
			df.loc[i, 'medical_' + str(ind) +'_count'] = data[i]['medical'][key]['count']['val']
			df.loc[i, 'medical_' + str(ind) +'_effect_info'] = data[i]['medical'][key]['effect']['info']
			df.loc[i, 'medical_' + str(ind) +'_effect_dosage'] = data[i]['medical'][key]['effect']['dosage']
			df.loc[i, 'medical_' + str(ind) +'_effect_val'] = data[i]['medical'][key]['effect']['val']

	if data[i]['parents'] and data[i]['parents']['strains']:
		df.loc[i, 'parent_info'] = data[i]['parents']['info'] if data[i]['parents']['info'] else ''
		for key in data[i]['parents']['strains'].keys():
			df.loc[i, 'parent_' + key +'_name'] = data[i]['parents']['strains'][key]['name'] if data[i]['parents']['strains'][key]['name'] else ''
			df.loc[i, 'parent_' + key +'_brname'] = data[i]['parents']['strains'][key]['brname'] if data[i]['parents']['strains'][key]['brname'] else ''

	if data[i]['hybrids']:
		for key in data[i]['hybrids'].keys():
			df.loc[i, 'hybrid_' + key +'_name'] = data[i]['hybrids'][key]['name'] if data[i]['hybrids'][key]['name'] else ''
			df.loc[i, 'hybrid_' + key +'_brname'] = data[i]['hybrids'][key]['brname'] if data[i]['hybrids'][key]['brname'] else ''
			df.loc[i, 'hybrid_' + key +'_cross'] = data[i]['hybrids'][key]['info'] if data[i]['hybrids'][key]['info'] else ''
	


#df.to_csv('data.csv',header = True, encoding='utf-8')
#df.to_csv('data1.csv',header = True, encoding='ISO-8859-1')
#list(df.columns.values)

new_col = []; before = []; 
col = df.columns.tolist()
for name in col:
	if 'taste' in name:
		new_col.append(name)

for name in col:
	if 'smell' in name:
		new_col.append(name)

for name in col:
	if 'effect' in name:
		new_col.append(name)

for name in col:
	if 'medical' in name:
		new_col.append(name)

for name in col:
	if 'parent' in name:
		new_col.append(name)

for name in col:
	if 'hybrid' in name:
		new_col.append(name)

for name in col:
	if name not in new_col:
		before.append(name)

before.extend(new_col)

df = df[before]












