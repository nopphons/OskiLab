import requests
import json
import csv
import re
from urllib2 import urlopen
import cjson
invalid_escape = re.compile(r'\\[0-7]{1,6}')  # up to 3 digits for byte values up to FF

def replace_with_byte(match):
    return chr(int(match.group(0)[1:], 8))

def repair(brokenjson):
    return invalid_escape.sub(replace_with_byte, brokenjson)

id_json_url = 'http://en.seedfinder.eu/api/json/ids.json?br=all&strains=1&ac=47c751c1d25fe55e5ef6447c18a6d6da'
strain_json_url = 'http://en.seedfinder.eu/api/json/strain.json?br=Sensi_Seeds&str=Skunk_Nr1&parents=1&medical=1&reviews=1&tasting=1&ac=47c751c1d25fe55e5ef6447c18a6d6da'
url_init = 'http://en.seedfinder.eu/api/json/strain.json?br='
url_end = '&comments=10&parents=1&hybrids=1&medical=1&pics=1&reviews=1&tasting=1&ac=47c751c1d25fe55e5ef6447c18a6d6da'

response = requests.get(id_json_url)
data = response.json()

f = csv.writer(open("strains.csv", "wb+"))
f.writerow(["name", "logo", "strains"])

for i in data:
    x = data[i]
    f.writerow([x["name"].encode('utf-8').strip(), 
                x["logo"].encode('utf-8').strip(), 
                (','.join(x['strains'].values())).encode('utf-8').strip()])


####################Strain info################################### 
url_init = 'http://en.seedfinder.eu/api/json/strain.json?br='
url_end = '&comments=10&parents=1&hybrids=1&medical=1&pics=1&reviews=1&tasting=1&ac=47c751c1d25fe55e5ef6447c18a6d6da'

strain_list = []
i = 0
for index in data:
	br = index
	print i
	i += 1
	for strain in data[br]['strains']:
		url = url_init + br.encode('utf-8').strip() + '&str=' + strain.encode('utf-8').strip() + url_end
		#response = requests.get(url)
		response = urlopen(url)
		cont = cjson.decode(response.read())
		#cont = repair(unicode(response.read(), "ISO-8859-1"))
		#temp = json.loads(cont, strict = False)
		#temp = json.loads(response)
		strain_list.append(cont)

with open('data.json', 'w') as outfile:
    json.dump(strain_list, outfile)







