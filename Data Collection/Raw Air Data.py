import requests
import os

print('Beginning file download with requests')

url = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69?api-key={ # Your API Key } '
r = requests.get(url)

i = 0
while os.path.exists("/Users/jhapi/Desktop/DATASET/db%s.csv" % i):
    i +=1
with open('/Users/jhapi/Desktop/DATASET/db%s.csv' % i, 'wb') as f:
	f.write(r.content)

print(r.status_code)
print(r.headers['content-type'])
print(r.encoding)