import requests

params = {
    'pagestart': 1,
    'pageend': 1,
    # more key=value pairs as appeared in your query string
}

from requests import Session, Request

#s = Session()
req = requests.Request('GET', 'http://apps.tga.gov.au/prod/ARTGSearch/ARTGWebService.svc/json/ARTGValueSearch/?', params=params).prepare()
print(req.url)
print(req.headers)
print(req.body)

# req1 = requests.get('http://google.com.au',headers={"content-type":"text"})
# print(req1.headers)
