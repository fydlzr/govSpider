# -*- coding: utf-8 -*-
import os, sys
import commands
fpath = sys.argv[1]
urls = open(fpath,'r').readlines()

c = commands.getstatusoutput("ps aux | grep BGY_ZC_ZD")
if 'scrapy' in c[1]:
        exit()

length = 20
start = 0
while start<len(urls):
	urllist = []
	for j in range(start,min(start+length, len(urls))):
		url = urls[j]
		if 'http' not in urls[j]:
			url = 'http://' + url
		urllist.append(url.strip())
	#os.popen("scrapy crawl govspider -L ERROR -a MONGODB_DB=BGY_ZC_ZD -a MONGODB_SERVER='10.10.165.175' -a MONGODB_PORT=23927  -a start_url_list='"+ '，'.join(urllist) +"'")
	os.system("scrapy crawl govspider -L ERROR -a MONGODB_DB=BGY_ZC_ZD -a MONGODB_SERVER='10.10.165.175' -a MONGODB_PORT=23927  -a start_url_list='"+ '，'.join(urllist) +"'")
	start += length
