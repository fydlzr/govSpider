import os, sys

fpath = sys.argv[1]

for line in open(fpath,'r'):
	line = line.strip()
	print line 
	print '='*50
	os.popen("scrapy crawl govspider -L WARNING -a MONGODB_DB=test2 -a start_url="+line)
