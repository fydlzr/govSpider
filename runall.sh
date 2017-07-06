#!/bin/bash

cd /root/spider/govSpider
PATH=$PATH:/usr/local/bin
export PATH

nohup python /root/spider/govSpider/runEN.py urls_en.txt &
nohup python /root/spider/govSpider/runCN.py urls_cn.txt &



