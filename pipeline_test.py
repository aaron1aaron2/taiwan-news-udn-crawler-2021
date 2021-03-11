  
# encoding: utf-8
"""
@author: yen-nan ho
@contact: aaron1aaron2@gmail.com
@gitHub: https://github.com/aaron1aaron2
@Create Date: 2021/3/1
"""

import re
import os
import requests
import urllib
import time
import tqdm

import pandas as pd
from bs4 import BeautifulSoup

target = "高嘉瑜"
page_id = 1 # 28 | total_page = (total_num//20)+1
wait_second = 1

url = "https://udn.com/api/more?page={}&id=search:{}&channelId=2&type=searchword&last_page=28".format(page_id, urllib.parse.quote(target))

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36'}

respond = requests.get(url, headers = headers)
respond_dt = respond.json() # ['state', 'page', 'end', 'lists'] 

assert (respond_dt['state'] | (respond.status_code == 200)), "wrong state"

respond_ls = respond_dt['lists']

for i in tqdm.tqdm(range(len(respond_ls))):
	start_time = time.time()
	respond_ls[i]['time'] = respond_ls[i]['time']['dateTime']

	entry_url = respond_ls[i]['titleLink']

	respond_2 = requests.get(entry_url, headers = headers)

	soup = BeautifulSoup(respond_2.text)

	if soup.find("section", class_="article-content__editor") != None:
		web_level_ls = soup.find_all("a", class_="breadcrumb-items")
		web_level_ls = [i.text for i in web_level_ls]
		respond_ls[i]['weblevels'] = '|'.join(web_level_ls)

		reporter_ls = soup.find_all("a", href= re.compile(r"/news/reporter/\w+"))
		reporter_ls = [i.text for i in reporter_ls]
		respond_ls[i]['reporters'] = '|'.join(reporter_ls)
		
		content = soup.find("section", class_="article-content__editor").text
		respond_ls[i]['CONTENT'] = content

		keywords = soup.find("section", class_="keywords")
		keyword_ls = [i.text for i in keywords.find_all("a")]
		respond_ls[i]['HashTag'] = '|'.join(keyword_ls)

	elif soup.find("div", class_="article") != None:
		web_levels = soup.find("div", class_="breadcrumb wrapper only_web")
		web_level_ls = [i.text.strip() for i in web_levels.find_all("a")]
		respond_ls[i]['weblevels'] = '|'.join(web_level_ls)

		reporter_txt = soup.find("div", class_="shareBar__info--author").text
		respond_ls[i]['reporters'] = name = re.search(r"記者(\w+)／", reporter_txt)[1]


		content = soup.find("div", class_="article").text
		respond_ls[i]['CONTENT'] = content[:content.find('更多新聞報導')-1]

		keywords = soup.find("div", class_="article-tag")
		keyword_ls = [i.text for i in keywords.find_all("a")]
		respond_ls[i]['HashTag'] = '|'.join(keyword_ls)

	else:
		respond_ls[i] = {'weblevels': None, 'reporters': None, 'CONTENT': None, 'HashTag': None}

	runtime = time.time() - start_time
	if runtime <= wait_second:
		time.sleep(wait_second - runtime)

# ==================================================
result = pd.DataFrame(respond_ls)

result.rename(columns = {"url":"img_url", "title":"TITLE", "time":"TIME", "titleLink":"LINK"}, inplace=True) 
result['SOURCE'] = '聯合報'

extract = result.weblevels.str.extract('(udn\|)?(\w+)\|(\w+)')
result['CATEGORY'] = extract[1]
result['CATEGORY2'] = extract[2]

if not os.path.exists('output/test/'):
	os.mkdirs('output/test/')

result[['TITLE', 'TIME', 'CATEGORY', 'HashTag', 'CONTENT' ,'SOURCE', 'LINK']].to_excel('output/test/test_result.xlsx', index=False)
result[['TITLE', 'TIME', 'CATEGORY', 'CATEGORY2', 'HashTag', 'CONTENT' ,'SOURCE', 
		'LINK', 'img_url','paragraph', 'cateLink', 'cateTitle', 'weblevels', 'reporters']].to_excel('output/test/test_result_all.xlsx', index=False)

# result[['TITLE', 'TIME', 'CATEGORY', 'CATEGORY2', 'HashTag', 'CONTENT' ,'SOURCE', 
# 		'LINK', 'img_url','paragraph', 'cateLink', 'cateTitle', 'weblevels', 'reporters']].to_csv('output/test/test_result_all.csv', mode='a', index=False, header=None)


# ==============================================================
