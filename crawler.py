# encoding: utf-8
"""
@author: yen-nan ho
@contact: aaron1aaron2@gmail.com
@gitHub: https://github.com/aaron1aaron2
@Create Date: 2021/3/5
"""

import re
import os
import sys
import time
import json
import urllib
import requests
import traceback
import warnings
warnings.simplefilter(action='ignore')

import pandas as pd
from bs4 import BeautifulSoup

from IPython import embed


class Worker:
    def __init__(self, keyword, output_path, record_path, wait_second, page_limit=None, check_records=False):
        # basic args
        self.keyword = keyword
        self.output_path = output_path
        self.record_path = record_path
        self.wait_second = wait_second
        self.page_limit = page_limit
        self.check_records = check_records

        # crawler info 
        self.keyword_urlencode = urllib.parse.quote(keyword)
        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36'}
        self.root_url = f"https://udn.com/search/word/2/{self.keyword_urlencode}"
        self.get_total_num()

        # crawler record 
        self.record_ls = []
        self.second_per_item_ls = []

    def get_total_num(self):
        respond = requests.get(self.root_url, headers = self.headers)
        soup = BeautifulSoup(respond.text, "html.parser")
        number = soup.find("div", class_="search-total").text

        if re.search(r"(\d+)筆", number) != None:
            self.total_num = int(re.search(r"(\d+)筆", number)[1])
            self.total_page = (self.total_num//20)+1
        else:
            self.total_num = -1
            self.total_page = -1

    def get_local_time(self):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

    def print_error_message(self, e):
        error_class = e.__class__.__name__ #取得錯誤類型
        detail = e.args[0] #取得詳細內容
        cl, exc, tb = sys.exc_info() #取得Call Stack
        lastCallStack = traceback.extract_tb(tb)[-1] #取得Call Stack的最後一筆資料
        fileName, lineNum, funcName = lastCallStack[0], lastCallStack[1], lastCallStack[2]
        errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(fileName, lineNum, funcName, error_class, detail)

        print(errMsg)

    def get_content_in_respond_ls(self, respond_ls):
        total_runtime = 0
        total_num = len(respond_ls)
        for i in range(len(respond_ls)):
            start_time = time.time()
            respond_ls[i]['time'] = respond_ls[i]['time']['dateTime']
            url = respond_ls[i]['titleLink']

            respond = requests.get(url, headers = self.headers)
            respond_ls[i]['server_respond_time'] = time.time() - start_time

            soup = BeautifulSoup(respond.text, "html.parser")

            # [APP] =========================================================
            # app_info = str(soup.find("script", type="application/ld+json"))
            # app_info_dt = json.loads(app_info[app_info.find('<script type="application/ld+json">')+35:app_info.find('</script>')])
            # [APP] =========================================================

            # Start Redirect flow >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
            page_type = None
            redirect_url = None

            search_ls = [soup.find("meta", property="og:url"), soup.find("script", language="javascript")] # 兩種 vip 的網址可能藏的地方

            for idx, test_item in enumerate(search_ls):
                if  re.search(r'https?://vip.udn.com/vip/story', str(test_item)) != None:
                    if idx == 0:
                        redirect_url = re.search(r'content="(https?://vip.udn.com/vip/story/\d+/\d+)"', str(test_item))[1] 
                    elif idx == 1:
                        redirect_url = re.search(r'window.location.href="(https?://vip.udn.com/vip/story/.+)"', str(test_item))[1] 

                    break

                elif re.search(r'from=udn-category', str(test_item)) != None:
                    redirect_url = re.search(r'window.location.href="(https?://udn.com/news/story/.+)"', str(test_item))[1] 

            # End Start Redirect flow <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

            if redirect_url != None:

                # Start [https://vip.udn.com/] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                page_type = 'vip'

                respond = requests.get(redirect_url, headers = self.headers)
                respond_ls[i]['server_respond_time'] = time.time() - start_time

                soup = BeautifulSoup(respond.text, "html.parser")

                try:
                    app_info = str(soup.find("script", type="application/ld+json"))
                    app_info_dt = json.loads(app_info[app_info.find('<script type="application/ld+json">')+35:app_info.find('</script>')])
                except Exception as e:
                    app_info_dt = None
                    self.print_error_message(e)

                # weblevels
                l1, l2 = None, None
                if app_info_dt:
                    l1 = app_info_dt['articleSection']

                if soup.find("a", class_="article-content__cate"):
                    l2 = soup.find("a", class_="article-content__cate").text

                if l1!=None and l2!=None:
                    respond_ls[i]['weblevels'] = f'{l1}|{l2}'
                elif l1!=None:
                    respond_ls[i]['weblevels'] = f'{l1}|'
                else:
                    respond_ls[i]['weblevels'] = None

                # reporters
                respond_ls[i]['reporters'] = None
                if app_info_dt:
                    if 'author' in app_info_dt.keys():
                        if str(app_info_dt['author']['name']).find('/author') != -1:
                            if re.search(r'(\w+)\</a>',app_info_dt['author']['name']):
                                respond_ls[i]['reporters'] = re.search(r'(\w+)\</a>',app_info_dt['author']['name'])[1]
                            else:
                                respond_ls[i]['reporters'] = app_info_dt['author']['name']
                        else:
                            respond_ls[i]['reporters'] = app_info_dt['author']['name']

                # CONTENT                    
                respond_ls[i]['CONTENT'] = soup.find("meta",  property="og:description")['content']

                # HashTag
                keyword = soup.find("meta", {"name":"news_keywords"})
                if keyword:
                    respond_ls[i]['HashTag'] = '|'.join(keyword['content'].split(','))
                else:
                    respond_ls[i]['HashTag'] = None 

                # End [https://udn.com/news] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


            if (url.find('https://house.udn.com/') != -1) & (page_type == None):

                # Start [https://house.udn.com/] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                page_type = 'house'

                # weblevels
                if soup.find("div", id="nav"):
                    web_level_ls = soup.find("div", id="nav").find_all({"a","b"})
                    web_level_ls = [i.text for i in web_level_ls]
                else:
                    web_level_ls = []

                respond_ls[i]['weblevels'] = '|'.join(web_level_ls)

                # reporters
                reporter_ls = soup.find_all("div", class_="shareBar__info--author")
                reporter_ls = [i.text.split(' ')[-1] for i in reporter_ls]
                respond_ls[i]['reporters'] = '|'.join(reporter_ls)
                
                # CONTENT
                if soup.find("div", id="story_body_content"):
                    content = soup.find("div", id="story_body_content").find_all("p")
                    content = ''.join([i.text for i in content])
                    content = content.replace('\r', '').replace('\n', '')
                else:
                    content = None

                respond_ls[i]['CONTENT'] = content

                # HashTag
                keyword = soup.find("meta", {"name":"news_keywords"})
                if keyword:
                    respond_ls[i]['HashTag'] = '|'.join(keyword['content'].split(','))
                else:
                    respond_ls[i]['HashTag'] = None 

                # End [https://house.udn.com/] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


            elif (url.find('https://style.udn.com/') != -1) & (page_type == None):

                # Start [https://style.udn.com/] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                page_type = 'style'

                # weblevels
                if soup.find("nav"):
                    web_level_ls = soup.find("nav").find_all("a")
                    web_level_ls = [i.text for i in web_level_ls]
                else:
                    web_level_ls = []

                respond_ls[i]['weblevels'] = '|'.join(web_level_ls)

                # reporters
                reporter_ls = soup.find_all("div", class_="shareBar__info--author")
                reporter_ls = [i.text.split(' ')[-1] for i in reporter_ls]
                respond_ls[i]['reporters'] = '|'.join(reporter_ls)
                
                # CONTENT
                if soup.find("section", id="story-main"):
                    content = soup.find("section", id="story-main").find_all("p") # {"p", "h2"}
                    content = ''.join([i.text for i in content])
                    content = content.replace('\r', '').replace('\n', '')
                else:
                    content = None

                respond_ls[i]['CONTENT'] = content

                # HashTag
                keyword = soup.find("meta", {"name":"news_keywords"})
                if keyword:
                    respond_ls[i]['HashTag'] = '|'.join(keyword['content'].split(','))
                else:
                    respond_ls[i]['HashTag'] = None 

                # End [https://style.udn.com/] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


            elif (url.find('https://stars.udn.com') != -1) & (page_type == None):

                # Start [https://stars.udn.com] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                page_type = 'stars'

                # weblevels
                web_levels = soup.find("div", class_="breadcrumb wrapper only_web")
                
                if web_levels:
                    web_level_ls = [i.text.strip() for i in web_levels.find_all("a")]
                else:
                    web_level_ls = []

                respond_ls[i]['weblevels'] = '|'.join(web_level_ls)

                # reporters
                reporter_txt = soup.find("div", class_="shareBar__info--author").text
                
                if re.search(r"記者(\w+)／", reporter_txt):
                    respond_ls[i]['reporters'] = re.search(r"記者(\w+)／", reporter_txt)[1]
                else:
                    respond_ls[i]['reporters'] = None

                # CONTENT
                content = soup.find("div", class_="article").text

                if content:
                    content = content.replace('\r', '').replace('\n', '')
                    respond_ls[i]['CONTENT'] = content[:content.find('更多新聞報導')-1]
                else:
                    respond_ls[i]['CONTENT'] = content

                # HashTag
                keyword = soup.find("meta", {"name":"news_keywords"})
                if keyword:
                    respond_ls[i]['HashTag'] = '|'.join(keyword['content'].split(','))
                else:
                    respond_ls[i]['HashTag'] = None 

                # End [https://stars.udn.com] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


            elif (url.find('https://health.udn.com') != -1) & (page_type == None):

                # Start [https://health.udn.com] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                page_type = 'health'

                # weblevels
                web_levels = soup.find("nav", id="navigate")
                
                if web_levels:
                    web_level_ls = [i.text.strip() for i in web_levels.find_all("a")]
                else:
                    web_level_ls = []

                respond_ls[i]['weblevels'] = '|'.join(web_level_ls)

                # reporters
                reporter_txt = soup.find("div", class_="shareBar__info--author").text
                
                if re.search(r"記者(\w+)／", reporter_txt):
                    respond_ls[i]['reporters'] = re.search(r"記者(\w+)／", reporter_txt)[1]
                else:
                    respond_ls[i]['reporters'] = None

                # CONTENT
                content = soup.find("div", id="story_body")

                if content:
                    content = [i.text for i in content.find_all("p")]
                    respond_ls[i]['CONTENT'] = ''.join(content).replace('\r', '').replace('\n', '')
                else:
                    respond_ls[i]['CONTENT'] = content

                # HashTag
                keywords = soup.find("dl", class_="tabsbox")

                if keywords:
                    keyword_ls = [i.text for i in keywords.find_all("a")]
                else:
                    keyword_ls = []

                respond_ls[i]['HashTag'] = '|'.join(keyword_ls)

                # End [https://health.udn.com] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


            elif (url.find("https://theme.udn.com") != -1) & (page_type == None):

                # Start [https://theme.udn.com] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                page_type = 'theme'

                # weblevels
                if soup.find("div", id="nav"):
                    web_level_ls = soup.find("div", id="nav").find_all({"a","b"})
                    web_level_ls = [i.text for i in web_level_ls]
                else:
                    web_level_ls = []

                respond_ls[i]['weblevels'] = '|'.join(web_level_ls)

                # reporters
                reporter_ls = soup.find("div", class_="shareBar__info--author")
                reporter_ls = [i.text.split(' ')[-1] for i in reporter_ls.find_all("a")]
                respond_ls[i]['reporters'] = '|'.join(reporter_ls)

                # CONTENT
                content = soup.find("div", id="story_body_content")

                if content:
                    content = [i.text for i in content.find_all("p")]
                    respond_ls[i]['CONTENT'] = ''.join(content).replace('\r', '').replace('\n', '')
                else:
                    respond_ls[i]['CONTENT'] = content

                # HashTag
                keyword = soup.find("meta", {"name":"news_keywords"})
                if keyword:
                    respond_ls[i]['HashTag'] = '|'.join(keyword['content'].split(','))
                else:
                    respond_ls[i]['HashTag'] = None 


                # End [https://theme.udn.com] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

            
            elif (url.find("https://opinion.udn.com") != -1) & (page_type == None):

                # Start [https://opinion.udn.com] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                page_type = 'opinion'

                # weblevels
                respond_ls[i]['weblevels'] = '鳴人堂|'

                # reporters
                reporter = soup.find("a", href= re.compile(r"/author/\w+"))

                if reporter:
                    respond_ls[i]['reporters'] = reporter.text
                else:
                    respond_ls[i]['reporters'] = None

                # CONTENT
                content = soup.find("main")

                if content:
                    content = [i.text for i in content.find_all("p")]
                    respond_ls[i]['CONTENT'] = ''.join(content).replace('\r', '').replace('\n', '')
                else:
                    respond_ls[i]['CONTENT'] = content

                # HashTag
                keyword = soup.find("meta", {"name":"news_keywords"})
                if keyword:
                    respond_ls[i]['HashTag'] = '|'.join(keyword['content'].split(','))
                else:
                    respond_ls[i]['HashTag'] = None 


                # End [https://opinion.udn.com] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


            elif (url.find("https://udn.com/umedia") != -1) & (page_type == None):

                # Start [https://udn.com/umedia] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                page_type = 'umedia'

                # weblevels
                respond_ls[i]['weblevels'] = None

                # reporters
                reporter = soup.find("a", href= re.compile(r"/author/\w+"))

                if reporter:
                    respond_ls[i]['reporters'] = reporter.text.split('/')[-1].strip()
                else:
                    respond_ls[i]['reporters'] = None

                # CONTENT
                content = soup.find("div", class_="article-content article-content-common")

                if content:
                    content = [i.text for i in content.find_all("p")]
                    respond_ls[i]['CONTENT'] = ''.join(content).replace('\r', '').replace('\n', '')
                else:
                    respond_ls[i]['CONTENT'] = None

                # HashTag
                app_info = str(soup.find("script", type="application/ld+json"))
                try:
                    app_info_dt = json.loads(app_info[app_info.find('type="application/ld+json">')+27:app_info.find('</script>')])
                except:
                    app_info_dt = None
                if app_info_dt:
                    respond_ls[i]['HashTag'] = '|'.join([i.strip() for i in app_info_dt['keywords'].split(',')])
                else:
                    respond_ls[i]['HashTag'] = None 


                # End [https://udn.com/umedia] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


            elif (url.find("https://game.udn.com/") != -1) & (page_type == None):

                # Start [https://game.udn.com/] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                page_type = 'game'

                # weblevels
                web_level = soup.find("div", id="nav")

                if web_level:
                    respond_ls[i]['weblevels'] = '|'.join([i.text for i in web_level.find_all({"a","b"})])
                else:
                    respond_ls[i]['weblevels'] = None



                # reporters
                reporter = soup.find("div", class_="shareBar__info--author")

                if reporter:
                    respond_ls[i]['reporters'] = reporter.text.split(' ')[-1].strip()
                else:
                    respond_ls[i]['reporters'] = None

                # CONTENT
                content = soup.find("div", id="story_body_content")

                if content:
                    content = [i.text for i in content.find_all("p")]
                    respond_ls[i]['CONTENT'] = ''.join(content).replace('\r', '').replace('\n', '').replace('facebook', '')
                else:
                    respond_ls[i]['CONTENT'] = None

                # HashTag
                respond_ls[i]['HashTag'] = None 

                # End [https://game.udn.com/] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


            elif (soup.find("section", class_="article-content__editor") != None) & (page_type == None):

                # Start [https://udn.com/news] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                page_type = 'news'

                # weblevels
                web_level_ls = soup.find_all("a", class_="breadcrumb-items")
                web_level_ls = [i.text for i in web_level_ls]
                respond_ls[i]['weblevels'] = '|'.join(web_level_ls)

                # reporters
                reporter_ls = soup.find_all("a", href= re.compile(r"/news/reporter/\w+"))
                reporter_ls = [i.text for i in reporter_ls]
                respond_ls[i]['reporters'] = '|'.join(reporter_ls)
                
                # CONTENT
                content = soup.find("section", class_="article-content__editor").text

                if content:
                    content = content.replace('\r', '').replace('\n', '')

                respond_ls[i]['CONTENT'] = content

                # HashTag
                keywords = soup.find("section", class_="keywords")

                if keywords:
                    keyword_ls = [i.text for i in keywords.find_all("a")]
                else:
                    keyword_ls = []

                respond_ls[i]['HashTag'] = '|'.join(keyword_ls)

                # End [https://udn.com/news] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


            elif page_type == None:
                page_type = 'unknown'
                respond_ls[i].update({'weblevels': None, 'reporters': None, 'CONTENT': None, 'HashTag': None})
            
            respond_ls[i]['page_type'] = page_type

            runtime = time.time() - start_time
            if runtime <= self.wait_second:
                time.sleep(self.wait_second - runtime)

            total_runtime+=runtime

        second_per_item = round(total_runtime/total_num, 3)
        self.second_per_item_ls.append(second_per_item)

        print(f'{second_per_item} s/it')

        return respond_ls


    def get_page_data(self, page_id):
        url = f"https://udn.com/api/more?page={page_id}&id=search:{self.keyword_urlencode}&channelId=2&type=searchword&last_page=28"
        
        start_datetime = self.get_local_time()
        start_time = time.time()

        # Start get articles list >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        respond = requests.get(url, headers = self.headers)
        server_respond_time = time.time() - start_time

        if respond.status_code == 200:
            respond_dt = respond.json()
            articles_ls = respond_dt['lists']
            respond_num_in_page = len(articles_ls)
            is_last_page = respond_dt['end']

        else:
            print('[error] Wrong response when getting article link list')
            respond_dt = {'state': None, 'page': None, 'end': None}
            respond_num_in_page = -1
            is_last_page = False

        # End get articles list <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        end_datetime = self.get_local_time()
        total_timeuse = time.time() - start_time

        record_dt = {
            'page_id': page_id,
            'url': url,
            'server_respond_time': server_respond_time,
            'start_datetime': start_datetime,
            'end_datetime': end_datetime,
            'total_timeuse': total_timeuse,
            'respond_num_in_page': respond_num_in_page,
            'is_last_page': is_last_page,
            'respond_state': respond_dt['state'],
            'respond_page': respond_dt['page'],
            'respond_end': respond_dt['end']
            }

        return record_dt, articles_ls 


    def output_data(self, respond_ls):

        # Start cleaning data  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        result = pd.DataFrame(respond_ls)

        result.rename(columns = {"url":"img_url", "title":"TITLE", "time":"TIME", "titleLink":"LINK"}, inplace=True) 
        result['SOURCE'] = '聯合報'

        tmp = result['weblevels']
        tmp.loc[tmp.str.count(r'\|')==3] = tmp.loc[tmp.str.count(r'\|')==3].apply(lambda x: '|'.join(x.split('|')[:-1]))
        extract = tmp.str.extract(r'(udn\|)?(\w+\W?\w+)?\|(\w+\W?\w+)') # 噓！星聞|熱搜 、 udn|產經|金融要聞、 udn|房地產|有‧房子

        result['CATEGORY'] = extract[1]
        result['CATEGORY2'] = extract[2]

        result.loc[result['CATEGORY'].isna(), 'CATEGORY'] = result.loc[result['CATEGORY'].isna(), 'cateTitle']

        # End of cleaning data <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        output_folder = os.path.dirname(self.output_path)
        if not os.path.exists(output_folder):
            os.mkdirs(output_folder)

        cols = ['TITLE', 'TIME', 'CATEGORY', 'CATEGORY2', 'HashTag', 'CONTENT' ,'SOURCE', 'LINK', 
                'img_url','paragraph', 'cateLink', 'cateTitle', 'weblevels', 'reporters', 'page_type']

        if os.path.exists(self.output_path):
            result[cols].to_csv(self.output_path, mode='a', index=False, header=None)
        else:
            result[cols].to_csv(self.output_path, mode='w', index=False)


    def run(self):
        page_id = 1
        pass_time = 0 
        total_item = 0

        if (os.path.exists(self.record_path) & self.check_records):
            print(f'final is complete - {self.record_path}')
        else:

            while True:
                if self.total_page != -1:
                    print(f'[stage: {page_id}/{self.total_page}] ({self.keyword})', '='*50)
                else:
                    print(f'([stage: {page_id}/?] {self.keyword})', '='*50)

                try:
                    # step 1: Get a list of article links
                    record_dt, articles_ls = self.get_page_data(page_id)
                    self.record_ls.append(record_dt)
                    total_item += len(articles_ls)

                    # step 2: Enter the article links and intercept the information
                    if len(articles_ls) != 0:
                        articles_ls = self.get_content_in_respond_ls(articles_ls)
                    else:
                        break
                        
                    if self.total_page != -1:
                        print(f'{total_item}/{self.total_num} (Number of articles crawled / Total articles)')
                    else:
                        print(f'(stage: {total_item}/', '='*50)

                    # step 3: Organize and save data
                    if len(articles_ls) != 0:
                        self.output_data(articles_ls)
                    else:
                        break

                    pass_time = 0
                    page_id += 1

                    if record_dt['is_last_page']:
                        print(f'({self.keyword}) finish !!')
                        break

                except Exception as e:
                    pass_time += 1
                    time.sleep(1)
                    print(f'({self.keyword}) restry {pass_time} !!')
                    self.print_error_message(e)

                if pass_time >= 3:
                    page_id += 1
                    pass_time = 0

                if self.page_limit != None:
                    if page_id >= self.page_limit:
                        break
                        
            if len(self.second_per_item_ls) != 0:
                print(f'Total item: {total_item} | Time use: {sum(self.second_per_item_ls)/len(self.second_per_item_ls)} s/item')
            else:
                print(f'Total item: {total_item} | Time use: ?')

            pd.DataFrame(self.record_ls).to_csv(self.record_path, index=False)
        
        

if __name__ == "__main__":

    df = pd.read_csv('data/keywords.csv')

    for idx, keyword in enumerate(df['立委姓名'].to_list()):

        print('='*80, f'\n[keyword({keyword}): {idx+1}/{df.shape[0]}]\n')

        worker = Worker(
                    keyword=keyword,
                    output_path=f'output/{keyword}.udn.csv',
                    record_path=f'output/{keyword}.udn.record.csv', 
                    wait_second=1,
                    check_records=True
                    # page_limit=2
                    )

        worker.run()