# -*- coding: utf-8 -*-
import re
import sys
import json
import requests
import time
import datetime
import codecs
import os
from bs4 import BeautifulSoup
from six import u

__version__ = '1.0'

projectPath = os.path.dirname(__file__)

# if python 2, disable verify flag in requests.get()
VERIFY = True
if sys.version_info[0] < 3:
    VERIFY = False
    requests.packages.urllib3.disable_warnings()

def checkAndCreateDirectory(path):
    if not os.path.exists(path):
        os.mkdir(path)

class PttWebCrawler(object):
    """docstring for PttWebCrawler"""
    def __init__(self, board, iOrA, start=None, end=None, article_id=None, titleCallback=lambda x:x, contentCallback=lambda x:x):
        self.board = board
        self.PTT_URL = 'https://www.ptt.cc'
        self.titleCallback = titleCallback
        self.contentCallback = contentCallback

        # check and create file
        checkAndCreateDirectory(projectPath+'\\'+board)
        checkAndCreateDirectory(projectPath+'\\'+board +'\\'+'historyData')
        # means crawl range of articles
        if iOrA:
            start, end = int(start), int(end)
            if end == -1:
                end = self.getLastPage()

            artilces_count = 0
            error_articles_count = 0
            current_date = datetime.datetime.strptime('2000-01-01', '%Y-%m-%d').date()
            articlesList = []
            for index in range(0, end-start+1):
                print('Processing index:', str(start+index))
                resp = requests.get(
                    url=self.PTT_URL + '/bbs/' + self.board + '/index' + str(start+index) + '.html',
                    cookies={'over18': '1'}, verify=VERIFY
                )
                if resp.status_code != 200:
                    print('invalid url:', resp.url)
                    continue
                soup = BeautifulSoup(resp.text)
                divs = soup.find_all("div", "r-ent")
                for div in divs:
                    try:
                        # ex. link would be <a href="/bbs/PublicServan/M.1127742013.A.240.html">Re: [問題] 職等</a>
                        href = div.find('a')['href']
                        link = self.PTT_URL + href
                        article_id = re.sub('\.html', '', href.split('/')[-1])
                        articleData = self.parse(link, article_id)
                        artilces_count += 1
                        if 'date' in articleData and articleData['article_title'] != '' and articleData['author'] != '':
                            if articleData['date'] == '':
                                articlesList.append(articleData)
                            else:
                                try:
                                    article_date = datetime.datetime.strptime(articleData['date'], '%a %b %d %H:%M:%S %Y').date()
                                    if article_date != current_date:
                                        if len(articlesList) != 0:
                                            self.saveJsonFile(board, articlesList, str(current_date))
                                        articlesList = []
                                        articlesList.append(articleData)
                                        current_date = article_date
                                    else:
                                        articlesList.append(articleData)
                                except Exception as e:
                                    articlesList.append(articleData)
                        else:
                            print(articleData['article_id'])
                            error_articles_count += 1
                    except Exception as e:
                        print(link)
                        print(str(e))
                        pass
                time.sleep(0.1)

            if len(articlesList) != 0:
                self.saveJsonFile(board, articlesList, current_date)
                print('save file finished, file_date: ', current_date, ' page: ', start+index)

            print(
                'articles_count:', artilces_count,
                'error_articles_count:',error_articles_count
            )

        else:  # means crawl only one article
            link = self.PTT_URL + '/bbs/' + self.board + '/' + article_id + '.html'
            self.filename = self.board + '-' + article_id + '.json'
            self.store(self.filename, self.parse(link, article_id), 'w')

    def parse(self, link, article_id):
        # print('Processing article:', article_id)
        resp = requests.get(url=link, cookies={'over18': '1'}, verify=VERIFY)
        if resp.status_code != 200:
            print('invalid url:', resp.url)
            return json.dumps({"error": "invalid url"}, sort_keys=True, ensure_ascii=False)
        soup = BeautifulSoup(resp.text)
        main_content = soup.find(id="main-content")
        metas = main_content.select('div.article-metaline')
        author = ''
        title = ''
        date = ''
        if metas:
            author = metas[0].select('span.article-meta-value')[0].string if metas[0].select('span.article-meta-value')[0] else author
            title = self.titleCallback(metas[1].select('span.article-meta-value')[0].string if metas[1].select('span.article-meta-value')[0] else title)
            date = metas[2].select('span.article-meta-value')[0].string if metas[2].select('span.article-meta-value')[0] else date

            # remove meta nodes
            for meta in metas:
                meta.extract()
            for meta in main_content.select('div.article-metaline-right'):
                meta.extract()

        else:
            metas = main_content.find_all('span', class_='b4')
            if metas:
                author = metas[0].string.strip(' \t\n\r') if metas[0] else author
                title = metas[2].string.strip(' \t\n\r') if metas[2] else title
                date = metas[3].string.strip(' \t\n\r') if metas[3] else date

        # remove and keep push nodes
        pushes = main_content.find_all('div', class_='push')
        for push in pushes:
            push.extract()

        try:
            ip = main_content.find(text=re.compile(u'※ 發信站:'))
            ip = re.search('[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*', ip).group()
        except:
            ip = ''
        
        if ip == '':
            try:
                ip = main_content.find(text=re.compile(u'◆ From:'))
                ip = re.search('[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*', ip).group()
            except:
                ip = ''

        if ip == '':
            try:
                ip = main_content.find(text=re.compile(u'※ 編輯:'))
                ip = re.search('[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*', ip).group()
            except:
                ip = ''

        # 移除 '※ 發信站:' (starts with u'\u203b'), '◆ From:' (starts with u'\u25c6'), 空行及多餘空白
        # 保留英數字, 中文及中文標點, 網址, 部分特殊符號
        filtered = [ v for v in main_content.stripped_strings if v[0] not in [u'※', u'◆'] and v[:2] not in [u'--'] ]
        expr = re.compile(u(r'[^\u4e00-\u9fa5\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b\s\w:/-_.?~%()]'))
        for i in range(len(filtered)):
            filtered[i] = re.sub(expr, '', filtered[i])

        filtered = filter(lambda x:bool(x)==True, filtered) # remove empty strings
        filtered = filter(lambda x:article_id not in x, filtered) # remove last line containing the url of the article
        content = ' '.join(filtered)
        content = re.sub(r'(\s)+', ' ', content)
        content = self.contentCallback(content)

        # push messages
        p, b, n = 0, 0, 0
        messages = []
        for push in pushes:
            if not push.find('span', 'push-tag'):
                continue
            push_tag = push.find('span', 'push-tag').string.strip(' \t\n\r')
            push_userid = push.find('span', 'push-userid').string.strip(' \t\n\r')
            # if find is None: find().strings -> list -> ' '.join; else the current way
            push_content = push.find('span', 'push-content').strings
            push_content = ' '.join(push_content)[1:].strip(' \t\n\r')  # remove ':'
            push_ipdatetime = push.find('span', 'push-ipdatetime').string.strip(' \t\n\r')
            push_ipdatetimelist = push_ipdatetime.split(' ')
            push_ip = ''
            push_datetime = ''
            if len(push_ipdatetimelist) == 3:
                push_ip = push_ipdatetimelist[0]
                push_datetime = push_ipdatetimelist[1] + ' ' + push_ipdatetimelist[2]
            elif len(push_ipdatetimelist) == 2:
                if '.' in push_ipdatetimelist[0]:
                    push_ip = push_ipdatetimelist[0]
                    push_datetime = push_ipdatetimelist[1]
                else:
                    push_datetime = push_ipdatetimelist[0] + ' ' + push_ipdatetimelist[1]
            messages.append( {'push_tag': push_tag, 'push_userid': push_userid, 'push_content': push_content, 'push_ip': push_ip, 'push_datetime': push_datetime} )
            if push_tag == u'推':
                p += 1
            elif push_tag == u'噓':
                b += 1
            else:
                n += 1

        # count: 推噓文相抵後的數量; all: 推文總數
        message_count = {'all': p+b+n, 'count': p-b, 'push': p, 'boo': b, "neutral": n}

        # print 'msgs', messages
        # print 'mscounts', message_count

        # json data
        data = {
            'board': self.board,
            'article_id': article_id,
            'article_title': title,
            'author': author,
            'date': date,
            'content': content,
            'ip': ip,
            'message_count': message_count,
            'messages': messages
        }
        # print 'original:', d
        # return json.dumps(data, sort_keys=True, ensure_ascii=False)
        return data

    def getLastPage(self):
        content = requests.get(
            url= 'https://www.ptt.cc/bbs/' + self.board + '/index.html',
            cookies={'over18': '1'}
        ).content.decode('utf-8')
        first_page = re.search(r'href="/bbs/' + self.board + '/index(\d+).html">&lsaquo;', content)
        if first_page is None:
            return 1
        return int(first_page.group(1)) + 1

    def store(self, filename, data, mode):
        with codecs.open(self.filename, mode, encoding='utf-8') as f:
            f.write(data)

    def getFilename(self):
        return self.filename

    def saveJsonFile(self, board, jsonlist, current_date):
        fileName = projectPath + '\\' + board + '/historyData/' + str(current_date) + '.json'
        if os.path.isfile(fileName):
            with open(fileName, 'r', encoding='utf-8') as File:
                jsonFile = json.load(File)
            articles_list = jsonFile['articles']
            articles_list.extend(jsonlist)
            articles = {}
            articles['articles'] = articles_list
            with open(fileName, 'w', encoding='utf-8') as outfile:
                json.dump(articles, outfile, sort_keys=True, ensure_ascii=False)
        else:
            articles = {}
            articles['articles'] = jsonlist
            with open(fileName, 'w', encoding='utf-8') as outfile:
                json.dump(articles, outfile, sort_keys=True, ensure_ascii=False)

def getHistory(board, start, end):
    PttWebCrawler(board ,True ,start=start,end=end)

if __name__ == "__main__":
    if len(sys.argv) == 4:
            getHistory(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
    else:
        print("\nPlease enter the correct parameters\n")
        print("HistoryDataCrawler.py <board> <start_page> <end_page>\n")

   
