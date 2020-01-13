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
import pymongo
import warnings
warnings.filterwarnings("ignore")

__version__ = '1.0'

# if python 2, disable verify flag in requests.get()
VERIFY = True
if sys.version_info[0] < 3:
    VERIFY = False
    requests.packages.urllib3.disable_warnings()


projectPath = os.path.dirname(__file__)
board_list_file_name = 'board_list.txt'
filter_list_file_name = 'filter_list.txt'
PttData_directory_name = 'PttData'
dailyData = 'DailyData'
historyData = 'HistoryData'


dbclient = pymongo.MongoClient("mongodb://140.120.13.244:27018/")
db = dbclient['Fb']
db_pagelist = db['pagelist']


def checkAndCreateDirectory(path):
    if not os.path.exists(path):
        os.mkdir(path)

class PttWebCrawler(object):
    """docstring for PttWebCrawler"""
    def __init__(self, board, iOrA, day_range, article_id=None, titleCallback=lambda x:x, contentCallback=lambda x:x):
        self.board = board
        self.PTT_URL = 'https://www.ptt.cc'
        self.titleCallback = titleCallback
        self.contentCallback = contentCallback
        self.parse_finished = False

        # check and create file
        checkAndCreateDirectory(os.path.join(projectPath, PttData_directory_name))
        checkAndCreateDirectory(os.path.join(projectPath, PttData_directory_name, board))
        checkAndCreateDirectory(os.path.join(projectPath, PttData_directory_name, board, dailyData))
        checkAndCreateDirectory(os.path.join(projectPath, PttData_directory_name, board, historyData))

        # means crawl range of articles
        if iOrA:
            start = self.getLastPage() - 1
            print('start page:', start)
            old_days_range = getdaysAgo(day_range)
            print("old_days_range", old_days_range)

            articlesList = []
            for index in range(start, 1, -1):
                print('Processing index:', str(index))
                resp = requests.get(
                    url=self.PTT_URL + '/bbs/' + self.board + '/index' + str(+index) + '.html',
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
                        articleData = self.parse(link, article_id, old_days_range)
                        if 'date' in articleData and articleData['article_title'] != '' and articleData['author'] != '' and articleData['date'] != '':
                            articlesList.append(articleData)
                    except Exception as e:
                        # print(link)
                        # print(str(e))
                        pass
                time.sleep(0.1)
                if self.parse_finished:
                    print("parse finished, end page: ", index)
                    break

            if len(articlesList) != 0:
                self.saveJsonFile(board, articlesList)
                print('save file finished')

    def parse(self, link, article_id, old_days_range):
        # print('Processing article:', article_id)
        resp = requests.get(url=link, cookies={'over18': '1'}, verify=VERIFY)
        if resp.status_code != 200:
            # print('invalid url:', resp.url)
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
            
            isFinished = self.isOlderThanDaysRange(old_days_range, date)
            if isFinished:
                self.parse_finished = True

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

    def saveJsonFile(self, board, jsonlist):
        articles = {}
        articles['articles'] = jsonlist
        today = getToday()
        fileName = os.path.join(projectPath, PttData_directory_name, board, dailyData, (str(today)+'.json'))
        with open(fileName, 'w', encoding='utf-8') as outfile:
            json.dump(articles, outfile, sort_keys=True, ensure_ascii=False)

    def isOlderThanDaysRange(self, old_days_range, day):
        try:
            old_days_datetime = datetime.datetime.strptime(str(old_days_range), '%Y-%m-%d')
            day_datetime = datetime.datetime.strptime(day, '%a %b %d %H:%M:%S %Y')
            return day_datetime < old_days_datetime
        except Exception as e:
            return False

def getJson(fileName):
    with open(fileName, 'r', encoding='utf-8') as File:
        jsonFile = json.load(File)
        return jsonFile

def saveJson(jsonData, fileName):
    with open(fileName, 'w', encoding='utf-8') as File:
        json.dump(jsonData, File, sort_keys=True, ensure_ascii=False)

def readBoardList():
    with open(os.path.join(projectPath, board_list_file_name), 'r', encoding='utf-8') as fp:
        board_list = []
        line = fp.readline()
        while line:
            board_info = line.replace(' ', '').strip('\n').split(',')
            board_list.append(board_info)
            line = fp.readline()
    return board_list

def readFilterList():
    with open(os.path.join(projectPath, filter_list_file_name), 'r', encoding='utf-8') as fp:
        filter_list = []
        line = fp.readline()
        while line:
            board_info = line.replace(' ', '').strip('\n')
            filter_list.append(board_info)
            line = fp.readline()
    return filter_list


def getToday():
    today=datetime.date.today()
    return today

def getdaysAgo(daysAgo): 
    today=datetime.date.today()
    someday=datetime.timedelta(days=daysAgo)
    day=today-someday
    return day

def getNewArticles(board, day_range):
    PttWebCrawler(board ,True, day_range=day_range)

# 將爬完的 Json檔依日期歸類到 history
def storeToHistory(board):
    print('開始歸類到 historyData')
    today = getToday()
    todayFilePath = os.path.join(projectPath, PttData_directory_name, board, dailyData, (str(today)+'.json'))
    if os.path.isfile(todayFilePath):
        toadyJson = getJson(todayFilePath)
        articles = toadyJson['articles']
        try:
            current_day = datetime.datetime.strptime(articles[0]['date'], '%a %b %d %H:%M:%S %Y').date()
        except Exception as e:
            current_day = datetime.datetime.strptime('2000-01-01', '%Y-%m-%d').date()

        history_file_path = os.path.join(projectPath, PttData_directory_name, board, historyData, (str(current_day)+'.json'))
        history_json = {}
        history_json['articles'] = []
        if os.path.isfile(history_file_path):
            history_json = getJson(history_file_path)

        for artilce in articles:
            try:
                article_day = datetime.datetime.strptime(artilce['date'], '%a %b %d %H:%M:%S %Y').date()
                if article_day != current_day:
                    saveJson(history_json, history_file_path)
                    current_day = article_day
                    history_file_path = os.path.join(projectPath, PttData_directory_name, board, historyData, (str(current_day)+'.json'))
                    history_json = {}
                    history_json['articles'] = []
                    if os.path.isfile(history_file_path):
                        history_json = getJson(history_file_path)

                is_found = False
                for history_article in history_json['articles']:
                    if artilce['article_id'] == history_article['article_id']:
                        is_found = True
                        artilce_length = len(artilce['messages'])
                        history_article_length = len(history_article['messages'])
                        if artilce_length > history_article_length:
                            history_article = artilce
                        break
                if is_found == False:
                    history_json['articles'].append(artilce)
            except Exception as e:
                pass
            
        if len(history_json['articles']) != 0:
            saveJson(history_json, history_file_path)
            
    else:
        print('歸類到 historyData 時找不到檔案', board, str(today))
    print('歸類到 historyData 結束')


def datetime2timestamp(date):
    if date == '':
        return ''
    date_time = datetime.datetime.strptime(date, '%a %b %d %H:%M:%S %Y')
    time_stamp = datetime.datetime.timestamp(date_time)
    return str(int(time_stamp))
        

def updateDataToMongodb(board, filter_list):
    today = getToday()
    todayFilePath = os.path.join(projectPath, PttData_directory_name, board, dailyData, (str(today)+'.json'))
    if os.path.isfile(todayFilePath):
        toadyJson = getJson(todayFilePath)
        for article in toadyJson['articles']:
            content_data = {}
            content_data['article_id'] = article['article_id']
            content_data['article_title'] = article['article_title']
            content_data['Name'] = article['author']
            content_data['board'] = article['board']
            try:
                content_data['Content'] =article['article_title'] + '\n' + article['content']
            except:
                content_data['Content'] =article['content']
            try:
                times_tamp =datetime2timestamp(article['date'])
            except:
                continue
            content_data['Time'] = times_tamp
            content_data['ip'] = article['ip']
            content_data['Type'] = 'content'
            link = 'https://www.ptt.cc/bbs/'+board+'/'+article['article_id']+'.html'
            content_data['Postlink'] = link
            db_board_contnet = db[board+'_content']
            db_board_contnet.update(content_data, content_data, upsert=True)

            db_board_contnet_filtered = db[board+'_content_filtered']
            for filter_word in filter_list:
                if filter_word in content_data['Content']:
                    db_board_contnet_filtered.update(content_data, content_data, upsert=True)
                    break

            db_board_comment = db[board+'_comment']
            db_board_comment_filtered = db[board+'_comment_filtered']
            for message in article['messages']:
                comment_data = {}
                comment_data['article_id'] = article['article_id']
                comment_data['article_title'] = article['article_title']
                comment_data['Name'] = message['push_userid']
                comment_data['Content'] = message['push_content']
                comment_data['ip'] = message['push_ip']
                comment_data['Time'] = times_tamp
                comment_data['Postlink'] = link
                comment_data['Type'] = 'comment'
                db_board_comment.update(comment_data, comment_data, upsert=True)

                for filter_word in filter_list:
                    if filter_word in comment_data['Content']:
                        db_board_comment_filtered.update(comment_data, comment_data, upsert=True)
                        break
                

    else:
        print('上傳時找不到檔案', board, str(today))


def crawlPttBoards(board_list, day_range=2):
    for board in board_list:
        if board[0] != '':
            print(f'start crawling artciles of {board[0]}  {board[1]}')
            getNewArticles(board[0], day_range)
            storeToHistory(board[0])
            print('=========================================\n')

def updatePageListToMongodb(board_list):
    print('start updating page list')
    for board in board_list:
        name = board[0] + ' ' + board[1]+ ' 文章'
        id = board[0]+'_content'
        data = {name: id}
        db_pagelist.update(data, data, upsert=True)
        name = board[0] + ' ' + board[1]+ ' 留言'
        id = board[0]+'_comment'
        data = {name: id}
        db_pagelist.update(data, data, upsert=True)
        name = board[0] + ' ' + board[1]+ ' 文章(已過濾)'
        id = board[0]+'_content_filtered'
        data = {name: id}
        db_pagelist.update(data, data, upsert=True)
        name = board[0] + ' ' + board[1]+ ' 留言(已過濾)'
        id = board[0]+'_comment_filtered'
        data = {name: id}
        db_pagelist.update(data, data, upsert=True)

    print('finished updating page list')
    print('=========================================\n')


def updatePostDetailToMongodb(board_list, filter_list):
    for board in board_list:
        print(f'start updating articles of {board[0]} to mongodb')
        updateDataToMongodb(board[0], filter_list)
        print(f'finished updateing articles of {board[0]}')

        
if __name__ == "__main__":
    board_list = readBoardList()
    crawlPttBoards(board_list, day_range=2)
    # updatePageListToMongodb(board_list)
    # filter_list = readFilterList()
    # updatePostDetailToMongodb(board_list, filter_list)
    
