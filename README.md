# PTT Crawler

根據[jwlin](https://github.com/jwlin)的[ptt-web-crawler](https://github.com/jwlin/ptt-web-crawler)改寫
* 根據指定頁數爬取[PTT](https://www.ptt.cc/bbs/index.html)網頁版各版的文章，並依照日期做歸檔
* 過濾資料內空白、空行及特殊字元
* JSON 格式輸出
* 修復部分文章抓不到IP的BUG

輸出 JSON 格式
```
{
    "article_id": 文章 ID,
    "article_title": 文章標題 ,
    "author": 作者,
    "board": 板名,
    "content": 文章內容,
    "date": 發文時間,
    "ip": 發文位址,
    "message_count": { # 推文
        "all": 總數,
        "boo": 噓文數,
        "count": 推文數-噓文數,
        "neutral": → 數,
        "push": 推文數
    },
    "messages": [ # 推文內容
      {
        "push_content": 推文內容,
        "push_ip": 推文位址,
		    "push_datetime": 推文時間,
        "push_tag": 推/噓/→ ,
        "push_userid": 推文者 ID
      },
      ...
      ]
}
```

### 執行參數說明

```commandline
python historyDataCrawler.py  看板名稱 起始頁數 結束頁數 (設為負數則以倒數第幾頁計算) 
```

### 範例

爬取 Gossiping 板第 1 頁 (https://www.ptt.cc/bbs/Gossiping/index1.html) 
到第 20 頁 (https://www.ptt.cc/bbs/Gossiping/index20.html) 的內容，
依照日期做歸檔，並輸出至 `Gossiping` 目錄下
