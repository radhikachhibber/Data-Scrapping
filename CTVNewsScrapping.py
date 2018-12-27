import bs4
from urllib.request import urlopen as uReq
import uuid
import urllib
import json
import pyodbc
from bs4 import BeautifulSoup as soup

#scraping of Nafta data from ctv.com
i = 0
containers = []
while i < 6:
    my_url = 'https://www.ctvnews.ca/search-results/search-ctv-news-7.137?page=0' + str(i) + '&sortOrder=date&q=nafta&fdate=&ftype=ctvnews.StandardArticle&fpage='
    uClient = uReq(my_url)
    page_html = uClient.read()
    uClient.close()
    page_soup = soup(page_html, "html.parser")
    containers = containers + page_soup.findAll("div",{"class":"search-main"})
    i = i + 1
#print(containers)
objCtvNews = []
for container in containers:
    list = container.ul.findAll("li",{"class":"searchHit"})
    for lst in list:
        ctv = {}
        ctv["NewsId"] = uuid.uuid4()
        ctv["NewsTitle"] = lst.a.text.strip()
        articleUrl = lst.a['href']
        client = uReq(articleUrl)
        page_html = client.read()
        client.close()
        pageSoup = soup(page_html, "html.parser")
        article_container = pageSoup.findAll("div",{"class","articleBody"})
        text = ""
        for article in article_container:
            pText = article.findAll("p")
            for p in pText:
                text = text+ p.text.replace("”", "").replace("“", "").replace("’", "").strip()
        ctv["NewsText"] = text
        objCtvNews.append(ctv)

connection = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"
                            r"SERVER=.\bdats2018;"
                            r"DATABASE=BDAT;"
                            r"Trusted_Connection=yes;")
cursor = connection.cursor()

# delete old records
cursor.execute("Delete from dbo.CTVNews")
connection.commit()

# save scrapped data into database
for ctvNews in objCtvNews:
    for key in ctvNews:
        NewsId = ctvNews['NewsId']
        NewsTitle = ctvNews['NewsTitle']
        NewsText = ctvNews['NewsText']
    sql_param = (NewsId,NewsTitle,NewsText)
    print(sql_param)
    sql = ("INSERT INTO dbo.CTVNews (NewsId, NewsTitle, NewsText) VALUES (?,?,?)")
    cursor.execute(sql,sql_param)
    connection.commit()