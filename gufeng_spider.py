#Author: cnr
#Date: 2018-12-5

import requests
from multiprocessing import Pool
import re
import json
from bs4 import BeautifulSoup
import pymongo


'''
初始化DB
GUFENG_LIST:古风歌单的DB
SONG：所有古风歌单里面的歌曲的DB
GUFENG_LYRIC：所有歌曲里面歌词及关键信息的DB
'''
client=pymongo.MongoClient('localhost',27017)
db=client['lyrics']
GUFENG_LIST=db['GUFENG_LIST']
GUFENT_SONG=db['SONG']
GUFENG_LYRIC=db['GUFENG_LYRIC']

#歌词和时间是另外的URL，所以单独拿出来，调用后会返回一个dic
def lyric_time(id):
    url = 'http://music.163.com/api/song/lyric?id='+str(id)+'&lv=-1&kv=-1&tv=-1'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36'}
    html=requests.get(url,headers=headers)
    if html.status_code==200:
        #pattern是歌词中所有【00:00：00】这样的时间数据pattern，这里默认最后一个为歌曲的长度。
        pattern=re.compile(r'\[(.*?)]',re.S)
        try:
            time=re.findall(pattern,json.loads(html.text)['lrc']['lyric'])
            lyric=re.sub(pattern,'',json.loads(html.text)['lrc']['lyric'])
            try:
                seconds=int(time[-1].split(':')[0])*60+int((time[-1].split(':')[-1]).split('.')[0])
                data={
                    'lyric':lyric,
                    'time':seconds
                }
            except:
                data={
                    'lyric':lyric,
                    'time':''
                }
        #如果没有的话很有可能是纯音乐，所以时间和歌词都是空。
        except Exception as e:
            print ('ERROR ON lyric_time,MAY BE PURE MUSIC,ID IS :',id,'ERROR IS ',e)
            data = {
                'lyric': '',
                'time': ''
            }
        return data
    else:
        data={
            'lyric':'',
            'time':''
        }
        return data

#这里是歌词爬取的fuc，输入歌曲的ID,调用后会写如数据库。
def lyric_crasler(id):
    url='https://music.163.com/song?id='+str(id)
    headers = {'User-Agent': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)'}
    html=requests.get(url,headers=headers)
    if html.status_code==200:
        soup=BeautifulSoup(html.text,'html.parser')
        lyric_and_time=lyric_time(id)
        data={
        'ID':id,
        'name':soup.find('div',class_='cnt').find('div',class_='tit').find('em').get_text(),
        'singer':soup.find('div',class_='cnt').find_all('p',class_='des s-fc4')[0].find('span').get_text(),
       'album':soup.find('div',class_='cnt').find_all('p',class_='des s-fc4')[1].find('a').get_text(),
        'lyric':lyric_and_time['lyric'].replace('\n',','),
        'time':lyric_and_time['time']
        }
        if GUFENG_LYRIC.insert_one(data):
            print ('SUCCESSFULLY INSERT TO DB:',data)
        else:
            print('FAILED TO INSERT!')
    else:
        print (html.status_code,' WRONG, ID IS',i)

#这里是歌曲函数，调用后会从GUFENG_LIST的数据库里选取所有的歌单ID，然后一个一个歌单把里面的歌曲名字和歌曲ID 写入GUFENG_SONG的数据库。
#这里把播放量小于10万的数据都去除了，为了让数据质量更高一点。
def song_crawler():
    i=0
    for list in GUFENG_LIST.find():
        if int(list['hot'])>=100000:
            id=list['id']
            url = 'https://music.163.com/playlist?id='+str(id)
            headers = {'User-Agent': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)'}
            html=requests.get(url,headers=headers)
            if html.status_code==200:
                pattern=re.compile(r'<li><a href="/song\?id=(\d+)">(.*?)</a></li>',re.S)
                songs=re.findall(pattern,html.text)
                for song in songs:
                    data={
                        'id':song[0],
                        'name':song[1]
                    }
                    if GUFENT_SONG.insert_one(data):
                        print ('insert successfully',data)
                    else:
                        print ('insert wrong')
                print(i)
                i += 1

#这里是歌单函数，把古风分类下所有的歌单的ID和名字还有播放量写入数据库。
def playlist_crawler():
    playlist=[]
    for i in range (0,1296,35):
        url='https://music.163.com/discover/playlist/?order=hot&cat=%E5%8F%A4%E9%A3%8E&limit=35&offset='+str(i)
        headers={'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)'}
        html=requests.get(url,headers=headers)
        if html.status_code==200:
            soup = BeautifulSoup(html.text,'html.parser')
            contents =soup.find('ul',class_='m-cvrlst f-cb').find_all('li')
            for content in contents:
                data={
                    'name':content.find('p',class_='dec').get_text().replace('\n',''),
                    'id':content.find('p', class_='dec').find('a')['href'].replace('/playlist?id=',''),
                    'author':content.find('a',class_='nm nm-icn f-thide s-fc3').get_text(),
                    'hot':int(content.find('div',class_='bottom').find_all('span')[1].get_text().replace('万','0000'))
                }
                if GUFENG_LIST.insert_one(data):
                    print ('successfully insert',data)
                else:
                    print ('failed insert')

#主函数，一个一个调用函数就可以，没有做成一次性的，因为写的时候是一边爬一边改的。
#各位如果有兴趣可以改成一套整体流程。
#为了加快速度，用了多进程，线程大家都知道是个坑，不过爬的时候好像没有被封ip的情况，所以没有加代理。
if __name__=="__main__":
    #playlist_crawler()
    #song_crawler()
    list=[]
    for song in GUFENT_SONG.find():
        list.append(song['id'])
    lists=set(list)
    pool=Pool(processes=3)
    for i in lists:
        pool.apply_async(lyric_crasler,(i,))
    pool.close()
    pool.join()
