from bs4 import BeautifulSoup
import csv
import requests
import json
import pandas as pd
import isodate

"""
DISCLAIMER: THIS SCRIPT WAS MADE FOR PERSONAL USE. IT'S OPTIMIZED FOR SPANISH'S TAKEOUT EXPORTS. 

INSTRUCTIONS:

BEFORE RUNNING THE SCRIPT:!
OPEN THE YOUTUBE'S HISTORY WITH THE NOTEPAD AND SAVE IT AS A .TXT FILE.

1) COMPLETE THE NECESSARY DATA ABOVE IN THE CODE
2) RUN CreateFile() and Scrapper(TAKEOUT_DATA)
3) ONCE THE PRIMARY CSV'S DATA GATHERING IS COMPLETE RUN CreateFile() and Scrapper(TAKEOUT_DATA).
    IF THE PRIMARY CSV CONTAINS MORE THAN 10.000 ROWS, SET THE BEGINNING AND THE END OF EVERY DAILY RUN.
    ! ! ! ! D O N ' T  F O R G E T ! ! !  TO COMMENT THE #CreateMetadataCSVFile() ON THE SECOND RUN.
    IF DON'T A NEW FILE WILL BE CREATED AND THE STORED DATA WILL BE LOSE

"""

#TAKEOUT DATA (MUST INCLUDE PATH)
TAKEOUT_DATA=" PATH+FILE_NAME OF THE .TXT FILE GENERATED BEFORE"
API_KEY="INSERT API KEY HERE" 


#SCRAPPED DATA FROM TAKEOUT WILL BE STORED IN THIS FIRST FILE
PRIMARY_CSV_NAME="youtube_history.csv" 
#PRIMARY DATA + METADATA GATHERED USING YOUTUBE'S API WILL BE STORED IN THIS FILE
TOTAL_DATA="final_data.csv"

#WHERE IN THE PRIMARY CSV FILE THE SCRIPT WILL START REQUESTING TO THE API (DAILY LIMIT 10.000)
START=0
END=1

month_dict={'ene':'01','feb':'02', 'mar':'03', 'abr':'04','may':'05','jun':'06','jul':'07','ago':'08','sept':'09','sep':'09','oct':'10', 'nov':'11', 'dic':'12'}

        
def InsertNewVideo(data):
    #WRITES IN THE PRIMARY CSV FILE A ROW WITH THE PRIMARY DATA
    with open (PRIMARY_CSV_NAME,'a', newline='',encoding='utf-8') as file:
        writer=csv.writer(file)
        title=data[0]
        video_url=data[1]
        channel_name=data[2]
        channel_url=data[3]
        time=(data[4].split(" ")[-2])
        
        #TEMPORAL_DATE IS A TUPLE, CONTAINS SEEN DATE AND TIME
        temp_date=data[4].split(" ")
        
        try:
            month=month_dict[str(temp_date[1].replace(".",""))]
        except KeyError:
            pass

        
        year=temp_date[2]
        
        
        t_day=[]
        #THE DAY'S VALUE COULD CONTAIN EXTRA CHARACTERS (LAST CHANNEL'S CHARACTER). THE FOLLOWING LOOP CLEANS IT.
        for c in temp_date[0]:
            if c.isnumeric()==True:
                #EVERY NUMERIC CHARACTER IS STORED IN TEMPORAL_DAY
                t_day.append(c)
        #TEMPORAL_DAY TOGHETHER.        
        pre_day=("").join(t_day)
        
        try:
            #IF THE DAY IS BIGGER THAN 31, SO THE FIRST CHARACTER IS SKIPPED
            if int(pre_day)>31:
                day=pre_day[1:]
            else:
                day=pre_day
        except ValueError:
            pass
        
        try:
            #WRITING THE ROW
            writer.writerow([title,video_url,channel_name,channel_url,year,month,day,time])
        except UnboundLocalError:
            print(data)

def Scrapper(TAKEOUT_DATA):
    #GATHERS DATA FROM THE HISTORY FILE.
    with open (TAKEOUT_DATA,encoding="utf8") as f:
        file=f.read()
        soup=BeautifulSoup(file, 'lxml')
        divs=soup.find_all('div')
        
        for element in divs:
            try:
                #RAW DATA
                chunk=element.find_all('a')
                #TITLE
                title=(chunk[0].get_text()).replace(",","").replace(";","")
                #VIDEO_URL
                link=(chunk[0]['href'])
                #CHANNEL_NAME
                channel_name=(chunk[1].get_text())
                #CHANNEL_URL
                channel_url=(chunk[1]['href'])
                
                #RAW DATE DATA
                text_chunk=element.find_all('div')
                #LAST 24 CHARACTERS FORM THE STR 
                date=((text_chunk[1].get_text())[-24:])
                if len(date)>20 and date !="":
                    data=(title,link,channel_name,channel_url,date)
                    InsertNewVideo(data)
            except AttributeError:
                pass
            except IndexError:
                pass

def CreateFile():
    #CREATES CSV FILE WHICH WILL CONTAIN THE FIRST ACCESS DATA
    with open (PRIMARY_CSV_NAME,'w', newline='') as file:
        writer=csv.writer(file)
        writer.writerow(["TITLE","VIDEO_URL","CHANNEL","CHANNEL_URL","YEAR","MONTH","DAY","TIME"])
    
def CreateMetadataCSVFile():  
    #CREATES THE FINAL CSV  
    with open (TOTAL_DATA,"w", newline="",encoding='utf-8') as f:
        writer=csv.writer(f)
        writer.writerow(["TITLE", "VIDEO_URL","CHANNEL", "CHANNEL_URL", "SEEN_YEAR", "SEEN_MONTH", "SEEN_DAY", "SEEN_TIME","VIDEO_CATEGORY", "VIDEO_TAGS", "DEFAULT_LANGUAGE", "DEFAULT_AUDIO_LANG", "VIDEO_PUBLISHED_DATE","VIDEO_PUBLISHED_TIME", "VIDEO_DESCRIPTION", "VIDEO_DURATION", "VIDEO_DEFINITION", "VIDEO_RATING"])

def MetadataCapure(url):
    #GETS VIDEO_ID FROM URL
    video_id=url.split("v=")[-1]
    #GET REQUEST FROM YOUTUBE API
    metadata=(requests.get(('https://www.googleapis.com/youtube/v3/videos?id={}&part=snippet,contentDetails,statistics,recordingDetails&key={}').format(video_id,API_KEY)).text)
    #CONVERTS RESPONSE TO JSON
    RAW_CAPTURE=json.loads(metadata)
   
    try:
        #SNIPPETS CHUNK
        snippet=RAW_CAPTURE["items"][0]['snippet']
        
        video_tags=snippet.get("tags")
        description=snippet.get("description")
        default_lang=snippet.get("defaultLanguage")
        default_audio_lang=snippet.get("defaultAudioLanguage")
        category_id=snippet.get("categoryId")
        video_publishedAt=((snippet.get("publishedAt")[:-1])).split('T')
        
        #CONTENT DETAILS CHUNK
        contentDetails=(json.loads(metadata)["items"][0]["contentDetails"])
        
        #USING ISODATE.PARSE_DURATION TO CONVERT PERIOD ISO 84601 FORMAT TO TIMESTAMP
        video_duration=isodate.parse_duration(contentDetails["duration"])
        video_definition=contentDetails["definition"]
        contentRating=contentDetails["contentRating"]
    
    except IndexError:
  
        video_tags=""
        description=""
        default_lang=""
        default_audio_lang=""
        category_id=""
        video_publishedAt=["",""]
           
        video_duration=""
        video_definition=""
        contentRating=""
        
        
    
    return category_id, video_tags, default_lang, default_audio_lang, video_publishedAt, description, video_duration, video_definition, contentRating

def WriteMetadata():
    
    data = pd.read_csv(PRIMARY_CSV_NAME)
    # OPENS THE PRIMARY CSV FILE
    
    for c in data.values[START:END]:
    #GATHERS DATA
        title=c[0]
        video_url=c[1]
        channel_name=c[2]
        channel_url=c[3]
        year=c[4]
        month=c[5]
        day=c[6]
        time=c[7]

        
        with open (TOTAL_DATA,"a", newline='',encoding='utf-8')as f:
            writer=csv.writer(f)
            
            #GATHERS METADATA FROM YOUTUBE'S API
            metadata=(MetadataCapure(video_url))
            
            #CLEANS DATA
            category_id=metadata[0]
            video_tags=metadata[1]
            default_lang=metadata[2]
            default_audio_lang=metadata[3]
            video_published_date=metadata[4][0]
            video_published_time=metadata[4][1]
            description=metadata[5]
            video_duration=metadata[6]
            video_definition=metadata[7]
            contentRating=metadata[8]
            
            writer.writerow([title, video_url,channel_name, channel_url, year, month, day, time, category_id, video_tags, default_lang, default_audio_lang, video_published_date,video_published_time, description, video_duration, video_definition, contentRating])
        
        print(title + "DONE")
        

#CreateFile()
#Scrapper(TAKEOUT_DATA)        


CreateMetadataCSVFile()
WriteMetadata()
