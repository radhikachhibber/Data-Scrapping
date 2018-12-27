from urllib.request import urlopen, Request
import uuid
import urllib, json
import pyodbc

url = 'https://pkgstore.datahub.io/JohnSnowLabs/new-york-city-leading-causes-of-death/1/new-york-city-leading-causes-of-death-csv_json/data/fafa528cb24d68721996ed9e25f3480f/new-york-city-leading-causes-of-death-csv_json.json'
request = Request(url)
response = urlopen(request)
html = json.loads(response.read())

DeathArray = []

for deaths in html:
    death = {}
    death["deathid"] = uuid.uuid4()
    death["date"] = deaths['Year']
    death["leadingcause"] = deaths['Leading_Cause']
    death["sex"] = deaths['Sex']
    death["raceethnicity"] = deaths['Race_Ethnicity']
    death["noofdeaths"] = deaths['Deaths']
    death["deathrate"] = deaths['Death_Rate']
    death["ageadjusteddeathrate"] = deaths['Age_Adjusted_Death_Rate']
    DeathArray.append(death)


connection = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"
                      r"SERVER=.\BDATS2018;"
                     r"DATABASE=BDAT;"
                      r"TRUSTED_CONNECTION=yes;"
                      )
cursor = connection.cursor()

for x in DeathArray:
    for key in x:
        deathId = uuid.uuid4()
        date = x['date']
        leadingcause = x['leadingcause']
        sex = x['sex']
        raceethnicity = x['raceethnicity']
        noofdeaths = x['noofdeaths']
        deathrate = x['deathrate']
        ageadjusteddeathrate = x['ageadjusteddeathrate']
    sql_param = (deathId,date,leadingcause,sex,raceethnicity,noofdeaths,deathrate,ageadjusteddeathrate)
    print(sql_param)
    sql = ("INSERT INTO dbo.Deaths (DeathId, Date, LeadingCause, Sex, RaceEthnicity, Deaths, DeathRate, AgeAdjustedDeathRate) VALUES (?,?,?,?,?,?,?,?)")
    cursor.execute(sql,sql_param)
    connection.commit()
        