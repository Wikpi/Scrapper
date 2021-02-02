from bs4 import BeautifulSoup
import requests, lxml
import mysql.connector

db = mysql.connector.connect(
    host="localhost",
    # Your user name
    user="root",
    # Your password
    passwd="root",
    database="vardai"
)

cursor = db.cursor()
# Create a database
#cursor.execute("CREATE DATABASE vardai")
# Delete tables
if 'Vardai' or 'Vardadieniai' in db:
    dropp = "DROP TABLE Vardadieniai"
    drop = "DROP TABLE vardai"
    cursor.execute(drop)
    cursor.execute(dropp)
# Create new tables
cursor.execute("CREATE TABLE Vardai"
               "(personID int PRIMARY KEY AUTO_INCREMENT, "
               "name LONGTEXT CHARACTER SET utf8 COLLATE utf8_lithuanian_ci, "
               "explanation VARCHAR(300) CHARACTER SET utf8 COLLATE utf8_lithuanian_ci, "
               "origin VARCHAR(150) CHARACTER SET utf8 COLLATE utf8_lithuanian_ci)")
cursor.execute("CREATE TABLE Vardadieniai "
               "(personID int PRIMARY KEY AUTO_INCREMENT, "
               "name LONGTEXT CHARACTER SET utf8 COLLATE utf8_lithuanian_ci, "
               "day_month int UNSIGNED, day_day bigint UNSIGNED)")

numb = 0
# Start
while True:
    numb += 1
    source = requests.get('https://www.vardai.org/tema/lietuviski-vardai/page/' + str(numb)).text
    if numb > 274:
        break
    # Track page
    print('https://www.vardai.org/tema/lietuviski-vardai/page/' + str(numb))
    soup = BeautifulSoup(source, 'lxml')

    for find in soup.findAll("div", {"class": "posttop"}):
        # Find name
        name = find.h2.text
        name = name.split(' ')[0]
        name = str(name)
        if ',' in name:
            name = name.replace(',', '')

        # Find new link
        link = find.h2
        link = str(link)
        link = link.split(' ')[2]
        link = link.split('"')[1]

        info_source = requests.get(link).text
        soup_info = BeautifulSoup(info_source, 'lxml')
        month = 'text'
        day = 'text'
        # Find the month and day
        try:
            find_date = soup_info.find('span', id='vardadienis')
            find_date = find_date.b.text
            if '-' in find_date:
                find_date = find_date.split('-')[0]
            if ' ' in find_date:
                find_date = find_date.split(' ')[0]
            month = find_date.split('.')[0]
            day = find_date.split('.')[1]

        except Exception:
            month = 0
            day = 0
            pass
        explanation = ''
        origin = ''
        # Find the explanation and the origin
        try:
            find_info = soup_info.find('span', id='plati').text
            find_info = find_info.split('\n')
            for v in find_info:
                if len(v) > 30 and len(v) < 300:
                    if 'Panašūs' in v:
                        continue
                    explanation = v
            find_origin = soup_info.find('span', id='kilme').text
            origin = find_origin.split(':')[1]
        except AttributeError:
            pass
        # Put everything into tables
        try:
            cursor.execute("INSERT INTO Vardai (name, explanation, origin) VALUES (%s, %s, %s)", (name, explanation, origin))
            cursor.execute("INSERT INTO Vardadieniai (name, day_month, day_day) VALUES (%s, %s, %s)", (name, month, day))
            db.commit()
        except Exception as e:
            pass
# Print tables in python
#cursor.execute("SELECT * FROM Vardai")
#for x in cursor:
#    print(x)
#cursor.execute("SELECT * FROM Vardadieniai")
#for h in cursor:
#    print(h)
cursor.close()
