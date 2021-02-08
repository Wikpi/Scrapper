from bs4 import BeautifulSoup
import requests, lxml
import mysql.connector

db = mysql.connector.connect(
    host="localhost",
    # Your user name
    user="root",
    # Your password
    passwd="root",
    database="name_scrapper"
)

cursor = db.cursor()
# // Create a database //
# cursor.execute("CREATE DATABASE name_scrapper")

# // Delete tables //
# // Comment out the whole if-block if tables don't exist //
if 'names' or 'name_dates' in db:
    drop_dates = "DROP TABLE name_dates"
    drop_names = "DROP TABLE names"
    cursor.execute(drop_names)
    cursor.execute(drop_dates)

# // Create new tables //
cursor.execute("CREATE TABLE names"
               "(ID int PRIMARY KEY AUTO_INCREMENT, "
               "name VARCHAR(50) CHARACTER SET utf8 COLLATE utf8_lithuanian_ci, "
               "gender VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_lithuanian_ci,"
               "explanation VARCHAR(300) CHARACTER SET utf8 COLLATE utf8_lithuanian_ci, "
               "origin VARCHAR(150) CHARACTER SET utf8 COLLATE utf8_lithuanian_ci,"
               "UNIQUE (name))")
cursor.execute("CREATE TABLE name_dates "
               "(ID int PRIMARY KEY AUTO_INCREMENT, "
               "name VARCHAR(50) CHARACTER SET utf8 COLLATE utf8_lithuanian_ci, "
               "day_month int UNSIGNED,"
               "day_day int UNSIGNED, "
               "UNIQUE (name))")

base_url = 'https://www.vardai.org/tema/lietuviski-vardai/page/'
page_numb = 0


# Main function start
while True:
    page_numb += 1
    crr_page = requests.get(base_url + str(page_numb)).text

    # failed to load page - it's critical, exit
    if crr_page.status_code != 200:
        break

    soup = BeautifulSoup(crr_page, 'lxml')

    # Track page
    print(base_url + str(page_numb))

    # Finds the last page
    last_page = 0
    try:
        last_page = soup.find('a', {'class': 'last'}).text
        last_page = last_page.split(' ')[0]
    except Exception:
        pass

    for find_item in soup.findAll("div", {"class": "posttop"}):
        # Find name
        try:
            name = find_item.h2.text
            name = name.split(' ')[0]
            name = str(name)
        except Exception:
            continue
        if ',' in name:
            name = name.replace(',', '')

        # Find new link
        soup_info = ''
        try:
            link = find_item.h2
            link = str(link)
            link = link.split(' ')[2]
            link = link.split('"')[1]
            info_source = requests.get(link).text
            soup_info = BeautifulSoup(info_source, 'lxml')
        except Exception:
            print("Error with accessing data from name: " + name)
            pass

        # Find the month and day
        month = '0'
        day = '0'
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
            pass

        # Find gender
        gender = ''
        try:
            find_gender = soup_info.find('span', id='lytis').text
            gender = find_gender.split(':')[1]
        except Exception:
            pass

        # Find the explanation and the origin
        explanation = ''
        origin = ''
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
            cursor.execute("INSERT INTO names (name, gender, explanation, origin) VALUES (%s, %s, %s, %s)", (name, gender, explanation, origin))
            cursor.execute("INSERT INTO name_dates (name, day_month, day_day) VALUES (%s, %s, %s)", (name, month, day))
            db.commit()
        except Exception as e:
            pass

    # WHEN to check exit condition??
    # Checks if it should end the program
    if page_numb == int(last_page):
        break
    elif 'nesėkmingai' in crr_page:
        break

# // Print tables in python //
# cursor.execute("SELECT * FROM names")
# for x in cursor:
#    print(x)
# cursor.execute("SELECT * FROM name_dates")
# for h in cursor:
#    print(h)
cursor.close()
