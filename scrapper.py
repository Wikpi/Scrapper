from bs4 import BeautifulSoup
import requests, lxml
import mysql.connector

# this is main change

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
try:
    cursor.execute("DROP TABLE names")
except Exception:
    pass

try:
    cursor.execute("DROP TABLE name_dates")
except Exception:
    pass

try:
    cursor.execute("DROP TABLE duplicates")
except Exception:
    pass

# // Create new tables //
cursor.execute("CREATE TABLE names"
               "(id int PRIMARY KEY AUTO_INCREMENT, "
               "name VARCHAR(50) CHARACTER SET utf8 COLLATE utf8_lithuanian_ci, "
               "gender VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_lithuanian_ci, "
               "short_explanation varchar(250) CHARACTER SET utf8 COLLATE utf8_lithuanian_ci, "
               "explanation VARCHAR(500) CHARACTER SET utf8 COLLATE utf8_lithuanian_ci, "
               "origin VARCHAR(150) CHARACTER SET utf8 COLLATE utf8_lithuanian_ci)")
cursor.execute("CREATE TABLE name_dates "
               "(id int PRIMARY KEY AUTO_INCREMENT, "
               "name VARCHAR(50) CHARACTER SET utf8 COLLATE utf8_lithuanian_ci, "
               "day_month int UNSIGNED,"
               "day_day int UNSIGNED)")

base_url = 'https://www.vardai.org/tema/lietuviski-vardai/page/'
page_numb = 0
last_page = 0
name_count = 0
err_count = 0

# Main function start
while True:
    page_numb += 1
    crr_page = requests.get(base_url + str(page_numb)).text

    # Track page
    print(base_url + str(page_numb))

    soup = BeautifulSoup(crr_page, 'lxml')

    # Finds the last page
    if last_page == 0:
        try:
            last_page = soup.find('a', {'class': 'last'}).text
            last_page = last_page.split(' ')[0]
            print(last_page)
        except Exception:
            err_count += 1
            pass

    # print('Error count: ' + str(err_count))
    for find_item in soup.findAll("div", {"class": "posttop"}):
        not_found = 0

        # Find name
        try:
            name = find_item.h2.text
            name = name.split(' ')[0]
            name = str(name)
            name_count += 1
        except Exception:
            err_count += 1
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
            err_count += 1
            pass

        # Find the month and day
        month = '0'
        day = '0'
        try:
            find_date = soup_info.find('span', id='vardadienis')

            find_date = find_date.b.text
            if find_date == '':
                not_found += 1
            else:
                pass

            if ' - ' in find_date:
                find_date = find_date.replace('-', ' ')

            find_date = find_date.split(' ')

            for i in find_date:
                if '-' in i:
                    find_date = find_date.replace('-', '.')
                if ' ' in i:
                    find_date = find_date.replace(' ', '.')
                if '.' not in i:
                    continue

                return_int = any(map(int, i))

                if return_int:
                    month = i.split('.')[0]
                    day = i.split('.')[1]
                elif not return_int:
                    month = '0'
                    day = '0'

                try:
                    cursor.execute("INSERT INTO name_dates (name, day_month, day_day) VALUES (%s, %s, %s)", (name, month, day))
                except Exception as bb:
                    print(bb)
                    print("Error putting into table")
                    pass

        except Exception as bug:
            err_count += 1
            print(bug)
            pass

        # Find gender
        gender = ''
        try:
            find_gender = soup_info.find('span', id='lytis')
            find_b = find_gender.b.text

            if find_b == '':
                not_found += 1
            else:
                pass

            find_gender = find_gender.text

            gender = find_gender.split(':')[1]
        except Exception as tt:
            err_count += 1
            print(tt)
            print("ERROR WITH GENDER")
            pass

        # Find short explanation
        short_explanation = ''
        try:
            find_short = soup_info.find('span', id='reiksme').text

            if find_short.split(':')[1] == ' .':
                not_found += 1

            short_explanation = find_short
        except Exception as ff:
            err_count += 1
            print(ff)
            pass

        # Find the explanation and the origin
        explanation = 'Vardo reikšmė: .'
        origin = ''
        try:
            find_info = soup_info.find('span', id='plati').text
            find_info = find_info.split('\n')
            for v in find_info:
                if len(v) > 30 and len(v) < 300:
                    if 'Panašūs' in v:
                        continue
                    else:
                        pass

                    if "Vardo reikšmė:" not in v:
                        if v[0] != ' ':
                            v = ' ' + v
                        explanation = "Vardo reikšmė:" + v
                    else:
                        explanation = v
        except Exception:
            err_count += 1
            pass

        # Find the origin
        try:
            find_origin = soup_info.find('span', id='kilme')
            find_b = find_origin.b.text

            if find_b == '':
                not_found += 1
            else:
                pass
            find_origin = find_origin.text

            origin = find_origin.split(':')[1]
        except Exception as gg:
            err_count += 1
            print(gg)
            print("ERROR WITH ORIGIN")
            pass

        if not_found >= 4:
            continue

        # Put name into table
        try:
            cursor.execute("INSERT INTO names (name, gender, short_explanation, explanation, origin)"
                           "VALUES (%s, %s, %s, %s, %s)", (name, gender, short_explanation, explanation, origin))
            db.commit()
        except Exception as e:
            print(e)
            err_count += 1

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

print('Names found: ' + str(name_count),
      '\nErrors encountered: ' + str(err_count))
