import os
import requests
from bs4 import BeautifulSoup
import time
import xlrd
# Python 3.x
from urllib.request import urlopen, urlretrieve
from urllib.parse import urlparse, parse_qs
import matplotlib.pyplot as plt

import mysql.connector
db_connection = mysql.connector.connect(
  host="localhost",
  user="root",
  database="PythonElstat"
)
db_cursor = db_connection.cursor(prepared=True)
db_cursor.execute("DROP TABLE IF EXISTS ElstatMetrics")
db_cursor.execute("CREATE TABLE ElstatMetrics (year INT, totals INT, car INT, sea INT, train INT, airplane INT, bestCountry VARCHAR(255), Q1 INT, Q2 INT, Q3 INT, Q4 INT)")


#Calculating Question 1, will be used in calculation of Question 3 and 4 too
def findTotal(sheet, column):
    rows = sheet.nrows - 1
    bcell_val = sheet.cell(rows, 1).value
    while bcell_val != "ΓΕΝΙΚΟ ΣΥΝΟΛΟ":
        rows = rows - 1
        bcell_val = sheet.cell(rows, 1).value
    return round(sheet.cell(rows, column).value)

def findMost(sheet):
    best = 0
    bestrow = 0
    for i in range (sheet.nrows - 1):
        try:
            now = round(float(sheet.cell(i, 6).value))
            if now > best:
                if sheet.cell(i, 1).value != "ΓΕΝΙΚΟ ΣΥΝΟΛΟ" and sheet.cell(i, 1).value and sheet.cell(i, 1).value != "από τΙς οποίες:":
                    best = now
                    bestrow = i
        except ValueError: pass
            
    return sheet.cell(bestrow, 1).value


    

years = []
totals = []
car = []
sea = []
train = []
airplane = []
bestCountry = []
Q1 = []
Q2 = []
Q3 = []
Q4 = []
URL = 'https://www.statistics.gr/el/statistics/-/publication/STO04/2011-Q4'
#next 5 lines are for easier gitignore (so that you can gitignore xlsFiles folder and not worry about git)
try:
    os.mkdir('Files')
except OSError: pass
os.chdir('Files')
if os.path.exists("csvFile.csv"):
    os.remove("csvFile.csv")

csvFile = open("csvFile.csv", 'w+')
csvFile.write("year,totals,car,sea,train,airplane,bestCountry,Q1,Q2,Q3,Q4\n")


#fetch only the files needed to pass to db
for x in range(2011, 2016):
    #we really only need to get Q4 xls files, as they have info on every month. Link is in form https://www.statistics.gr/el/statistics/-/publication/STO04/YYYY-Q4
    newURL = URL.replace('2011', str(x))
    u = urlopen(newURL)
    try:
        html = u.read().decode('utf-8')
    finally:
        u.close()
    soup = BeautifulSoup(html, "html.parser")
    #get every link in the webpage
    for link in soup.select('a[href^="https://"]'):
        href = link.get('href')
        #narrow down links to only those weird download links elstat gives
        if not any(href.endswith(x) for x in ['el']):
            continue
        parsed = urlparse(href)
        #more weird link handling
        ppid = parse_qs(parsed.query).get('p_p_id')[0]
        instance = ppid.split('_')[-1]
        filename = parse_qs(parsed.query).get('_documents_WAR_publicationsportlet_INSTANCE_'+instance+'_documentID')[0]
        response = requests.get(href, {'User-Agent': 'Mozilla/5.0'})
        #if link does not return us a pdf, so its definately an xls
        if not any(str(response.content).startswith(y) for y in ['b\'%PDF']):
            boo = xlrd.open_workbook(file_contents=response.content, encoding_override="cp1252", on_demand=True)
            title = boo.get_sheet(0).cell(0,0).value
            #we just found the only xls we really want
            if ("ΑΦΙΞΕΙΣ ΜΗ-ΚΑΤΟΙΚΩΝ ΑΠΟ ΤΟ ΕΞΩΤΕΡΙΚΟ ΑΝΑ ΧΩΡΑ ΠΡΟΕΛΕΥΣΗΣ  ΚΑΙ MΕΣΟ ΜΕΤΑΦΟΡΑΣ 20" in title):
                xlsfile = str(x)+'.xls'
                #download it, just for the sake of completeness. Not really needed to, though, as xlrd can read from response.content
                print("Downloading %s to %s..." % (href, xlsfile) )
                if not os.path.exists(xlsfile):
                    open(xlsfile,'wb').write(response.content)
                print("Done.")
                #from here on we can start extracting info from the file to our mysql db
                #What we do know:
                #When on sheet 12 (11 if you count from 0)
                #1)Last row with a G collumn has total amount of tourists
                #2)The highest amount on G cell that has a B cell (but not the last)
                #  can give us the country with most tourists per year
                #3)Columns C, D, E and F that have their own B column can give us tourists
                #  per means of transport
                # By looking case #1 for sheets 3, 6, 9, 12 (2, 5, 8, 11 respectively)
                # we can find arrivals per quarter
                book = xlrd.open_workbook(xlsfile, on_demand=True,encoding_override="cp1252")
                #for num in (0,len(book.sheet_names())-1):
                sheet = book.get_sheet(11)
                totalQ1 = findTotal(book.get_sheet(2), 6)
                totalQ2 = findTotal(book.get_sheet(5), 6) - totalQ1
                totalQ3 = findTotal(book.get_sheet(8), 6) - totalQ2
                totalQ4 = findTotal(book.get_sheet(11), 6) - totalQ3
                years.append(x)
                totals.append(findTotal(sheet, 6))
                car.append(findTotal(sheet, 5))
                sea.append(findTotal(sheet, 4))
                train.append(findTotal(sheet, 3))
                airplane.append(findTotal(sheet, 2))
                bestCountry.append(findMost(sheet))
                Q1.append(findTotal(book.get_sheet(2), 6))
                Q2.append(totalQ2)
                Q3.append(totalQ3)
                Q4.append(totalQ4)
                insert_query  = "INSERT INTO ElstatMetrics (year, totals, car, sea, train, airplane, bestCountry, Q1 , Q2 , Q3 , Q4 ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)"
                preinsert_set = x, findTotal(sheet, 6),findTotal(sheet, 5),findTotal(sheet, 4),findTotal(sheet, 3),findTotal(sheet, 2),findMost(sheet),totalQ1,totalQ2,totalQ3,totalQ4
                csvset = str(preinsert_set)
                csvset = csvset[1:-1]
                csvset = csvset.replace(", ", ',')
                csvset = csvset.replace("'", '"')
                csvFile.write(csvset + str("\n"))
                insert_set = (preinsert_set)
                db_cursor.execute(insert_query, insert_set)
                db_connection.commit()


fig = plt.figure()
fig.canvas.set_window_title('Plots')

ax1 = fig.add_subplot(321)
ax1.plot(years, totals)
ax1.set_title('Total tourists')

ax2 = fig.add_subplot(322)
ax2.plot(years, Q1)
ax2.set_title('Tourists at First Quarter')

ax3 = fig.add_subplot(323)
ax3.plot(years, Q2)
ax3.set_title('Tourists at Second Quarter')

ax4 = fig.add_subplot(324)
ax4.plot(years, Q3)
ax4.set_title('Tourists at Third Quarter')

ax5 = fig.add_subplot(325)
ax5.plot(years, Q4)
ax5.set_title('Tourists at Fourth Quarter')

fig1 = plt.figure()
fig1.canvas.set_window_title('Pies')
labels = 'Car', 'Sea', 'Train', 'Airplane'
sizes0 = [car[0], sea[0], train[0], airplane[0]]
sizes1 = [car[1], sea[1], train[1], airplane[1]]
sizes2 = [car[2], sea[2], train[2], airplane[2]]
sizes3 = [car[3], sea[3], train[3], airplane[3]]
sizes4 = [car[4], sea[4], train[4], airplane[4]]
ax0 = fig1.add_subplot(321)
ax0.set_title('2011')
ax0.pie(sizes0, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
ax1 = fig1.add_subplot(322)
ax1.set_title('2012')
ax1.pie(sizes1, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
ax2 = fig1.add_subplot(323)
ax2.set_title('2013')
ax2.pie(sizes2, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
ax3 = fig1.add_subplot(324)
ax3.set_title('2014')
ax3.pie(sizes3, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
ax4 = fig1.add_subplot(325)
ax4.set_title('2015')
ax4.pie(sizes4, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
ax0.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
ax1.axis('equal')
ax2.axis('equal')
ax3.axis('equal')
ax4.axis('equal')
plt.show()

db_cursor.close()
db_connection.close()
print("MySQL Connection Closed")