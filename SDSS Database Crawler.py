import sqlite3, requests
import pandas as pd

# read the contents of the kaggle csv into an SQL database
conn = sqlite3.connect('SDSS.db')
df = pd.read_csv('Skyserver_SQL2_27_2018 6_51_39 PM.csv')
df.to_sql('DR14', conn, if_exists='replace', index=False)
conn.commit()

# pull just the right ascension and declination coordinates from the database
c = conn.cursor()
c.execute('SELECT ra, dec FROM DR14')
df = pd.DataFrame(c.fetchall(), columns=['ra', 'dec'])

# use the coordinates to pull images from the server and add column to data frame for filepath
urltemplate = r'http://skyserver.sdss.org/dr15/SkyServerWS/ImgCutout/getjpeg?ra={}&dec={}&width=128&height=128'
imgtemplate = 'Images/RA{}DEC{}.jpeg'
images = []

for row in df.itertuples():
    # uncomment this section to download the pictures - takes a while
    #img = requests.get(urltemplate.format(row.ra, row.dec))
    #f = open(imgtemplate.format(row.ra, row.dec), 'wb')
    #f.write(img.content)
    #f.close()
    images.append(imgtemplate.format(row.ra, row.dec))
    
df['img_filepath'] = images

# add the image filepaths to a new database with some columns from the old
# data included is SDSS object id, RA/DEC coordinates, brightness in 5 wavelengths, class, redshift, and image filepath
df.to_sql('temp', conn, schema='ra, dec, img_filepath', if_exists='replace', index=False)
c = conn.cursor()

c.execute("""SELECT d.objid, d.ra, d.dec, d.u, d.g, d.r, d.i, d.z, d.specobjid, d.class, d.redshift, temp.img_filepath 
          FROM DR14 d INNER JOIN temp ON d.ra = temp.ra AND d.dec = temp.dec""")
newdf = pd.DataFrame(c.fetchall())
newdf.to_sql('DR14images', conn, if_exists='replace', index=False)
conn.commit()
conn.close()