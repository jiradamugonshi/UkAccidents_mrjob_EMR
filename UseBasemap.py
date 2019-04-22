import matplotlib.pyplot as plt
import csv
import matplotlib.cm
import pandas as pd
import numpy as np

from mpl_toolkits.basemap import Basemap
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection
from matplotlib.colors import Normalize

#Create a list of dictionaries from the entries in the csv file 
def ouvrir_fichier(file_name):
    list = []
    with open(file_name) as f:   
        next(f)
        lecteur = csv.reader(f, delimiter=",", quoting=csv.QUOTE_NONE)
        
        for entree in lecteur:
            try:
                c = dict(area = entree[0], longitude=float(entree[1]), latitude=float(entree[2]), accidents_count=int(entree[3]))
            except:
                pass
            list.append(c)
        return list

#Draw the UK map
fig, ax = plt.subplots(figsize=(10,20))

m = Basemap(resolution='i', # c, l, i, h, f or None
            projection='merc',
            lat_0=54.7, lon_0=-4.36,
            llcrnrlon=-7.5600,llcrnrlat=49.7600,
            urcrnrlon=2.7800,urcrnrlat=60.840)

m.drawmapboundary(fill_color='#46bcec')
m.fillcontinents(color='#f2f2f2',lake_color='#46bcec')
m.drawcoastlines()
#Using shapefiles to draw regions
m.readshapefile('C:\Users\majdi\Desktop\UK-postcode\Distribution\Areas', 'areas')


#Create a DataFrame using the csv file
list_coordinates = ouvrir_fichier(r"C:\Users\majdi\Downloads\accidents_to_areas.csv")
df_coordinates = pd.DataFrame(list_coordinates)
#Add a new column to store the total accidents count by area
df_coordinates['total_accidents_by_area'] = df_coordinates['accidents_count'].groupby(df_coordinates['area']).transform('sum')

#Create a new DataFrame that will hold all the information we need about the regions boundaries and their names
df_poly = pd.DataFrame({
        'shapes': [Polygon(np.array(shape), True) for shape in m.areas],
        'area': [area['name'] for area in m.areas_info]
    })

#Merge the 2 DataFrames on the area column
df_poly = df_poly.merge(df_coordinates, on='area', how='left')

#Create a colormap
cmap = plt.get_cmap('Oranges')   
pc = PatchCollection(df_poly.shapes, zorder=2)
norm = Normalize()
 
pc.set_facecolor(cmap(norm(df_poly['total_accidents_by_area'].fillna(0).values)))
ax.add_collection(pc)

#Create the colorbar
mapper = matplotlib.cm.ScalarMappable(norm=norm, cmap=cmap) 
mapper.set_array(df_poly['total_accidents_by_area'])
plt.colorbar(mapper, shrink=0.4)

plt.show()