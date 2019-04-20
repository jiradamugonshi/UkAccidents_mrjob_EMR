#!/usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon 
import shapefile

class MapAccidentsToArea(MRJob):
  
    def configure_options(self):
        super(MapAccidentsToArea, self).configure_options()
        self.add_file_option('--shp', help='Path to shp file')
        self.add_file_option('--shx', help='Path to shx file')
        self.add_file_option('--dbf', help='Path to dbf file')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_coordinates,                   
                   reducer=self.reduce_count_coordinates),
            MRStep(mapper_init = self.mapper_init,
                   mapper = self.mapper_get_areas,                   
                   reducer = self.reducer_list_areas)
        ]


    def mapper_init(self):
        data_in_shapefile = shapefile.Reader('Areas')
        self.shapes = data_in_shapefile.shapes()
        self.infos = data_in_shapefile.records()
        
    
    def mapper_get_coordinates(self, _,line):
        line = line.strip()
        try:
            (Accident_Index,Location_Easting_OSGR,Location_Northing_OSGR,Longitude,Latitude,Police_Force,Accident_Severity,Number_of_Vehicles,Number_of_Casualties,Date,Day_of_Week,Time,Local_Authority_District,Local_Authority_Highway,First_Road_Class,First_Road_Number,Road_Type,Speed_limit,Junction_Detail,Junction_Control,Second_Road_Class,Second_Road_Number,Pedestrian_Crossing_Human_Control,Pedestrian_Crossing_Physical_Facilities,Light_Conditions,Weather_Conditions,Road_Surface_Conditions,Special_Conditions_at_Site,Carriageway_Hazards,Urban_or_Rural_Area,Did_Police_Officer_Attend_Scene_of_Accident,LSOA_of_Accident_Location) = line.split(",")
        except:
            pass

        try:
            latitude = round(float(Latitude),4)
            longitude = round(float(Longitude),4)
            yield (latitude,longitude), 1
        except ValueError:
            pass        

    def reduce_count_coordinates(self, key, values):
        yield key, sum(values)

    def mapper_get_areas(self, key, value):
        latitude, longitude = key[0], key[1]
        point = (float(longitude),float(latitude))
        for i in range(len(self.shapes)):
            boundary = self.shapes[i].points
            if Point(point).within(Polygon(boundary)):
                area_name = self.infos[i]
                yield area_name, (point, value)        

    def reducer_list_areas(self, area, records):
        for record in records:
            yield area, record

if __name__ == '__main__':
    MapAccidentsToArea.run()
