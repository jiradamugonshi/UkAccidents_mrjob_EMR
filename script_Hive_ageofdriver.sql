
Drop Table  IF Exists age_band;
Drop Table  IF Exists vehicles;



Create Table age_band (code int, label string)
row format delimited
fields terminated by ','
stored as textfile;

Load Data INPATH '/user/hdfs/sample_hive/Age_Band.csv' INTO TABLE age_band;

Create Table vehicles (Accident_Index string, Vehicle_Reference int, 
						Vehicle_Type int, Towing_and_Articulation int,
						Vehicle_Manoeuvre int, Vehicle_Location_Restricted_Lane int,
						Junction_Location int,Skidding_and_Overturning int, Hit_Object_in_Carriageway int,
						Vehicle_Leaving_Carriageway int, Hit_Object_off_Carriageway int,
						first_Point_of_Impact int, Was_Vehicle_Left_Hand_Drive int,
						Journey_Purpose_of_Driver int, Sex_of_Driver int,
						Age_of_Driver int, Age_Band_of_Driver int,
						Engine_Capacity_CC int, Propulsion_Code int,
						Age_of_Vehicle int, Driver_IMD_Decile int,
						Driver_Home_Area_Type int)
row format delimited
fields terminated by ','
stored as textfile;

Load Data INPATH '/user/hdfs/sample_hive/Vehicles0515.csv' INTO TABLE vehicles;

Select a.label as age_bd, count(*) as accidents_count
From vehicles v join age_band a
on (v.Age_Band_of_Driver = a.code)
Where v.Age_Band_of_Driver Not IN (-1, 1, 2, 3)
Group By a.label
Order By accidents_count Desc;
