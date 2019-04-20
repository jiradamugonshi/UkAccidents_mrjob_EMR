--Find the accidents and casualities count By Region in the UK

REGISTER s3://majdi-bucket/piggybank.jar;

Accidents = Load 's3://majdi-bucket/Accidents0515.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

Casualities = Load 's3://majdi-bucket/Casualties0515.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

B = foreach Accidents GENERATE (chararray)$0 As Accident_Index, (int)$12 As Code_District;

C = Filter B By Code_District is not null;

D = COGROUP C By Accident_Index, Casualities by $0;

E = FOREACH D GENERATE  Flatten(C.Accident_Index) As Accident_Index, Flatten(C.Code_District) As Code_District, COUNT(Casualities) As Casualities_Count_By_Accident;

F = GROUP E By Code_District;

G = FOREACH F GENERATE group As Code_District, COUNT(E.Accident_Index) As Accidents_Count, SUM(E.Casualities_Count_By_Accident) As Casualities_Sum;

Districts = Load 's3://majdi-bucket/Local_Authority_District.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER')
AS (Code_District:int, District:chararray);

Joined_Table = join Districts by Code_District, G by $0;

Result = FOREACH Joined_Table GENERATE (chararray)$1 As District, (int)$3 As Accidents_Count, (int)$4 As Casualities_Sum;

Ranked_Result = Order Result By $1 Desc;

Store Ranked_Result INTO 's3://majdi-bucket/output';