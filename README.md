# UkAccidents_mrjob_EMR
Use Amazon EMR to create Hadoop cluster and run mrjob jobs in Python and Pig Latin scripts to explore “UK Car Accidents 2005-2015" dataset. 

## Environment
Windows(local machine), AWS.
## Technologies
Hadoop, EMR, S3, MapReduce, CANOPY, Python 2.7, mrjob, Pig Latin.

## Dataset
UK Car Accidents 2005-2015 (Data from the UK Department for Transport).
You can get this data form https://www.kaggle.com/silicon99/dft-accident-data

For the shapefiles (geospatial vector data format) of the UK postcode boundaries, you can upload them from http://www.opendoorlogistics.com/wp-content/uploads/Data/UK-postcode-boundaries-Jan-2015.zip

## Introduction
We will create a Hadoop cluster in Amazon EMR using mrjob commands within a local machine terminal (PowerShell in my case). Later, we will run scripts in Python/Pig Latin to map the coordinates of car accidents to the areas of the UK and find the regions having the high count of accidents and casualities.

## Steps
1. Create a S3 bucket and upload the files into it. For the mrjob script and the configuration file (mrjob.conf), you can keep them in the local machine.

2. Run the following command in your local machine terminal to create the Hadoop cluster in Amazon EMR. You have to run the mrjob command from the directory where your Python environment lies and the mrjob package is installed. As a response you get your cluster ID freshly created.
```
mrjob create-cluster --max-hours-idle 1 --conf-path C:\Users\majdi\Desktop\DataSet\mrjob.conf 
```
![](Images/image1.png)

