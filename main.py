from datasets_generator import DatasetsGenerator
from datetime import date, datetime  
from datetime import timedelta

for i in range(0,4):
    ds = DatasetsGenerator("2018-04-01", 3)
    ds.run()