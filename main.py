from datasets_generator import DatasetsGenerator
from ingestion import load_datasets
from query import run_queries

if __name__ == "__main__":
    nb_days = 40
    for y in range(2018, 2021):
        datasets_generator = DatasetsGenerator("{}-04-01".format(y), nb_days)
        datasets_generator.run()
        load_datasets()
        run_queries()
        nb_days *= 2




