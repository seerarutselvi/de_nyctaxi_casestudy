
# Running the pipeline to create the table in the Postgres,

from data_pipeline import DataPipeline

if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.main()