import argparse
import os
import json
import logging
import pandas as pd
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from smart_open import open

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r'/Users/sohumpatel/Desktop/JUMP/capstone/primordial-port-367916-940c43b42601.json'

# Large Dataset
#json_file = r'/Users/sohumpatel/Desktop/JUMP/capstone/archive/yelp_academic_dataset_review.json'
#csv_file_name = "review.csv"

# Small Dataset
json_file = r'/Users/sohumpatel/Desktop/JUMP/capstone/archive/yelp_academic_dataset_tip.json'
csv_file_name = "tip.csv"

# Large Dataset
#json_file = r'/Users/sohumpatel/Desktop/JUMP/capstone/archive/yelp_academic_dataset_user.json'
#csv_file_name = "user.csv"


class ReadFile(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path

    def start_bundle(self):
        self.client = storage.Client()

    def process(self, something):
        header_data = []
        csv_data = []
        with open(json_file) as fin:
            setHeader = False
            for line in fin:
                json_data = json.loads(line)
                if setHeader == False:
                    header_data = list(json_data.keys())
                    setHeader = True
                csv_data.append( [ json_data.get(key) for key in json_data.keys() ] )
        #print(header_data + csv_data)
        yield [ header_data ] + csv_data


class WriteCSVFIle(beam.DoFn):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def start_bundle(self):
        self.client = storage.Client()

    def process(self, mylist):
        df = pd.DataFrame(mylist)

        bucket = self.client.get_bucket(self.bucket_name)
        bucket \
          .blob(csv_file_name) \
          .upload_from_string(df.to_csv(index=False), 'text/csv')


class DataflowOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_path', type=str, help='Enter file input path')
        parser.add_argument('--output_bucket', type=str, help='Enter file output bucket path', default='storage_capstone_sohum')


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    dataflow_options = pipeline_options.view_as(DataflowOptions)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (pipeline
         | 'Start Pipeline' >> beam.Create([None])
         | 'Read the JSON file' >> beam.ParDo(ReadFile(dataflow_options.input_path))
         | 'Write the CSV file' >> beam.ParDo(WriteCSVFIle(dataflow_options.output_bucket))
         )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()