import warnings
warnings.filterwarnings("ignore")

import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

# Objective
''' The main goal of the project is to transform the input_data into clean input_data,
to be able to create analysis using Data Science. To achieve this, it will
create a pipeline to extract, transform and clean the input_data.'''

# Analyzing the Dataset - Insight
''' To connect both datasets, the field will be the month and the year,
but the dataset with the total precipitation has measured multiple times
per day while the dataset with the epidemiological report had measured per
week. 
    In reason to that, the input_data cleaning will be more intensive in the precipitation 
dataset, so to it fit the epidemiological report dataset.'''

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)


# Methods
def text_to_list(element, delimiter="|"):
    return element.split(delimiter)


def list_to_dictionary(element, columns):
    return dict(zip(columns, element))


def process_datetime(element):
    """ Receives from input a dictionary and adds a new column, assembling the Month
    and Year from "data_iniSE", in order that we can join the tables by this field."""
    """ 2016-08-01 >> 2016-08."""
    element['year-month'] = "-".join(element['data_iniSE'].split("-")[:2])
    return element


def key_uf(element):
    """ Receives a dictionary and returns a tuple with state (UF) and the element
    >> (UF, dictionary), in order to group by uf field."""
    key = element['uf']
    return key, element


def dengue_cases(element):
    """ Receives a tuple ('UF', [{}, {}])
        Returns a tuple ('UF-YEAR-MONTH', NUMBER_OF_CASES)"""

    uf, elements_reports = element
    for report in elements_reports:
        if bool(re.search(r'\d', report['casos'])):
            # Return the float field if it is numeric
            yield f"{uf}-{report['year-month']}", float(report['casos'])
        else:
            # Those that aren't numeric, it's " ", so returns 0.0
            yield f"{uf}-{report['year-month']}", 0.0


def process_rain_data(element):
    """ Receives a list of elements
        Returns a tuple with the key ('UF-YEAR-MONTH') and rain value"""

    date, value, uf = element
    date = "-".join(date.split("-")[:2])
    key = f"{uf}-{date}"

    # Negative Raining Value doesn't exist
    if float(value) < 0:
        value = 0.0
    else:
        value = float(value)

    return key, value


def filter_null_data(element):
    """ Receives an element that has null input_data
        Receives the tuple like ('CE-2015-10', {'rain': [0.0], 'dengue': []})
        Returns a tuple without null input_data, dropping them"""

    key, data = element
    if all([data['rain'], data['dengue']]):
        # If there are input_data in both fields, returns true and maintains the element
        return True

    return False


def unzip_elements(element):
    """ Receives a tuple like ('CE-2015-10', {'rain': [0.0], 'dengue': [182.5]})
        Returns a tuple like ('CE', '2015', '10', '0.0', '182.5')"""

    key, data = element
    rain_data = data['rain'][0]
    dengue_data = data['dengue'][0]
    uf, year, month = key.split('-')

    return uf, year, month, str(rain_data), str(dengue_data)


def prepare_csv(element, delimiter=';'):
    """ Receives a tuple like ('CE', '2015', '10', '0.0', '182.5')
        Returns a string with delimiter like 'CE;2015;10;0.0;182.5' """

    return f"{delimiter}".join(element)


# Pipeline for Dengue Dataset

## Transform the input_data that is the list format into dictionary
dengue_columns = ['id', 'data_iniSE', 'casos', 'ibge_code',
                  'cidade', 'uf', 'cep', 'latitude', 'longitude']

dengue = (
        pipeline
        | "Dengue Dataset Reading" >> ReadFromText('input_data/casos_dengue.txt', skip_header_lines=1)
        | "Text to List" >> beam.Map(text_to_list)  # For each line
        | "List to Dictionary" >> beam.Map(list_to_dictionary, dengue_columns)
        | "Add field year-month" >> beam.Map(process_datetime)
        | "Create key by the state" >> beam.Map(key_uf)
        | "Group by State" >> beam.GroupByKey()
        | "Unzip dengue cases" >> beam.FlatMap(dengue_cases)  # FlatMap can handle yield
        | "Sum of cases by Key" >> beam.CombinePerKey(sum)  # Sum of each uf-year-month key
        # | "Show Results" >> beam.Map(print)
)


# Pipeline for rain dataset
rain = (
    pipeline
    | "Rain Dataset Reading" >> ReadFromText('input_data/chuvas.csv', skip_header_lines=1)
    | "Text to List (Rain Data)" >> beam.Map(text_to_list, delimiter=",")
    | "Create key UF-YEAR-MONTH" >> beam.Map(process_rain_data)
    | "Group by Key Total Rain" >> beam.CombinePerKey(sum)
    | "Rounding Rain Values" >> beam.Map(lambda x: (x[0], round(x[1], 1)))
    # | "Show Rain Results" >> beam.Map(print)
)


# Combining the PCollections
result = (
    ({'rain': rain, 'dengue': dengue})
    | "Merge Pcols" >> beam.CoGroupByKey()
    | "Filter Null Data" >> beam.Filter(filter_null_data)
    | "Unzip the results" >> beam.Map(unzip_elements)
    | "Prepare the CSV archive" >> beam.Map(prepare_csv)
    # | "Show the results of combining PCollections" >> beam.Map(print)
)


# Create CSV archive
header = "uf;year;month;rain;dengue"

result | "Create CSV archive" >> WriteToText("./output_data/result",
                                             file_name_suffix=".csv",
                                             header=header)


pipeline.run()