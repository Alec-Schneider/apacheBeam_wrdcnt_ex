import re
import click
import logging
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText

logger = logging.getLogger(__package__)


@click.option("--input_path", default=None, 
    help="Directory containing one or more .txt files"
)
@click.option("--output_path", default=None,
    help="Directory where the word counts should be written to"
)
@click.command()
def main(input_path, output_path):

    beam_options = PipelineOptions(
        runner='DirectRunner',
        project='apacheBeam_wrdcnt_ex',
        job_name='word-counter',
        temp_location="~/tmp"
    )

    with beam.Pipeline(options=beam_options) as pipeline:

        logger.info("Reading inputs")
        lines = pipeline | ReadFromText(input_path)
        logger.info("Count each word in line")
        counts = (
            lines
            | 'Split' >> (
                beam.FlatMap(
                    lambda x: re.findall(r'[A-Za-z\']+', x)).with_output_types(str))
                | 'PairWithOne' >> beam.Map(lambda x: (x,1))
                | 'GroupAndSum' >> beam.CombinePerKey(sum))

        # Format the coutns inta PCollection of strings
        def format_results(word_count):
            (word, count) = word_count
            return f"{word}: {count}"

        logger.info("Format count results")
        output = counts | 'Format' >> beam.Map(format_results)
        logger.info("Write formatted counts to output")
        output | WriteToText(output_path)

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    main()