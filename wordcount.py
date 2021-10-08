import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText


if __name__ == "__main__":

    input_files = "input/*"
    output_path = "output/counts.txt"

    beam_options = PipelineOptions(
        runner='DirectRunner',
        project='pacheBeam_wrdcnt_ex',
        job_name='mlk-tr-cnt',
        temp_location="~/apacheBeam_wrdcnt_ex/temp"
    )

    with beam.Pipeline(options=beam_options) as pipeline:

        lines = pipeline | ReadFromText(input_files)

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

        output = counts | 'Format' >> beam.Map(format_results)

        output | WriteToText(output_path)         
            
        

