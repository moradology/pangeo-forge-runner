"""
Bakery for baking pangeo-forge recipes in Direct Runner
"""
from apache_beam import Pipeline
from apache_beam.pipeline import PipelineOptions
from traitlets import Unicode

from .base import Bakery

import json


class ProcessHarnessBakery(Bakery):
    """
    Bake recipes to be run by a custom-built beam `boot` harness.
    Suitable for running spark-backed beam jobs on EMR.
    https://beam.apache.org/documentation/runtime/sdk-harness-config/
    """

    harness_binary = Unicode(
        "/home/hadoop/boot",
        config=True,
        help="""
        Path to the sdk harness 'boot' binary to use.

        https://beam.apache.org/documentation/runtime/sdk-harness-config/
        """,
    )

    runner = Unicode(
        "SparkRunner",
        config=True,
        help="""
        Beam runner to use. Spark by default but likely compatible with other backends.
        https://beam.apache.org/documentation/runners/spark/
        """,
    )

    def bake(self, pipeline: Pipeline, name: str, extra: dict) -> None:
        """
        Implementation specifics for this bakery's run
        """
        # self.log.info(
        #     f"Running job for recipe {name}\n",
        #     extra=extra | {"status": "running"},
        # )
        # pipeline.run()
        import apache_beam as beam
        from apache_beam.options.pipeline_options import PipelineOptions
        options = PipelineOptions([
            "--runner=SparkRunner",
            "--environment_type=PROCESS",
            "--environment_config={\"command\": \"/home/hadoop/boot\"}"
        ])
        with beam.Pipeline(options=options) as p:
            (p
            | 'ReadFromText' >> beam.io.ReadFromText('s3://nzimmerman-testing/texts/mobydick.txt')
            | 'SplitWords' >> beam.FlatMap(lambda x: x.split())
            | 'CountWords' >> beam.combiners.Count.PerElement()
            | 'FormatResults' >> beam.Map(lambda word_count: f"{word_count[0]}: {word_count[1]}")
            | 'WriteToText' >> beam.io.WriteToText('s3://nzimmerman-testing/beam/output2')
            )


    def get_pipeline_options(
        self, job_name: str, container_image: str, extra_options: dict
    ) -> PipelineOptions:
        """
        Return PipelineOptions for use with this Bakery
        """
        # Set flags explicitly to empty so Apache Beam doesn't try to parse the commandline
        # for pipeline options - we have traitlets doing that for us.
        return PipelineOptions(
            flags=[],
            save_main_session=True,
            runner=self.runner,
            environment_type="PROCESS",
            environment_config=json.dumps({"command": self.harness_binary}),
            # this might solve serialization issues; cf. https://beam.apache.org/blog/beam-2.36.0/
            pickle_library="cloudpickle",
            **extra_options,
        )