"""
Bakery for baking pangeo-forge recipes in Direct Runner
"""
from apache_beam import Pipeline
from apache_beam.pipeline import PipelineOptions
from traitlets import Unicode

from .base import Bakery

import json
import os


class SerializeBakery(Bakery):
    """
    Bake recipes to a serialized format so that they might be run manually.

    NOTE: WILL NOT RUN PIPELINE
    """

    directory = Unicode(
        "/tmp/pangeo-forge-pipelines",
        config=True,
        help="""
        Path to the directory to use.

        This should be the path where the pipelines and options will be serialized.
        """,
    )

    def bake(self, pipeline: Pipeline, name: str, extra: dict) -> None:
        serialized_pipeline = pipeline.to_runner_api()
        options_dict = pipeline.options.get_all_options(drop_default=True)
        pipeline_file = os.path.join(self.directory, f"{name}-pipeline.pb")
        options_file = os.path.join(self.directory, f"{name}-options.json")
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        # Write serialized pipeline+options to a file
        with open(pipeline_file, 'wb') as pipeline_file:
            pipeline_file.write(serialized_pipeline.SerializeToString())
        with open(options_file, 'w') as options_file:
            options_file.write(json.dumps(options_dict))



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
            # this might solve serialization issues; cf. https://beam.apache.org/blog/beam-2.36.0/
            pickle_library="cloudpickle",
            **extra_options,
        )
