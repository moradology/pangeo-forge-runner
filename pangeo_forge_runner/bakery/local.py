"""
Bakery for baking pangeo-forge recipes in Direct Runner
"""
from apache_beam import Pipeline
from apache_beam.pipeline import PipelineOptions
from traitlets import Integer

from .base import Bakery


class LocalDirectBakery(Bakery):
    """
    Bake recipes on your local machine, without docker.

    Uses the Apache Beam DirectRunner
    """

    num_workers = Integer(
        0,
        config=True,
        help="""
        Number of workers to use when baking the recipe.

        Defaults to 0, which is interpreted by Apache beam to be the
        number of CPUs on the machine
        """,
    )

    def bake(self, pipeline: Pipeline, name: str, extra: dict) -> None:
        """
        Implementation specifics for this bakery's run
        """
        self.log.info(
            f"Running job for recipe {name}\n",
            extra=extra | {"status": "running"},
        )
        pipeline.run()

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
            runner="DirectRunner",
            direct_running_mode="multi_processing",
            direct_num_workers=self.num_workers,
            save_main_session=True,
            # this might solve serialization issues; cf. https://beam.apache.org/blog/beam-2.36.0/
            pickle_library="cloudpickle",
            **extra_options,
        )
