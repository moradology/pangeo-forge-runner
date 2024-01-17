from apache_beam import Pipeline
from apache_beam.pipeline import PipelineOptions
from traitlets import Bool
from traitlets.config import LoggingConfigurable

import logging


class Bakery(LoggingConfigurable):
    """
    Base class for Bakeries where recipes can be run.

    A Bakery provides an opinionated and consistent wrapper to an
    Apache Beam runner. Users only configure what is important to them,
    and the Bakery takes care of the rest.
    """

    def __init__(self, **kwargs):
        # Initialize traitlets
        super().__init__(**kwargs)
        # Set up logging
        self.log = logging.getLogger(__name__)

    def bake(pipeline: Pipeline, name: str, extra: dict) -> None:
        """
        Implementation specifics for this bakery's run.
        """
        raise NotImplementedError("Override bake in subclass")

    def get_pipeline_options(
        self, job_name: str, container_image: str, extra_options: dict
    ) -> PipelineOptions:
        """
        Return a PipelineOptions object that will configure a Pipeline to run on this Bakery

        job_name: A unique string representing this particular run
        container_image: A docker image spec that should be used in this run,
                         if the Bakery supports using docker images
        extra_options: Dictionary of extra options that should be passed on to PipelineOptions

        """
        raise NotImplementedError("Override get_pipeline_options in subclass")
