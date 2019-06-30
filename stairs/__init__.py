from .core.project import StairsProject
from .core.session.project_session import get_project

from stairs.core.app import App

from .core.utils.execeptions import StopPipelineFlag

from .core.pipeline.data_pipeline import concatenate, DataPoint, DataFrame
from .core.pipeline import PipelineInfo, Pipeline
from .core.flow import Flow
from .core.flow.step import step
from .core.producer import Producer
from .core.producer.batch import BatchProducer
from .core.producer.spark import SparkProducer
from .core.consumer import Consumer
from .core.consumer.iter import ConsumerIter
from .core.consumer.standalone import StandAloneConsumer


__version__ = "0.1.6"

__title__ = "stairs"
__description__ = "Data Pipelines framework"
__url__ = "https://stairspy.com/"
__uri__ = __url__
__doc__ = __description__ + " <" + __uri__ + ">"

__author__ = "Oleg Shydlouski"
__email__ = "oleg.ivye@gmail.com"

__license__ = "Apache-2.0"

