from .core.flow.flow import Flow
from .core.flow.step import step
from .core.utils.execeptions import StopPipelineFlag
from .core.worker.data_pipeline import concatenate, DataPoint, DataFrame

from .core.worker.pipeline_model import PipelineInfo

from .core.app import App

from .core.session.project_session import get_project


__version__ = "0.1.6"

__title__ = "stairs"
__description__ = "Data Pipelines"
__url__ = "https://stairspy.com/"
__uri__ = __url__
__doc__ = __description__ + " <" + __uri__ + ">"

__author__ = "Oleg Shydlouski"
__email__ = "oleg.ivye@gmail.com"

__license__ = "Apache-2.0"

