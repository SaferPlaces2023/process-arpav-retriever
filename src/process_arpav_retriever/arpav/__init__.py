from .arpav_precipitation_retriever import _ARPAVPrecipitationRetriever
from .arpav_water_level_retriever import _ARPAVWaterLevelRetriever

_ARPAV_RETRIEVERS = {
    'precipitation': _ARPAVPrecipitationRetriever,
    'water_level': _ARPAVWaterLevelRetriever
}

import importlib.util
if importlib.util.find_spec('pygeoapi') is not None:
    from .arpav_retriever_processor import ARPAVRetrieverProcessor