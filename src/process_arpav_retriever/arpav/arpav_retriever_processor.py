# =================================================================
#
# Authors: Valerio Luzzi <valluzzi@gmail.com>
#
# Copyright (c) 2023 Valerio Luzzi
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import os
import json
import uuid
import datetime
import requests

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError


from ..cli.module_log import Logger, set_log_debug
from ..utils import filesystem, module_s3
from ..utils.status_exception import StatusException

from . import _ARPAV_RETRIEVERS
from .arpav_precipitation_retriever import _ARPAVPrecipitationRetriever
from .arpav_water_level_retriever import _ARPAVWaterLevelRetriever


PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'arpav_retriever_process',
    'title': {
        'en': 'ARPAV Retriever Process',
    },
    'description': {
        'en': 'Process to retrieve data from the ARPAV seonsors.',
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['ARPAV', 'retriever', 'process', 'sensor', 'pygeoapi'],

    'inputs': {
        'token': {
            'title': 'secret token',
            'description': 'identify yourself',
            'schema': {
                'type': 'string'
            }
        },

        'lat_range': {
            'title': 'Latitude range',
            'description': 'The latitude range in format [lat_min, lat_max]. Values must be in EPSG:4326 crs. If no latitude range is provided, all latitudes will be returned',
            'schema': {
            }
        },
        'long_range': {
            'title': 'Longitude range',
            'description': 'The longitude range in format [long_min, long_max]. Values must be in EPSG:4326 crs. If no longitude range is provided, all longitudes will be returned',
            'schema': {
            }
        },
        'time_range': {
            'title': 'Time range',
            'description': 'The time range in format [time_start, time_end]. Both time_start and time_end must be in ISO-Format and related to at least one week ago. If no time range is provided, all times will be returned',
            'schema': {
            }
        },

        'variable': {
            'title': 'Variable',
            'description': 'The variable to retrieve. Possible values are "precipitation" or "water_level".',
            'schema': {
                'type': 'string',
                'enum': ['precipitation', 'water_level']
            }
        },

        'out': {
            'title': 'Output file path',
            'description': 'The output file path for the retrieved data. If neither out nor bucket_destination are provided, the output will be returned as a feature collection.',
            'schema': {
                'type': 'string'
            }
        },
        'out_format': {
            'title': 'Return format type',
            'description': 'The return format type. Possible values are "geojson" or "dataframe". "geojson" is default and preferable.',
            'schema': {
            }
        }, 
        'bucket_destination': {
            'title': 'Bucket destination',
            'description': 'The bucket destination where the data will be stored. If not provided, the data will not be stored in a bucket. If neither out nor bucket_destination are provided, the output will be returned as a feature collection.',
            'schema': {
                'type': 'string'
            }
        },

        'debug': {
            'title': 'Debug',
            'description': 'Enable Debug mode. Can be valued as true or false',
            'schema': {
            }
        }
    },

    'outputs': {
        'id': {
            'title': 'ID',
            'description': 'The ID of the process execution',
            'schema': {
            }
        },
    },

    'example': {
        "inputs": {
            'token': 'your_secret_token',
            'lat_range': [ 43.92, 44.77 ],
            'long_range': [ 12.20, 12.83 ],
            'time_range': ['2025-07-23T10:00:00', '2025-07-23T12:00:00'],
            'variable': 'precipitation',
            'out': 'path/to/output/file.geojson',
            'out_format': 'geojson',
            'bucket_destination': 's3://your-bucket-name/store/data/prefix',
            'debug': True
        }
    }
}


class ARPAVRetrieverProcessor(BaseProcessor):
    """
    ARPAV Retriever Process Processor
    """

    def __init__(self, processor_def):
        """
        Initialize the ARPAV Retriever Processor.
        """

        super().__init__(processor_def, PROCESS_METADATA)

        self.name = 'ARPAVRetrieverProcessor'
        self.variable = None
        self._tmp_data_folder = None

        # REF: https://www.arpa.veneto.it/dati-ambientali/dati-in-diretta/meteo-idro-nivo/variabili_idro


    def argument_validation(self, data):
        """
        Validate the arguments passed to the processor.
        """

        token = data.get('token', None)
        debug = data.get('debug', False)

        if token is None or token != os.getenv("INT_API_TOKEN", "token"):
            raise StatusException(StatusException.DENIED, 'ACCESS DENIED: wrong token')
            
        if type(debug) is not bool:
            raise StatusException(StatusException.INVALID, 'debug must be a boolean')
        if debug:
            set_log_debug()

        variable = data.get('variable', None)
        if variable is None:
            raise StatusException(StatusException.INVALID, 'variable must be provided')
        if variable not in _ARPAV_RETRIEVERS:
            raise StatusException(StatusException.INVALID, 'variable must be either "precipitation" or "water_level"')
        self.variable = variable

        self._tmp_data_folder = _ARPAV_RETRIEVERS[self.variable]
        if not os.path.exists(self._tmp_data_folder):
            os.makedirs(self._tmp_data_folder)


    def execute(self, data):

        mimetype = 'application/json'

        outputs = {}

        ARPAVRetriever = None

        try:
            
            # DOC: Args validation
            self.argument_validation(data)
            Logger.debug(f'Validated process parameters')

            ARPAVRetriever = _ARPAV_RETRIEVERS[self.variable]()

            # DOC: Set up the ARPAV Retriever
            outputs = ARPAVRetriever.run(**data)

            if type(outputs) is gpd.GeoDataFrame:
                outputs = ARPAVRetriever.data_to_feature_collection(outputs)


        except StatusException as err:
            outputs = {
                'status': err.status,
                'message': str(err)
            }
        except Exception as err:
            outputs = {
                'status': StatusException.ERROR,
                'error': str(err)
            }
            raise ProcessorExecuteError(str(err))
        
        filesystem.garbage_folders(self._tmp_data_folder)
        Logger.debug(f'Cleaned up temporary data folder: {self._tmp_data_folder}')
        
        return mimetype, outputs


    def __repr__(self):
        return f'<ARPAVRetrieverProcessor> {self.name}[{self.variable}]'