import os
import json
import uuid
import datetime
import urllib3
import requests

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from ..cli.module_log import Logger
from ..utils import filesystem, module_s3
from ..utils.status_exception import StatusException


urllib3.disable_warnings()



class _ARPAVWaterLevelRetriever():
    """
    Class to retrieve data from ARPAV Water Level sensors.
    """

    name = 'ARPAVWaterLevelRetriever'

    # REF: https://www.arpa.veneto.it/dati-ambientali/dati-in-diretta/meteo-idro-nivo/variabili_idro
    
    _tmp_data_folder = os.path.join(os.getcwd(), f'{name}_water_level_tmp')

    _variable_name = None

    _properties = [
        "codice_stazione",
        "codseqst",
        "nome_stazione",
        "longitudine",
        "latitudine",
        "quota",
        "nome_sensore",
        "dataora",
        "valore",
        "misura",
        "gestore",
        "provincia"
    ]

    def __init__(self):

        self._data_provider_service = "https://api.arpa.veneto.it/REST/v1/meteo_meteogrammi"
        
        if not os.path.exists(self._tmp_data_folder):
            os.makedirs(self._tmp_data_folder)


    def argument_validation(self, **kwargs):
        """
        Validate the arguments passed to the processor.
        """

        lat_range = kwargs.get('lat_range', None)
        long_range = kwargs.get('long_range', None)
        time_range = kwargs.get('time_range', None)
        time_start = time_range[0] if type(time_range) in [list, tuple] else time_range
        time_end = time_range[1] if type(time_range) in [list, tuple] else None
        out_format = kwargs.get('out_format', None)
        bucket_destination = kwargs.get('bucket_destination', None)
        out = kwargs.get('out', None)

        if lat_range is not None:
            if type(lat_range) is not list or len(lat_range) != 2:
                raise StatusException(StatusException.INVALID, 'lat_range must be a list of 2 elements')
            if type(lat_range[0]) not in [int, float] or type(lat_range[1]) not in [int, float]:
                raise StatusException(StatusException.INVALID, 'lat_range elements must be float')
            if lat_range[0] < -90 or lat_range[0] > 90 or lat_range[1] < -90 or lat_range[1] > 90:
                raise StatusException(StatusException.INVALID, 'lat_range elements must be in the range [-90, 90]')
            if lat_range[0] > lat_range[1]:
                raise StatusException(StatusException.INVALID, 'lat_range[0] must be less than lat_range[1]')
        
        if long_range is not None:
            if type(long_range) is not list or len(long_range) != 2:
                raise StatusException(StatusException.INVALID, 'long_range must be a list of 2 elements')
            if type(long_range[0]) not in [int, float] or type(long_range[1]) not in [int, float]:
                raise StatusException(StatusException.INVALID, 'long_range elements must be float')
            if long_range[0] < -180 or long_range[0] > 180 or long_range[1] < -180 or long_range[1] > 180:
                raise StatusException(StatusException.INVALID, 'long_range elements must be in the range [-180, 180]')
            if long_range[0] > long_range[1]:
                raise StatusException(StatusException.INVALID, 'long_range[0] must be less than long_range[1]')
        
        if time_start is None:
            raise StatusException(StatusException.INVALID, 'Cannot process without a time valued')
        if type(time_start) is not str:
            raise StatusException(StatusException.INVALID, 'time_start must be a string')
        if type(time_start) is str:
            try:
                time_start = datetime.datetime.fromisoformat(time_start)
            except ValueError:
                raise StatusException(StatusException.INVALID, 'time_start must be a valid datetime iso-format string')
        
        if time_end is not None:
            if type(time_end) is not str:
                raise StatusException(StatusException.INVALID, 'time_end must be a string')
            if type(time_end) is str:
                try:
                    time_end = datetime.datetime.fromisoformat(time_end)
                except ValueError:
                    raise StatusException(StatusException.INVALID, 'time_end must be a valid datetime iso-format string')
            if time_start > time_end:
                raise StatusException(StatusException.INVALID, 'time_start must be less than time_end')
            
        time_start = time_start.replace(minute=(time_start.minute // 5) * 5, second=0, microsecond=0)
        time_end = time_end.replace(minute=(time_end.minute // 5) * 5, second=0, microsecond=0) if time_end is not None else time_start + datetime.timedelta(hours=1)
        if time_end < (datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(hours=48)).replace(tzinfo=None):
            raise StatusException(StatusException.INVALID, 'Time range must be within the last 48 hours')

        if out_format is not None:  
            if type(out_format) is not str:
                raise StatusException(StatusException.INVALID, 'out_format must be a string or null')
            if out_format not in ['geojson']:
                raise StatusException(StatusException.INVALID, 'out_format must be one of ["geojson"]')
        else:
            out_format = 'geojson'
        
        if bucket_destination is not None:
            if type(bucket_destination) is not str:
                raise StatusException(StatusException.INVALID, 'bucket_destination must be a string')
            if not bucket_destination.startswith('s3://'):
                raise StatusException(StatusException.INVALID, 'bucket_destination must start with "s3://"')
            
        if out is not None:
            if type(out) is not str:
                raise StatusException(StatusException.INVALID, 'out must be a string')
            if not out.endswith('.geojson'):
                raise StatusException(StatusException.INVALID, 'out must end with ".geojson"')
            dirname, _ = os.path.split(out)
            if dirname != '' and not os.path.exists(dirname):
                os.makedirs(dirname)

        return {
            'lat_range': lat_range,
            'long_range': long_range,
            'time_start': time_start,
            'time_end': time_end,
            'out_format': out_format,
            'bucket_destination': bucket_destination,
            'out': out
        }
    

    def retrieve_data(self, lat_range, long_range, time_start, time_end):
        start_hour_delta = int((time_start - datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)).total_seconds() // 3600)
        end_hour_delta = int((time_end - datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)).total_seconds() // 3600)

        hour_dfs = []
        for hour_delta in range(start_hour_delta, end_hour_delta + 1):
            params = {
                'rete': 'MGRAMMI',  # ???: meaning to be defined
                'coordcd': '20005',    # ???: meaning to be defined
                'orario': hour_delta
            }
            response = requests.get(self._data_provider_service, params=params)
            if response.status_code != 200:
                raise StatusException(StatusException.ERROR, f'Failed to retrieve data from ARPAV service: {response.status_code} - {response.text}')
            data = response.json().get('data', [])
            df = pd.DataFrame(data, columns=self._properties)
            hour_dfs.append(df)
        
        df = pd.concat(hour_dfs, ignore_index=True)
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['longitudine'], df['latitudine'], crs='EPSG:4326'), crs='EPSG:4326')
        gdf.rename(columns={'dataora': 'date_time'}, inplace=True)
        gdf['date_time'] = gdf['date_time'].apply(lambda x: datetime.datetime.fromisoformat(x) if isinstance(x, str) else x)
        
        def extract_level(level):
            try:
                return json.loads(level).get('LIVELLO', np.nan)
            except:
                return np.nan        
        gdf['valore'] = gdf['valore'].apply(lambda x: extract_level(x))

        gdf = gdf[gdf['date_time'] >= time_start]
        if time_end is not None:
            gdf = gdf[gdf['date_time'] <= time_end]
        if lat_range is not None:
            gdf = gdf[(gdf.geometry.y >= lat_range[0]) & (gdf.geometry.y <= lat_range[1])]
        if long_range is not None:
            gdf = gdf[(gdf.geometry.x >= long_range[0]) & (gdf.geometry.x <= long_range[1])]

        return gdf


    def data_to_feature_collection(self, sensors_gdf):
        """
        Convert the sensors GeoDataFrame to a GeoJSON FeatureCollection.
        """

        def build_metadata():
            info_metadata = [
                # DOC: each ealement is a { '@name': 'name', '@alias': 'alias' } ... not used beacuse the names are self-explanatory
            ]        
            variable_metadata = [
                {
                    '@name': 'water_level',
                    '@alias': 'water_level',
                    '@unit': 'mm',
                    '@type': 'level'
                }
            ]
            field_metadata = info_metadata + variable_metadata
            return field_metadata
            
        def build_crs():
            return {
                "type": "name",
                "properties": {
                    "name": "urn:ogc:def:crs:OGC:1.3:CRS84"  # REF: https://gist.github.com/sgillies/1233327 lines 256:271
                }
            }

        gdf_agg = sensors_gdf.groupby(by='codice_stazione').aggregate({ prop: 'first' for prop in self._properties if prop in sensors_gdf.columns } | { 'valore': list, 'date_time': list }).reset_index(drop=True)
        
        features = []
        for _, row in gdf_agg.iterrows():
            geometry = {
                'type': 'Point',
                'coordinates': [row['longitudine'], row['latitudine']]
            }
            properties = { prop: row[prop] for prop in self._properties if prop not in ['longitudine', 'latitudine', 'dataora', 'date_time', 'valore'] }
            properties['water_level'] = [ 
                [ dt.isoformat(), val if not np.isnan(val) else None ]
                for dt, val in zip(row['date_time'], row['valore']) 
            ]
            
            features.append({
                'type': 'Feature',
                'geometry': geometry,
                'properties': properties
            })
        
        feature_collection = {
            'type': 'FeatureCollection',
            'features': features,
            'metadata': {
                'field': build_metadata(),
            },
            'crs': build_crs()
        }

        return feature_collection
    

    def run(
        self,
        lat_range = None,
        long_range = None,
        time_range = None,
        out_format = None,
        bucket_destination = None,
        out = None,
        **kwargs
    ):
        
        """
        Run the ARPAV Retriever.
        """

        try:

            # DOC: Validate the arguments
            validated_args = self.argument_validation(
                lat_range=lat_range,
                long_range=long_range,
                time_range=time_range,
                out=out,
                out_format=out_format,
                bucket_destination=bucket_destination,
            )
            lat_range = validated_args['lat_range']
            long_range = validated_args['long_range']
            time_start = validated_args['time_start']
            time_end = validated_args['time_end']
            out_format = validated_args['out_format']
            out = validated_args['out']
            bucket_destination = validated_args['bucket_destination']
            Logger.debug(f"Running ARPAV Retriever with parameters: {validated_args}")

            # DOC: Retrieve data from ARPAV API
            sensors_gdf = self.retrieve_data(
                long_range=long_range,
                lat_range=lat_range,
                time_start=time_start,
                time_end=time_end
            )
            Logger.debug(f"Retrieved {len(sensors_gdf)} sensors data from ARPAV API")

            # DOC: Build feature collection
            if out_format == 'geojson':
                feature_collection = self.data_to_feature_collection(sensors_gdf)
                feature_collection_fn = filesystem.normpath(f'{self.name}__{time_start.isoformat()}__{time_end.isoformat() if time_end else datetime.datetime.now(tz=datetime.timezone.utc).isoformat()}.geojson')
                feature_collection_fp = os.path.join(self._tmp_data_folder, feature_collection_fn) if out is None else out
                with open(feature_collection_fp, 'w') as f:
                    json.dump(feature_collection, f)
                output_filespaths = [feature_collection_fp]
                Logger.debug(f"Feature collection saved to {feature_collection_fp}")

            # DOC: Store data in bucket if bucket_destination is provided
            if bucket_destination is not None:
                bucket_uris = []
                for output_filepath in output_filespaths:
                    output_filename = os.path.basename(output_filepath)
                    bucket_uri = f'{bucket_destination}/{output_filename}'
                    upload_status = module_s3.s3_upload(output_filepath, bucket_uri)
                    if not upload_status:
                        raise StatusException(StatusException.ERROR, f"Failed to upload data to bucket {bucket_destination}")
                    bucket_uris.append(bucket_uri)
                    Logger.debug(f"Data stored in bucket: {bucket_uri}")

            # DOC: Prepare outputs
            if bucket_destination is not None or out is not None:
                outputs = { 'status': 'OK' }
                if bucket_destination is not None:
                    outputs = {
                        ** outputs,
                        ** ( {'uri': bucket_uris[0]} if len(bucket_uris) == 1 else {'uris': bucket_uris} )
                    }
                if out is not None:
                    outputs = {
                        ** outputs,
                        ** ( {'filepath': output_filespaths[0]} if len(output_filespaths) == 1 else {'filepaths': output_filespaths} )
                    }
            else:
                outputs = sensors_gdf
                
            Logger.debug(f"Outputs prepared")

            return outputs
    
        except Exception as ex:
            raise StatusException(StatusException.ERROR, f"An error occurred while running the ARPAV Retriever: {str(ex)}") from ex
        
        finally:
            # DOC: Clean up temporary data folder
            filesystem.garbage_folders(self._tmp_data_folder)
            Logger.debug(f'Cleaned up temporary data folder: {self._tmp_data_folder}')