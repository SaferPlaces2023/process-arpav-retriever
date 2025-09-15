# Process ARPAV Retriever

**process-arpav-retriever** is a script with an optional PyGeoAPI Process implementation to retrieve ARPAV sensor data.

```
pip install "process-arpav-retriever[pygeoapi] @ git+https://github.com/SaferPlaces2023/process-arpav-retriever.git"

pip install .[pygeoapi]
```

It will install extra dependecies to use you program as a PyGeoAPI process

## CLI

**Command name**: `arpav-retriever` **[N.B. keep date-range inside last 24 hours]** 
```
arpav-retriever --lat_range 45.28 45.63 --long_range 11.95 12.46 --time_range 2025-09-15T09:00:00 2025-09-15T14:00:00 --variable water_level --out out_water_level.geojson --debug
```

### Arguments

| **Argument**                      | **Description**                                                                                                                                         | **Example** |
|-------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------|
| **`--lat_range`**, `--lat`, `--latitude_range`, `--latitude`, `--lt` | Latitude range as two floats (min, max). | `--lat_range 40.0 42.0` |
| **`--long_range`**, `--long`, `--longitude_range`, `--longitude`, `--lg` | Longitude range as two floats (min, max). | `--long_range 12.0 14.0` |
| **`--time_range`**, `--time`, `--datetime_range`, `--datetime`, `--t`| Time range as two ISO 8601 UTC0 strings (start, end). | `--time_range 2025-07-23T00:00:00 2025-07-24T00:00:00` |
| **`--variable`**, `--var` | Variable to retrieve, either `"precipitation"` or `"water_level"` | `--variable precipitation` |
| **`--out`**, `--output`, `--o` | Output file path for the retrieved data. If not provided, the output will be returned as a dictionary. | `--out /path/to/output.json` |
| **`--out_format`**, `--output_format`, `--of` | Output format of the retrieved data. | `--out_format geojson` |
| **`--bucket_destination`**, `--bucket`, `--s3` | Destination bucket for the output data. | `--bucket_destination s3://my-bucket/path/to/prefix` |
| **`--version`**, `-v`                    | Print version.                                                                                                                         | `--version` |
| **`--debug`**                            | Enable debug mode.                                                                                                                     | `--debug` |
| **`--verbose`**                          | Enable verbose mode.                                                                                                                   | `--verbose` |
| `--help`                             | Show this message and exit.                                                                                                            | `--help` |
