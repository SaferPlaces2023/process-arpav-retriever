# -------------------------------------------------------------------------------
# MIT License:
# Copyright (c) 2012-2022 Luzzi Valerio
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
#
# Name:        s3.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     21/04/2022
# -------------------------------------------------------------------------------
import os
import random
import shutil
import tempfile
import fnmatch
import boto3
import requests
import logging
from requests.exceptions import RequestException
from botocore.exceptions import ClientError, NoCredentialsError
from .filesystem import justext, justpath, justfname, forceext
from .strings import startswith
from ..cli.module_log import Logger

logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("boto3").setLevel(logging.CRITICAL)

shpext = ("shp", "dbf", "shx", "prj", "qml", "qix", "qlr", "mta", "qmd", "cpg")

def tmp(filename):
    """
    tmp - return the temporary directory
    """
    #yyyymmdd = datetime.datetime.now().strftime("%Y-%m-%d %H.00")
    jid = os.environ.get(f"{__package__}-JID", os.getpid())
    rand = int(random.random()* 1e9)
    workdir= f"{tempfile.gettempdir()}/{__package__}/{jid}"
    os.makedirs(workdir, exist_ok=True)
    ext = f".{justext(filename)}" if filename else ""
    return f"{workdir}/tmp_{rand}{ext}"


def clean():
    """
    clean - remove the temporary directory
    """
    jid = os.environ.get(f"{__package__}-JID", os.getpid())
    workdir = f"{tempfile.gettempdir()}/{__package__}/{jid}"
    if os.path.isdir(workdir):
        try:
            shutil.rmtree(workdir)
        except Exception as ex:
            Logger.warning("Error removing %s:%s", workdir, ex)
            return False
        finally:
            os.environ.pop("JID", None)  # Remove JID from environment variables
    return True


def http_exists(url):
    """
    http_exists use requests
    """
    if isinstance(url, str) and url.startswith("http"):
        try:
            # download a byte-ranege of 1 byte to check if the URL exists
            headers = {"Range": "bytes=0-1"}
            with requests.get(url, headers=headers, timeout=5) as response:
                if response.status_code in (200, 206):
                    return True
            #r = requests.head(url, timeout=5)
            #return r.status_code == 200
        except RequestException as ex:
            Logger.warning(ex)
    return False


def http_get(url, headers=None, mode="text"):
    """
    http_get use requests
    """
    if url and isinstance(url, str) and url.startswith("http"):
        try:
            with requests.get(url, headers=headers, timeout=5) as response:
                if response.status_code == 200:
                    if mode == "json":
                        return response.json()
                    elif mode == "text":
                        return response.text
                    elif mode == "bytes":
                        return response.content
                    return response.content
        except RequestException as ex:
            Logger.error(ex)
    return None


def iss3(filename):
    """
    iss3
    """
    return filename and isinstance(filename, str) and \
        startswith(filename, ("s3://", "/vsis3/"))


def isuri(filename):
    """
    isuri - check if the filename is a URI
    """
    return filename and isinstance(filename, str) and \
        startswith(filename, ("http://", "https://"))


def isfile(filename):
    """
    isfile
    """
    if not filename:
        return False
    elif isinstance(filename, str) and os.path.isfile(filename):
        return True
    elif isuri(filename):
        return http_exists(filename)
    elif iss3(filename):
        return s3_exists(filename)
    return False


def get_bucket_name_key(uri):
    """
    get_bucket_name_key - get bucket name and key name from uri
    """
    bucket_name, key_name = None, None
    if not uri:
        pass
    elif uri.startswith("s3://"):
        # s3://saferplaces.co/tests/rimini/dem.tif
        _, _, bucket_name, key_name = uri.split("/", 3)
    elif uri.startswith("s3:/"):
        # s3:/saferplaces.co/tests/rimini/dem.tif
        _, bucket_name, key_name = uri.split("/", 2)
    elif uri.startswith("/vsis3/"):
        # /vsis3/saferplaces.co/tests/rimini/dem.tif
        _, _, bucket_name, key_name = uri.split("/", 3)
    elif uri.startswith("https://s3.amazonaws.com/"):
        _, _, bucket_name, key_name = uri.split("/", 3)
    elif uri.startswith("https://s3."):
        _, _, bucket_name, key_name = uri.split("/", 3)
    else:
        bucket_name, key_name = None, uri
    return bucket_name, key_name


def get_client(client=None):
    """
    get_client
    """
    return client if client else boto3.client('s3')



def s3_upload(filename, uri, remove_src=False, client=None):
    """
    Upload a file to an S3 bucket
    Examples: s3_upload(filename, "s3://saferplaces.co/a/rimini/lidar_rimini_building_2.tif")
    """

    # Upload the file
    try:
        bucket_name, key = get_bucket_name_key(uri)
        if bucket_name and key and filename and os.path.isfile(filename):
            client = get_client(client)
            extra_args = {}

            client.upload_file(Filename=filename,
                                Bucket=bucket_name, Key=key,
                                ExtraArgs=extra_args)
     
            if remove_src:
                Logger.debug("removing %s", filename)
                os.unlink(filename)  # unlink and not ogr_remove!!!
            return True

    except ClientError as ex:
        Logger.error(ex)
    except NoCredentialsError as ex:
        Logger.error(ex)

    return False


def s3_download(uri, fileout=None, remove_src=False, client=None):
    """
    Download a file from an S3 bucket
    """
    bucket_name, key = get_bucket_name_key(uri)
    if bucket_name:
        try:
            # check the cache
            client = get_client(client)

            if key and not key.endswith("/"):

                fileout = fileout or tmp(key)

                if os.path.isdir(fileout):
                    fileout = f"{fileout}/{justfname(key)}"

                Logger.debug("downloading %s into %s...", uri, fileout)
                os.makedirs(justpath(fileout), exist_ok=True)
                client.download_file(
                    Filename=fileout, Bucket=bucket_name, Key=key)
                if remove_src:
                    client.delete_object(Bucket=bucket_name, Key=key)
            else:
                objects = client.list_objects_v2(
                    Bucket=bucket_name, Prefix=key)['Contents']
                for obj in objects:
                    pathname = obj['Key']
                    if not pathname.endswith("/"):
                        dst = fileout
                        pathname = pathname.replace(key, "")
                        s3_download(f"{uri.rstrip('/')}/{pathname}",
                                    f"{dst}/{pathname}", client)

        except ClientError as ex:
            Logger.error(ex)
            return None
        except NoCredentialsError as ex:
            Logger.error(ex)
            return None

    return fileout if os.path.isfile(fileout) else None


def s3_exists(uri, client=None):
    """
    s3_exists
    """
    res = False
    try:
        bucket_name, filepath = get_bucket_name_key(uri)
        if bucket_name and filepath:
            client = get_client(client)
            client.head_object(Bucket=bucket_name, Key=filepath)
            res = True
    except ClientError as ex:
        Logger.error(ex)
        
    return res


def s3_remove(uri, filter=None, client=None):
    """
    s3_remove
    """
    res = False
    try:
        bucket_name, filepath = get_bucket_name_key(uri)
        if bucket_name and filepath and filter is None:
            client = get_client(client)
            client.delete_object(Bucket=bucket_name, Key=filepath)
            res = True
        elif bucket_name and filepath and filter:
            client = get_client(client)
            files = s3_list(uri, client=client)
            Objects =[]
            for f in files:
                _ , key = get_bucket_name_key(f)
                if fnmatch.fnmatch(key, filter):
                    Objects.append({'Key': key})
            client.delete_objects(Bucket=bucket_name, Delete={'Objects': Objects, 'Quiet': True})
            res = True
    except ClientError as ex:
        Logger.error(ex)
    return res


def s3_copy(src, dst, client=None):
    """
    s3_copy
    """
    res = False
    try:
        src_bucket_name, src_filepath = get_bucket_name_key(src)
        dst_bucket_name, dst_filepath = get_bucket_name_key(dst)
        if src_bucket_name and src_filepath and dst_bucket_name and dst_filepath:
            client = get_client(client)
            if s3_exists(src, client):
                client.copy_object(Bucket=dst_bucket_name, Key=dst_filepath,
                               CopySource={'Bucket': src_bucket_name, 'Key': src_filepath})
            res = True
    except ClientError as ex:
        Logger.error("!!!")
        Logger.error(ex)
    return res


def s3_move(src, dst, client=None):
    """
    s3_move
    """
    res = False
    try:
        src_bucket_name, src_filepath = get_bucket_name_key(src)
        dst_bucket_name, dst_filepath = get_bucket_name_key(dst)
        if src_bucket_name and src_filepath and dst_bucket_name and dst_filepath:
            client = get_client(client)
            client.copy_object(Bucket=dst_bucket_name, Key=dst_filepath,
                               CopySource={'Bucket': src_bucket_name, 'Key': src_filepath})
            client.delete_object(Bucket=src_bucket_name, Key=src_filepath)
            res = True
    except ClientError as ex:
        Logger.error(ex)
    return res


def s3_list(uri, etag=False, client=None):
    """
    s3_list
    regexp: s3://saferplaces.co/tests/rimini
    """
    res = []
    try:
        uri = f"{uri}/*" if not "*" in uri else uri
        bucket_name, pattern = get_bucket_name_key(uri)
        key_name, _ = pattern.split("/*", 1)
        Logger.debug(bucket_name, key_name)
        if bucket_name and key_name:
            client = get_client(client)
            response = client.list_objects_v2(Bucket=bucket_name, Prefix=key_name)
            for obj in response['Contents']:
                if fnmatch.fnmatch(obj['Key'], pattern):
                    item = f"s3://{bucket_name}/{obj['Key']}"
                    checksum = obj['ETag'].strip('"') if etag else None
                    if etag:
                        res.append((item, checksum))
                    else:
                        res.append(item)
    except ClientError as ex:
        Logger.error(ex)
    return res



def copy(src, dst=None, client=None):
    """
    copy
    """
    if not src:
        Logger.warning("No source file provided.")
        return None
    
    if isinstance(src, (tuple, list)): # and dst is None:
        return [copy(file, client=client) for file in src]

    dst = dst if dst else tmp(src)
    # if the source and destination are the same file do nothing
    if src and dst and os.path.isfile(src) and os.path.abspath(src) == os.path.abspath(dst):
        return dst
    
    # 0) if the source is a URI
    if isuri(src):
        src = http_get(src, mode="bytes")
    # 1) if the destination is a s3 file
    if os.path.isfile(src) and iss3(dst):
        s3_upload(src, dst, client=client)
    # 2) if the source is a s3 file
    elif iss3(src) and not iss3(dst):
        s3_download(src, dst, client=client)
    # 3) if the source and destination are s3 files
    elif iss3(src) and iss3(dst):
        s3_copy(src, dst, client=client)
    # 4) if the source is a file and the destination is a local file
    elif os.path.isfile(src) and not iss3(dst):
        shutil.copy2(src, dst)
    # 5) if the source is a folder
    elif os.path.isdir(src):
        if not iss3(dst):
            os.makedirs(dst, exist_ok=True)
        # copy all files in src folder recursively
        for root, _, files in os.walk(src):
            for file in files:
                copy(f"{root}/{file}", f"{dst}/{file}", client=client)
    # 6) if the source is a list of files
    
    
    # Finally
    # if the source is a shapefile or a tiff file, copy the related files
    exts = []
    if ".shp" in src.lower():
        exts = list(shpext)
        exts.remove("shp")
    elif src.endswith(".tif"):
        exts = [] #["tfw", "jpw", "prj", "aux.xml"]

    # copy the related files
    _ = [copy(forceext(src, ext), forceext(dst, ext), client=client) for ext in exts]
    # ----------------------------------------------------------------------

    return dst

def move(src, dst, client=None):
    """
    move
    """
    dst = dst if dst else tmp(src)

    # if the source and destination are the same file do nothing
    if src and dst and os.path.abspath(src) == os.path.abspath(dst):
        return dst

    if os.path.isfile(src) and iss3(dst):
        s3_upload(src, dst, remove_src=True, client=client)
    elif iss3(src) and not iss3(dst):
        s3_download(src, dst, remove_src=True, client=client)
    elif iss3(src) and iss3(dst):
        s3_move(src, dst, client=client)
    elif os.path.isfile(src) and not iss3(dst):
        try:
            os.makedirs(justpath(dst), exist_ok=True)
            shutil.move(src, dst)
        except Exception as ex:
            Logger.warning(ex)
    
    exts = []
    if src.endswith(".shp"):
        exts = list(shpext)
        exts.remove("shp")
    elif src.endswith(".tif"):
        exts = ["tfw", "jpw", "prj", "aux.xml"]
        
    for ext in exts:
        move(forceext(src, ext), forceext(dst, ext), client=client)

    return dst


def delete(uri, client=None):
    """
    delete
    """
    if iss3(uri):
        s3_remove(uri, client=client)
    elif os.path.isfile(uri):
        os.unlink(uri)
    elif os.path.isdir(uri):
        shutil.rmtree(uri)
    return uri


def hive_path(hive_dict):
    return '/'.join([f"{k}=={v}" for k, v in hive_dict.items()])