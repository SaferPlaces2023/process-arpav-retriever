# -------------------------------------------------------------------------------
# License:
# Copyright (c) 2025 Gecosistema S.r.l.
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
# Name:        filesystem.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     16/12/2019
# -------------------------------------------------------------------------------

import os
import glob
import shutil
import datetime
import tempfile
import hashlib
import platform


def now():
    """
    now
    """
    return datetime.datetime.now()


def total_seconds_from(t):
    """
    total_seconds_from
    """
    return (datetime.datetime.now() - t).total_seconds()


def is_windows():
    """
    is_windows - Check if the current platform is Windows
    """
    return platform.system() == "Windows"

def is_linux():
    """
    is_linux - Check if the current platform is Linux
    """
    return platform.system() == "Linux"

def is_mac():
    """
    is_mac - Check if the current platform is macOS
    """
    return platform.system() == "Darwin"

def is_unix():
    """
    is_unix - Check if the current platform is Unix-like (Linux or macOS)
    """
    return is_linux() or is_mac()


def normpath(pathname):
    """
    normpath
    """
    if not pathname:
        return ""
    pathname = os.path.normpath(pathname.replace("\\", "/")).replace("\\", "/")
    if is_windows():
        dirname, filename = os.path.split(pathname)
        pathname = os.path.join(dirname, filename.replace(':','_'))
    return pathname


def juststem(pathname):
    """
    juststem
    """
    pathname = os.path.basename(pathname)
    (root, _) = os.path.splitext(pathname)
    return root


def justpath(pathname, n=1):
    """
    justpath
    """
    for _ in range(n):
        pathname, _ = os.path.split(normpath(pathname))
    if pathname == "":
        return "."
    return normpath(pathname)


def justfname(pathname):
    """
    justfname - returns the basename
    """
    return normpath(os.path.basename(normpath(pathname)))


def justext(pathname):
    """
    justext
    """
    pathname = os.path.basename(normpath(pathname))
    (_, ext) = os.path.splitext(pathname)
    return ext.lstrip(".")


def forceext(pathname, newext):
    """
    forceext
    """
    (root, _) = os.path.splitext(normpath(pathname))
    newext = newext.lstrip(".")
    pathname = root + ("." + newext if len(newext.strip()) > 0 else "")
    return normpath(pathname)


def isfile(pathname):
    """
    isfile
    """
    return pathname and isinstance(pathname, str) and os.path.isfile(pathname)


def israster(pathname):
    """
    israster
    """
    
    raster_extensions = [".tif", ".tiff", ".geotiff"]
    return isfile(pathname) and any(pathname.lower().endswith(ext) for ext in raster_extensions)


def isvector(pathname):
    """
    isvector
    """
    
    vector_extensions = [".json", ".geojson", ".shp"]
    return isfile(pathname) and any(pathname.lower().endswith(ext) for ext in vector_extensions)


def iss3(filename):
    """
    iss3
    """
    return filename and (filename.startswith("s3://") or filename.startswith("/vsis3/"))


def mkdirs(pathname):
    """
    mkdirs - create a folder
    """
    try:
        if os.path.isfile(pathname):
            pathname = justpath(pathname)
        os.makedirs(pathname)
    except:
        pass
    return os.path.isdir(pathname)


def tempdir(name=""):
    """
    tempdir
    :return: a temporary directory
    """
    foldername = normpath(tempfile.gettempdir() + "/" + name)
    os.makedirs(foldername, exist_ok=True)
    return foldername


def tempfilename(prefix="", suffix=""):
    """
    return a temporary filename
    """
    return normpath(tempfile.gettempdir() + "/" + datetime.datetime.strftime(now(), f"{prefix}%Y%m%d%H%M%S%f{suffix}"))


def md5sum(filename):
    """
    md5sum - returns themd5 of the file
    """
    res = ""
    with open(filename, mode='rb') as stream:
        digestor = hashlib.md5()
        while True:
            buf = stream.read(4096)
            if not buf:
                break
            digestor.update(buf)
        res = digestor.hexdigest()
        return res


def md5text(text):
    """
    md5text - Returns the md5 of the text
    """
    if text is not None:
        digestor = hashlib.md5()
        if isinstance(text, (bytes, bytearray)):
            digestor.update(text)
        else:
            digestor.update(text.encode("utf-8"))
        return digestor.hexdigest()
    return None


def garbage_folders(*folders):
    """
    Remove all files in folders from the filesystem (but not the folder itself).
    """
    for folder in folders:
        contents_fns = glob.glob(f'{folder}/*', recursive=True)
        for content in contents_fns:
            if os.path.isfile(content):
                try:
                    os.remove(content)
                except Exception as e:
                    print(f"Error removing file {content}: {e}")
            elif os.path.isdir(content):
                try:
                    shutil.rmtree(content, ignore_errors=True)
                except Exception as e:
                    print(f"Error removing directory {content}: {e}")
