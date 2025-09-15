# -----------------------------------------------------------------------------
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
# Name:        http.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     21/10/2022
# -----------------------------------------------------------------------------
import json
import requests
import datetime
from ..cli.module_log import Logger


def patch(url, data):
    """
    patch - send a PATCH request to the given URL with the provided data.
    :param url: The URL to send the PATCH request to.
    :param data: The data to send in the PATCH request, as a dictionary.
    :return: The response from the server, parsed as JSON.
    """
    try:
        headers = {"content-type": "application/json"}
        # url = url if url.startswith("http") else f"http://{backend}:8000{url}"
        response = requests.patch(url, data=json.dumps(data), headers=headers, timeout=3)
        return json.loads(response.text)
    except Exception as ex:
        Logger.error("Error in patch:%s", ex)
        return {}


def set_status(backend, jid, progress, message=""):
    """
    set_status
    """
    if message and progress >=0:
        Logger.debug(message)
    elif message and progress < 0:
        Logger.error(message)
    if backend and jid:
        # localhost/{jid}
        # localhost:8000/{jid}
        if not backend.startswith("http"):
            backend = f"https://{backend}"

        # default port is 8000
        if ":" not in backend and not backend.endswith("/"):
            backend = f"{backend}:8000"

        url = f"{backend}/api/jobs/status/{jid}"
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        if isinstance(progress, str) and "+" in progress:
            data = {
                "status": "running",
                "progress": progress
            }
            patch(url, data)
            return

        progress = int(progress)

        if progress < 0:
            data = {
                "status": "error",
                "progress": progress,
                "error": message,
                "endtime": now
            }
        elif progress == 0:
            data = {
                "status": "pending",
                "progress": progress
            }
        elif progress >= 100:
            data = {
                "status": "done",
                "progress": progress,
                "endtime": now
            }
        else:
            data = {
                "status": "running",
                "progress": progress
            }
        patch(url, data)
