# ---------------------------------------------------------------------------
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
# Name:        module_prologo.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     27/12/2022
# ---------------------------------------------------------------------------

import os
import sys
import logging
from ..cli.module_log import Logger
from ..cli.module_version import get_version
from ..cli.module_logo import logo
from .filesystem import now,total_seconds_from
from .module_s3 import clean
from .module_status import set_status

def prologo(backend, jid, version, verbose, debug):
    """
    prologo - print the logo
    """
    t = now()
    jid = jid or os.getpid()

    os.environ[f"{__package__}-JID"] = str(jid)
    
    if verbose:
        Logger.setLevel(logging.INFO)
    if debug:
        Logger.setLevel(logging.DEBUG)

    set_status(backend, jid, 0, "Starting job...")

    if debug:
        print(logo())

    if version:
        print(f"Version: {get_version()}")
        sys.exit(0)

    return t, jid



def epilogo(t, backend, jid):
    """
    epilogo - print the epilogo
    """

    clean()
    set_status(backend, jid, 100, f"Job completed in {total_seconds_from(t):.2f}s.")