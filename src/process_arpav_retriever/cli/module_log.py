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
# Name:        module_log.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     27/12/2022
# -----------------------------------------------------------------------------
import logging

# Configure the logger
logging.basicConfig(format="[%(levelname)-8s] %(message)s")

Logger = logging.getLogger(__name__)
Logger.setLevel(logging.WARNING)

# Some functions
def set_log_debug():
    """Set the logger to debug level."""
    Logger.setLevel(logging.DEBUG)
    Logger.debug("Logger set to DEBUG level.")

def set_log_info():
    """Set the logger to info level."""
    Logger.setLevel(logging.INFO)
    Logger.info("Logger set to INFO level.")

def set_log_warning():
    """Set the logger to warning level."""
    Logger.setLevel(logging.WARNING)
    Logger.warning("Logger set to WARNING level.")

def set_log_error():
    """Set the logger to error level."""
    Logger.setLevel(logging.ERROR)
    Logger.error("Logger set to ERROR level.")

def set_log_critical():
    """Set the logger to critical level."""
    Logger.setLevel(logging.CRITICAL)
    Logger.critical("Logger set to CRITICAL level.")