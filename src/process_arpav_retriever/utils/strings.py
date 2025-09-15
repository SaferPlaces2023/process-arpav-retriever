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
# Name:        strings.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     26/10/2023
# -----------------------------------------------------------------------------
import inspect


def is_string(s):
    """
    is_string
    """
    return isinstance(s, str)


def is_integer(s):
    """
    is_integer
    """
    try:
        int(s)
        return True
    except ValueError:
        return False


def is_float(s):
    """
    is_float
    """
    try:
        float(s)
        return True
    except ValueError:
        return False

def is_array(s):
    """
    is_array
    """
    return isinstance(s, (list, tuple))

def startswith(s, prefixes):
    """
    startswith
    """
    for prefix in prefixes:
        if s.startswith(prefix):
            return True
    return False


def listify(text, sep=",", trim=False):
    """
    listify -  make a list from string
    """
    if text is None:
        return []
    elif is_string(text):
        arr = text.split(sep)
        if trim:
            arr = [item.strip() for item in arr]
        return arr
    elif is_array(text):
        return text
    return [text]


def get_default_values(func):
    """
    get_default_values
    """
    signature = inspect.signature(func)
    return {
        k:  v.default if v.default is not inspect.Parameter.empty else None
        for k, v in signature.parameters.items()
    }


def parse_event(event, func):
    """
    parse_event
    """
    # Copy the event to avoid side effects
    # kwargs = defaults.copy()
    kwargs = get_default_values(func)

    # Update kwargs with event
    for key in event:
        if key in kwargs:
            kwargs[key] = event[key]
        else:
            print(f"Option <{key}> is not available")

    # patch numeric and boolean params because can be passed as string
    for key in kwargs:
        value = kwargs[key]
        if is_string(value):
            if value.lower() == "true":
                kwargs[key] = True
            elif value.lower() == "false":
                kwargs[key] = False
            elif is_integer(value):
                kwargs[key] = int(value)
            elif is_float(value):
                kwargs[key] = float(value)
    return kwargs
