#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course.
#
# StreapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#

from urllib.parse import urlparse, unquote
from typing import Any

from .dictionary import CaseInsensitiveDict

def get_auth_from_url(url):
    """Given a url with authentication components, extract them into a tuple of
    username,password.

    :rtype: (str,str)
    """
    parsed = urlparse(url)

    try:
        auth = (unquote(parsed.username), unquote(parsed.password))
    except (AttributeError, TypeError):
        auth = ("", "")

    return auth


def _parse_content_type_header(header: str) -> tuple[str, dict[str, Any]]:
    """Returns content type and parameters from given header.

    :param header: string
    :return: tuple containing content type and dictionary of
         parameters.
    """

    tokens = header.split(";")
    content_type, params = tokens[0].strip(), tokens[1:]
    params_dict: dict[str, str | bool] = {}
    strip_chars = "\"' "

    for param in params:
        param = param.strip()
        if param and (idx := param.find("=")) != -1:
            key = param[:idx].strip(strip_chars)
            value = param[idx + 1 :].strip(strip_chars)
            params_dict[key.lower()] = value
    return content_type, params_dict


def get_encoding_from_headers(headers: CaseInsensitiveDict[str]) -> str | None:
    """Returns encodings from given HTTP Header Dict.

    :param headers: dictionary to extract encoding from.
    :rtype: str
    """

    content_type = headers.get("content-type")

    if not content_type:
        return None

    content_type, params = _parse_content_type_header(content_type)

    if "charset" in params:
        return params["charset"].strip("'\"")

    if "text" in content_type:
        return "ISO-8859-1"

    if "application/json" in content_type:
        # Assume UTF-8 based on RFC 4627: https://www.ietf.org/rfc/rfc4627.txt since the charset was unset
        return "utf-8"