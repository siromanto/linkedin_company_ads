""" Pyhton test API client for LinkedIn

"""

import httplib2
import json
from urllib.parse import urlencode


class LinkedinAdsApi:

    def __init__(self, headers, timeout=60):
        self.headers = headers
        self.endpoint = 'https://api.linkedin.com/v2/'
        self.h = httplib2.Http("/tmp/.cache", timeout=timeout)

    def __getattr__(self, item):
        def call(params):
            _uri = '{endpoint}{function}?{query}'.format(
                endpoint=self.endpoint,
                function=item,
                query=params
            )
            resp, content = self.h.request(_uri, 'GET', headers=self.headers)

            if not resp.status == 200:
                if resp.status == 404:
                    raise Exception('Method does not exists!')
                elif resp.status == 400:
                    _c = json.loads(content)
                    _ec = _c.get('ErrorCode')
                    if _ec == 8:
                        raise Exception('Invalid parameter given to %s function.' % item)
                    elif _ec == 14:
                        raise Exception('Not authorized.')
                raise Exception('Unknown error occured [status code: %d] with response: %s' % (resp.status, content))
            else:
                return json.loads(content.decode('utf-8'))

        return call
