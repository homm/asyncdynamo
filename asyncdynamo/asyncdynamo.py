#
# Copyright 2012 bit.ly
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""
Created by Dan Frank on 2012-01-23.
Copyright (c) 2012 bit.ly. All rights reserved.
"""

import asyncio
import json
from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError
import logging
from urllib.parse import urlparse

from boto.connection import AWSAuthConnection
from boto.exception import DynamoDBResponseError
from boto.auth import HmacAuthV4Handler
from boto.provider import Provider

from .async_aws_sts import AsyncAwsSts, InvalidClientTokenIdError


class AsyncDynamoDB(AWSAuthConnection):
    """
    The main class for asynchronous connections to DynamoDB.

    The user should maintain one instance of this class (though more than one is ok),
    parametrized with the user's access key and secret key. Make calls with make_request
    or the helper methods, and AsyncDynamoDB will maintain session tokens in the background.


    As in Boto Layer1:
    "This is the lowest-level interface to DynamoDB.  Methods at this
    layer map directly to API requests and parameters to the methods
    are either simple, scalar values or they are the Python equivalent
    of the JSON input as defined in the DynamoDB Developer's Guide.
    All responses are direct decoding of the JSON response bodies to
    Python data structures via the json or simplejson modules."
    """

    DefaultHost = 'dynamodb.us-east-1.amazonaws.com'
    """The default DynamoDB API endpoint to connect to."""

    ServiceName = 'DynamoDB'
    """The name of the Service"""

    Version = '20111205'
    """DynamoDB API version."""

    ThruputError = "ProvisionedThroughputExceededException"
    """The error response returned when provisioned throughput is exceeded"""

    ExpiredSessionError = 'com.amazon.coral.service#ExpiredTokenException'
    """The error response returned when session token has expired"""

    UnrecognizedClientException = 'com.amazon.coral.service#UnrecognizedClientException'
    '''Another error response that is possible with a bad session token'''

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 host=None, debug=0, session_token=None, endpoint=None,
                 authenticate_requests=True, validate_cert=True,
                 max_sts_attempts=3):
        if not host:
            host = self.DefaultHost
        if endpoint is not None:
            self.url = endpoint
            parse_url = urlparse(self.url)
            self.host = parse_url.hostname
            self.port = parse_url.port
            self.protocol = parse_url.scheme
        else:
            self.protocol = 'https' if is_secure else 'http'
            self.host = host
            self.port = port

            url = '{0}://{1}'.format(self.protocol, self.host)

            if self.port:
                url += ':{}'.format(self.port)

            self.url = url
        self.validate_cert = validate_cert
        self.authenticate_requests = authenticate_requests
        AWSAuthConnection.__init__(self, self.host,
                                   aws_access_key_id,
                                   aws_secret_access_key,
                                   is_secure, self.port, proxy, proxy_port,
                                   debug=debug, security_token=session_token,
                                   validate_certs=self.validate_cert)
        self.http_client = AsyncHTTPClient()
        self.sts = AsyncAwsSts(aws_access_key_id, aws_secret_access_key,
                               is_secure, self.port, proxy, proxy_port)
        assert (isinstance(max_sts_attempts, int) and max_sts_attempts >= 0)
        self.max_sts_attempts = max_sts_attempts
        self._wait_session_token = None

    def _required_auth_capability(self):
        return ['hmac-v4']

    def update_session_token(self, already_used_future=None):
        def reset_on_error(f):
            if f.exception():
                self._wait_session_token = None

        # This future was already tried and is invalid
        if already_used_future == self._wait_session_token:
            self._wait_session_token = None

        if self._wait_session_token is None:
            self._wait_session_token = asyncio.ensure_future(
                self._get_session_token())
            self._wait_session_token.add_done_callback(reset_on_error)
        return self._wait_session_token

    async def _get_session_token(self):
        attempts = 0
        while True:
            try:
                creds = await self.sts.get_session_token()
            except InvalidClientTokenIdError:
                raise
            except Exception as e:
                if attempts >= self.max_sts_attempts:
                    raise
                seconds_to_wait = 0.1 * (2 ** attempts)
                logging.warning("Got error[ %s ] getting session token, retrying in %.02f seconds" % (e, seconds_to_wait))
                await gen.sleep(seconds_to_wait)
                attempts += 1
            else:
                self._set_credentionals(creds)
                break

    def _set_credentionals(self, creds):
        self.provider = Provider('aws',
                                 creds.access_key,
                                 creds.secret_key,
                                 creds.session_token)
        # force the correct auth, with the new provider
        self._auth_handler = HmacAuthV4Handler(self.host, None, self.provider)

    async def make_request(self, action, body='', object_hook=None):
        """
        Make an asynchronous HTTP request to DynamoDB. Callback should operate on
        the decoded json response (with object hook applied, of course). It should also
        accept an error argument, which will be a boto.exception.DynamoDBResponseError.

        If there is not a valid session token, this method will ensure that a new one is fetched
        and cache the request when it is retrieved.
        """
        token_future = None
        if self.authenticate_requests:
            token_future = self.update_session_token()
            await token_future

        binary_body = body.encode('utf-8')
        headers = {'X-Amz-Target': '%s_%s.%s' % (self.ServiceName,
                                                 self.Version, action),
                   'Content-Type': 'application/x-amz-json-1.0',
                   'Content-Length': str(len(binary_body))}
        request = HTTPRequest(self.url, method='POST', headers=headers,
                              body=binary_body, validate_cert=self.validate_cert)
        request.path = '/'  # Important! set the path variable for signing by boto (<2.7). '/' is the path for all dynamodb requests
        request.auth_path = '/'  # Important! set the auth_path variable for signing by boto(>2.7). '/' is the path for all dynamodb requests
        request.params = {}
        request.port = self.port
        request.protocol = self.protocol
        request.host = self.host
        if self.authenticate_requests:
            self._auth_handler.add_auth(request)  # add signature to headers of the request

        response = await self.http_client.fetch(request, raise_error=False)
        if response.error and not isinstance(response.error, HTTPError):
            raise response.error

        """
        Check for errors and decode the json response (in the tornado response body), then pass on to orig callback.
        This method also contains some of the logic to handle reacquiring session tokens.
        """
        try:
            json_response = json.loads(response.body, object_hook=object_hook)
        except TypeError:
            json_response = None

        if json_response and response.error:
            # Normal error handling where we have a JSON response from AWS.
            if json_response.get('__type', '') in [self.ExpiredSessionError,
                                                   self.UnrecognizedClientException]:
                # the token that we used has expired. wipe it out
                self.update_session_token(token_future)
                resp = await self.make_request(action, body, object_hook)
                return resp
            else:
                # because some errors are benign, include the response when an error is passed
                raise DynamoDBResponseError(response.error.code,
                                            response.error.message,
                                            json_response)

        if json_response is None:
            # We didn't get any JSON back, but we also didn't receive an error response. This can't be right.
            raise DynamoDBResponseError(response.code, response.body)
        return json_response

    def get_item(self, table_name, key, attributes_to_get=None,
                 consistent_read=False, object_hook=None):
        """
        Return a set of attributes for an item that matches
        the supplied key.

        The callback should operate on a dict representing the decoded
        response from DynamoDB (using the object_hook, if supplied)

        :type table_name: str
        :param table_name: The name of the table to delete.

        :type key: dict
        :param key: A Python version of the Key data structure
            defined by DynamoDB.

        :type attributes_to_get: list
        :param attributes_to_get: A list of attribute names.
            If supplied, only the specified attribute names will
            be returned.  Otherwise, all attributes will be returned.

        :type consistent_read: bool
        :param consistent_read: If True, a consistent read
            request is issued.  Otherwise, an eventually consistent
            request is issued.
        """
        data = {'TableName': table_name,
                'Key': key}
        if attributes_to_get:
            data['AttributesToGet'] = attributes_to_get
        if consistent_read:
            data['ConsistentRead'] = True
        return self.make_request('GetItem', body=json.dumps(data),
                                 object_hook=object_hook)

    def batch_get_item(self, request_items):
        """
        Return a set of attributes for a multiple items in
        multiple tables using their primary keys.

        The callback should operate on a dict representing the decoded
        response from DynamoDB (using the object_hook, if supplied)

        :type request_items: dict
        :param request_items: A Python version of the RequestItems
            data structure defined by DynamoDB.
        """
        data = {'RequestItems': request_items}
        json_input = json.dumps(data)
        return self.make_request('BatchGetItem', json_input)

    def put_item(self, table_name, item, expected=None, return_values=None, object_hook=None):
        """
        Create a new item or replace an old item with a new
        item (including all attributes).  If an item already
        exists in the specified table with the same primary
        key, the new item will completely replace the old item.
        You can perform a conditional put by specifying an
        expected rule.

        The callback should operate on a dict representing the decoded
        response from DynamoDB (using the object_hook, if supplied)

        :type table_name: str
        :param table_name: The name of the table to delete.

        :type item: dict
        :param item: A Python version of the Item data structure
            defined by DynamoDB.

        :type expected: dict
        :param expected: A Python version of the Expected
            data structure defined by DynamoDB.

        :type return_values: str
        :param return_values: Controls the return of attribute
            name-value pairs before then were changed.  Possible
            values are: None or 'ALL_OLD'. If 'ALL_OLD' is
            specified and the item is overwritten, the content
            of the old item is returned.
        """
        data = {'TableName': table_name,
                'Item': item}
        if expected:
            data['Expected'] = expected
        if return_values:
            data['ReturnValues'] = return_values
        json_input = json.dumps(data)
        return self.make_request('PutItem', json_input, object_hook=object_hook)

    def query(self, table_name, hash_key_value, range_key_conditions=None,
              attributes_to_get=None, limit=None, consistent_read=False,
              scan_index_forward=True, exclusive_start_key=None,
              object_hook=None):
        """
        Perform a query of DynamoDB.  This version is currently punting
        and expecting you to provide a full and correct JSON body
        which is passed as is to DynamoDB.

        The callback should operate on a dict representing the decoded
        response from DynamoDB (using the object_hook, if supplied)

        :type table_name: str
        :param table_name: The name of the table to delete.

        :type hash_key_value: dict
        :param hash_key_value: A DynamoDB-style HashKeyValue.

        :type range_key_conditions: dict
        :param range_key_conditions: A Python version of the
            RangeKeyConditions data structure.

        :type attributes_to_get: list
        :param attributes_to_get: A list of attribute names.
            If supplied, only the specified attribute names will
            be returned.  Otherwise, all attributes will be returned.

        :type limit: int
        :param limit: The maximum number of items to return.

        :type consistent_read: bool
        :param consistent_read: If True, a consistent read
            request is issued.  Otherwise, an eventually consistent
            request is issued.

        :type scan_index_forward: bool
        :param scan_index_forward: Specified forward or backward
            traversal of the index.  Default is forward (True).

        :type exclusive_start_key: list or tuple
        :param exclusive_start_key: Primary key of the item from
            which to continue an earlier query.  This would be
            provided as the LastEvaluatedKey in that query.
        """
        data = {'TableName': table_name,
                'HashKeyValue': hash_key_value}
        if range_key_conditions:
            data['RangeKeyCondition'] = range_key_conditions
        if attributes_to_get:
            data['AttributesToGet'] = attributes_to_get
        if limit:
            data['Limit'] = limit
        if consistent_read:
            data['ConsistentRead'] = True
        if scan_index_forward:
            data['ScanIndexForward'] = True
        else:
            data['ScanIndexForward'] = False
        if exclusive_start_key:
            data['ExclusiveStartKey'] = exclusive_start_key
        json_input = json.dumps(data)
        return self.make_request('Query', body=json_input,
                                 object_hook=object_hook)
