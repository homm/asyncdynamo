from __future__ import unicode_literals

import boto
from boto.dynamodb import exceptions
from boto.dynamodb.types import LossyFloatDynamizer
from boto.dynamodb.schema import Schema
from tornado import gen

from .asyncdynamo import AsyncDynamoDB


class Table(object):
    def __init__(self, connection, table_name, schema,
                 dynamizer=LossyFloatDynamizer):
        self.connection = connection  # type: AsyncDynamoDB
        self.table_name = table_name
        self.schema = schema  # type: Schema
        self.layer2 = boto.connect_dynamodb(
            dynamizer=dynamizer,
            aws_access_key_id='dummy', aws_secret_access_key='dummy')

    @gen.coroutine
    def get_item(self, hash_key, range_key=None, **kwargs):
        if self.schema.range_key_name is not None:
            if range_key is None:
                raise ValueError('Range key is required')

        key = self.layer2.build_key_from_values(
            self.schema, hash_key, range_key)
        object_hook = self.layer2.dynamizer.decode

        (resp,), kwargs = yield gen.Task(self.connection.get_item,
                                         self.table_name, key,
                                         object_hook=object_hook, **kwargs)
        if kwargs['error']:
            raise kwargs['error']

        if 'Item' not in resp:
            raise exceptions.DynamoDBKeyNotFoundError("Key does not exist.")

        raise gen.Return(resp['Item'])

    @gen.coroutine
    def query(self, hash_key, range_key_conditions=None,
              exclusive_start_key=None, **kwargs):
        key = self.layer2.build_key_from_values(
            self.schema, hash_key)['HashKeyElement']
        if exclusive_start_key:
            exclusive_start_key = self.layer2.build_key_from_values(
                self.schema, *exclusive_start_key)
        if range_key_conditions:
            range_key_conditions = range_key_conditions.to_dict()
        object_hook = self.layer2.dynamizer.decode

        (resp,), kwargs = yield gen.Task(
            self.connection.query,
            self.table_name, key,
            exclusive_start_key=exclusive_start_key,
            range_key_conditions=range_key_conditions,
            object_hook=object_hook, **kwargs)

        if kwargs['error']:
            raise kwargs['error']

        last_key = None
        if 'LastEvaluatedKey' in resp:
            last_key = resp['LastEvaluatedKey']
            last_key = last_key['HashKeyElement'], last_key['RangeKeyElement']

        raise gen.Return((resp.get('Items', []), last_key))

    @gen.coroutine
    def query_all(self, *args, **kwargs):
        assert 'exclusive_start_key' not in kwargs

        all_items = []
        last_key, first_run = None, True
        while last_key or first_run:
            first_run = False
            items, last_key = yield self.query(
                *args, exclusive_start_key=last_key, **kwargs)
            all_items.extend(items)

        raise gen.Return(all_items)
