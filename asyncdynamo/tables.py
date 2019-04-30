from __future__ import unicode_literals

import boto
from boto.dynamodb import exceptions
from boto.dynamodb.types import LossyFloatDynamizer
from boto.dynamodb.schema import Schema
from tornado import gen

from .asyncdynamo import AsyncDynamoDB


class Item(object):
    def __repr__(self):
        table = getattr(type(self), 'table')  # type: Table
        table_name = table.table_name if table else 'unknown'
        return "<{}({}): {}>".format(
            type(self).__name__, table_name, repr(self.__dict__))


class Table(object):
    base_item_class = Item

    def __init__(self, connection, table_name, schema, defaults=None,
                 dynamizer=LossyFloatDynamizer):
        self.connection = connection  # type: AsyncDynamoDB
        self.table_name = table_name
        self.schema = schema  # type: Schema
        self.defaults = defaults  # type: dict
        self.layer2 = boto.connect_dynamodb(
            dynamizer=dynamizer,
            aws_access_key_id='dummy', aws_secret_access_key='dummy')

        class TableItem(self.base_item_class):
            table = self
        self.item_class = TableItem

    def __repr__(self):
        return "<{}: {}>".format(type(self).__name__, self.table_name)

    def make_item(self, data):
        item = self.item_class()
        if self.defaults:
            item.__dict__.update(self.defaults)
        item.__dict__.update(data)
        return item

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

        raise gen.Return(self.make_item(resp['Item']))

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

        items = resp.get('Items', [])
        items = [self.make_item(item) for item in items]

        raise gen.Return((items, last_key))

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
