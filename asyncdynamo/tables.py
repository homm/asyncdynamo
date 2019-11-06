import boto
from boto.dynamodb import exceptions
from boto.dynamodb.types import LossyFloatDynamizer
from boto.dynamodb.schema import Schema

from .asyncdynamo import AsyncDynamoDB


class Item(object):
    def __repr__(self):
        table = getattr(type(self), 'table')  # type: Table
        table_name = table.table_name if table else 'unknown'
        return "<{}({}): {}>".format(
            type(self).__name__, table_name, repr(self.__dict__))


class Table(object):
    base_item_class = Item

    def __init__(self, connection, table_name, primary_key, defaults=None,
                 dynamizer=LossyFloatDynamizer):
        self.connection = connection  # type: AsyncDynamoDB
        self.table_name = table_name
        self.primary_key = primary_key  # type: Schema
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

    async def get_item(self, hash_key, range_key=None, **kwargs):
        if self.primary_key.range_key_name is not None:
            if range_key is None:
                raise ValueError('Range key is required')

        key = self.layer2.build_key_from_values(
            self.primary_key, hash_key, range_key)
        object_hook = self.layer2.dynamizer.decode

        resp = await self.connection.get_item(
            self.table_name, key,
            object_hook=object_hook, **kwargs)

        if 'Item' not in resp:
            raise exceptions.DynamoDBKeyNotFoundError("Key does not exist.")

        return self.make_item(resp['Item'])

    async def query(self, hash_key, range_key_conditions=None,
                    exclusive_start_key=None, **kwargs):
        key = self.layer2.build_key_from_values(
            self.primary_key, hash_key)['HashKeyElement']
        if exclusive_start_key:
            exclusive_start_key = self.layer2.build_key_from_values(
                self.primary_key, *exclusive_start_key)
        if range_key_conditions:
            range_key_conditions = range_key_conditions.to_dict()
        object_hook = self.layer2.dynamizer.decode

        resp = await self.connection.query(
            self.table_name, key,
            exclusive_start_key=exclusive_start_key,
            range_key_conditions=range_key_conditions,
            object_hook=object_hook, **kwargs)

        last_key = None
        if 'LastEvaluatedKey' in resp:
            last_key = resp['LastEvaluatedKey']
            last_key = last_key['HashKeyElement'], last_key['RangeKeyElement']

        items = resp.get('Items', [])
        items = [self.make_item(item) for item in items]

        return (items, last_key)

    async def query_all(self, *args, **kwargs):
        assert 'exclusive_start_key' not in kwargs

        all_items = []
        last_key, first_run = None, True
        while last_key or first_run:
            first_run = False
            items, last_key = await self.query(
                *args, exclusive_start_key=last_key, **kwargs)
            all_items.extend(items)

        return all_items
