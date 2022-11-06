import boto3
import json

class DeleteItemDynamoDB:
    def __init__(self, file):
       self.table, self.items  = self._read_file(file)
       self._read_json_to_delete_items()

    def delete_item(self, item_pk, item_value):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(self.table)

        response = table.delete_item(
            Key={
                item_pk: item_value
            }
        )
        return response


    def _read_json_to_delete_items(self):
        for item in self.items:
            item_pk = list(item.keys())[0]
            item_value = item[item_pk]
            # delete_item(item_pk, item_value)
            print(item_pk)
            print(item_value)


    def _read_file(self, file_json):
        f = open(file_json)
        file = json.load(f)
        table = file.get('table_name')
        items = file.get('items_to_delete')
        if table and items:
            return table, items
        raise ValueError("missing values")

