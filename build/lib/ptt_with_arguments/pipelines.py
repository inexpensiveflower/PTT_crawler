# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import ptt_with_arguments.items as items
from scrapy.exceptions import DropItem
import pymongo


class PttWithArgumentsPipeline:
    def process_item(self, item, spider):
        return item

class AbstractMongoPipeline(object):

	def __init__(self, mongo_uri, mongo_db, collection_name):
		self.mongo_uri = mongo_uri
		self.mongo_db = mongo_db
		# 連線上 mongoDB server
		self.client = pymongo.MongoClient(self.mongo_uri)
		# 選擇要連線的 DB
		self.db = self.client[self.mongo_db]
		# 選擇要連線的 collection
		self.collection = self.db[collection_name]


	@classmethod
	def from_crawler(cls, crawler):
		return cls(
			mongo_uri = crawler.settings.get('MONGO_URI'),
			mongo_db = crawler.settings.get('MONGO_DATABASE'),
			# 從 spider 那邊去拿使用者給的參數
			# pass 上去 __init__()
			collection_name = str(getattr(crawler.spider, 'db_collection'))
		)

	def close_spider(self, spider):
		self.client.close()

class InsertArticleReplyPipeline(AbstractMongoPipeline):
	

	def process_item(self, item, spider):
		if type(item) is items.PttWithArgumentsItem:
			document = self.collection.find_one({'post_url': item['post_url']})

			if not document:
				insert_result = self.collection.insert_one(dict(item))
				item['_id'] = insert_result.inserted_id
				print(item['title'], "   新增成功!")
			else:
				self.collection.update_one(
					{'_id': document['_id']},
					{'$set': dict(item)},
					upsert=True
				)

				item['_id'] = document['_id']
				print(item['title'], "   更新成功!")
			
		return item