# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PttWithArgumentsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    _id = scrapy.Field()
    author = scrapy.Field()
    title = scrapy.Field()
    post_time = scrapy.Field()
    post_score = scrapy.Field()
    update_time = scrapy.Field()
    post_url = scrapy.Field()
    content = scrapy.Field()
    comments = scrapy.Field()
