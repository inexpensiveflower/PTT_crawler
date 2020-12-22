import scrapy
import re
import time
import logging
import ptt_with_arguments.items as items
from datetime import datetime
from scrapy.http import FormRequest
import sys


class PttSpider(scrapy.Spider):
	name = 'ptt'
	allowed_domains = ['ptt.cc']

	_page = 0
	# MAX_PAGES = 1

	_retries = 0
	MAX_RETRY = 1

	# 在 __init__() 裡面設定 spider 要接收的參數
	# 啟動爬蟲的時候要給 category類別、DB名稱、Collection名稱、要爬得最大頁數
	def __init__(self, category = 'Sex', db_collection = 'Sex_Article_Reply', max_page = 10, *args, **kwargs):
		super(PttSpider, self).__init__(*args, **kwargs)

		if isinstance(category, str):
			if isinstance(db_collection, str):
				self.start_urls = [f'http://www.ptt.cc/bbs/{category}/index.html',]
				self.category = category
				self.db_collection = db_collection
				self.MAX_PAGES = max_page
			else:
				print("Please assign a database collection to store the items.")
				sys.exit(0)
		else:
			print("Please assign a category to crawl.")
			sys.exit(0)
			

	def parse(self, response):

		if len(response.xpath('//div[@class="over18-notice"]')) > 0:
			if self._retries < PttSpider.MAX_RETRY:
				self._retries += 1
				logging.warning('retry {} times...'.format(self._retries))
				yield FormRequest.from_response(response,
					formdata = {'yes':'yes'},
					callback = self.parse)
			else:
				logging.warning('you cannot pass')
		else:
			self._page += 1

			for href in response.css('div.r-ent > div.title > a::attr(href)'):
				url = href.get()
				yield scrapy.Request(response.urljoin(url), callback = self.parse_post)

			if self._page < int(getattr(self, 'MAX_PAGES', )):
				next_page = response.css('#action-bar-container > div > div.btn-group.btn-group-paging  > a::attr(href)').getall()

				if next_page:
					url = response.urljoin(next_page[1])
					time.sleep(1)
					yield scrapy.Request(url, self.parse)
				else:
					logging.warning("no next page")
			else:
				logging.warning("max page reached")

	def parse_post(self, response):
		post_info = items.PttWithArgumentsItem()

		try:
			post_author = response.xpath('//*[@id="main-content"]/div[1]/span[2]/text()')[0].get().split(' ')[0]
		except:return(0)
		try:
			post_title = response.xpath('//*[@id="main-content"]/div[3]/span[2]/text()')[0].get()
		except:return(0)
		try:
			post_time = datetime.strptime(response.xpath('//*[@id="main-content"]/div[4]/span[2]/text()')[0].get(), '%a %b %d %H:%M:%S %Y')
		except:return(0)

		content = response.css('div#main-content').css('::text').getall()
		content = ", ".join(content)
		content = content.split('--')[0]
		content = content.split('\n')
		content = "\n".join(content[1:])

		push_tag_list = response.css('div.push > span.push-tag').css('::text').getall()
		score = 0
		push_count = 0
		abstract_count = 0

		for push in push_tag_list:
			if "推" in push:
				push_count += 1
			elif "噓" in push:
				abstract_count += 1
			else:pass

		score = score + push_count - abstract_count
		comments = []
		reply_list = response.css('div.push')

		for reply in reply_list:
			reply_id = reply.css('span.push-userid::text').get()
			reply_tag = reply.css('span.push-tag::text').get()
			reply_content = reply.css('span.push-content::text').get()

			comments.append({'reply_id':reply_id,
				'reply_tag':reply_tag,
				'reply_content':reply_content})

		post_info['author'] = post_author
		post_info['title'] = post_title
		post_info['post_time'] = post_time
		post_info['post_score'] = score
		post_info['post_url'] = response.url
		post_info['update_time'] = datetime.now()
		post_info['content'] = content
		post_info['comments'] = comments

		yield post_info