# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
from urllib.parse import urlparse

import scrapy
from scrapy.exceptions import DropItem
from scrapy.pipelines.images import ImagesPipeline


class StoryCrawlerPipeline:
    def process_item(self, item, spider):
        return item


class CustomImagesPipeline(ImagesPipeline):

    def file_path(self, request, response=None, info=None, *, item=None):
        path = urlparse(request.url).path
        return os.path.basename(path)

    def get_media_requests(self, item, info):
        for image_url in item['image_urls']:
            if any(extension in image_url.lower() for extension in ['.jpg', '.jpeg', '.png']):
                yield scrapy.Request(image_url)
            else:
                raise DropItem(f'Dropping non-jpg/png image: {image_url}')
