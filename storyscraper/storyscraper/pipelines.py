# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import sqlite3

import cloudinary
import cloudinary.uploader


class StoryCrawlerPipeline:
    def process_item(self, item, spider):
        return item


class DatabasePipeline:
    def open_spider(self, spider):
        self.conn = sqlite3.connect('data.sqlite3')
        self.create_tables()

    def create_tables(self):
        # ... execute your SQL commands to create tables ...
        pass

    def process_item(self, item, spider):
        # This pipeline does not modify the item, so simply return it.
        return item

    def close_spider(self, spider):
        self.conn.close()


class CloudinaryPipeline:
    def open_spider(self, spider):
        cloudinary.config(
            cloud_name="ezcode97",
            api_key="282633384362776",
            api_secret="_lKNowTjkgcktrr0SYansQU3x9w"
        )

    def process_item(self, item, spider):
        cover_url = item['cover_url']
        upload_result = cloudinary.uploader.upload(cover_url)
        item['cover_photo'] = upload_result['url']
        return item


class DataStoragePipeline:
    def open_spider(self, spider):
        self.conn = sqlite3.connect('data.sqlite3')
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        # ... execute your SQL commands to insert data into the database ...
        return item

    def close_spider(self, spider):
        self.conn.close()
