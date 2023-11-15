# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import sqlite3

import cloudinary
import cloudinary.uploader
from itemadapter import ItemAdapter


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


class BookScraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        field_names = adapter.field_names()

        for field_name in field_names:
            if field_name != 'description':
                value = adapter.get(field_name)
                adapter[field_name] = value.strip()

        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£', '')
            adapter[price_key] = float(value)

        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability'] = 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['availability'] = int(availability_array[0])

        num_reviews_string = adapter.get('num_reviews')
        adapter['num_reviews'] = int(num_reviews_string)

        stars_string = adapter.get('stars')
        split_stars_array = stars_string.split(' ')
        stars_text_value = split_stars_array[1].lower()
        if stars_text_value == 'zero':
            adapter['stars'] = 0
        elif stars_text_value == 'one':
            adapter['stars'] = 0
        elif stars_text_value == 'two':
            adapter['stars'] = 0
        elif stars_text_value == 'three':
            adapter['stars'] = 0
        elif stars_text_value == 'four':
            adapter['stars'] = 0
        elif stars_text_value == 'five':
            adapter['stars'] = 0

        return item
