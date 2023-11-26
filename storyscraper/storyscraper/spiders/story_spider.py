import scrapy

from story_crawler.items import StoryCrawlerItem


class StorySpider(scrapy.Spider):
    name = 'story'
    allowed_domains = ['truyenfull.vn']
    start_urls = ['https://truyenfull.vn/']
    custom_settings = {
        'ITEM_PIPELINES': {
            'scrapy.pipelines.images.ImagesPipeline': 1,
        }
    }

    def parse(self, response):
        item = StoryCrawlerItem()
        item['image_urls'] = [response.urljoin(url) for url in response.css('img::attr(src)').extract()]
        yield item
