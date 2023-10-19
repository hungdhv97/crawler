import scrapy

from story_crawler.items import StoryCrawlerItem


class ImagesSpider(scrapy.Spider):
    name = 'images_spider'
    allowed_domains = ['truyenfull.vn']
    start_urls = ['https://truyenfull.vn/']

    def parse(self, response):
        item = StoryCrawlerItem()
        item['image_urls'] = [response.urljoin(url) for url in response.css('img::attr(src)').extract()]
        yield item
