import scrapy


class TikiSpider(scrapy.Spider):
    name = "tiki_spider"

    def start_requests(self):
        urls = ["https://tiki.vn/dien-thoai-may-tinh-bang"]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        page = response.url.split("/")[-2]
        filename = f'tiki-{page}.html'
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log(f'Saved file {filename}')