from scrapling.spiders import Spider, Request, Response

class AdondeVivirSpider(Spider):
    name = "sercoplus"
    start_urls = ["https://sercoplus.com/731-arma-tu-pc"]
    concurrent_requests = 10

    async def parse_product(self, response: Response):
        yield {
            "name": response.css("h1::text").get(""),
            "price": response.css(".price::text").get(""),
        }
    
    async def parse(self, response: Response):
        for product in response.css('.product-miniature'):
            yield {
                "title": product.css('.product-title h6::text').get(),
                "price": product.css('.price::text').get(),
            }
            
        next_page = response.css('a.next')
        if next_page:
            yield response.follow(next_page[0].attrib['href'])

result = AdondeVivirSpider().start()
print(f"Scraped {len(result.items)} products")
result.items.to_json("sercoplus_products.json")
