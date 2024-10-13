import scrapy
import json
import os
import time
from datetime import datetime
from scrapy import signals

class FreeProxySpider(scrapy.Spider):
    name = "free_proxy"
    start_urls = [
        "https://www.freeproxy.world/?type=&anonymity=&country=&speed=&port=&page=1",
    ]

    token = "t_6221d24e"
    get_url = "https://test-rg8.ddns.net/api/get_token"
    post_url = "https://test-rg8.ddns.net/api/post_proxies"
    
    max_retries = 3  # Number of retries on 403 errors
    handle_httpstatus_list = [403, 429]

    results_file = 'results.json'
    time_file = 'time.txt'

    def __init__(self, *args, **kwargs):
        super(FreeProxySpider, self).__init__(*args, **kwargs)
        self.start_time = None
        self.end_time = None

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(FreeProxySpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        """Capture the start time when the spider opens."""
        self.start_time = datetime.now()
        self.log(f"Spider {spider.name} started at: {self.start_time}")

    def spider_closed(self, spider, reason):
        """Capture the finish time when the spider closes, and calculate the duration."""
        self.end_time = datetime.now()
        self.log(f"Spider {spider.name} finished at: {self.end_time}")

        # Calculate execution time
        time_taken = self.end_time - self.start_time
        time_taken_str = str(time_taken).split('.')[0]  # Format time in hh:mm:ss

        # Log execution time in the console
        self.log(f"Spider {spider.name} executed for: {time_taken_str}")

        # Save execution time to time.txt
        with open(self.time_file, 'w') as f:
            f.write(f"Start Time: {self.start_time}\n")
            f.write(f"End Time: {self.end_time}\n")
            f.write(f"Execution Time: {time_taken_str}\n")

    def parse(self, response):
        proxies = []
        for proxy in response.css("tr"):
            ip = proxy.css("td.show-ip-div::text").get()
            port = proxy.css("td a::text").re(r"^\d+$")
            if ip and port:
                proxies.append(f'{ip.strip()}:{port[0].strip()}')

        # If proxies exist, start uploading in batches of 10
        if proxies:
            for i in range(0, len(proxies), 10):
                proxy_batch = proxies[i:i + 10]  # Send proxies in batches of 10
                # print("BATCH: ", i)
                yield self.start_request(proxy_batch)

        # Pagination handling (parse next page)
        current_page = int(response.url.split("page=")[-1])
        if current_page < 5:
            next_page = current_page + 1
            next_page_url = response.url.replace(f"page={current_page}", f"page={next_page}")
            yield scrapy.Request(url=next_page_url, callback=self.parse)

    def start_request(self, proxy_batch):
        form_data = {
            'user_id': self.token,
            'len': len(proxy_batch),
            "proxies": ", ".join(proxy_batch)
        }

        return scrapy.Request(
            url=self.get_url,
            method='GET',
            callback=self.upload_proxies,
            meta={'post_data': form_data, 'retry_count': 0},  # Add retry count
            cookies={'user_id': self.token},
            dont_filter=True,
            # headers={'Access-Control-Allow-Origin': '*'}
        )

    def upload_proxies(self, response):
        data = response.meta['post_data']

        return scrapy.Request(
            url=self.post_url,
            method='POST',
            body=json.dumps(data),
            callback=self.upload_callback,
            meta={'proxies': data['proxies'].split(','), 'retry_count': response.meta['retry_count']},  # Add retry count
            headers={'Content-Type': 'application/json'},
            # handle_httpstatus_list=[403],
        )

    def upload_callback(self, response):
        retry_count = response.meta['retry_count']
        # print('RESPONSE: ', response.status)
        if response.status == 200:
            # Extract save_id from the page content
            json_response = response.json()
            save_id = json_response.get('save_id')
            # print("SAVE_ID: ", save_id)

            if save_id:
                proxies = response.meta['proxies']
                self.log(f"Proxies uploaded successfully with save_id: {save_id}")

                # Save to results.json
                self.save_results(save_id, proxies)
            else:
                self.log("Failed to extract save_id from the response.")

        elif (response.status == 403 or response.status == 429) and retry_count <= self.max_retries:
            # Retry logic for 403 error
            self.log(f"{response.status} error. Retrying... (Attempt {retry_count + 1})")
            time.sleep(5 * (retry_count + 1))  # Backoff
            retry_count += 1

            proxy_batch = response.meta['proxies']
            form_data = {
                'user_id': self.token,
                'len': len(proxy_batch),
                "proxies": ", ".join(proxy_batch)
            }

            # Refetch token and retry
            yield scrapy.Request(
                url=self.get_url,
                method='GET',
                callback=self.upload_proxies,
                meta={'post_data': form_data, 'retry_count': retry_count},
                dont_filter=True
            )
        else:
            self.log(f"Failed to upload proxies: {response.status} - {response.text}")

    def save_results(self, save_id, proxies):
        # Load existing results
        if os.path.exists(self.results_file):
            with open(self.results_file, 'r') as file:
                results = json.load(file)
        else:
            results = {}

        # Add new proxies with their save_id
        results[save_id] = proxies

        # Save back to results.json
        with open(self.results_file, 'w') as file:
            json.dump(results, file, indent=4)
