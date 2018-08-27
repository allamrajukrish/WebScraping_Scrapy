# -*- coding: utf-8 -*-

import scrapy
import re
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.exceptions import CloseSpider
from pymongo import MongoClient
import csv

# https://www.yellowpages.com/search?search_terms=digital%20marketing%20agencies&geo_location_terms=New%20York%2C%20NY&page=5
Search_str = "digital%20marketing%20agencies"
Location_str = "New+York%2C+NY"
# Location_str = ""
URL = "http://www.yellowpages.com/search?search_terms=%s&geo_location_terms=%s&page=%d"
Locations_List = []


class YellowpagesSpider(scrapy.Spider):
    name = 'yellowpages'
    allowed_domains = ['www.yellowpages.com']
    # start_urls = []
    Locations_List = []

    # self.Locations_List=[]
    connection = MongoClient('localhost', 27017)
    # you can add db-url and port as parameter to MongoClient(), localhost by default
    db = connection['YelloPages']
    collection = db['Cities']

    cities_list = collection.find()
    '''use appropriate finding criteria here according to the structure of data resides in that collection'''
    my_List = list(cities_list)
    # my_List consists of list of dictionaries and each dictionary consists of id and city name

    for mydict in my_List:
        print(mydict)
        # Here extracting only city name and adding the city name to a list
        Locations_List.append(mydict['name'])

    print(Locations_List)

    start_urls = [URL % (Search_str, Locations_List[0], 1)]

    def __init__(self):
        self.page_number = 1
        self.city_index = 0

    def parse(self, response):

        root_path = response.xpath('//div[@class="search-results organic"]//div[@class="v-card"]')

        sel = Selector(response)
        base_path = sel.xpath('//div[@class="search-results organic"]//div[@class="v-card"]')
        if not base_path:
            raise CloseSpider('No more pages')

        for item in root_path:

            business_name = item.xpath('.//a[@class="business-name"]//text()').extract_first()
            street_address = item.xpath('.//*[@class="street-address"]/text()').extract_first()
            address_locality = ""
            try:
                address_locality = item.xpath('.//span[@itemprop="addressLocality"]/text()').extract_first().replace(
                    ',\xa0', '').strip()
            except:
                address_locality = ""
            address_region = item.xpath('.//span[@itemprop="addressRegion"]/text()').extract_first()
            zipcode = item.xpath('.//span[@itemprop="postalCode"]/text()').extract_first()
            phoneno = item.xpath('.//*[@class="phones phone primary"]/text()').extract_first()
            #categories = []
            categories = item.xpath('.//*[@class="categories"]/a/text()').extract()
            website = item.xpath('.//*[@class="links"]/a/@href').extract_first()
            rating_count = ''
            rating_total = item.xpath(
                './/div[contains(@class,"info-section")]//div[contains(@class,"result-rating")]//span//text()').extract_first()
            if rating_total:
                try:
                    rating_total = re.findall(r'\d+', rating_total)[0]
                    rating_count = int(rating_total)
                except ValueError as e:
                    print('Error converting rating value to int: {}'.format(rating_total))

            rating = item.xpath(
                './/div[contains(@class,"info-section")]//div[contains(@class,"result-rating")]').extract_first()

            nums = {
                'one': 1,
                'two': 2,
                'three': 3,
                'four': 4,
                'five': 5,
            }

            # this feels particularly delicate and heavy, i'd probably move
            # into a result parser module or something
            rating_avg = ''
            if rating:
                rating = rating.split(' ')
                for rating_word in rating:
                    if rating_word in nums.keys():
                        rating_avg = nums[rating_word]
                if 'half' in rating:
                    rating_avg += 0.5

            MyDict = {'Name': business_name, 'Street_adr': street_address, 'Locality': address_locality,
                      'Region': address_region, 'Zip_code': zipcode, 'Phone': phoneno, 'Categories': categories,
                      'Website': website, 'Rating_Avg': rating_avg, 'Rating_Count': rating_count}
            # Final_Result.append(MyDict)
            yield MyDict

        self.page_number += 1
        if self.page_number <= 1:
            yield Request(URL % (Search_str, self.Locations_List[self.city_index], self.page_number))
            # for Location_str in self.cities_list:
            # start_urls = [URL % (Search_str, Location_str, 1)]

        else:
            self.page_number = 1
            self.city_index += 1
            if (self.city_index in range(len(self.Locations_List))):
                yield Request(URL % (Search_str, self.Locations_List[self.city_index], self.page_number))
            else:
                raise CloseSpider('No more pages')
