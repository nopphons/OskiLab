from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from course_scraper.items import CourseInfoItem

class UCBCourseSpider(Spider):
    name = "ucbcourses"
    allowed_domains = ["bulletin.berkeley.edu"]
    start_urls = [
        "http://bulletin.berkeley.edu/courses/"
    ]

    def parse(self, response):
       """ Parse is initially called by default on the response(s) from start_urls """
       sel = Selector(response)
       majors = sel.xpath('body/div[2]/div[2]/main/div/div/div[2]/ul/li/a/@href')
       for major in majors:
           yield Request(response.url+major.extract().encode('utf-8').replace('/courses/',''),
                         callback=self.parse_course) #'/courses/aerospc/'

    def parse_course(self, response):
        """ Called on response object as result of resolved Request from parse"""
        info = CourseInfoItem()
        sel = Selector(response)
         #Assuming on course page i.e http://bulletin.berkeley.edu/courses/aerospc/
        courses = sel.xpath('body/div[2]/div[2]/main/div/div[2]/div/div/p/a')
        info['department'] = sel.xpath('//h1/text()').extract()[0].encode('utf-8')
        for course in courses:
            #'AEROSPC\xc2\xa01A' 
            info['course_code'] = course.xpath('span[1]/text()').extract()[0].encode('utf-8').replace('\xc2\xa0',' ')  
            # Foundations of the U.S. Air Force
            info['course_name'] = course.xpath('span[2]/text()').extract()[0].encode('utf-8') 
             # '1 Unit'
            info['units'] = course.xpath('span[3]/text()').extract()[0].encode('utf-8').rstrip(' Units')
            yield info
            
            
