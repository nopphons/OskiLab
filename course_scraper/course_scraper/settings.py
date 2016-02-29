# Scrapy settings for class_scraper project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#
from scrapy.log import ERROR
BOT_NAME = 'course_scraper'

SPIDER_MODULES = ['course_scraper.spiders']
NEWSPIDER_MODULE = 'course_scraper.spiders'
ITEM_PIPELINES = {
    'course_scraper.pipelines.CleanUnicodePipeline': 1, 
    'course_scraper.pipelines.WritePipeline': 2, 
}

LOG_LEVEL = ERROR

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'class_scraper (+http://www.yourdomain.com)'
