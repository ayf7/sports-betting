import os, sys

main_directory = os.path.dirname(__file__)
parent_directory = os.path.abspath(os.path.join(main_directory, '..'))
data_directory = os.path.join(parent_directory, 'data/')
scraper_directory = os.path.join(parent_directory, 'scrape/')

sys.path.append(parent_directory)
