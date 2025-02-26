from lstore.config import MAX_BASE_PAGES
from lstore.page import BasePage, TailPage

class PageRange:
    def __init__(self, num_cols):
        # Initialize the page
        # Store the base and tail pages
        self.base_pages = []
        self.tail_pages = []
        
        # Store the count of the base and tail pages
        self.num_base_pages = 0
        self.num_tail_pages = 0
        
        # Store the index of the beginning of the page range
        self.rid_index = 0
       
    def has_capacity(self):
        # Check if the page range has capacity for more base pages
        return self.num_base_pages < MAX_BASE_PAGES
    
    def create_page_range(self, num_records):
        # When creating the page range for the first time, store the rid of the beginning of the page range
        self.rid_index = num_records + 1
        
    def add_base_page(self, num_cols):
        # Check if the page range has capacity for more base pages
        if self.has_capacity():
            self.base_pages.append(BasePage(num_cols))
            self.num_base_pages += 1
            
    def add_tail_page(self, num_cols):
        # Add a tail page to the page range
        self.tail_pages.append(TailPage(num_cols))
        self.num_tail_pages += 1