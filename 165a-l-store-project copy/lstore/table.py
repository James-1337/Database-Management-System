from lstore.index import Index
from lstore.page_range import PageRange
from lstore.page import BasePage, LogicalPage
from lstore.config import MERGE_THRESHOLD, PAGE_SIZE, RECORDS_PER_PAGE, DATA_SIZE
import threading
import datetime


INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    def __init__(self, name, num_columns, key, bufferpool=None):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.bufferpool = bufferpool
        self.page_directory = {}
        self.index = Index(self)
        self.page_ranges = []
        self.merge_counter = 0
        self.lock = threading.Lock()
        
        # Initialize the first page range
        self.add_page_range(num_columns)
    
    def find_current_base_page(self):
        # Find the first base page with capacity
        for page_range in self.page_ranges:
            for base_page in page_range.base_pages:
                if base_page.has_capacity():
                    return page_range, base_page

        # If no base page has capacity, create a new base page
        if not self.page_ranges[-1].has_capacity():
            self.add_page_range(self.num_columns)
        self.page_ranges[-1].add_base_page(self.num_columns)
        
        return self.page_ranges[-1], self.page_ranges[-1].base_pages[-1]

    def create_rid(self):
        # Get the current base page
        page_range, base_page = self.find_current_base_page()
        
        # Create a new rid for a base page record
        rid = (self.page_ranges.index(page_range), page_range.base_pages.index(base_page), base_page.num_records, 'b')
        base_page.rid.append(rid)  # Ensure the rid is appended to the list
        
        return rid

    def find_record(self, key, rid, projected_columns_index):
        page_type = 'base' if rid[3] == 'b' else 'tail'
        page_id = f"{self.name}_{rid[0]}_{page_type}_{rid[1]}"
        page_data = self.bufferpool.get_page(page_id, self.name)
        self.bufferpool.pins[page_id] = self.bufferpool.pins.get(page_id, 0) + 1

        pages = []
        bytes_per_column = PAGE_SIZE
        for i in range(self.num_columns):
            lp = LogicalPage()
            start = i * bytes_per_column
            end = (i + 1) * bytes_per_column
            lp.data = bytearray(page_data[start:end])
            lp.num_records = min(RECORDS_PER_PAGE, len(page_data[start:end]) // DATA_SIZE)
            pages.append(lp)

        record = []
        for i, projected in enumerate(projected_columns_index):
            if projected == 1:
                value = int.from_bytes(pages[i].data[rid[2] * 8:(rid[2] + 1) * 8], 'big')
                record.append(value)

        self.bufferpool.unpin_page(page_id)
        return Record(rid, key, record)


    def find_current_base_page(self):
        # Find the first base page with capacity
        for page_range in self.page_ranges:
            for base_page in page_range.base_pages:
                if base_page.has_capacity():
                    return page_range, base_page

        # If no base page has capacity, create a new base page
        if not self.page_ranges[-1].has_capacity():
            self.add_page_range(self.num_columns)
        self.page_ranges[-1].add_base_page(self.num_columns)
        
        return self.page_ranges[-1], self.page_ranges[-1].base_pages[-1]

    def insert_record(self, start_time, schema_encoding, *columns):
        page_range, base_page = self.find_current_base_page()
        rid = self.create_rid()
        indirection = rid
        schema_encoding = [0] * self.num_columns
        base_page.insert_base_page_record(rid, start_time, schema_encoding, indirection, *columns)
        self.page_directory[rid] = Record(rid, columns[self.key], columns)
        key = columns[0]
        self.index.insert(key, rid)
        page_id = f"{self.name}_{self.page_ranges.index(page_range)}_base_{page_range.base_pages.index(base_page)}"
        page_data = b''.join(col.data for col in base_page.pages)
        self.bufferpool.set_page(page_id, self.name, page_data)
        return True

    def update(self, primary_key, *columns):
        rid = self.index.locate(self.key, primary_key)
        if not rid:
            return False
        rid = rid[0]

        if columns[self.key] is not None and columns[self.key] != primary_key:
            if self.index.locate(self.key, columns[self.key]):
                return False

        page_range_index, page_index, record_index, _ = rid
        if page_range_index >= len(self.page_ranges):
            print(f"Invalid page_range_index: {page_range_index}")
            return False
        page_range = self.page_ranges[page_range_index]
        if page_index >= len(page_range.base_pages):
            print(f"Invalid page_index: {page_index}")
            return False
        base_page = page_range.base_pages[page_index]
        if record_index >= len(base_page.indirection):
            print(f"Invalid record_index: {record_index}")
            return False

        current_rid = base_page.indirection[record_index]
        record = self.find_record(primary_key, current_rid, [1] * self.num_columns)

        if not page_range.tail_pages or not page_range.tail_pages[-1].has_capacity():
            page_range.add_tail_page(self.num_columns)

        current_tp = len(page_range.tail_pages) - 1
        tail_page = page_range.tail_pages[current_tp]
        start_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        tail_page.insert_tail_page_record(*columns, record=record)
        tail_page.start_time.append(start_time)
        tail_page.indirection.append(rid)
        new_record_index = tail_page.num_records - 1
        update_rid = (page_range_index, current_tp, new_record_index, 't')
        tail_page.rid.append(update_rid)
        base_page.indirection[record_index] = update_rid

        # Update schema_encoding as integers
        for i in range(self.num_columns):
            if tail_page.schema_encoding[new_record_index][i] == 1:
                base_page.schema_encoding[record_index][i] = 1

        base_page_id = f"{self.name}_{page_range_index}_base_{page_index}"
        tail_page_id = f"{self.name}_{page_range_index}_tail_{current_tp}"
        base_data = b''.join(col.data for col in base_page.pages)
        tail_data = b''.join(col.data for col in tail_page.pages)
        self.bufferpool.set_page(base_page_id, self.name, base_data)
        self.bufferpool.set_page(tail_page_id, self.name, tail_data)
        return True
    
    def add_page_range(self, num_columns):
        page_range = PageRange(num_columns)
        self.page_ranges.append(page_range)

    def trigger_merge(self):
        # print("<----triggering merge---->")
        merge_thread = threading.Thread(target=self.merge)
        merge_thread.start()

    def merge(self):
        with self.lock:
            print("<----merging---->")
            for page_range in self.page_ranges:
                merged_base_pages = []
                for base_page in page_range.base_pages:
                    # Create a copy of the base page
                    merged_base_page = BasePage(self.num_columns)
                    merged_base_page.pages = [LogicalPage() for _ in range(self.num_columns)]
                    
                    # Copy the base page records to the merged base page
                    for i in range(base_page.num_records):
                        for j in range(self.num_columns):
                            value = base_page.pages[j].read(i, 1)[0]
                            merged_base_page.pages[j].write(value)
                        merged_base_page.indirection.append(base_page.indirection[i])
                        merged_base_page.schema_encoding.append(base_page.schema_encoding[i])
                        merged_base_page.start_time.append(base_page.start_time[i])
                        merged_base_page.rid.append(base_page.rid[i])
                    
                    # Track the latest updates for each base record
                    latest_updates = {}  # base_rid -> (tail_page_index, record_index)
                    for tail_page_index, tail_page in enumerate(reversed(page_range.tail_pages)):
                        for record_index in range(tail_page.num_records):
                            base_rid = tail_page.indirection[record_index]
                            if base_rid in base_page.rid:
                                latest_updates[base_rid] = (len(page_range.tail_pages) - 1 - tail_page_index, record_index)
                    
                    # Apply only the most recent updates
                    updated_columns = {base_rid: set() for base_rid in base_page.rid}
                    for base_rid, (tail_page_index, record_index) in latest_updates.items():
                        tail_page = page_range.tail_pages[tail_page_index]
                        base_index = base_page.rid.index(base_rid)
                        for j in range(self.num_columns):
                            if record_index < len(tail_page.schema_encoding) and tail_page.schema_encoding[record_index][j] == '1' and j not in updated_columns[base_rid]:
                                value = tail_page.pages[j].read(record_index, 1)[0]
                                merged_base_page.pages[j].write(value)
                                updated_columns[base_rid].add(j)
                    
                    # Traverse the lineage to incorporate all recent updates
                    for i in range(merged_base_page.num_records):
                        current_rid = merged_base_page.indirection[i]
                        while current_rid[3] == 't':
                            tail_page = page_range.tail_pages[current_rid[1]]
                            record_index = current_rid[2]
                            for j in range(self.num_columns):
                                if record_index < len(tail_page.schema_encoding) and tail_page.schema_encoding[record_index][j] == '1' and j not in updated_columns[merged_base_page.rid[i]]:
                                    value = tail_page.pages[j].read(record_index, 1)[0]
                                    merged_base_page.pages[j].write(value)
                                    updated_columns[merged_base_page.rid[i]].add(j)
                                current_rid = tail_page.indirection[record_index]
                    
                    # Create new keys for the merged records
                    new_keys = []
                    for i in range(merged_base_page.num_records):
                        new_key = max(self.page_directory.keys()) + 1
                        new_keys.append(new_key)
                        self.page_directory[new_key] = self.page_directory[merged_base_page.rid[i]]
                        self.page_directory[new_key].key = new_key
                    
                    # Update TPS for the merged base page
                    merged_base_page.tps = max(tail_page.tps for tail_page in page_range.tail_pages)
                    
                    # Add the merged base page to the list
                    merged_base_pages.append(merged_base_page)
                
                # Replace old base pages with merged base pages
                page_range.base_pages = merged_base_pages
                page_range.num_base_pages = len(merged_base_pages)
            
            print("<----merging complete---->")