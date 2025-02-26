import os
import msgpack
from lstore.config import BUFFERPOOL_SIZE
from lstore.config import PAGE_SIZE
from lstore.table import Table, Record
from lstore.page_range import PageRange
from lstore.page import BasePage, TailPage
import datetime


class Database:
    def __init__(self):
        self.tables = []
        self.path = None
        self.bufferpool = None
        self.bufferpool_size = BUFFERPOOL_SIZE

    def open(self, path):
        """
        Opens the database from the specified path.
        If the database does not exist, it initializes an empty database.
        """
        self.path = path

        # Create the directory if it doesn't exist
        if not os.path.exists(path):
            os.makedirs(path)
            return

        self.bufferpool = Bufferpool(self.bufferpool_size, self.path)

        # Load the database metadata (list of tables) / USING MSG INSTEAD OF PICKLE
        metadata_path = os.path.join(path, "db_metadata.msg")
        if os.path.exists(metadata_path):
            with open(metadata_path, "rb") as f:
                table_metadata = msgpack.unpackb(f.read(), raw=False)

            # Reconstruct tables from metadata
            for table_info in table_metadata:
                name = table_info["name"]

                # Create a new table
                table = self.create_table(name, table_info["num_columns"], table_info["key"])
                
                # Load table data from disk if the directory exists
                table_path = os.path.join(path, name)
                if os.path.exists(table_path):
                    self.load_table_data(table, table_info)

    def close(self):
        """
        Saves the current state of the database to disk and closes it.
        """
        if not self.path:
            raise Exception("Database is not open")

        # Contains the metadata of each table in the database
        table_metadata = []
        for table in self.tables:
            table_info = {
                "name": table.name,
                "num_columns": table.num_columns,
                "key": table.key,
            }
            table_metadata.append(table_info)
            self.save_table_data(table)

        # Save table metadata / USING MSG INSTEAD OF PICKLE
        metadata_path = os.path.join(self.path, "db_metadata.msg")
        with open(metadata_path, "wb") as f:
            f.write(msgpack.packb(table_metadata, use_bin_type=True))

        # Clear in-memory state
        if self.bufferpool:
            self.bufferpool.reset()

        self.tables = []
        self.path = None
        self.bufferpool = None

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key):
        # Check if the table already exists
        for table in self.tables:
            if table.name == name:
                raise Exception(f"Table {name} already exists")

        # Create a new table and add it to the list of tables
        table = Table(name, num_columns, key)
        table = Table(name, num_columns, key, self.bufferpool)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        # Check if the table exists and delete it
        for i, table in enumerate(self.tables):
            if table.name == name:
                self.tables.pop(i)
                return

        raise Exception(f"Table {name} does not exist")

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        # Check if the table exists and return it
        for table in self.tables:
            if table.name == name:
                return table

        raise Exception(f"Table {name} does not exist")

    # Need to implement later
    def load_table_data(self, table, table_info):
        table_path = os.path.join(self.path, table.name)
        if not os.path.exists(table_path):
            os.makedirs(table_path)
            return

        table_meta_path = os.path.join(table_path, "tb_metadata.msg")
        if os.path.exists(table_meta_path):
            with open(table_meta_path, "rb") as f:
                metadata = msgpack.unpackb(f.read(), raw=False)
            table.num_columns = metadata["num_columns"]
            table.key = metadata["key"]
            page_range_count = metadata["page_range_count"]

        page_ranges_path = os.path.join(table_path, "page_ranges")
        for pr_index in range(page_range_count):
            pr_file = os.path.join(page_ranges_path, f"page_range_{pr_index}.msg")
            if os.path.exists(pr_file):
                with open(pr_file, "rb") as f:
                    pr_data = msgpack.unpackb(f.read(), raw=False)
                page_range = PageRange(table.num_columns)
                table.page_ranges.append(page_range)

                for base_data in pr_data["base_pages"]:
                    base_page = BasePage(table.num_columns)
                    base_page.indirection = base_data["indirection"]
                    base_page.schema_encoding = base_data["schema_encoding"]
                    base_page.start_time = base_data["start_time"]
                    base_page.rid = base_data["rid"]
                    base_page.num_records = len(base_page.rid)
                    page_range.base_pages.append(base_page)
                    page_range.num_base_pages += 1

                for tail_data in pr_data["tail_pages"]:
                    tail_page = TailPage(table.num_columns)
                    tail_page.indirection = tail_data["indirection"]
                    tail_page.schema_encoding = tail_data["schema_encoding"]
                    tail_page.start_time = tail_data["start_time"]
                    tail_page.rid = tail_data["rid"]
                    tail_page.num_records = len(tail_page.rid)
                    tail_page.tps = tail_data.get("tps", 0)
                    page_range.tail_pages.append(tail_page)
                    page_range.num_tail_pages += 1

        page_directory_path = os.path.join(table_path, "pg_directory.msg")
        if os.path.exists(page_directory_path):
            with open(page_directory_path, "rb") as f:
                pg_data = msgpack.unpackb(f.read(), raw=False)
            for rid_list, columns in zip(pg_data["rid"], pg_data["data"]):
                rid = tuple(rid_list)
                key = columns[table.key]
                table.page_directory[rid] = Record(rid, key, columns)
                table.index.insert(key, rid)

        

    # Need to implement later
    def save_table_data(self, table):
        table_path = os.path.join(self.path, table.name)
        page_ranges_path = os.path.join(table_path, "page_ranges")
        os.makedirs(page_ranges_path, exist_ok=True)

        metadata = {
            "name": table.name,
            "num_columns": table.num_columns,
            "key": table.key,
            "page_range_count": len(table.page_ranges),
        }
        with open(os.path.join(table_path, "tb_metadata.msg"), "wb") as f:
            f.write(msgpack.packb(metadata, use_bin_type=True))

        for pr_index, page_range in enumerate(table.page_ranges):
            pr_data = {"base_pages": [], "tail_pages": []}
            for bp_index, base_page in enumerate(page_range.base_pages):
                page_id = f"{table.name}_{pr_index}_base_{bp_index}"
                page_data = b''.join(col.data for col in base_page.pages)
                self.bufferpool.set_page(page_id, table.name, page_data)
                pr_data["base_pages"].append({
                    "indirection": base_page.indirection[:base_page.num_records],
                    "schema_encoding": base_page.schema_encoding[:base_page.num_records],
                    "start_time": base_page.start_time[:base_page.num_records],
                    "rid": base_page.rid[:base_page.num_records]
                })
            for tp_index, tail_page in enumerate(page_range.tail_pages):
                page_id = f"{table.name}_{pr_index}_tail_{tp_index}"
                page_data = b''.join(col.data for col in tail_page.pages)
                self.bufferpool.set_page(page_id, table.name, page_data)
                pr_data["tail_pages"].append({
                    "indirection": tail_page.indirection[:tail_page.num_records],
                    "schema_encoding": tail_page.schema_encoding[:tail_page.num_records],
                    "start_time": tail_page.start_time[:tail_page.num_records],
                    "rid": tail_page.rid[:tail_page.num_records],
                    "tps": tail_page.tps
                })
            with open(os.path.join(page_ranges_path, f"page_range_{pr_index}.msg"), "wb") as f:
                f.write(msgpack.packb(pr_data, use_bin_type=True))

        pg = {"rid": list(table.page_directory.keys()), "data": [r.columns for r in table.page_directory.values()]}
        with open(os.path.join(table_path, "pg_directory.msg"), "wb") as f:
            f.write(msgpack.packb(pg, use_bin_type=True))


class Bufferpool:
    def __init__(self, size, path):
        self.size = size # bufferpool size
        self.path = path # database path
        self.pages = {}  # page_id -> (page_data, is_dirty)
        self.page_paths = {}  # page_id -> disk_path
        self.pins = {} # page_id -> pins in place (int)
        self.access_times = {}  # page_id -> last_access_time
        self.access_counter = 0

    # Function for getting a page from the buffer pool and disk
    def get_page(self, page_id, table_name):
        # If page is in the bufferpool, access it and update access time and pin it
        if page_id in self.pages:
            self.access_counter += 1
            self.access_times[page_id] = self.access_counter
            self.pins[page_id] = self.pins.get(page_id, 0) + 1
            return self.pages[page_id][0]  # Return page_data from (page_data, is_dirty)

        # If the page is not in the bufferpool and we are at capacity, evict one page
        if len(self.pages) >= self.size:
            self.evict_page()

        # Construct the disk file path
        # Not too sure about this about the page path
        page_path = os.path.join(self.path, table_name, f"page_{page_id}.msg")
        self.page_paths[page_id] = page_path

        # Load the page from disk if it exists; otherwise create a new empty page
        if os.path.exists(page_path):
            with open(page_path, "rb") as f:
                page_data = f.read()
        else:
            page_data = bytearray(PAGE_SIZE)

        # Insert the new page into the bufferpool.
        self.pages[page_id] = (page_data, False)  # Set to not dirty
        self.pins[page_id] = 1                    # Page is pinned upon loading
        self.access_counter += 1
        self.access_times[page_id] = self.access_counter

        return page_data


    def set_page(self, page_id, table_name, page_data):
        # First check if page exists already in bufferpool. If true, update access_counter and pin it.

        if page_id in self.pages:
            self.pages[page_id] = (page_data, True)
            self.access_counter += 1
            self.access_times[page_id] = self.access_counter
            self.pins[page_id] = self.pins.get(page_id, 0) + 1
            return

        # If bufferpool is full, evict LRU page
        if len(self.pages) >= self.size:
            self.evict_page()

        page_path = os.path.join(self.path, table_name, f"{page_id}.msg")
        self.page_paths[page_id] = page_path
        self.pages[page_id] = (page_data, True)
        self.pins[page_id] = 1
        self.access_counter += 1
        self.access_times[page_id] = self.access_counter

    # Function for unpinning a page after done with it
    def unpin_page(self, page_id):
        # Decrement the pin count
        # Can only evict if pin count == 0
        if page_id in self.pins and self.pins[page_id] > 0:
            self.pins[page_id] -= 1

    # Function for evicting a page from the bufferpool using LRU
    def evict_page(self):
        # Using LRU policy
        # Checks if dirty, if True write to disk first
        # Checks if page is pinned, if not remove from bufferpool
        lru_page = None
        oldest = None
        for pid, access in self.access_times.items():
            if self.pins.get(pid, 0) == 0:
                if oldest is None or access < oldest:
                    oldest = access
                    lru_page = pid

        if lru_page is None:
            raise Exception("No unpinned page available for eviction.")

        # If the evict page is dirty, write it to disk
        page_data, is_dirty = self.pages[lru_page]
        if is_dirty:
            self.write_dirty(lru_page, page_data)

        # Remove the evict page from the bufferpool
        del self.pages[lru_page]
        del self.page_paths[lru_page]
        del self.pins[lru_page]
        del self.access_times[lru_page]

    # Evicts all the pages from a table
    # Call this in drop_table()
    def evict_table(self, table_name):
        evicts = [pid for pid, path in self.page_paths.items() if table_name in path]
        for pid in evicts:
            # Only evict pages that are not currently pinned
            if self.pins.get(pid, 0) == 0:
                page_data, is_dirty = self.pages[pid]
                if is_dirty:
                    self.write_dirty(pid, page_data)
                del self.pages[pid]
                del self.page_paths[pid]
                del self.pins[pid]
                del self.access_times[pid]

    # Writes a dirty page back into disk
    def write_dirty(self, page_id, page_data):
        if page_id in self.page_paths:
            path = self.page_paths[page_id]
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(page_data)
            # Mark the page as clean (not dirty)
            if page_id in self.pages:
                self.pages[page_id] = (page_data, False)

    # Write all dirty pages back into disk and evict all pages (flush)
    def reset(self):
        for pid in list(self.pages.keys()):
            page_data, is_dirty = self.pages[pid]
            if is_dirty:
                self.write_dirty(pid, page_data)
        self.pages.clear()
        self.page_paths.clear()
        self.pins.clear()
        self.access_times.clear()
        self.access_counter = 0
