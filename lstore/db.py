import os
import msgpack
from lstore.config import BUFFERPOOL_SIZE, MAX_BASE_PAGES, RECORDS_PER_PAGE, DEFAULT_DB_PATH
from lstore.table import Table, Record
from threading import RLock



class Database:
    def __init__(self):
        self.tables = []
        self.path = DEFAULT_DB_PATH
        self.bufferpool = None
        self.bufferpool_size = BUFFERPOOL_SIZE
        self.lock_manager = LockManager()
        self.open(DEFAULT_DB_PATH)
        #self.create_grades_table()



    def open(self, path):
        """
        Opens the database at the specified path.
        """
        self.path = path

        # Create the directory if it doesn't exist
        if not os.path.exists(path):
            os.makedirs(path)

        # Initialize the bufferpool
        self.bufferpool = Bufferpool(self.bufferpool_size, self.path)

        # Load database metadata if it exists
        metadata_path = os.path.join(path, "db_metadata.msg")
        if os.path.exists(metadata_path):
            with open(metadata_path, "rb") as f:
                table_metadata = msgpack.unpackb(f.read(), raw=False)

            # Recreate tables from metadata
            for table_info in table_metadata:
                name = table_info["name"]
                table = self.create_table(
                    name, table_info["num_columns"], table_info["key"]
                )

                # Load table data if the directory exists
                table_path = os.path.join(path, name)
                if os.path.exists(table_path):
                    self.load_table_data(table, table_info)

    def close(self):
        """
        Saves the current state of the database to disk and closes it.
        """
        if not self.path:
            raise Exception("Database is not open")

        # Save table metadata and data
        table_metadata = []
        for table in self.tables:
            table_info = {
                "name": table.name,
                "num_columns": table.num_columns,
                "key": table.key,
            }
            table_metadata.append(table_info)
            self.save_table_data(table)

        # Save database metadata
        metadata_path = os.path.join(self.path, "db_metadata.msg")
        with open(metadata_path, "wb") as f:
            f.write(msgpack.packb(table_metadata, use_bin_type=True))

        # Flush all bufferpool pages to disk
        if self.bufferpool:
            self.bufferpool.reset()

        # Clear in-memory state
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
        # Check if the table already existsdef create_table(self, name, num_columns, key):
        """
        Creates a new table with a reference to the database.
        """
        # Check if the table already exists
        for table in self.tables:
            if table.name == name:
                raise Exception(f"Table {name} already exists")

        # Create a new table
        table = Table(name, num_columns, key)

        # Give the table a reference to this database
        table.database = self

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

        # Load table metadata
        table_meta_path = os.path.join(table_path, "tb_metadata.msg")
        if os.path.exists(table_meta_path):
            with open(table_meta_path, "rb") as f:
                metadata = msgpack.unpackb(f.read(), raw=False)
            table.num_columns = metadata["num_columns"]
            table.key = metadata["key"]
        else:
            return

        # Calculate number of page ranges
        num_pages = metadata.get("num_pages", 0)
        page_range_count = (num_pages + MAX_BASE_PAGES - 1) // MAX_BASE_PAGES

        # Initialize page ranges and load base page metadata
        for pr_idx in range(page_range_count):
            table.add_page_range(table.num_columns)
            page_range = table.page_ranges[pr_idx]

            # Load base page metadata
            base_idx = 0
            while True:
                page_id = ("base", pr_idx, base_idx)
                page_path = os.path.join(table_path, f"base_{pr_idx}_{base_idx}.msg")
                if not os.path.exists(page_path):
                    break
                page_range.add_base_page(table.num_columns)
                base_page = page_range.base_pages[base_idx]
                page_data = self.bufferpool.get_page(page_id, table.name, table.num_columns)
                # Pre-load metadata, not columns
                base_page.indirection = page_data.get("indirection", [])
                base_page.rid = page_data.get("rid", [None] * RECORDS_PER_PAGE)
                base_page.start_time = page_data.get("timestamp", [])
                base_page.schema_encoding = page_data.get("schema_encoding", [])
                base_page.num_records = len(page_data["columns"][0]) if "columns" in page_data and page_data["columns"] else 0
                self.bufferpool.unpin_page(page_id, table.name)
                base_idx += 1

            # Initialize tail pages as empty (load lazily)
            tail_idx = 0
            while os.path.exists(os.path.join(table_path, f"tail_{pr_idx}_{tail_idx}.msg")):
                page_range.add_tail_page(table.num_columns)
                tail_idx += 1

        # Create indices for all columns
        for x in range(table.num_columns):
            table.index.create_index(x)

        # Load Page Directory and rebuild them
        page_directory_path = os.path.join(table_path, "pg_directory.msg")
        if os.path.exists(page_directory_path):
            with open(page_directory_path, "rb") as f:
                pg_data = msgpack.unpackb(f.read(), raw=False)
            for rid_str, columns in zip(pg_data["rid"], pg_data["data"]):
                rid = tuple(rid_str)
                key = columns[table.key]
                record = Record(rid, key, columns)
                table.page_directory[rid] = record
                table.index.insert(key, rid)

    # Need to implement later
    def save_table_data(self, table):
        table_path = os.path.join(self.path, table.name)
        os.makedirs(table_path, exist_ok=True)

        metadata = {
            "name": table.name,
            "num_columns": table.num_columns,
            "key": table.key,
            "num_pages": sum(
                len(pr.base_pages) + len(pr.tail_pages) for pr in table.page_ranges
            ),
        }

        # Save table metadata
        with open(os.path.join(table_path, "tb_metadata.msg"), "wb") as f:
            f.write(msgpack.packb(metadata, use_bin_type=True))

        # Save each page separately
        for pr_idx, page_range in enumerate(table.page_ranges):
            for page_idx, page in enumerate(page_range.base_pages):
                page_id = f"base_{pr_idx}_{page_idx}"
                self.save_page(table, page, page_id)

            for page_idx, page in enumerate(page_range.tail_pages):
                page_id = f"tail_{pr_idx}_{page_idx}"
                self.save_page(table, page, page_id)

        # Save page directory separately
        pg_directory = {
            "rid": list(table.page_directory.keys()),
            "data": [record.columns for record in table.page_directory.values()],
        }
        with open(os.path.join(table_path, "pg_directory.msg"), "wb") as f:
            f.write(msgpack.packb(pg_directory, use_bin_type=True))

    def save_page(self, table, page, page_id):
        """Helper function to save a single page."""
        table_path = os.path.join(self.path, table.name)
        page_path = os.path.join(table_path, f"{page_id}.msg")

        page_data = {
            "columns": [col.read(0, col.num_records) for col in page.pages],
            "indirection": page.indirection,
            "rid": page.rid,
            "timestamp": page.start_time,
            "schema_encoding": page.schema_encoding,
            "tps": getattr(page, "tps", None),
        }

        with open(page_path, "wb") as f:
            f.write(msgpack.packb(page_data, use_bin_type=True))


class Bufferpool:
    def __init__(self, size, path):
        self.size = size  # maximum number of pages in memory
        self.path = path  # database path
        self.pages = {}  # page_id -> (page_data, is_dirty)
        self.page_paths = {}  # page_id -> disk_path
        self.pins = {}  # page_id -> pin count
        self.access_times = {}  # page_id -> last_access_time
        self.access_counter = 0  # counter for tracking access order
        self.lock = RLock()

    def get_page(self, page_id, table_name, num_columns=None):
        """
        Get a page from the bufferpool. If not in memory, load from disk.
        Returns the page data and pins the page.
        """
        composite_key = (table_name, page_id)
        with self.lock:
            # If page is in bufferpool, access it and update access time
            if composite_key in self.pages:
                self.access_counter += 1
                self.access_times[composite_key] = self.access_counter
                self.pins[composite_key] = self.pins.get(composite_key, 0) + 1
                return self.pages[composite_key][0]  # Return page_data

        # Construct the disk file path
        page_path = self._construct_page_path(table_name, page_id)
        with self.lock:
            self.page_paths[composite_key] = page_path

        # Load page from disk if it exists, otherwise create empty page
        if os.path.exists(page_path):
            try:
                with open(page_path, "rb") as f:
                    page_data = msgpack.unpackb(f.read(), raw=False)
            except Exception as e:
                print(f"Error reading page from disk: {e}")
                page_data = self._create_empty_page(num_columns)
        else:
            page_data = self._create_empty_page(num_columns)
            
        with self.lock:
            # If bufferpool is full, evict pages until space is available
            while len(self.pages) >= self.size:
                self.evict_page()
            # Insert the page into the bufferpool
            self.pages[composite_key] = (page_data, False)  # Not dirty initially
            self.pins[composite_key] = 1  # Pin on load
            self.access_counter += 1
            self.access_times[composite_key] = self.access_counter

        return page_data

    def set_page(self, page_id, table_name, page_data, num_columns=None):
        """
        Update or insert a page in the bufferpool and mark it as dirty.
        """
        composite_key = (table_name, page_id)
        
        # If bufferpool is full, evict pages until space is available
        with self.lock:
            while len(self.pages) >= self.size:
                try:
                    self.evict_page()
                except Exception:
                    # If no pages can be evicted, forcibly evict a page
                    if self.pages:
                        # Find the page with the lowest pin count (even if it's 1)
                        min_pin_page = min(self.pins, key=self.pins.get)
                        self.write_dirty(min_pin_page, self.pages[min_pin_page][0])
                        del self.pages[min_pin_page]
                        del self.page_paths[min_pin_page]
                        del self.pins[min_pin_page]
                        del self.access_times[min_pin_page]
                    else:
                        raise Exception("Cannot evict any pages from bufferpool")

        # Construct page path and store it
        page_path = self._construct_page_path(table_name, page_id)
        self.page_paths[composite_key] = page_path

        # Add page to bufferpool
        self.pages[composite_key] = (page_data, True)  # Mark as dirty
        self.pins[composite_key] = 1
        self.access_counter += 1
        self.access_times[composite_key] = self.access_counter

    def evict_page(self):
        """
        Evict the least recently used unpinned page.
        If the page is dirty, write it to disk first.
        """
        # Find the least recently used unpinned page
        with self.lock:
            comp_key_evict = None
            oldest_access = float("inf")

            for comp_key, access_time in list(self.access_times.items()):
                if self.pins.get(comp_key, 0) == 0 and access_time < oldest_access:
                    oldest_access = access_time
                    comp_key_evict = comp_key

            if comp_key_evict is None:
                raise Exception("No unpinned page available for eviction.")

            # If the page is dirty, write it to disk
            page_data, is_dirty = self.pages[comp_key_evict]
            if is_dirty:
                self.write_dirty(comp_key_evict, page_data)

            # Remove the page from the bufferpool
            del self.pages[comp_key_evict]
            del self.page_paths[comp_key_evict]
            del self.pins[comp_key_evict]
            del self.access_times[comp_key_evict]

    def unpin_page(self, page_id, table_name = None):
        """
        Decrement the pin count for a page.
        The page can be evicted only when pin count is 0.
        """
        # If there is a table_name, use composite key, otherwise we just use page_id
        composite_key = (table_name, page_id) if table_name is not None else page_id
        with self.lock:
            if composite_key in self.pins and self.pins[composite_key] > 0:
                self.pins[composite_key] -= 1

    def _create_empty_page(self, num_columns):
        """Create an empty page data structure with the expected format."""
        return {
            "columns": [[] for _ in range(num_columns)] if num_columns else [],
            "indirection": [],
            "rid": [],
            "timestamp": [],
            "schema_encoding": [],
            "tps": None,
        }

    def write_dirty(self, composite_key, page_data):
        """
        Write a dirty page back to disk.
        """
        with self.lock:
            if composite_key in self.page_paths:
                path = self.page_paths[composite_key]
                # Ensure directory exists
                os.makedirs(os.path.dirname(path), exist_ok=True)

                # Serialize and write data
                with open(path, "wb") as f:
                    f.write(msgpack.packb(page_data, use_bin_type=True))

                # Mark page as clean
                if composite_key in self.pages:
                    self.pages[composite_key] = (page_data, False)

    def reset(self):
        """
        Write all dirty pages to disk and clear the bufferpool.
        """
        with self.lock:
            for composite_key, (page_data, is_dirty) in self.pages.items():
                if is_dirty:
                    self.write_dirty(composite_key, page_data)

            # Clear all bufferpool data structures
            self.pages.clear()
            self.page_paths.clear()
            self.pins.clear()
            self.access_times.clear()
            self.access_counter = 0

    def _construct_page_path(self, table_name, page_id):
        """
        Standardize page path construction.
        """
        # Create a structured path for different page types
        if isinstance(page_id, tuple):
            # Handle structured page IDs (e.g., (range_id, page_type, page_num))
            page_type = page_id[0]
            if page_type == "base":
                return os.path.join(
                    self.path, table_name, f"base_{page_id[1]}_{page_id[2]}.msg"
                )
            elif page_type == "tail":
                return os.path.join(
                    self.path, table_name, f"tail_{page_id[1]}_{page_id[2]}.msg"
                )

        # Default case for simple page IDs
        return os.path.join(self.path, table_name, f"{page_id}.msg")

class LockManager:
    def __init__(self):
        """
        Initializes the LockManager.
        - self.locks: A dictionary where the key is the record ID (rid) and the value is a tuple of:
          - A set of transaction IDs holding shared locks.
          - The transaction ID holding an exclusive lock (or None if no exclusive lock exists).
        - self.mutex: A threading Lock to ensure thread-safe access to the locks dictionary.
        """
        self.locks = {}  # key: rid, value: (set of shared lock tids, exclusive lock tid or None)
        self.mutex = RLock()

    def acquire_lock(self, transaction_id, record_id, operation):
        """
        Acquires a lock for a transaction on a specific record

        Arguments:
            transaction_id (int): The ID of the transaction requesting the lock
            record_id (int): The ID of the record to lock
            operation (str): The type of operation ("read", "update", "insert", "delete")

        Returns:
            bool: True if the lock was acquired and False otherwise
        """
        # Determine the lock type based on the operation
        lock_type = "exclusive" if operation in ["update", "insert", "delete"] else "shared"
        with self.mutex:
            # Initialize the lock state if the record is not yet present.
            if record_id not in self.locks:
                self.locks[record_id] = (set(), None)
            shared_lock_tids, exclusive_lock_tid = self.locks[record_id]

            if lock_type == "shared":
                # Grant shared lock if no exclusive lock exists or if this transaction already holds exclusive
                if exclusive_lock_tid is None or exclusive_lock_tid == transaction_id:
                    shared_lock_tids.add(transaction_id)
                    self.locks[record_id] = (shared_lock_tids, exclusive_lock_tid)
                    return True
                return False

            elif lock_type == "exclusive":
                # If the transaction already holds an exclusive lock, nothing to do
                if exclusive_lock_tid == transaction_id:
                    return True
                # If no locks are held, grant exclusive lock
                if not shared_lock_tids and exclusive_lock_tid is None:
                    self.locks[record_id] = (set(), transaction_id)
                    return True
                # Allow lock upgrade if this transaction is the only one holding a shared lock
                if transaction_id in shared_lock_tids and len(shared_lock_tids) == 1 and exclusive_lock_tid is None:
                    self.locks[record_id] = (set(), transaction_id)
                    return True
                return False

    def release_lock(self, transaction_id, record_id):
        """
        Releases any lock held by the transaction on the record
        If no locks remain, the record entry is removed
        """
        with self.mutex:
            # Do nothing if the record is not locked
            if record_id not in self.locks:
                return

            shared_lock_tids, exclusive_lock_tid = self.locks[record_id]

            # Remove the transaction from the shared locks if it holds one
            if transaction_id in shared_lock_tids:
                shared_lock_tids.remove(transaction_id)

            # Clear the exclusive lock if this transaction holds it
            if exclusive_lock_tid == transaction_id:
                exclusive_lock_tid = None

            # If no locks remain, remove the entry; otherwise update it
            if not shared_lock_tids and exclusive_lock_tid is None:
                del self.locks[record_id]
            else:
                self.locks[record_id] = (shared_lock_tids, exclusive_lock_tid)