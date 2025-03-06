from lstore.index import Index
from lstore.page_range import PageRange
from lstore.page import BasePage, LogicalPage
from lstore.config import MERGE_THRESHOLD
import threading
from datetime import datetime


INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __str__(self):
        # Return a string representation that matches the test expectations
        # Should display as a list of column values
        return str(self.columns)

    def __repr__(self):
        # This makes the record print like a list in error messages
        return str(self.columns)


class Table:
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.page_ranges = []
        self.merge_counter = 0
        self.lock = threading.Lock()
        self.database = None  # Add this line to store the database reference

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
        rid = (
            self.page_ranges.index(page_range),
            page_range.base_pages.index(base_page),
            base_page.num_records,
            "b",
        )
        base_page.rid.append(rid)  # Ensure the rid is appended to the list

        return rid

    def find_record(self, key, rid, projected_columns_index):
        """
        Find a record in the bufferpool using its RID.
        """
        # Extract the page type and location from the RID
        page_range_idx, page_idx, record_idx, page_type = rid

        # Determine if we're looking at a base or tail page
        is_base_page = page_type == "b"
        page_type_str = "base" if is_base_page else "tail"

        # Create a page identifier for the bufferpool
        page_identifier = (page_type_str, page_range_idx, page_idx)

        # Get the page from the bufferpool
        page_data = self.database.bufferpool.get_page(
            page_identifier, self.name, self.num_columns
        )

        # Extract the values for the projected columns
        values = []
        for i, include in enumerate(projected_columns_index):
            if include == 1:
                # Only include columns that are requested
                try:
                    if (
                            "columns" in page_data
                            and i < len(page_data["columns"])
                            and record_idx < len(page_data["columns"][i])
                    ):
                        # Append the value from the specified column at the record index
                        values.append(page_data["columns"][i][record_idx])
                    else:
                        # Default to 0 if column data is missing or index is out of bounds
                        values.append(0)
                except Exception as e:
                    # Handle any errors, defaulting to 0
                    print(f"Error reading column {i} value: {e}")
                    values.append(0)

        # Unpin the page when done
        self.database.bufferpool.unpin_page(page_identifier)

        # Create a record with the extracted values
        return Record(rid, key, values)

    def insert_record(self, start_time, schema_encoding, *columns):
        """
        Insert a record using the bufferpool for page access.
        """
        # Get the current base page
        page_range, base_page = self.find_current_base_page()
        record_index = base_page.num_records  # Current index for the new record

        # Determine page identifiers
        page_range_id = self.page_ranges.index(page_range)
        page_id = page_range.base_pages.index(base_page)

        # Create RID
        rid = (page_range_id, page_id, record_index, "b")

        # Create page identifier for bufferpool
        page_identifier = ("base", page_range_id, page_id)

        # Get page from bufferpool
        page_data = self.database.bufferpool.get_page(
            page_identifier, self.name, self.num_columns
        )

        # Make sure the page has the expected structure
        if "columns" not in page_data:
            page_data["columns"] = [[] for _ in range(self.num_columns)]
        if "indirection" not in page_data:
            page_data["indirection"] = []
        if "rid" not in page_data:
            page_data["rid"] = []
        if "timestamp" not in page_data:
            page_data["timestamp"] = []
        if "schema_encoding" not in page_data:
            page_data["schema_encoding"] = []

        # Insert record metadata
        page_data["indirection"].append(rid)
        page_data["rid"].append(rid)
        page_data["timestamp"].append(start_time)
        page_data["schema_encoding"].append(schema_encoding)

        # Insert column values
        for i, value in enumerate(columns):
            # Make sure there are enough column lists
            while i >= len(page_data["columns"]):
                page_data["columns"].append([])

            # Add the value to the appropriate column
            page_data["columns"][i].append(value)

            # Also insert into the direct page (for consistency)
            try:
                base_page.pages[i].write(value)
            except Exception as e:
                print(f"Warning: Failed to write to direct page: {e}")

        # Update the page in the bufferpool
        self.database.bufferpool.set_page(page_identifier, self.name, page_data)

        # Unpin the page
        self.database.bufferpool.unpin_page(page_identifier)

        # Update the base_page metadata
        base_page.num_records += 1
        base_page.indirection.append(rid)
        base_page.schema_encoding.append(schema_encoding)
        base_page.start_time.append(start_time)
        base_page.rid.append(rid)

        # Add to page directory
        self.page_directory[rid] = Record(rid, columns[self.key], columns)

        # Insert key to the index
        key = columns[self.key]
        self.index.insert(key, rid)

        return True

    def update(self, primary_key, *columns):
        # Get the RID of the record
        rid = self.index.locate(self.key, primary_key)
        if not rid:
            return False
        rid = rid[0]

        # Check if the updated values lead to duplicate primary key
        if columns[self.key] is not None and columns[self.key] != primary_key:
            if self.index.locate(self.key, columns[self.key]):
                return False

        # Extract page and record information
        page_range_index, page_index, record_index, _ = rid

        # Ensure the indices are valid
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

        # Get the current record
        current_rid = base_page.indirection[record_index]

        # Find the record
        record = self.find_record(primary_key, current_rid, [1] * self.num_columns)

        # Check that we have space in the tail page
        if not page_range.tail_pages or not page_range.tail_pages[-1].has_capacity():
            page_range.add_tail_page(self.num_columns)

        # Insert the new record in the tail page
        current_tp = len(page_range.tail_pages) - 1
        tail_page = page_range.tail_pages[current_tp]

        start_time = datetime.now().strftime("%Y%m%d%H%M%S")
        tail_page.insert_tail_page_record(*columns, record=record)
        tail_page.start_time.append(start_time)
        tail_page.indirection.append(rid)

        # Update the base page indirection
        new_record_index = tail_page.num_records - 1
        update_rid = (page_range_index, current_tp, new_record_index, "t")
        tail_page.rid.append(update_rid)
        base_page.indirection[record_index] = update_rid

        # Update the schema encoding
        for i in range(self.num_columns):
            if tail_page.schema_encoding[new_record_index][i] == 1:
                base_page.schema_encoding[record_index][i] = 1

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
            # print("<----merging---->")
            for page_range in self.page_ranges:
                merged_base_pages = []
                for base_page in page_range.base_pages:
                    # Create a copy of the base page
                    merged_base_page = BasePage(self.num_columns)
                    merged_base_page.pages = [
                        LogicalPage() for _ in range(self.num_columns)
                    ]

                    # Copy the base page records to the merged base page
                    for i in range(base_page.num_records):
                        for j in range(self.num_columns):
                            value = base_page.pages[j].read(i, 1)[0]
                            merged_base_page.pages[j].write(value)
                        merged_base_page.indirection.append(base_page.indirection[i])
                        merged_base_page.schema_encoding.append(
                            base_page.schema_encoding[i]
                        )
                        merged_base_page.start_time.append(base_page.start_time[i])
                        merged_base_page.rid.append(base_page.rid[i])

                    # Track the latest updates for each base record
                    latest_updates = {}  # base_rid -> (tail_page_index, record_index)
                    for tail_page_index, tail_page in enumerate(
                        reversed(page_range.tail_pages)
                    ):
                        for record_index in range(tail_page.num_records):
                            base_rid = tail_page.indirection[record_index]
                            if base_rid in base_page.rid:
                                latest_updates[base_rid] = (
                                    len(page_range.tail_pages) - 1 - tail_page_index,
                                    record_index,
                                )

                    # Apply only the most recent updates
                    updated_columns = {base_rid: set() for base_rid in base_page.rid}
                    for base_rid, (
                        tail_page_index,
                        record_index,
                    ) in latest_updates.items():
                        tail_page = page_range.tail_pages[tail_page_index]
                        base_index = base_page.rid.index(base_rid)
                        for j in range(self.num_columns):
                            if (
                                record_index < len(tail_page.schema_encoding)
                                and tail_page.schema_encoding[record_index][j] == "1"
                                and j not in updated_columns[base_rid]
                            ):
                                value = tail_page.pages[j].read(record_index, 1)[0]
                                merged_base_page.pages[j].write(value)
                                updated_columns[base_rid].add(j)

                    # Traverse the lineage to incorporate all recent updates
                    for i in range(merged_base_page.num_records):
                        current_rid = merged_base_page.indirection[i]
                        while current_rid[3] == "t":
                            tail_page = page_range.tail_pages[current_rid[1]]
                            record_index = current_rid[2]
                            for j in range(self.num_columns):
                                if (
                                    record_index < len(tail_page.schema_encoding)
                                    and tail_page.schema_encoding[record_index][j]
                                    == "1"
                                    and j
                                    not in updated_columns[merged_base_page.rid[i]]
                                ):
                                    value = tail_page.pages[j].read(record_index, 1)[0]
                                    merged_base_page.pages[j].write(value)
                                    updated_columns[merged_base_page.rid[i]].add(j)
                                current_rid = tail_page.indirection[record_index]

                    # Create new keys for the merged records
                    new_keys = []
                    for i in range(merged_base_page.num_records):
                        new_key = max(self.page_directory.keys()) + 1
                        new_keys.append(new_key)
                        self.page_directory[new_key] = self.page_directory[
                            merged_base_page.rid[i]
                        ]
                        self.page_directory[new_key].key = new_key

                    # Update TPS for the merged base page
                    merged_base_page.tps = max(
                        tail_page.tps for tail_page in page_range.tail_pages
                    )

                    # Add the merged base page to the list
                    merged_base_pages.append(merged_base_page)

                # Replace old base pages with merged base pages
                page_range.base_pages = merged_base_pages
                page_range.num_base_pages = len(merged_base_pages)

            # print("<----merging complete---->")

    def read_column_from_page(
        self, page_range_id, page_id, column_id, record_id, is_base_page=True
    ):
        """
        Read a value from a specific column in a page using the bufferpool.
        """
        # Check if database reference exists
        if not hasattr(self, "database") or self.database is None:
            raise Exception(
                "Table has no database reference. Cannot access bufferpool."
            )

        # Construct a page identifier
        page_type = "base" if is_base_page else "tail"
        page_identifier = (page_type, page_range_id, page_id)

        # Get the page from bufferpool
        page_data = self.database.bufferpool.get_page(
            page_identifier, self.name, self.num_columns
        )

        # Check if the data exists
        if "columns" not in page_data:
            return None

        if column_id >= len(page_data["columns"]):
            return None

        if record_id >= len(page_data["columns"][column_id]):
            return None

        # Extract the column value
        value = page_data["columns"][column_id][record_id]

        # Unpin the page when done
        self.database.bufferpool.unpin_page(page_identifier)

        return value

    def write_column_to_page(
        self, page_range_id, page_id, column_id, record_id, value, is_base_page=True
    ):
        """
        Write a value to a specific column in a page using the bufferpool.
        """
        # Check if database reference exists
        if not hasattr(self, "database") or self.database is None:
            raise Exception(
                "Table has no database reference. Cannot access bufferpool."
            )

        # Construct a page identifier
        page_type = "base" if is_base_page else "tail"
        page_identifier = (page_type, page_range_id, page_id)

        # Get the page from bufferpool, passing the number of columns
        page_data = self.database.bufferpool.get_page(
            page_identifier, self.name, self.num_columns
        )

        # Make sure the "columns" list exists and has enough entries
        if "columns" not in page_data:
            page_data["columns"] = [[] for _ in range(self.num_columns)]

        # Make sure we have enough columns
        while len(page_data["columns"]) <= column_id:
            page_data["columns"].append([])

        # Make sure the column has enough entries
        while len(page_data["columns"][column_id]) <= record_id:
            page_data["columns"][column_id].append(None)

        # Set the value
        page_data["columns"][column_id][record_id] = value

        # Update the page in bufferpool and mark it dirty
        self.database.bufferpool.set_page(page_identifier, self.name, page_data)

        # Unpin the page when done
        self.database.bufferpool.unpin_page(page_identifier)
