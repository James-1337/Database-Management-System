from lstore.config import PAGE_SIZE, RECORDS_PER_PAGE

class LogicalPage:
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < RECORDS_PER_PAGE

    def write(self, value):
        # Only handle 8-byte integers for milestone 1
        if not isinstance(value, int):
            raise ValueError("Value must be an integer")
        if value.bit_length() > 64:
            raise OverflowError("int too big to convert")
        
        # Convert the integer to a bytearray
        value_bytes = value.to_bytes(8, byteorder='big')
        
        # Get the start and end index of the value in the data bytearray
        start = self.num_records * 8
        end = (self.num_records + 1) * 8
        
        # Write the value to the data bytearray
        self.data[start:end] = value_bytes
        self.num_records += 1

    def read(self, index, num_values):
        values = []
        
        # Read the values from the data bytearray and append them to the list
        for i in range(num_values):
            start = (index + i) * 8
            end = (index + i + 1) * 8
            value_bytes = self.data[start:end]
            value = int.from_bytes(value_bytes, byteorder='big')
            values.append(value)
        return values

# compressed, read-only pages
class BasePage:
    def __init__(self, num_cols):
        # Initialize the page 
        # Records are logically alligned
        self.rid = []
        self.num_cols = num_cols
        self.num_records = 0
        self.indirection = []
        self.schema_encoding = []
        self.start_time = []
        self.pages = []
        
        # Initialize a LogicalPage for each column
        for _ in range(self.num_cols):
            self.pages.append(LogicalPage())
    
    def has_capacity(self):
        # Check if the page has capacity for more records
        return self.num_records < RECORDS_PER_PAGE
    
    def insert_base_page_record(self, rid, start_time, schema_encoding, indirection, *columns):
        # Check if the page has capacity for more records
        if not self.has_capacity():
            return False
        
        # Write the record to the page
        for i in range(self.num_cols):
            self.pages[i].write(columns[i])

        # Append the record information to the page
        self.rid.append(rid)
        self.start_time.append(start_time)
        self.schema_encoding.append(schema_encoding)
        self.indirection.append(indirection)
        
        # Increment the number of records in the page
        self.num_records += 1
        
        # Return True to indicate that the record was successfully inserted
        return True
        

# uncompressed, append-only updates
class TailPage:
    def __init__(self, num_cols):
        # Initialize the page 
        # Records are logically alligned
        self.rid = []
        self.num_cols = num_cols
        self.num_records = 0
        self.indirection = []
        self.schema_encoding = []
        self.start_time = []
        self.pages = []
        self.tps = 0
        
        # Initialize a LogicalPage for each column
        for _ in range(self.num_cols):
            self.pages.append(LogicalPage())
    
    def has_capacity(self):
        # Check if the page has capacity for more records
        return self.num_records < RECORDS_PER_PAGE
    
    def insert_tail_page_record(self, *columns, record):
        # Generate the schema by checking which columns are being updated
        # Write the new data to the columns that are being updated
        # Write the old data to the columns that are not being updated
        schema = ''
        for i in range(self.num_cols):
            if columns[i] is not None:
                schema += '1'
                self.pages[i].write(columns[i])
            else:
                schema += '0'
                self.pages[i].write(record.columns[i])
                
        # Append the schema to the page
        self.schema_encoding.append(schema)
        
        # Increment the number of records in the page
        self.num_records += 1
        
        # Return True to indicate that the record was successfully inserted
        return True