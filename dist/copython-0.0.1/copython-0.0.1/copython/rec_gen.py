import csv
import pyodbc
def record_generator(metadata):
    if metadata.__class__.__name__ == "CSVMetadata":
        with open(metadata.path, 'r', encoding=metadata.encoding) as csvfile:
            if metadata.has_header is True:
                next(csvfile)
            reader = csv.reader(csvfile,delimiter=metadata.delimiter,quotechar='"')
            for line in reader:
                yield(line)

    elif metadata.__class__.__name__ == "SQLTableMetadata":
        conn = pyodbc.connect(str(metadata.conn_str))
        sql = "SELECT * FROM {}.{}".format(metadata.schema_name,metadata.table_name)
        cursor = conn.cursor()
        for row in cursor.execute(sql).fetchall():
            yield(list(row))

    elif metadata.__class__.__name__ == "SQLQueryMetadata":
        conn = pyodbc.connect(str(metadata.conn_str))
        sql = metadata.sql_str
        cursor = conn.cursor()
        for row in cursor.execute(sql).fetchall():
            yield(list(row))

