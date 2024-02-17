import dlt
import duckdb

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

# Create a pipeline
pipeline = dlt.pipeline(
    pipeline_name="person_gen",
    destination="duckdb", 
    dataset_name="my_data"
)

# Run the pipeline with the generator people_1
load_info = pipeline.run(people_1(), table_name="my_table")

print(load_info)

# show the outcome
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
print('Loaded tables: ')
print(conn.sql("show tables"))

print("\n\n\n Age Calculated from people_1() gen data")
total_age = conn.sql("SELECT SUM(age) FROM my_table").df()
print(f"{total_age}")

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

# # Ques-03: Execute pipeline to append data from `people_2()` generator to `my_table`.
# load_info = pipeline.run(
#     people_2(), 
#     table_name="my_table",
#     write_disposition="append",    
# )

# print(load_info)

# print("\n\n\n Age Calculated after appending people_2() gen data")
# total_age = conn.sql("SELECT SUM(Age) FROM my_table").df()
# print(f"{total_age}")

# Ques-04: Execute pipeline to merge data from `people_2()` generator into `my_table`.
load_info = pipeline.run(
    people_2(), 
    table_name="my_table",
    write_disposition="merge", 
    primary_key="id"
)
print(load_info)

print("\n\n\n Age Calculated after appending people_2() gen data")
total_age = conn.sql("SELECT SUM(age) FROM my_table").df()
print(f"{total_age}")
