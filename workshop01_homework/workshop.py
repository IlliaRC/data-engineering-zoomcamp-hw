import dlt
import duckdb


def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1


def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}


def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


if __name__ == "__main__":

    # 1. Use a generator
    # 1.1 What is the sum of the outputs of the generator for limit = 5?
    limit = 5
    generator = square_root_generator(limit)

    result = 0
    for sqrt_value in generator:
        result = result + sqrt_value
    print('Sum of the outputs of the generator for limit = 5: ', result)  # 8.382332347441762

    # 2.1 What is the 13th number yielded
    limit = 13
    generator = square_root_generator(limit)
    iteration = 0
    for sqrt_value in generator:
        iteration += 1
        if iteration == 13:
            print('13th number yielded: ', sqrt_value)  # 3.605551275463989

    # 2. Append a generator to a table with existing data
    # 2.1 Load the first generator and calculate the sum of ages of all people. Make sure to only load it once.
    pipeline = dlt.pipeline(destination='duckdb', dataset_name='people', full_refresh=True)
    pipeline.run(people_1(), table_name='people', write_disposition="replace")

    table_name = f"{pipeline.dataset_name}.people"
    conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
    print(
        'Sum of all ages of people for the first generator: ',
        conn.sql(f"SELECT * FROM {table_name}").df()['age'].sum()
    )

    # 2.2 Append the second generator to the same table as the first.
    pipeline.run(people_2(), table_name='people', write_disposition="append")

    # 2.3 After correctly appending the data, calculate the sum of all ages of people.
    table_name = f"{pipeline.dataset_name}.people"
    conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
    print('Sum of all ages of people for both generators: ', conn.sql(f"SELECT * FROM {table_name}").df()['age'].sum())
    # 353

    # 3. Merge a generator
    # 3.1 Load your first generator first, and then load the second one with merge. Since they have overlapping IDs,
    # some of the records from the first load should be replaced by the ones from the second load.
    pipeline = dlt.pipeline(destination='duckdb', dataset_name='people', full_refresh=True)
    pipeline.run(people_1(), table_name='people', write_disposition="replace", primary_key='ID')
    pipeline.run(people_2(), table_name='people', write_disposition="merge", primary_key='ID')

    # 3.2 Calculate the sum of ages of all the people loaded as described above.
    table_name = f"{pipeline.dataset_name}.people"
    print('Sum of all ages of people: ', conn.sql(f"SELECT * FROM {table_name}").df()['age'].sum())
