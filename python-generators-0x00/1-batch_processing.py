import mysql.connector


def stream_users_in_batches(batch_size):
    """Generator to yield users in batches from the database"""
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="*******",
            database="ALX_prodev",
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM user_data")

        batch = []
        for row in cursor:
            batch.append(row)
            if len(batch) == batch_size:
                yield batch
                batch = []

        if batch:
            yield batch  # Yield the remaining rows

        return

    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def batch_processing(batch_size):
    """Filter users over 25 and print them"""
    for batch in stream_users_in_batches(batch_size):
        for user in batch:
            if user["age"] > 25:  # ← لازم الرقم 25 يكون ظاهر كده بالضبط
                print(user)
