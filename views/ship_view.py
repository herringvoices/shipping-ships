import sqlite3
import json


def update_ship(id, ship_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            UPDATE Ship
                SET
                    name = ?,
                    hauler_id = ?
            WHERE id = ?
            """,
            (ship_data["name"], ship_data["hauler_id"], id),
        )

        rows_affected = db_cursor.rowcount

    return True if rows_affected > 0 else False


def delete_ship(pk):
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute(
            """
        DELETE FROM Ship WHERE id = ?
        """,
            (pk,),
        )
        number_of_rows_deleted = db_cursor.rowcount

    return True if number_of_rows_deleted > 0 else False


def list_ships(url):
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        query_params = url["query_params"]

        if "_expand" in query_params and query_params["_expand"][0] == "hauler":
            db_cursor.execute(
                """
                SELECT
                    s.id AS ship_id,
                    s.name AS ship_name,
                    s.hauler_id AS hauler_id,
                    h.id AS hauler_id_actual,
                    h.name AS hauler_name,
                    h.dock_id AS hauler_dock_id
                FROM Ship s
                JOIN Hauler h ON s.hauler_id = h.id
            """
            )

            ships = []
            for row in db_cursor.fetchall():
                ships.append(
                    {
                        "id": row["ship_id"],
                        "name": row["ship_name"],
                        "hauler_id": row["hauler_id"],
                        "hauler": {
                            "id": row["hauler_id_actual"],
                            "name": row["hauler_name"],
                            "dock_id": row["hauler_dock_id"],
                        },
                    }
                )

            return json.dumps(ships)

        else:
            # Regular no-expand behavior
            db_cursor.execute(
                """
                SELECT id, name, hauler_id FROM Ship
            """
            )
            ships = [dict(row) for row in db_cursor.fetchall()]
            return json.dumps(ships)


def retrieve_ship(pk):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute(
            """
        SELECT
            s.id,
            s.name,
            s.hauler_id
        FROM Ship s
        WHERE s.id = ?
        """,
            (pk,),
        )
        query_results = db_cursor.fetchone()

        # Serialize Python list to JSON encoded string
        dictionary_version_of_object = dict(query_results)
        serialized_ship = json.dumps(dictionary_version_of_object)

    return serialized_ship


def add_ship(ship_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            INSERT INTO Ship (name, hauler_id)
            VALUES (?, ?)
            """,
            (ship_data["name"], ship_data["hauler_id"]),
        )
        new_id = db_cursor.lastrowid
        return {
            "id": new_id,
            "name": ship_data["name"],
            "hauler_id": ship_data["hauler_id"],
        }
