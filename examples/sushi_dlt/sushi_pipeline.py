import typing as t
import dlt


# Example sushi_types table
@dlt.resource(name="sushi_types", primary_key="id", write_disposition="merge")
def sushi_types() -> t.Iterator[t.Dict[str, t.Any]]:
    yield from [
        {"id": 0, "name": "Tobiko"},
        {"id": 1, "name": "Sashimi"},
        {"id": 2, "name": "Maki"},
        {"id": 3, "name": "Temaki"},
    ]


# Example waiters table
@dlt.resource(name="waiters", primary_key="id", write_disposition="merge")
def waiters() -> t.Iterator[t.Dict[str, t.Any]]:
    yield from [
        {"id": 0, "name": "Toby"},
        {"id": 1, "name": "Tyson"},
        {"id": 2, "name": "Ryan"},
        {"id": 3, "name": "George"},
        {"id": 4, "name": "Chris"},
        {"id": 5, "name": "Max"},
        {"id": 6, "name": "Vincent"},
        {"id": 7, "name": "Iaroslav"},
        {"id": 8, "name": "Emma"},
        {"id": 9, "name": "Maia"},
    ]


# Example sushi menu table with extra one and two levels of nesting tables
@dlt.resource(name="sushi_menu", primary_key="id", write_disposition="merge")
def sushi_menu() -> t.Iterator[t.Dict[str, t.Any]]:
    yield from [
        {
            "id": 0,
            "name": "Tobiko",
            "fillings": ["Red Tobiko", "Black Tobiko", "Wasabi Tobiko", "Green Tobiko"],
            "details": {
                "preparation": "Raw",
                "ingredients": ["Seaweed", "Rice", "Tobiko"],
                "price": 12.99,
                "spicy": False,
            },
        },
        {
            "id": 1,
            "name": "Sashimi",
            "fillings": [
                "Tuna Sashimi",
                "Salmon Sashimi",
                "Yellowtail Sashimi",
                "Octopus Sashimi",
                "Scallop Sashimi",
            ],
            "details": {
                "preparation": "Raw",
                "ingredients": ["Fish", "Soy Sauce", "Wasabi"],
                "price": 19.99,
                "spicy": False,
            },
        },
        {
            "id": 2,
            "name": "Maki",
            "fillings": ["Cucumber", "Tuna", "Salmon", "Avocado", "Tempura Shrimp"],
            "details": {
                "preparation": "Rolled",
                "ingredients": ["Seaweed", "Rice", "Fish", "Vegetables"],
                "price": 14.99,
                "spicy": True,
            },
        },
        {
            "id": 3,
            "name": "Temaki",
            "fillings": ["Tuna Temaki", "Salmon Temaki", "Vegetable Temaki", "Ebi Temaki"],
            "details": {
                "preparation": "Hand Roll",
                "ingredients": ["Seaweed", "Rice", "Fish", "Vegetables"],
                "price": 10.99,
                "spicy": True,
            },
        },
    ]


# Run the pipeline
p = dlt.pipeline(pipeline_name="sushi", destination="duckdb")
info = p.run([sushi_types(), waiters(), sushi_menu()])
