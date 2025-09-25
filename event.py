"""
Automated event generator module for HR data.
"""

from datetime import date
import random
from faker import Faker
import polars as pl
from base_data_generator import HRDataGenerator, OutputFormat, EventType
from hr.mapping import DEPARTMENT_POSITION_MAPPING


def get_data(record_count: int = 1) -> list[dict]:
    """
    Generates initial HR data.
    :param record_count: Number of records to generate.

    :return: List of dictionaries containing HR data.

    TODO: Add support for real world scenarios.
    """

    generator = HRDataGenerator()
    return generator.generate(count=record_count, output_format=OutputFormat.DICT)


def get_data_from_csv(path: str) -> list[dict]:
    """
    Get initial data from a csv.
    """

    data = pl.read_csv(path)
    return data.to_dicts()


def get_data_from_json(path: str) -> list[dict]:
    """
    Get initial data from a json.
    """

    data = pl.read_json(path)
    return data.to_dicts()


def generate_event(record_count: int = 1, path: str = None, sample_size: int = 50):
    """
    Automated events for HR data.
    :param record_count: Number of records to generate.
    :path: file path to read initial data.
    :sample_size: count of records to read from original data (randomly)
    :return: List of dictionaries containing HR event data.
    """

    fake = Faker()
    event_type = fake.random_element(elements=list(EventType))
    if ".csv" in path:
        input_data = random.sample(get_data_from_csv(path=path), sample_size)
    elif ".json" in path:
        input_data = random.sample(get_data_from_json(path=path), sample_size)
    else:
        input_data = random.sample(get_data(record_count), sample_size)

    if event_type == EventType.HIRE:
        new_employee = HRDataGenerator().generate(
            count=1, output_format=OutputFormat.DICT
        )[0]
        new_employee["event_type"] = EventType.HIRE.value
        new_employee["hire_date"] = date.today().isoformat()
        return [new_employee]
    elif event_type == EventType.RESIGNATION:
        for record in input_data:
            record["event_type"] = EventType.RESIGNATION.value
            record["event_date"] = date.today().isoformat()
        return input_data
    elif event_type == EventType.PROMOTION:
        for record in input_data:
            record["event_type"] = EventType.PROMOTION.value
            record["salary"] = round(record["salary"] * 1.2 / 1000) * 1000
            record["event_date"] = date.today().isoformat()
        return input_data
    elif event_type == EventType.SALARY_INCREASE:
        for record in input_data:
            record["event_type"] = EventType.SALARY_INCREASE.value
            record["salary"] = round(record["salary"] * 1.1 / 1000) * 1000
            record["event_date"] = date.today().isoformat()
        return input_data
    elif event_type == EventType.DEPARTMENT_CHANGE:
        for record in input_data:
            new_department = fake.random_element(
                elements=list(DEPARTMENT_POSITION_MAPPING.keys())
            )
            new_position = fake.random_element(
                elements=DEPARTMENT_POSITION_MAPPING[new_department]
            )
            record["event_type"] = EventType.DEPARTMENT_CHANGE.value
            record["department"] = new_department
            record["position"] = new_position
            record["salary"] = (
                round(fake.random_int(min=35000, max=120000) / 1000) * 1000
            )
            record["event_date"] = date.today().isoformat()
        return input_data

    return []  # Fallback, should not reach here


if __name__ == "__main__":
    event_data = generate_event(50, "hr_data.json", 500)
    pl.from_dicts(event_data).write_json("event_data.json")
