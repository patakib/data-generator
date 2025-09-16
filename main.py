"""Main module for data generation."""

from abc import ABC, abstractmethod
from datetime import date
from enum import StrEnum, auto
from faker import Faker
import polars as pl
from pydantic_settings import BaseSettings, SettingsConfigDict

from hr.mapping import DEPARTMENT_POSITION_MAPPING


fake = Faker()


class EventType(StrEnum):
    """Enumeration for event types."""
    HIRE = auto()
    RESIGNATION = auto()
    PROMOTION = auto()
    SALARY_INCREASE = auto()
    DEPARTMENT_CHANGE = auto()


class OutputFormat(StrEnum):
    """Enumeration for output formats."""
    DICT = auto()
    CSV = auto()
    PARQUET = auto()
    JSON = auto()


class UpdateType(StrEnum):
    """Enumeration for update types."""
    RESIGNATION = auto()
    PROMOTION = auto()
    SALARY_INCREASE = auto()
    DEPARTMENT_CHANGE = auto()


class DataGenerator(ABC):
    """Abstract base class for data generators."""
    @abstractmethod
    def generate(self):
        pass


class HRDataGenerator(DataGenerator):
    """Generates fake HR-related data."""

    def _generate_data_in_dict(self, count: int) -> list[dict]:
        """Generates a list of dictionaries with fake HR data."""
        fake_data = []
        for _ in range(count):

            # choosing a department randomly
            department = fake.random_element(elements=DEPARTMENT_POSITION_MAPPING.keys())
            position = fake.random_element(elements=DEPARTMENT_POSITION_MAPPING[department])

            fake_data.append({
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "phone_number": fake.phone_number(),
                "address": fake.address(),
                "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=65).isoformat(),
                "department": department,
                "position": position,
                "hire_date": fake.date_between(start_date='-10y', end_date='today').isoformat(),
                "salary": round(fake.random_int(min=35000, max=120000) / 1000) * 1000,
            })
        return fake_data
    
    def update_data(self, input_data: list[dict], update_type: UpdateType, count_of_records_to_update: int = 1) -> list[dict]:
        """
            Updates existing data with new fake HR data.

            Args:
                input_data (list[dict]): Existing data to be updated.
                update_type (UpdateType): Type of update to perform.
                count_of_records_to_update (int): Number of records to update.
            Returns:
                list[dict]: Updated data.
        """
        updated_data = []
        for _ in range(count_of_records_to_update):
            record = fake.random_element(elements=input_data)
            record["event_date"] = date.today().isoformat()

            if update_type == UpdateType.RESIGNATION:
                record["resignation_date"] = date.today().isoformat()
            elif update_type == UpdateType.PROMOTION:
                possible_other_positions = [pos for pos in DEPARTMENT_POSITION_MAPPING[record["department"]] if pos != record["position"]]
                if possible_other_positions:
                    record["position"] = fake.random_element(elements=possible_other_positions)
                record["salary"] = round((record["salary"] * 1.1) / 1000) * 1000  # 10% increase
            elif update_type == UpdateType.SALARY_INCREASE:
                record["salary"] = round((record["salary"] * 1.05) / 1000) * 1000  # 5% increase
            elif update_type == UpdateType.DEPARTMENT_CHANGE:
                possible_departments = [dept for dept in DEPARTMENT_POSITION_MAPPING.keys() if dept != record["department"]]
                if possible_departments:
                    new_department = fake.random_element(elements=possible_departments)
                    record["department"] = new_department
                    record["position"] = fake.random_element(elements=DEPARTMENT_POSITION_MAPPING[new_department])
                    record["salary"] = round((record["salary"] * 1.1) / 1000) * 1000  # 10% increase for department change
            updated_data.append(record)
        return updated_data

    def generate(self, count: int = 1000, output_format: OutputFormat = OutputFormat.DICT, output_path: str = "hr_data") -> list[dict] | str:
        """
        Generates fake HR data in desired format.
        Args:
            count (int): Number of records to generate.
            format (OutputFormat): Desired output format.
            output_path (str): Path to save the output file (if applicable).
        """

        if output_format == OutputFormat.DICT:
            return self._generate_data_in_dict(count)
        elif output_format == OutputFormat.CSV:
            data = self._generate_data_in_dict(count)
            df = pl.DataFrame(data)
            df.write_csv(f"{output_path}.csv")
            return f"Data saved to {output_path}.csv"
        elif output_format == OutputFormat.PARQUET:
            data = self._generate_data_in_dict(count)
            df = pl.DataFrame(data)
            df.write_parquet(f"{output_path}.parquet")
            return f"Data saved to {output_path}.parquet"
        elif output_format == OutputFormat.JSON:
            data = self._generate_data_in_dict(count)
            df = pl.DataFrame(data)
            df.write_json(f"{output_path}.json")
            return f"Data saved to {output_path}.json"
        else:
            raise ValueError("Unsupported format. Choose from DICT, CSV, PARQUET, JSON.")


class Settings(BaseSettings):
    """Configuration settings for data generation."""
    model_config = SettingsConfigDict(cli_parse_args=True)

    data_generator: DataGenerator = HRDataGenerator()
    type_of_update: UpdateType|None = None
    update_count: int = 1

    count: int = 1000
    output_format: OutputFormat = OutputFormat.DICT
    output_path: str = "hr_data"


def main():
    """Main function to run data generation based on settings."""
    settings = Settings()

    if not settings.type_of_update:
        result = settings.data_generator.generate(
            count=settings.count,
            output_format=settings.output_format,
            output_path=settings.output_path
        )
        if isinstance(result, str):
            print(result)
        else:
            print(f"Generated {len(result)} records.")
            print(result[:5])  # Print first 5 records for verification
    else:
        # For demonstration, generating initial data first
        initial_data = settings.data_generator.generate(
            count=settings.count,
            output_format=OutputFormat.DICT
        )
        updated_data = settings.data_generator.update_data(
            input_data=initial_data,
            update_type=settings.type_of_update,
            count_of_records_to_update=settings.update_count
        )
        print(f"Updated {len(updated_data)} records with {settings.type_of_update}.")
        print(updated_data[:5])  # Print first 5 updated records for verification

if __name__ == "__main__":
    main()
