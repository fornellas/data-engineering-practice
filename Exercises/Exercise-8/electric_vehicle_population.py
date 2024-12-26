from typing import Dict, Tuple, Optional, List, Any
import duckdb
import pandas
import csv
from collections import defaultdict
import re


class ElectricVehiclePopulation:
    _csv_path: str
    _conn: duckdb.DuckDBPyConnection

    def __init__(self, csv_path: str) -> None:
        self._csv_path = csv_path

    @staticmethod
    def _parse_vehicle_location(vehicle_location: str) -> Tuple[float, float]:
        match = re.match(r"^POINT \(([^ ]+) ([^ ]+)\)$", vehicle_location)
        if not match:
            raise ValueError("Invalid point string format.")
        longitude = float(match.group(1))
        latitude = float(match.group(2))
        return (longitude, latitude)

    def _load_csv_row(self, dtDict: Dict[str, Any], row: Dict[str, Any]):
        dtDict["VIN (1-10)"].append(row["VIN (1-10)"])
        dtDict["County"].append(row["County"])
        dtDict["City"].append(row["City"])
        dtDict["State"].append(row["State"])
        dtDict["Postal Code"].append(int(row["Postal Code"]))
        model_year: int = int(row["Model Year"])
        dtDict["Model Year"].append(model_year)
        make: str = row["Make"]
        dtDict["Make"].append(make)
        model: str = row["Model"]
        dtDict["Model"].append(model)
        # Adding this extra column simplifies the queries by vehicle later on.
        # I'm no car expert... assuming make + model as the definition of vehicle.
        dtDict["Vehicle"].append(f"{make} {model}")
        dtDict["Electric Vehicle Type"].append(row["Electric Vehicle Type"])
        dtDict["Clean Alternative Fuel Vehicle (CAFV) Eligibility"].append(
            row["Clean Alternative Fuel Vehicle (CAFV) Eligibility"]
        )
        dtDict["Electric Range"].append(int(row["Electric Range"]))
        dtDict["Base MSRP"].append(int(row["Base MSRP"]))
        legislative_district: Optional[int] = None
        if len(row["Legislative District"]) > 0:
            legislative_district = int(row["Legislative District"])
        dtDict["Legislative District"].append(legislative_district)
        dtDict["DOL Vehicle ID"].append(legislative_district)
        longitude: Optional[float] = None
        latitude: Optional[float] = None
        if len(row["Vehicle Location"]) > 0:
            longitude, latitude = self._parse_vehicle_location(row["Vehicle Location"])
        dtDict["Longitude"].append(longitude)
        dtDict["Latitude"].append(latitude)
        dtDict["Electric Utility"].append(row["Electric Utility"])
        dtDict["2020 Census Tract"].append(int(row["2020 Census Tract"]))

    def __enter__(self) -> "ElectricVehiclePopulation":
        if hasattr(self, "_conn"):
            raise RuntimeError("already connected")

        dtDict: Dict[str, Any] = defaultdict(list)
        with open(self._csv_path, newline="") as io:
            for row in csv.DictReader(io):
                self._load_csv_row(dtDict, row)

        self._conn = duckdb.connect()
        create_table = """
        CREATE TABLE electric_vehicles_population (
            VIN_1_10 VARCHAR,
            County VARCHAR,
            City VARCHAR,
            State VARCHAR,
            Postal_Code UINTEGER,
            Model_Year USMALLINT,
            Make VARCHAR,
            Model VARCHAR,
            Vehicle VARCHAR,
            Electric_Vehicle_Type VARCHAR,
            Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility VARCHAR,
            Electric_Range USMALLINT,
            Base_MSRP UINTEGER,
            Legislative_District USMALLINT NULL,
            DOL_Vehicle_ID UINTEGER,
            Longitude DOUBLE NULL,
            Latitude DOUBLE NULL,
            Electric_Utility VARCHAR,
            y_2020_Census_Tract UBIGINT
        );
        """
        self._conn.execute(create_table)
        dataFrame = pandas.DataFrame.from_dict(dtDict)
        self._conn.execute(
            "INSERT INTO electric_vehicles_population SELECT * FROM dataFrame"
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._conn.close()
        delattr(self, "_conn")

    # Count the number of electric cars per city.
    # Returns a dict of city to number of electric cars in it.
    def count_cars_by_city(self) -> Dict[str, int]:
        query = """
        SELECT
            City,
            COUNT(*) as Count
        FROM electric_vehicles_population
        GROUP BY City
        ORDER BY Count DESC;
        """
        df = self._conn.execute(query).fetchdf()
        return dict(zip(df["City"], df["Count"]))

    # Find the top 3 most popular electric vehicles.
    # Returns a list of the top 3 popular electric vehicles. Each item is a tuple containing the
    # rank (1, 2 or 3), the vehicle and the count. If there are ties for a given rank, multiple
    # items will be returned for it.
    def top_3_electric_vehicles(self) -> List[Tuple[int, str, int]]:
        query = """
        WITH vehicles_rank AS (
            SELECT
                DENSE_RANK() OVER (
                    ORDER BY COUNT(*) DESC
                ) as Rank,
                Vehicle,
                COUNT(*) AS Count,
            FROM electric_vehicles_population
            GROUP BY Vehicle
        )
        SELECT Rank, Vehicle, Count
        FROM vehicles_rank
        WHERE Rank <= 3
        ORDER BY Rank, Vehicle ASC;
        """
        df = self._conn.execute(query).fetchdf()
        return list(
            zip(
                tuple(map(int, df["Rank"])),
                tuple(map(str, df["Vehicle"])),
                tuple(map(int, df["Count"])),
            )
        )

    # Find the most popular electric vehicle in each postal code.
    # Returns a dict of the postal code to a list of tuples for the most popular vehicle. The tuple
    # holds the vehicle name and the count. When there are no ties, this list will contain a single
    # item, when there are ties, it'll contain all tied vehicles.
    def most_popular_by_postal_code(self) -> Dict[str, List[Tuple[str, int]]]:
        query = """
        WITH postal_code_vehicle_rank AS (
            SELECT
                DENSE_RANK() OVER (
                    PARTITION BY Postal_Code
                    ORDER BY COUNT(*) DESC
                ) AS Rank,
                Postal_Code,
                Vehicle,
                COUNT(*) AS Count,
            FROM electric_vehicles_population
            GROUP BY
                Postal_Code,
                Vehicle
        )
        SELECT
            Postal_Code,
            Vehicle,
            Count
        FROM postal_code_vehicle_rank
        WHERE Rank = 1
        ORDER BY
            Postal_Code,
            Vehicle
        """
        df = self._conn.execute(query).fetchdf()
        ret: Dict[str, List[Tuple[str, int]]] = defaultdict(list)
        for value in df.values:
            ret[value[0]].append((value[1], value[2]))
        return ret

    # Count the number of electric cars by model year.
    # Write out the answer as parquet files partitioned by year.
    def count_cars_by_year(self, prefix: str) -> Dict[str, int]:
        query = """
        SELECT Model_Year, COUNT(*) as Count
        FROM electric_vehicles_population
        GROUP BY Model_Year
        """
        df = self._conn.execute(query).fetchdf()

        copy_to_parquet = f"""
        COPY (
            SELECT * FROM df
        ) TO {repr(prefix)} (FORMAT PARQUET, PARTITION_BY (Model_Year));
        """
        self._conn.execute(copy_to_parquet)

        return dict(zip(df["Model_Year"], df["Count"]))
