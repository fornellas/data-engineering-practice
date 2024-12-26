from typing import Tuple, List
from electric_vehicle_population import ElectricVehiclePopulation
import tempfile
import os
import pytest
import duckdb

csv_contents = """VIN (1-10),County,City,State,Postal Code,Model Year,Make,Model,Electric Vehicle Type,Clean Alternative Fuel Vehicle (CAFV) Eligibility,Electric Range,Base MSRP,Legislative District,DOL Vehicle ID,Vehicle Location,Electric Utility,2020 Census Tract
5YJ3E1EB4L,Yakima,Yakima,WA,98908,2020,TESLA,MODEL 3,Battery Electric Vehicle (BEV),Clean Alternative Fuel Vehicle Eligible,322,0,14,127175366,POINT (-120.56916 46.58514),PACIFICORP,53077000904
5YJ3E1EA7K,San Diego,San Diego,CA,98908,2019,TESLA,MODEL 3,Battery Electric Vehicle (BEV),Clean Alternative Fuel Vehicle Eligible,220,0,,266614659,POINT (-117.16171 32.71568),,06073005102
7JRBR0FL9M,Lane,Eugene,OR,98908,2021,VOLVO,S60,Plug-in Hybrid Electric Vehicle (PHEV),Not eligible due to low battery range,22,0,,144502018,POINT (-123.12802 44.09573),,41039002401
5YJXCBE21K,Yakima,Yakima,WA,98909,2019,TESLA,MODEL X,Battery Electric Vehicle (BEV),Clean Alternative Fuel Vehicle Eligible,289,0,14,477039944,POINT (-120.56916 46.58514),PACIFICORP,53077000401
5UXKT0C5XH,Snohomish,Bothell,WA,98909,2017,BMW,X5,Plug-in Hybrid Electric Vehicle (PHEV),Not eligible due to low battery range,14,0,1,106314946,POINT (-122.18384 47.8031),PUGET SOUND ENERGY INC,53061051918
1N4AZ0CP4F,Snohomish,Everett,WA,98201,2015,NISSAN,LEAF,Battery Electric Vehicle (BEV),Clean Alternative Fuel Vehicle Eligible,84,0,38,107901699,POINT (-122.20596 47.97659),PUGET SOUND ENERGY INC,53061040500
5YJ3E1EBXJ,Kitsap,Poulsbo,WA,98201,2018,TESLA,MODEL 3,Battery Electric Vehicle (BEV),Clean Alternative Fuel Vehicle Eligible,215,0,23,475036313,POINT (-122.64681 47.73689),PUGET SOUND ENERGY INC,53035940100
WDC0G5EB0K,Yakima,Naches,WA,98937,2019,MERCEDES-BENZ,GLC-CLASS,Plug-in Hybrid Electric Vehicle (PHEV),Not eligible due to low battery range,10,0,14,338148968,POINT (-120.69972 46.7309),PACIFICORP,53077003002
1N4AZ0CP3D,Kitsap,Port Orchard,WA,98366,2013,NISSAN,LEAF,Battery Electric Vehicle (BEV),Clean Alternative Fuel Vehicle Eligible,75,0,26,249239623,POINT (-122.63847 47.54103),PUGET SOUND ENERGY INC,53035092200"""

csv_contents_bad_postal_code = """VIN (1-10),County,City,State,Postal Code,Model Year,Make,Model,Electric Vehicle Type,Clean Alternative Fuel Vehicle (CAFV) Eligibility,Electric Range,Base MSRP,Legislative District,DOL Vehicle ID,Vehicle Location,Electric Utility,2020 Census Tract
5YJ3E1EB4L,Yakima,Yakima,WA,ABCD,2020,TESLA,MODEL 3,Battery Electric Vehicle (BEV),Clean Alternative Fuel Vehicle Eligible,322,0,14,127175366,POINT (-120.56916 46.58514),PACIFICORP,53077000904"""


# Context manager that creates a named temporary file with given contents
class TmpFileWithContents:
    _contents: str

    def __init__(self, contents: str):
        self._contents = contents

    def __enter__(self) -> str:
        self.temp_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        self.temp_file.write(self._contents)
        self.temp_file.close()
        return self.temp_file.name

    def __exit__(self, *args):
        os.unlink(self.temp_file.name)


class TestElectricVehiclePopulation:

    def test_load_csv(self):
        with TmpFileWithContents(csv_contents) as csv_path:
            with ElectricVehiclePopulation(csv_path):
                pass

        with TmpFileWithContents(csv_contents_bad_postal_code) as csv_path:
            with pytest.raises(ValueError):
                with ElectricVehiclePopulation(csv_path):
                    pass

    def test_count_cars_by_city(self):
        with TmpFileWithContents(csv_contents) as csv_path:
            with ElectricVehiclePopulation(csv_path) as evp:
                result = evp.count_cars_by_city()
                assert result == {
                    "Bothell": 1,
                    "Eugene": 1,
                    "Everett": 1,
                    "Naches": 1,
                    "Port Orchard": 1,
                    "Poulsbo": 1,
                    "San Diego": 1,
                    "Yakima": 2,
                }

    def test_top_3_electric_vehicles(self):
        with TmpFileWithContents(csv_contents) as csv_path:
            with ElectricVehiclePopulation(csv_path) as evp:
                result = evp.top_3_electric_vehicles()
                assert result == [
                    (1, "TESLA MODEL 3", 3),
                    (2, "NISSAN LEAF", 2),
                    (3, "BMW X5", 1),
                    (3, "MERCEDES-BENZ GLC-CLASS", 1),
                    (3, "TESLA MODEL X", 1),
                    (3, "VOLVO S60", 1),
                ]

    def test_most_popular_by_postal_code(self):
        with TmpFileWithContents(csv_contents) as csv_path:
            with ElectricVehiclePopulation(csv_path) as evp:
                result = evp.most_popular_by_postal_code()
                assert result == {
                    98201: [("NISSAN LEAF", 1), ("TESLA MODEL 3", 1)],
                    98366: [("NISSAN LEAF", 1)],
                    98908: [("TESLA MODEL 3", 2)],
                    98909: [("BMW X5", 1), ("TESLA MODEL X", 1)],
                    98937: [("MERCEDES-BENZ GLC-CLASS", 1)],
                }

    def test_count_cars_by_year(self):
        expectation_list: List[Tuple[int, int]] = [
            (2013, 1),
            (2015, 1),
            (2017, 1),
            (2018, 1),
            (2019, 3),
            (2020, 1),
            (2021, 1),
        ]
        with TmpFileWithContents(csv_contents) as csv_path:
            with tempfile.TemporaryDirectory() as prefix:
                with ElectricVehiclePopulation(csv_path) as evp:
                    result = evp.count_cars_by_year(prefix)
                    assert result == dict(expectation_list)
                with duckdb.connect() as conn:
                    glob: str = f"{prefix}/Model_Year=*/*.parquet"
                    df = conn.execute(
                        f"SELECT * FROM read_parquet({repr(glob)})"
                    ).fetchdf()
                    assert (
                        list(
                            zip(
                                df["Model_Year"],
                                df["Count"],
                            )
                        )
                        == expectation_list
                    )
