from electric_vehicle_population import ElectricVehiclePopulation
from typing import Optional


def main():
    with ElectricVehiclePopulation("data/Electric_Vehicle_Population_Data.csv") as evp:
        print("Count the number of electric cars per city.")
        for city, count in evp.count_cars_by_city().items():
            print(f"  {city}: {count}")

        print("Find the top 3 most popular electric vehicles.")
        last_rank: Optional[int] = None
        for row in evp.top_3_electric_vehicles():
            rank, vehicle, count = row[0], row[1], row[2]
            if rank != last_rank:
                print(f"  #{rank}")
                last_rank = rank
            print(f"    {vehicle} ({count})")

        print("Find the most popular electric vehicle in each postal code.")
        for postal_code, vehicles in evp.most_popular_by_postal_code().items():
            vehicles_str: str = ", ".join(
                list(map(lambda v: f"{v[0]} ({v[1]})", vehicles))
            )
            print(f"  {postal_code}: {vehicles_str}")

        print("Count the number of electric cars by model year.")
        print("Write out the answer as parquet files partitioned by year.")
        for year, count in evp.count_cars_by_year("count_cars_by_year").items():
            print(f"  {year}: {count}")


if __name__ == "__main__":
    main()
