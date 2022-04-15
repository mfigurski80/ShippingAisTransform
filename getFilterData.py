import csv
import os
from datetime import datetime, timedelta

from functools import partial
from forEach import forEachCSV, getFromZip
from distance import distance

PORTS = [
    (40.808, 124.163),  # Humbolt Bay
    (24.149, 119.208),  # Hueneme
    (33.754, 118.216),  # Long Beach -- seems to be most common
    (33.729, -118.262),  # Los Angeles
    (37.796, 122.279),  # Oakland
    (37.508, 122.209),  # Redwood City
    (37.913, 122.36),  # Richmond
    (32.735, 117.177),  # San Diego
    (37.796, 122.395),  # San Francisco
    (37.951, 121.327),  # Stockton
    (38.565, 121.549),  # West Sacramento
]


def initCSV(filename, columns) -> bool:  # return whether was initted
    if os.path.exists(filename) and os.stat(filename).st_size != 0:
        return False
    f = open(filename, "w")
    wr = csv.writer(f)
    wr.writerow(columns)
    f.close()
    return True


def readLastLine(filename) -> str:
    with open(filename, "rb") as f:
        try:  # catch OSError in case of a one line file
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b"\n":
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            f.seek(0)
        last_line = f.readline().decode()
    return last_line


def readShipSet(filename) -> set:
    with open(filename, "r") as f:
        r = csv.reader(f)
        next(r)
        return {i[0] for i in r}


def writeFilteredCargoShips(r, out_w, uniq: set, ship_w):
    """Main function that iterates over rows of data and writes the ones it wants to
    output csvs. Takes csv readers and writers (and a set for storing unique ships)
    """
    headers = next(r)
    for row in r:
        try:
            typ = int(row[10])
        except:
            continue
        if typ > 70 and typ < 90:

            lo = float(row[2])
            la = float(row[3])
            d = [distance(lo, la, p[0], p[1]) for p in PORTS]
            min_index = min(range(len(d)), key=d.__getitem__)
            if d[min_index] > 200:  # filter more than 200km away
                continue
            out_w.writerow(
                (
                    row[0],
                    row[1],
                    round(lo, 2),  #  row[2],
                    round(la, 2),  #  row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[15],
                    min_index,
                    round(d[min_index], 2),
                )
            )
            if row[0] not in uniq:
                uniq.add(row[0])
                ship_w.writerow((row[0], row[7], row[10], row[12], row[13]))


def processCSV(filename, out_w, uniq: set, ship_w):
    in_f = open(filename)
    print(f"Processing CSV: {filename}")
    writeFilteredCargoShips(csv.reader(in_f), out_w, uniq, ship_w)
    in_f.close()


def buildFullDataset():

    # set up initial variables
    start_time = datetime(2018, 1, 1)
    end_time = datetime(2022, 1, 1)

    # set up aggregate dataset
    agg_fname = "AGGREGATE_AIS.csv"
    agg_cols = [
        "MMSI",
        "Time",
        "Lat",
        "Lon",
        "SOG",
        "COG",
        "Heading",
        "Cargo",
        "Near",
        "Distance",
    ]

    didInitAggregate = initCSV(agg_fname, agg_cols)
    if not didInitAggregate:
        last = readLastLine(agg_fname).strip().split(",")
        if last[1] != agg_cols[1]:
            start_time = datetime.strptime(last[1], "%Y-%m-%dT%H:%M:%S") + timedelta(
                days=1
            )
            print(f"FOUND data up to: {start_time.date()}")
    out_f = open(agg_fname, "a")

    # set up unique ships dataset
    uniq = set({})
    ship_fname = "ships.csv"
    ship_cols = ["MMSI", "Name", "Type", "Length", "Width"]
    didInitShips = initCSV(ship_fname, ship_cols)
    if not didInitShips:
        uniq = readShipSet(ship_fname)
        print(f"FOUND #{len(uniq)} existing ship entries")
    ships_f = open(ship_fname, "a")

    # continue building
    forEachCSV(
        partial(
            processCSV, out_w=csv.writer(out_f), uniq=uniq, ship_w=csv.writer(ships_f)
        ),
        start_time,
        end_time,
    )

    # clean up
    out_f.close()
    ships_f.close()


if __name__ == "__main__":
    #  getFromZip(
    #  "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/AIS_2018_01_01.zip"
    #  )
    buildFullDataset()

    #  in_fname = "AIS_2018_01_01.csv"
    #  out_f = open("FILTERED_AIS.csv", "w")
    #  ships_f = open("ships_2018_01_01.csv", "w")
    #  uniq = set({})
    #  processExcel(in_fname, csv.writer(out_f), uniq, csv.writer(ships_f))
    #  in_f.close()
    #  out_f.close()
    #  ships_f.close()
