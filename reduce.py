import csv
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


def writeFilteredCargoShips(r, out_w, uniq: set, ship_w):
    headers = next(r)
    out_w.writerow(
        (
            headers[0],
            headers[1],
            headers[2],
            headers[3],
            headers[4],
            headers[5],
            headers[6],
            headers[15],
            "Near",
            "Distance",
        )
    )
    for row in r:
        if row[10] != "" and int(row[10]) > 70:

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
                    row[2],
                    row[3],
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
    writeFilteredCargoShips(csv.reader(in_f), out_w, uniq, ship_w)
    in_f.close()


def buildFullDataset():
    out_f = open("FILTERED_AIS.csv", "w")
    ships_f = open("ships.csv", "w")
    uniq = set({})
    forEachCSV(
        partial(
            processCSV, out_w=csv.writer(out_f), uniq=uniq, ship_w=csv.writer(ships_f)
        )
    )
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
