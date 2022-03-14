import csv
import numpy as np
from forEach import forEachDataset, getFromZip


def findUniqueShips(in_f, out_w):
    r = csv.reader(in_f)
    uniq = set({})
    headers = next(r)
    out_w.writerow(
        (headers[0], headers[7], headers[10], headers[12], headers[13], headers[15])
    )
    for row in r:
        if int("0" + row[10]) > 70:  # if cargo or tanker...
            if row[0] not in uniq:
                uniq.add(row[0])
                out_w.writerow((row[0], row[7], row[10], row[12], row[13], row[15]))


if __name__ == "__main__":
    #  getFromZip(
    #  "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/AIS_2018_01_01.zip"
    #  )
    in_f = open("AIS_2018_01_01.csv")
    ships_f = open("ships_2018_01_01.csv", "w")
    findUniqueShips(in_f, csv.writer(ships_f))
    in_f.close()
    ships_f.close()
