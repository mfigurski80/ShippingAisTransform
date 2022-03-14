import csv
import numpy as np
from forEach import forEachDataset, getFromZip


def findUniqueShips(in_f, out_w):
    r = csv.reader(in_f)
    uniq = set({})
    to_pull = np.array([0, 7, 10, 12, 13, 15])
    headers = np.array(next(r))
    out_w.writerow(headers[to_pull])
    for row in r:
        if int("0" + row[10]) > 70:  # if cargo or tanker...
            row = np.array(row)
            if row[0] not in uniq:
                uniq.add(row[0])
                out_w.writerow(row[to_pull])


if __name__ == "__main__":
    print("HIIII")
    #  getFromZip(
    #  "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/AIS_2018_01_01.zip"
    #  )
    in_f = open("AIS_2018_01_01.csv")
    ships_f = open("ships_2018_01_01.csv", "w")
    findUniqueShips(in_f, csv.writer(ships_f))
    in_f.close()
    ships_f.close()
