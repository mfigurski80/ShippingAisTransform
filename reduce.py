import csv
from forEach import forEachDataset, getFromZip


def findUniqueShips(in_f, out_w):
    r = csv.reader(in_f)
    uniq = set({})
    headers = next(r)
    out_w.writerow(
        (headers[0], headers[7], headers[10], headers[12], headers[13], headers[15])
    )
    for row in r:
        if int("0" + row[10]) > 70:  # if cargo or oil...
            if row[0] not in uniq:
                uniq.add(row[0])
                out_w.writerow((row[0], row[7], row[10], row[12], row[13], row[15]))


def filterCargoShips(r, out_w, uniq: set, ship_w):
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
        )
    )
    for row in r:
        if row[10] != "" and int(row[10]) > 70:
            out_w.writerow(
                (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[15])
            )
            if row[0] not in uniq:
                uniq.add(row[0])
                ship_w.writerow((row[0], row[7], row[10], row[12], row[13]))


if __name__ == "__main__":
    #  getFromZip(
    #  "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/AIS_2018_01_01.zip"
    #  )
    in_f = open("AIS_2018_01_01.csv")
    out_f = open("FILTERED_AIS.csv", "w")
    ships_f = open("ships_2018_01_01.csv", "w")
    uniq = set({})
    filterCargoShips(csv.reader(in_f), csv.writer(out_f), uniq, csv.writer(ships_f))
    #  findUniqueShips(in_f, csv.writer(ships_f))

    in_f.close()
    out_f.close()
    ships_f.close()
