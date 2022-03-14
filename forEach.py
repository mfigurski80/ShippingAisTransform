from datetime import datetime, timedelta
import requests
import zipfile
from io import BytesIO
import os
from multiprocessing import Pool, cpu_count


def getFromZip(url: str):
    # example: https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/AIS_2018_01_01.zip
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    z = zipfile.ZipFile(BytesIO(resp.content))
    files = z.namelist()
    if len(files) > 1:
        raise OSError(f"Extracted zipfile with multiple files: {files}")
    z.extractall()
    z.close()
    return files[0]


def makeUrlFor(t):
    return f"https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{t.year}/AIS_{t.year}_{str(t.month).zfill(2)}_{str(t.day).zfill(2)}.zip"


def __processFunc(url):
    try:
        print(url)
        savedName = getFromZip(url)
        action(savedName)
        os.remove(savedName)
        print(f"Done with {savedName}")
    except requests.exceptions.RequestsException as e:
        print(f"Failed for {url}")


def forEachCSV(action, startTime=datetime(2018, 1, 1), endTime=datetime.now()):
    baseurl = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler"
    print(f"{startTime.date()} -> {endTime.date()}")
    if startTime < datetime(2018, 1, 1):
        print("Cannot get datasets before 2018")
        return
    n = (endTime - startTime).days
    urls = [makeUrlFor(startTime + timedelta(days=i)) for i in range(n)]

    pool = Pool(cpu_count())

    res = pool.map(__processFunc, urls)
    pool.close()
    pool.join()
    return


if __name__ == "__main__":
    print("Testing dataset download...\n")

    forEachCSV(print, datetime(2021, 1, 1))
