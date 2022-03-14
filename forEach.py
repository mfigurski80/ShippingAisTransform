from datetime import datetime, timedelta
import requests
import zipfile
from io import BytesIO
import os


def getFromZip(url: str):
    # example: https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/AIS_2018_01_01.zip
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    z = zipfile.ZipFile(BytesIO(resp.content))
    files = z.namelist()
    if len(files) > 1:
        raise OSError(f"Extracted zipfile with multiple files: {files}")
    z.extractall()
    return files[0]


def forEachDataset(action, startTime=datetime(2018, 1, 1), endTime=datetime.now()):
    baseurl = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler"
    print(f"{startTime.date()} -> {endTime.date()}")
    if startTime < datetime(2018, 1, 1):
        print("Cannot get datasets before 2018")
        return
    curTime = startTime
    while curTime < endTime:
        url = f"{baseurl}/{curTime.year}/AIS_{curTime.year}_{str(curTime.month).zfill(2)}_{str(curTime.day).zfill(2)}.zip"
        print(f"{curTime.date()} : {url}")
        #  print(f"{curTime.day} {curTime.month} {curTime.year}")
        try:
            savedName = getFromZip(url)
            # DO SOMETHING...
            action(savedName)
            os.remove(savedName)
        except requests.exceptions.RequestException as e:
            print(f"Failed for {curTime}")
        curTime += timedelta(days=1)


if __name__ == "__main__":
    print("Testing dataset download...\n")

    def printSize(setName):
        print(setName)

    forEachDataset(
        printSize,
    )
