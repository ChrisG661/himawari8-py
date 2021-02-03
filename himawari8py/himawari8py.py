#!/usr/bin/env python

import os
import requests
import datetime
import dateutil
import dateutil.parser
import itertools
import multiprocessing
import multiprocessing.dummy

from PIL import Image
from tqdm import tqdm

IMAGE = "D531106"
HIMAWARI = "himawari8-dl.nict.go.jp"
BASE_URL = f"https://{HIMAWARI}/himawari8/img/{IMAGE}"


def latestdate(retries=10):
    """
    Parameters
        - retries: Number of retries on requests

    Return: 
        Retrieves latest available date of the last satellite image
    """

    session = requests.Session()
    for _ in range(retries):
        try:
            response = session.get(f"{BASE_URL}/latest.json")
            if response.status_code != 200:
                continue
            date = datetime.datetime.strptime(
                response.json().get("date"), "%Y-%m-%d %H:%M:%S")
            session.close()
            return date
        except:
            continue
    session.close()
    raise Exception("Failed to connect to server: Connection timed out.")


def get_tile(x, y, level, date, retries=10):
    """
    Parameters
        - x: horizontal position of tile
        - y: vertical position of tile
        - date: Image date in GMT/UTC
        - level: 
        - retries: Number of retries on requests 

    Return: 
        Tile of image taken from planet Earth of the Japanese satellite Himawari 8
    """

    url = format_url(x, y, level, date)
    session = requests.Session()
    for _ in range(retries):
        try:
            response = session.get(url, stream=True)
            if response.status_code != 200:
                continue
            session.close()
            return x, y, Image.open(response.raw)
        except:
            continue
    session.close()
    raise Exception(f"Failed to connect to server: Response {response.status_code}")


def format_url(x, y, level, date):
    """
    Parameters
        - level: Zoom level of image or tile multiplier: 1, 2, 4, 8, 16, 20
        - date: Date of tile
        - x: horizontal position of tile
        - v: vertical position of tile

    Return:
        URL string of a tile 
    """

    return f"""{BASE_URL}/{level}d/550/{date.strftime("%Y/%m/%d/%H%M%S")}_{x}_{y}.png"""


def __get_tile_thread(args):
    return get_tile(*args)


def get_image(date=None, scale=550, level=4, retries=10, multithread=True, nthread=None,
              save_img=False, img_path="", img_name="himawari.png", show_progress=False):
    """
    Parameters
        - date: Image date in GMT/UTC
        - scale: Resolution of each tile in pixel
        - level: Zoom level of image or Tile multiplier: 1, 2, 4, 8, 16, 20
        - retries: Number of retries on requests
        - multithread: Enable tile download multithreading
        - nthread: Number of thread to allocate
        - save_img: Save image option. Function will not return if True
        - img_path: Path where the image will be saved
        - img_name: Name of the image if it is saved
        - show_progress: Show progress bar

    Return:
        Complete image from Himawari 8
    """

    date = _parsedate(date, retries)

    path = os.path.join(img_path, img_name)
    imgsize = (scale * level, scale * level)
    image = Image.new("RGB", imgsize)

    if multithread:
        pool = multiprocessing.dummy.Pool(
            level * level if nthread is None else nthread)
        result = list(tqdm(
            pool.imap_unordered(__get_tile_thread,
                                itertools.product(range(level), range(level), (level,), (date,))),
            total=level*level, unit="tile", desc="Downloading tiles ", disable=not show_progress))
        pool.close()
        pool.join()

    elif not multithread:
        result = list()
        for x, y, d, l, r in itertools.product(range(level), range(level), (date,), (level,), (retries,)):
            result.append(get_tile(x, y, d, l, r))

    for (x, y, tile) in tqdm(iterable=result, unit="tile", desc="Stitching tiles   ", disable=not show_progress):
        box = tuple(n * scale for n in (x, y))
        image.paste(tile.resize((scale, scale), Image.BILINEAR), box)

    if save_img:
        image.save(path)
    else:
        return image


def get_images(start, finish, save_img=False, img_path="", prefix="himawari8", img_name="{prefix}_{date}.png",
               show_progress=False, scale=550, level=4, retries=10, multithread=True, nthread=None):
    """
    Parameters
        - start: Start date in GMT/UTC
        - finish: End date in GMT/UTC
        - save_img: Save image option
        - img_path: Path where the image will be saved
        - prefix: Image name prefix
        - img_name: Name of the image if it is saved
        - show_progress: Show progress bar
        - scale: Resolution of each tile in pixel
        - level: Zoom level of image or Tile multiplier: 1, 2, 4, 8, 16, 20
        - retries: Number of retries on requests
        - multithread: Enable tile download multithreading
        - nthread: Number of thread to allocate

    Return:
        List of images between start and finish from Himawari 8
    """

    dates = [date for date in daterange(start, finish)]
    return [get_image(
            scale=scale,
            level=level,
            retries=retries,
            multithread=multithread,
            nthread=nthread,
            save_img=save_img,
            img_path=img_path,
            img_name=img_name.replace("{prefix}", prefix).replace(
                "{date}", date.strftime("%Y-%m-%d_%H%M%S")),
            show_progress=False,
            date=date)
            for date in tqdm(iterable=dates, unit="image", desc=f"Downloading {len(dates)} images", disable=not show_progress)]


def daterange(start, finish, increment=10):
    """
    Parameters
        - start: Start date
        - finish: End date
        - increment: Increment in minute

    Return: 
        range of date between start and finish dates
    """

    start = _parsedate(start)
    finish = _parsedate(finish)

    if finish < start:
        raise Exception(
            f"End date {start} should be less than start date {finish}")

    base = datetime.datetime(
        start.year, start.month, start.day, start.hour, round(start.minute/10)*10, 0, 0)
    total_minutes = (finish - start).total_seconds()/60
    for i in range(int(total_minutes/increment)):
        yield (base + datetime.timedelta(minutes=i * increment))


def _parsedate(date, retries=10):
    if isinstance(date, str):
        date = dateutil.parser.parse(date)
    elif isinstance(date, datetime.datetime):
        date = date
    elif date is None:
        date = latestdate(retries)
    return date
