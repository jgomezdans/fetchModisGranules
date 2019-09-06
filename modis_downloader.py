#!/usr/bin/env python
# modis_downloader A MODIS land product downloading tool
# Copyright (c) 2013-2019 J Gomez-Dans. All rights reserved.
#
# This file is part of modis_downloader.
#
# modis_downloader is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# modis_downloader is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with modis_downloader.  If not, see <http://www.gnu.org/licenses/>.
import optparse
import os
import datetime
import time
import re

import requests

from pathlib import Path
from concurrent import futures
from functools import partial

import logging

__author__ = "J Gomez-Dans"
__copyright__ = "Copyright 2013-2019 J Gomez-Dans"
__version__ = "2.0.0"
__license__ = "GPLv3"
__email__ = "j.gomez-dans@ucl.ac.uk"

HELP_TEXT = """
SYNOPSIS
./modis_downloader.py 
DESCRIPTION
A program to download MODIS data from the USGS website using the HTTP
transport. This program is able to download daily, monthly, 8-daily, etc
products for a given year, it only requires the product names (including the
collection number), the year, the MODIS reference tile and additionally, where
to save the data to, and whether to verbose. The user may also select a
temporal period in terms of days of year. Note that as of summer 2016, NASA
requires that all downloads are identified with a username and password.
EXAMPLES
./modis_downloader.py -u <username> -P <password> -v \
          -p MOD13A3.006 -t h17v04,h17v05 -o . -b 2018-01-01 -e 2018-02-01
Download the MODIS monthly product for tiles h17v04, h17v05 to the current
folder between Jan 1st and Feb 1st 2018

EXIT STATUS
    No exit status yet, can't be bothered.
AUTHOR
    J Gomez-Dans <j.gomez-dans@ucl.ac.uk>
    See also http://github.com/jgomezdans/get_modis/
"""


logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%s',
                    )
LOG = logging.getLogger(__name__)
BASE_URL = "http://e4ftl01.cr.usgs.gov/"

product_regex = r"M[CYO]D\d\dA.+?(?=.)\d\d\d"

class WebError(RuntimeError):
    """An exception for web issues"""

    def __init__(self, arg):
        self.args = arg


def get_available_dates(url, start_date, end_date=None):
    """
    This function gets the available dates for a particular
    product, and returns the ones that fall within a particular
    pair of dates. If the end date is set to ``None``, it will
    be assumed it is today.
    """
    if end_date is None:
        end_date = datetime.datetime.now()
    r = requests.get(url)
    if not r.ok:
        raise WebError(
            "Problem contacting NASA server. Either server "
            + "is down, or the product you used (%s) is kanckered" % url
        )
    html = r.text
    avail_dates = []
    for line in html.splitlines()[19:]:
        if line.find("[DIR]") >= 0 and line.find("href") >= 0:
            this_date = line.split("href=")[1].split('"')[1].strip("/")
            this_datetime = datetime.datetime.strptime(this_date, "%Y.%m.%d")
            if this_datetime >= start_date and this_datetime <= end_date:
                avail_dates.append(url + "/" + this_date)
    return avail_dates


def download_granule_list(url, tiles):
    """For a particular product and date, obtain the data granule URLs.

    """
    
    if not isinstance(tiles, type([])):
        tiles = [tiles]
    while True:
        try:
            r = requests.get(url)
            break
        except requests.execeptions.ConnectionError:
            time.sleep(240)

    grab = []
    for line in r.text.splitlines():
        for tile in tiles:
            if (
                line.find(tile) >= 0
                and line.find(".xml") < 0
                and line.find("BROWSE") < 0
            ):
                fname = line.split("href=")[1].split('"')[1]
                grab.append(url + "/" + fname)
    return grab


def download_granules(url, session, username, password, output_dir):

    r1 = session.request("get", url)
    r = session.get(r1.url, stream=True)
    fname = url.split("/")[-1]
    LOG.debug("Getting %s from %s(-> %s)" % (fname, url, r1.url))
    if not r.ok:
        raise IOError("Can't start download... [%s]" % fname)
    file_size = int(r.headers["content-length"])
    LOG.debug("\t%s file size: %d" % (fname, file_size))
    output_fname = os.path.join(output_dir, fname)
    # Save with temporary filename...
    with open(output_fname + ".partial", "wb") as fp:
        for block in r.iter_content(65536):
            fp.write(block)
    # Rename to definitive filename
    os.rename(output_fname + ".partial", output_fname)
    LOG.info("Done with %s" % output_fname)
    return output_fname


def required_files(url_list, output_dir):
    """Checks for files that are already available in the system."""

    all_files_present = os.listdir(output_dir)
    hdf_files_present = [
        fich for fich in all_files_present if fich.endswith(".hdf")
    ]
    hdf_files_present = set(hdf_files_present)

    flist = [url.split("/")[-1] for url in url_list]
    file_list = dict(list(zip(flist, url_list)))

    flist = set(flist)
    files_to_download = list(flist.difference(hdf_files_present))
    to_download = [file_list[k] for k in files_to_download]
    return to_download


def get_modis_data(
    username,
    password,
    platform,
    product,
    tiles,
    output_dir,
    start_date,
    end_date=None,
    n_threads=5,
):
    """The main workhorse of MODIS downloading. This function will grab
    products for a particular platform (MOLT, MOLA or MOTA). The products
    are specified by their MODIS code (e.g. MCD45A1.051 or MOD09GA.006).
    You need to specify a tile (or a list of tiles), as well as a starting
    and end date. If the end date is not specified, the current date will
    be chosen. Additionally, you can specify the number of parallel threads
    to use. And you also need to give an output directory to dump your files.

    Parameters
    -----------
    usearname: str
        The username that is required to download data from the MODIS archive.
    password: str
        The password required to download data from the MODIS archive.
    platform: str
        The platform, MOLT, MOLA or MOTA. This basically relates to the sensor
        used (or if a combination of AQUA & TERRA is used)
    product: str
        The MODIS product. The product name should be in MODIS format
        (MOD09Q1.006, so product acronym dot collection)
    tiles: str or iter
        A string with a single tile (e.g. "h17v04") or a lits of such strings.
    output_dir: str
        The output directory
    start_date: datetime
        The starting date as a datetime object
    end_date: datetime
        The end date as a datetime object. If not specified, taken as today.
    n_threads: int
        The number of concurrent downloads to envisage. I haven't got a clue
        as to what a good number would be here...

    """
    # Ensure the platform is OK
    assert platform.upper() in ["MOLA", "MOLT", "MOTA"], (
        "%s is not a valid platform. Valid ones are MOLA, MOLT, MOTA" % platform
    )
    # If output directory doesn't exist, create it
    LOG.debug(f"Creating output folder {output_dir:s}")
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    # Cook the URL for the product
    url = BASE_URL + platform + "/" + product
    # Get all the available dates in the NASA archive...
    LOG.debug("Getting available dates from NASA server...")
    the_dates = get_available_dates(url, start_date, end_date=end_date)
    LOG.debug(f"{len(the_dates):d} available...")
    LOG.debug(f"First:{str(the_dates[0]):s}")
    LOG.debug(f"Last:{str(the_dates[-1]):s}")
    # We then explore the NASA archive for the dates that we are going to
    # download. This is done in parallel. For each date, we will get the
    # url for each of the tiles that are required.
    the_granules = []
    download_granule_patch = partial(download_granule_list, tiles=tiles)
    with futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
        for granules in executor.map(download_granule_patch, the_dates):
            the_granules.append(granules)
    # Flatten the list of lists...
    gr = [g for granule in the_granules for g in granule]
    gr.sort()
    LOG.info(f"Found {len(gr):d} remote granules.")
    # Check whether we have some files available already
    gr_to_dload = required_files(gr, output_dir)
    gr = gr_to_dload
    req_fnames = [fich.split("/")[-1] for fich in gr]
    LOG.info("Will download %d files" % len(gr))
    if len(gr) == 0:
        LOG.info("Done")
        return []
    have_all_files = False
    while not have_all_files:
        # Wait for a few minutes before downloading the data
        time.sleep(10)
        # The main download loop. This will get all the URLs with the filenames,
        # and start downloading them in parallel.
        dload_files = []
        with requests.Session() as s:
            s.auth = (username, password)
            download_granule_patch = partial(
                download_granules,
                session=s,
                output_dir=output_dir,
                username=username,
                password=password,
            )

            with futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
                for fich in executor.map(download_granule_patch, gr):
                    dload_files.append(fich)
        
        gotten_files = [Path(fich).name for fich in dload_files]
        if all(fich in gotten_files  for fich in req_fnames):
            have_all_files = True
            LOG.info(f"{len(gotten_files):d} were successfully downloaded!")
        else:
            raise IOError("Not all files were succesfully downloaded!\n" +
                    "Try the command again to complete file downloading")
        
    return dload_files

def main():
    parser = optparse.OptionParser(formatter=optparse.TitledHelpFormatter(),
                                   usage=HELP_TEXT)
    parser.add_option('-u', '--username', action="store", dest="username",
                      help="EarthData username")
    parser.add_option('-P', '--password', action="store", dest="password",
                      help="EarthData password")
    parser.add_option('-v', '--verbose', action='store_true',
                      default=False, help='verbose output')
    parser.add_option('-p', '--product', action='store', dest="product",
                      type=str,
                      help="MODIS product name with collection tag at the end " +
                           "(e.g. MOD09GA.005)")
    parser.add_option('-t', '--tiles', action="store", dest="tiles",
                      type=str, help="Required tiles (h17v04, for example" + 
                      " or h17v04,h17v05 for several")
    parser.add_option('-o', '--output', action="store", dest="dir_out",
                      default=".", type=str, help="Output directory")
    parser.add_option('-b', '--begin', action="store", dest="start_date",
                      type=str, help="Start date (YYYY-MM-DD)")
    parser.add_option('-e', '--end', action="store", dest="end_date",
                      type=str, default=None, help="End date (YYYY-MM-DD)")
    parser.add_option ('-x', '--xml', action="store_true", dest="get_xml",
                     default=False,
                     help="Get the XML metadata files too.")
    (options, args) = parser.parse_args()
    if options.verbose:
        LOG.setLevel(logging.DEBUG)
    else:
        LOG.setLevel(logging.INFO)
    if 'username' not in options.__dict__:
        parser.error("You need to provide a username! Sgrunt!")
    if 'password' not in options.__dict__:
        parser.error("You need to provide a password! Sgrunt!")
    product = options.product.upper()
    if re.match(product_regex, product) is None:
        raise ValueError("Product names should look like MCD43A1.006!")
    if options.product.find("MOD") >= 0:
        platform = "MOLT"
    elif options.product.find("MYD") >= 0:
        platform = "MOLA"
    elif options.product.find("MCD") >= 0:
        platform = "MOTA"
    else:
        raise ValueError(f"Your product name should be something")
    tiles = options.tiles.split(",")
    start_date = datetime.datetime(*list(map(int,
                                    options.start_date.split("-"))))
    if options.end_date is not None:
        end_date =  datetime.datetime(*list(map(int,
                                    options.end_date.split("-"))))
    LOG.info("MODIS downloader by J Gomez-Dans...")
    LOG.info("Starting downloading")
    dload_files = get_modis_data(
        options.username,
        options.password,
        platform,
        product,
        tiles,
        options.dir_out,
        start_date,
        end_date=end_date,
        n_threads=3,
    )

if __name__ == "__main__":
    main()