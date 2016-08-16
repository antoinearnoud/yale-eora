# -*- coding: utf-8 -*-
"""
Created on Fri Aug 07 13:35:59 2015

@author: Antoine
"""

import argparse
import logging
import os
import pkg_resources
import shutil
import sys
import urllib
import zipfile


from yale_eora.config import Config


log = logging.getLogger(__name__)

parser = Config(
    config_files_directory = os.path.join(pkg_resources.get_distribution('yale-eora').location)
    )

eora_directory = parser.get('data', 'eora_directory')
eora26_directory = parser.get('data', 'eora26_directory')
eora_summary_directory = parser.get('data', 'eora_summary_directory')
eora_output_directory = parser.get('data', 'eora_output_directory')


def getunzipped(theurl, thedir, thename):
    # download the file from theurl into thedir,
    # give it the name thename,
    # and unzip it in the same folder
    name = os.path.join(thedir, thename)
    # name is the path + name + extension of zip file
    if not os.path.exists(thedir):
        os.makedirs(thedir)
    try:
        name, hdrs = urllib.urlretrieve(theurl, name)
    except IOError as err:
        print("OS error: {0}".format(err))        
        #print "Can't retrieve %r to %r: %s" % (theurl, thedir, e)
        return
    try:
        z = zipfile.ZipFile(name)
    except zipfile.error as err:
        print("OS error: {0}".format(err))
        #print "Bad zipfile (from %r): %s" % (theurl, e)
        return
    z.close()

    with zipfile.ZipFile(name) as zip_file:
        for member in zip_file.namelist():
            filename = os.path.basename(member)
            # skip directories
            if not filename:
                continue  # go on with next iteration of the loop

            # copy file (taken from zipfile's extract)
            source = zip_file.open(member)
            #target = file(os.path.join(thedir, filename), "wb")
            target = open(os.path.join(thedir, filename), "wb")
            with source, target:
                shutil.copyfileobj(source, target)


def eora_downloader(year, price):
    theurl = 'http://www.worldmrio.com//ComputationsE/Phase199/Loop082/simplified/Eora26_{}_{}.zip'.format(year, price)
    thedir = os.path.join(eora26_directory, 'Eora26_{}_{}'.format(year, price))
    thename = 'Eora26_{}_{}.zip'.format(year, price)
    getunzipped(theurl, thedir, thename)


def eora_downloader_list(years, prices):
    for year in years:
        for price in prices:
            eora_downloader(year, price)


def eora_summary_downloader(year):  # THIS IS NOT WORKING. DOWNLOAD BAD ZIPFILE. DONWLOAD WORKS MANUALLY. WEIRD
    theurl = 'http://worldmrio.com/dl.jsp?aishadirf=true&file=Summary/{}/Summary_{}.zip'.format(year, year)
    thedir = os.path.join(eora_summary_directory, 'Eora_summary_{}'.format(year))
    thename = 'Summary_{}.zip'.format(year)
    getunzipped(theurl, thedir, thename)


def eora_summary_downloader_list(years):
    for year in years:
        eora_summary_downloader(year)

# eora_summary_downloader_list(years)  # not working for now


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--end', default = 2012, help = 'ending year to be downloaded')
    parser.add_argument('-s', '--start', default = 1970, help = 'starting year to be downloaded')
    parser.add_argument('-t', '--target', default = eora26_directory, help = 'path where to store downloaded files')
    parser.add_argument('-p', '--price', default = ['bp', 'pp'], help = 'prices to be downloaded')
    parser.add_argument('-v', '--verbose', action = 'store_true', default = False, help = "increase output verbosity")
    args = parser.parse_args()
    args.start = int(args.start)
    args.end = int(args.end)
    logging.basicConfig(level = logging.DEBUG if args.verbose else logging.WARNING, stream = sys.stdout)
    eora_downloader_list(years = range(args.start, args.end + 1), prices = args.price)


if __name__ == "__main__":
    sys.exit(main())
