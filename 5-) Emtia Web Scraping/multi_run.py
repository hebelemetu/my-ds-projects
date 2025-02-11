import multiprocessing
import os
import time
from oop_scrape import  Scrapes

scraper = Scrapes()


def api2_ice():
    scraper.api2_ice()


def api2_mw():
    scraper.api2_mw()


def api2_barchart():
    scraper.api2_barchart()


def bp_mw():
    scraper.bp_mw()


def bp_barchart():
    scraper.bp_barchart()


def bp_ice():
    scraper.bp_ice()

def ttf_ice():
    scraper.ttf_ice()


def ttf_barchart():
    scraper.ttf_barchart()


if __name__ == "__main__":
    scrapers = [api2_ice,api2_mw,api2_barchart,bp_ice,bp_barchart,bp_mw,ttf_barchart,ttf_ice]
    processes = []
    for task in scrapers:
        p = multiprocessing.Process(target=task)
        p.start()
        processes.append(p)

    # Joins all the processes
    for p in processes:
        p.join()