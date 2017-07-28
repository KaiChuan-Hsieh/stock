#!/usr/bin/env python
import sys
import traceback
import requests
import argparse
import os
import logging
import psycopg2
import getpass
from datetime import datetime
from bs4 import BeautifulSoup

def parser():
    parser = argparse.ArgumentParser(description='Create/Update U.S. yield table')
    parser.add_argument('dbname', type=str, help='DB name')
    parser.add_argument('-f', '--log-file', nargs='?', default='default', help='Enable logging to a file, omit default log file')
    parser.add_argument('-n', '--tbl-name', nargs='?', default='USTY', help='Storing table name')

    return parser

def get_USTY():
    url = 'https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Datasets/yield.xml'

    page = requests.get(url)

    if not page.ok:
        logging.error('Can\'t get USTY from %s: %s' % (url, page.reason))
        return None

    return page.content

def update_USTY_tbl(dbname, tbl_name, xml_doc):
    soup = BeautifulSoup(xml_doc, 'lxml-xml')
    dates = soup.find_all('G_NEW_DATE')

    conn = psycopg2.connect(database=dbname, user=getpass.getuser())
    cursor = conn.cursor()
    cmd = 'select exists ( select 1 from information_schema.tables where table_name = \'%s\' )' % tbl_name
    cursor.execute(cmd)
    rows = cursor.fetchall()
    for row in rows:
        if not row[0]:
            # not exist, create table
            cmd = 'create table "%s" ( date date, m1 real, m3 real, m6 real, y1 real, y2 real, y3 real, y5 real, y7 real, y10 real, y20 real, y30 real )' % tbl_name
            cursor.execute(cmd)
            conn.commit()

    for date in dates:
        date_str = date.find('BID_CURVE_DATE').get_text(strip=True)
        dt = datetime.strptime(date_str, '%d-%b-%y')
        dt_str = dt.strftime('%Y%m%d')
        m1_str = date.find('BC_1MONTH').get_text(strip=True)
        m3_str = date.find('BC_3MONTH').get_text(strip=True)
        m6_str = date.find('BC_6MONTH').get_text(strip=True)
        y1_str = date.find('BC_1YEAR').get_text(strip=True)
        y2_str = date.find('BC_2YEAR').get_text(strip=True)
        y3_str = date.find('BC_3YEAR').get_text(strip=True)
        y5_str = date.find('BC_5YEAR').get_text(strip=True)
        y7_str = date.find('BC_7YEAR').get_text(strip=True)
        y10_str = date.find('BC_10YEAR').get_text(strip=True)
        y20_str = date.find('BC_20YEAR').get_text(strip=True)
        y30_str = date.find('BC_30YEAR').get_text(strip=True)

        try:
            m1 = float(m1_str)
            m3 = float(m3_str)
            m6 = float(m6_str)
            y1 = float(y1_str)
            y2 = float(y2_str)
            y3 = float(y3_str)
            y5 = float(y5_str)
            y7 = float(y7_str)
            y10 = float(y10_str)
            y20 = float(y20_str)
            y30 = float(y30_str)
        except ValueError as e:
            logging.error('%s: date can\'t convert' % dt_str)
            continue
        # row exist
        cmd = 'select exists ( select 1 from "%s" where date = \'%s\' and m1 is not null and m3 is not null and m6 is not null and y1 is not null and y2 is not null and y3 is not null and y5 is not null and y7 is not null and y10 is not null and y20 is not null and y30 is not null )' % (tbl_name, dt_str)
        cursor.execute(cmd)
        rows = cursor.fetchall()
        for row in rows:
            # check row
            if not row[0]:
                cmd = 'insert into "%s" values ( \'%s\', %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f )' % ( tbl_name, dt_str, m1, m3, m6, y1, y2, y3, y5, y7, y10, y20, y30 )
                cursor.execute(cmd)
                conn.commit()

    conn.close()

def main(argv):
    args = parser().parse_args(argv[1:])

    log_file = args.log_file
    if log_file == "default":
        log_dir = '%s/log' % os.getcwd()
    log_file = '%s/updateUSTY.log' % log_dir

    logging.basicConfig(filename=log_file, level=logging.ERROR,
                        format='%(asctime)s\t%(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    tbl_name = args.tbl_name
    xml_doc = get_USTY()

    if xml_doc:
        update_USTY_tbl(args.dbname, tbl_name, xml_doc)
    else:
        raise RuntimeError('No data')

if __name__ == '__main__':
    try:
        main(sys.argv)
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)
