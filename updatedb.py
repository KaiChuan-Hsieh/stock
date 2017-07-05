import os
import sys
import traceback
import datetime
import psycopg2
import getpass
import requests
import json
import time
import logging

def parser():
    import argparse

    parser = argparse.ArgumentParser(description='Create/Update db tables')
    parser.add_argument('dbname', type=str, help='DB name to operate')
    parser.add_argument('-d', '--date', type=str, help='Date format YYYYMMDD (ex. 20170621)')
    parser.add_argument('-c', '--count', type=int, help='Number traded date')
    parser.add_argument('-f', '--log-file', nargs='?', default='default', help='Enable logging to a file, omit default log file')

    return parser

def get_tse_price_info(date):
    url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX'

    query_params = {
        'date': date,
        'response': 'json',
        'type': 'ALL',
        '_': str(round(time.time() * 1000) - 500)
    }

    page = requests.get(url, params=query_params)

    if not page.ok:
        logging.error('Can\'t get %s' % page.url)
        return None

    content = page.json()

    try:
        data = content['data5']
    except KeyError as e:
        logging.error('No \'data5\' key in %s' % page.url)
        return None

    return data

def update_price_info(dbname, date, data):
    for row in data:
        # retrieve data and transfer to suited type
        stockno = row[0]
        numlist = row[2].split(',')
        tmp = ""
        for num in numlist:
            tmp += num
        try:
            traded_share = int(tmp)
            open_p = float(row[5])
            high_p = float(row[6])
            low_p = float(row[7])
            close_p = float(row[8])
        except ValueError as e:
            logging.error('%s: %s: price data can\'t covert' % (stockno, date))
            continue

        #print('%s,%s,%d,%f,%f,%f,%f' % (date, stockno, traded_share, open_p, high_p, low_p, close_p))
        conn = psycopg2.connect(database=dbname, user=getpass.getuser())
        cursor = conn.cursor()

        # check if table exist
        cmd = 'select exists ( select 1 from information_schema.tables where table_name = \'%s\' )' % stockno
        cursor.execute(cmd)
        rows = cursor.fetchall()
        for row in rows:
            if not row[0]:
                # not exist, create table
                cmd = 'create table "%s" ( date date, traded_share integer, open real, high real, low real, close real )' % stockno
                cursor.execute(cmd)
                conn.commit()
                break

        # check if row exist
        cmd = 'select exists ( select 1 from "%s" where date = \'%s\' and open > 0 )' % (stockno, date)
        cursor.execute(cmd)
        rows = cursor.fetchall()
        for row in rows:
            if not row[0]:
                cmd = 'insert into "%s" values ( \'%s\', %d, %f, %f, %f, %f )' % (stockno, date, traded_share, open_p, high_p, low_p, close_p)
                cursor.execute(cmd)
                break

        conn.commit()
        conn.close()

def get_tse_trade_info(date):
    url = 'http://www.twse.com.tw/fund/T86'

    query_params = {
        'date': date,
        'response': 'json',
        'selectType': 'ALL',
        '_': str(round(time.time() * 1000) - 500)
    }

    page = requests.get(url, params=query_params)

    if not page.ok:
        logging.error('Can\'t get %s' % page.url)
        return None

    content = page.json()

    try:
        data = content['data']
    except KeyError as e:
        logging.error('No \'data\' key in %s' % page.url)
        return None

    return data

def update_trade_info(dbname, date, data):
    for d in data:
        exist = True
        stockno = d[0]
        try:
            numlist = d[4].split(',')
            tmp = ""
            for num in numlist:
                tmp += num
            f_trade = int(tmp)
            numlist = d[7].split(',')
            tmp = ""
            for num in numlist:
                tmp += num
            l_trade = int(tmp)
        except ValueError as e:
            logging.error('%s: %s: trade info can\'t convert' % (stockno, date))
            continue

        #print('%s: f_trade = %d, l_trade = %d' % (stockno, f_trade, l_trade))
        conn = psycopg2.connect(database=dbname, user=getpass.getuser())
        cursor = conn.cursor()

        # check if table exist
        cmd = 'select exists ( select 1 from information_schema.tables where table_name = \'%s\' )' % stockno
        cursor.execute(cmd)
        rows = cursor.fetchall()
        for row in rows:
            if not row[0]:
                exist = False
                break

        if not exist:
            conn.close()
            continue

        # check if column created
        cmd = 'select exists ( select 1 from information_schema.columns where table_name = \'%s\' and column_name = \'f_trade\' )' % (stockno)
        cursor.execute(cmd)
        rows = cursor.fetchall()
        for row in rows:
            if not row[0]:
                cmd = 'alter table "%s" add column f_trade integer' % stockno
                cursor.execute(cmd)
                cmd = 'alter table "%s" add column l_trade integer' % stockno
                cursor.execute(cmd)
                conn.commit()
                break

        # check if f_trade and l_trade column are filled
        cmd = 'select exists ( select 1 from "%s" where date =\'%s\' and f_trade is not null and l_trade is not null )' % (stockno, date)
        cursor.execute(cmd)
        rows = cursor.fetchall()
        for row in rows:
            if row[0]:
                exist = False
                break

        if not exist:
            conn.close()
            continue

        # update the row's f_trade and l_trade info
        cmd = 'update "%s" set f_trade = %d, l_trade = %d where date = \'%s\'' % (stockno, f_trade, l_trade, date)
        cursor.execute(cmd)
        conn.commit()
        conn.close()

def main(argv):
    args = parser().parse_args(argv[1:])

    log_file = args.log_file
    if log_file == "default":
        log_dir = '%s/log' % os.getcwd()
        if not os.path.isdir(log_dir):
            os.makedirs(log_dir)
        log_file = '%s/%s.log' % (log_dir,
                    datetime.datetime.now().strftime('%Y%m%d%H%M%S'))

    logging.basicConfig(filename=log_file, level=logging.ERROR,
                        format='%(asctime)s\t%(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # if date is None, use current time
    if args.date == None:
        date = datetime.datetime.now().strftime('%Y%m%d')
    else:
        date = args.date

    # check date
    try:
        datetime_obj = datetime.datetime.strptime(date, '%Y%m%d')
    except ValueError as e:
        logging.error('Invalid Date: %s' % date)
        sys.exit(1)

    # check dbname
    try:
        conn = psycopg2.connect(database=args.dbname, user=getpass.getuser())
        conn.close()
    except Exception as e:
        logging.error('Database Error: %s' % e)
        sys.exit(1)

    if args.count:
        count = args.count
    else:
        count = 0

    getnum = 0
    while getnum <= count:
        date = datetime_obj.strftime('%Y%m%d')
        # get json data
        data = get_tse_price_info(date)
        if data:
            update_price_info(args.dbname, date, data)
            info = get_tse_trade_info(date)
            if info:
                update_trade_info(args.dbname, date, info)
            getnum += 1

        datetime_obj -= datetime.timedelta(1)

if __name__ == '__main__':
    try:
        main(sys.argv)
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)
