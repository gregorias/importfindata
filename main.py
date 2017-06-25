#!/usr/bin/env python
# Copyright (C) 2017 Grzegorz Milka
import datetime
import decimal
import re
import sys
import urllib
from shutil import copyfileobj
from tempfile import TemporaryFile
from unidecode import unidecode
from zipfile import ZipFile

import pandas as pd

import gnucash
from gnucash import Session


def download_polish_funds_list():
    """Downloads list of polish funds from bossa.pl.

    Returns:
        A dictionary from name of a fund to (file, date) tuple."""
    MSTFUN_LST_URL = 'http://bossa.pl/pub/fundinwest/mstock/mstfun.lst'

    # Get in-zip filenames of each fund.
    fund_to_file = {}
    mstfun = urllib.urlopen(MSTFUN_LST_URL)
    try:
        if mstfun.getcode() != 200:
            raise Exception(
                ('The url request for mstfun.lst has not returned OK ' +
                 'but: {0}.').format(mstfun.getcode()))
        mstfun_lst = mstfun.read().decode('ASCII')
    finally:
        mstfun.close()

    date_file_name_pat = re.compile('(\d\d\d\d-\d\d-\d\d)' + '\s+\S+' +
                                    '\s+\S+\s+\S+' + '\s+(\S+)\s+(\S.*\S)\s*$')

    # Remove header and tail, and process the list of funds
    for l in mstfun_lst.splitlines()[3:-2]:
        m = date_file_name_pat.match(l)
        if m is None:
            raise Exception('Could not extract information from' +
                            'lst\'s line: {0}'.format(l))
        date, file, name = m.groups()
        fund_to_file[name] = (file, datetime.datetime.strptime(date,
                                                               '%Y-%m-%d'))

    return fund_to_file


class MstFun:
    """An accessor to the bossa.pl price database."""

    def __init__(self):
        self.fund_to_file_date = {}

    def __enter__(self):
        try:
            self.fund_to_file_date = download_polish_funds_list()
        except Exception as e:
            raise Exception('Could not download list of available funds.', e)

        MSTFUN_ZIP_URL = 'http://bossa.pl/pub/fundinwest/mstock/mstfun.zip'
        self.tmpfile = TemporaryFile()
        try:
            mstfun_zip = urllib.urlopen(MSTFUN_ZIP_URL)
            try:
                if mstfun_zip.getcode() != 200:
                    raise Exception(
                        ('The url request for mstfun.zip has not ' +
                         'returned OK but: {0}.').format(mstfun_zip.getcode()))
                copyfileobj(mstfun_zip, self.tmpfile)
            finally:
                mstfun_zip.close()
            self.mstfun = ZipFile(self.tmpfile, 'r')
        except Exception as e:
            self.tmpfile.close()
            raise e
        return self

    def __exit__(self, type, value, traceback):
        self.mstfun.close()
        self.tmpfile.close()

    def get_fund_price(self, name):
        """Gets the latest quote for name.

        Args:
            name - A fund's name.
        Returns:
            (price, date) if fund is available, None otherwise.
        Raises:
            An exception on errors, like unexpected data format or missing
            file."""
        if name not in self.fund_to_file_date:
            return None
        filename, date = self.fund_to_file_date[name]

        with self.mstfun.open(filename) as fundfile:
            data = pd.read_csv(
                fundfile, usecols=['<DTYYYYMMDD>', '<CLOSE>'], dtype=str)
            data.columns = ['date', 'price']
            last_row = data.iloc[-1]
            return (decimal.Decimal(last_row.price),
                    datetime.datetime.strptime(last_row.date, '%Y%m%d').date())


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: main.py WALLET')
        sys.exit(1)

    url = 'xml://{0}'.format(sys.argv[1])
    session = Session(url, True, False, False)
    try:
        book = session.book
        pdb = book.get_price_db()
        comm_table = book.get_table()
        cur = comm_table.lookup('CURRENCY', 'PLN')
        commodities = comm_table.get_commodities('FUND')

        with MstFun() as mstfun:
            for cmdt in commodities:
                pl = pdb.lookup_latest(cmdt, cur)
                if not pl:
                    print(('Skipping {0}, because there are no price ' +
                           'entries in gnucash denominated in PLN.'
                           ).format(cmdt.get_fullname()))
                    continue
                quote = mstfun.get_fund_price(
                    unidecode(cmdt.get_fullname().decode('utf8')))
                if not quote:
                    print(
                        ('Skipping {0}, because its price is not ' +
                         'available on Bossa.pl.').format(cmdt.get_fullname()))
                    continue

                if pl.get_time() >= quote[1]:
                    print(('Skipping {0}, because its latest price ' +
                           'is not older ({1}) than reference ({2}).').format(
                               cmdt.get_fullname(), pl.get_time(), quote[1]))
                    continue
                print('Updating {0} with price {1} at {2}.'.format(
                    cmdt.get_fullname(), quote[0], quote[1]))
                p_new = pl.clone(book)
                p_new = gnucash.GncPrice(instance=p_new)
                p_new = pl
                p_new.set_time(quote[1])
                v = p_new.get_value()
                v.num = (quote[0] * v.denom).to_integral()
                p_new.set_value(v)
                pdb.add_price(p_new)
        session.save()
    finally:
        session.end()
        session.destroy()
