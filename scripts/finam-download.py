#!/usr/bin/env python
import sys
import time
import os.path
import datetime
import logging
from operator import attrgetter
from functools import partial

import click
from click_datetime import Datetime
import pandas as pd

from finam import (Exporter,
                   Timeframe,
                   Market,
                   FinamExportError,
                   FinamObjectNotFoundError,
                   Fileformat)
from finam.utils import click_validate_enum


"""
Helper script to download a set of assets
"""

logger = logging.getLogger(__name__)


def _arg_split(ctx, param, value):
    if value is None:
        return value

    try:
        items = value.split(',')
    except ValueError:
        raise click.BadParameter('comma-separated {} is required, got {}'
                                 .format(param, value))
    return items


@click.command()
@click.option('--contracts',
              help='Contracts to lookup',
              required=False,
              callback=_arg_split)
@click.option('--market',
              help='Market to lookup',
              callback=partial(click_validate_enum, Market),
              required=False)
@click.option('--timeframe',
              help='Timeframe to use (DAILY, HOURLY, MINUTES30 etc)',
              default=Timeframe.DAILY.name,
              callback=partial(click_validate_enum, Timeframe),
              required=False)
@click.option('--destdir',
              help='Destination directory name',
              required=True,
              type=click.Path(exists=True, file_okay=False, writable=True,
                              resolve_path=True))
@click.option('--skiperr',
              help='Continue if a download error occurs. False by default',
              required=False,
              default=True,
              type=bool)
@click.option('--lineterm',
              help='Line terminator',
              default='\r\n')
@click.option('--delay',
              help='Seconds to sleep between requests',
              type=click.IntRange(0, 600),
              default=1)
@click.option('--startdate', help='Start date',
              type=Datetime(format='%Y-%m-%d'),
              default='2007-01-01',
              required=False)
@click.option('--enddate', help='End date',
              type=Datetime(format='%Y-%m-%d'),
              default=datetime.date.today().strftime('%Y-%m-%d'),
              required=False)
@click.option('--ext',
              help='Resulting file extension',
              default='csv')
@click.option('--fileformat',
              help='Format of output file with data',
              default='CSV',
              callback=partial(click_validate_enum, Fileformat),
              required=False)
@click.option('--append',
              type=click.Choice(['y', 'n'], case_sensitive=False))

def main(contracts, market, timeframe, destdir, lineterm,
         delay, startdate, enddate, skiperr, ext, fileformat, append):
    
    append_flag = append is not None and append == 'y'
    exporter = Exporter()

    if not any((contracts, market)):
        raise click.BadParameter('Neither contracts nor market is specified')

    if append and fileformat[:3] == 'CSV':
        raise click.BadParameter('Cannot append to csv file')

    market_filter = dict()
    if market:
        market_filter.update(market=Market[market])
        if not contracts:
            contracts = exporter.lookup(**market_filter)['code'].tolist()

    for contract_code in contracts:
        logging.info('Handling {}'.format(contract_code))
        try:
            contracts = exporter.lookup(code=contract_code, **market_filter)
        except FinamObjectNotFoundError:
            logger.error('unknown contract "{}"'.format(contract_code))
            sys.exit(1)
        else:
            contract = contracts.reset_index().iloc[0]

        logger.info(u'Downloading contract {}'.format(contract))
        destpath = os.path.join(destdir, f'{contract.code}-{timeframe}')
        
        # extention is taken from param if output file is in csv format
        compression = None
        if fileformat == 'CSV': 
            destpath += f'.{ext}'
            compression = None
        elif fileformat == 'CSVGZ': 
            destpath += f'.csv.gz'
            compression = 'gzip'
        if fileformat == 'PKL': 
            destpath += f'.pkl'
            compression = None
        if fileformat == 'PKLXZ': 
            destpath += f'.pkl.xz'
            compression = 'xz'

        if append_flag:
            if os.path.exists(destpath):
                
                df_local = pd.read_pickle(destpath)
                last_existing_date = df_local.iloc[-1].astype(str)['<DATE>']
                # if we're in append mode take startdate from file
                startdate = datetime.datetime.strptime(last_existing_date, '%Y%m%d')
                logger.info(f'Found local file with last date {last_existing_date}')
            else:
                df_local = pd.DataFrame()
                logger.info(f'Append mode, but no local file found. Downloading everthing')


        try:
            data = exporter.download(contract.id,
                                     start_date=startdate,
                                     end_date=enddate,
                                     timeframe=Timeframe[timeframe],
                                     market=Market(contract.market))
            logger.info(f'{data.shape[0]} lines are downloaded')
            
        except FinamExportError as e:
            if skiperr:
                logger.error(repr(e))
                continue
            else:
                raise
        
        # extention is taken from param if output file is in csv format
        if fileformat[:3] == 'CSV':
            data.to_csv(destpath, index=False, line_terminator=lineterm, compression=compression)
        else:
            if append_flag:
                initial_cnt = df_local.shape[0]
                data = pd.concat([df_local, data]).drop_duplicates()
                result_cnt = data.shape[0]
                logger.info(f'{result_cnt-initial_cnt} records will be added to already existing {initial_cnt}')

            if fileformat[:3] == 'PKL':
                data.to_pickle(destpath, compression=compression)
        logger.info(f'data is saved to {destpath}')

        if delay > 0:
            logger.info('Sleeping for {} second(s)'.format(delay))
            time.sleep(delay)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()


# python -m scripts.finam-download --contracts DSKY --destdir . --timeframe MINUTES1 --startdate 2020-10-10 --enddate 2020-10-17 --fileformat PKL
# python -m scripts.finam-download --contracts DSKY --destdir . --timeframe MINUTES1 --startdate 2020-10-10 --fileformat PKL --append y