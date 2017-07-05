import sys
import traceback
import requests


def parser():
    import argparse

    parser = argparse.ArgumentParser(description='Rate of return of foreign currency')
    parser.add_argument('invest', type=float, help='Investment (NT dolloar)')
    parser.add_argument('--base', type=str, nargs=1, help='The base currency (ex. TWD)')
    parser.add_argument('-u', '--usd', type=float, nargs=1, help='Having U.S. currency')
    parser.add_argument('-a', '--aud', type=float, nargs=1, help='Having Australia currency')
    parser.add_argument('-e', '--eur', type=float, nargs=1, help='Having European currency')
    parser.add_argument('-c', '--cny', type=float, nargs=1, help='Having Chinese currency')
    parser.add_argument('-j', '--jpy', type=float, nargs=1, help='Having Japan currency')

    return parser

def fcurrencyrates(base):
    url = 'https://v3.exchangerate-api.com/bulk/deee1f1aa64988a61cce9d1e/%s' % base

    page = requests.get(url)

    if not page.ok:
        page.raise_for_status()

    content = page.json()

    try:
        rates = content['rates']
    except KeyError as e:
        raise KeyError('No \'rates\' key contained')

    return rates

def main(argv):
    args = parser().parse_args(argv[1:])

    if not args.base:
        base = 'TWD'
    else:
        base = args.base[0]

    rates = fcurrencyrates(base)

    if args.usd:
        usd_e = args.usd[0]/rates['USD']
    else:
        usd_e = 0

    if args.aud:
        aud_e = args.aud[0]/rates['AUD']
    else:
        aud_e = 0

    if args.eur:
        eur_e = args.eur[0]/rates['EUR']
    else:
        eur_e = 0

    if args.cny:
        cny_e = args.cny[0]/rates['CNY']
    else:
        cny_e = 0

    if args.jpy:
        jpy_e = args.jpy[0]/rates['JPY']
    else:
        jpy_e = 0

    total_back = usd_e + aud_e + jpy_e + cny_e + eur_e
    rate_of_return = (total_back - args.invest)/args.invest

    print('Investment: %f' % args.invest)
    print('Exchange back: %f' % total_back)
    print('Rate of return: %f' % rate_of_return)

if __name__ == '__main__':
    try:
        main(sys.argv)
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)
