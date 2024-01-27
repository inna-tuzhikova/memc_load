from optparse import OptionParser

from memc_load.loader import run_loader


def main():
    parser = OptionParser(
        description='Parses gzip logs and loads it to memcache',
        prog='Memc Load'
    )
    parser.add_option('-l', '--log', action='store', default=None,
                      help='Path to save logs. If not specified stdout is used')
    parser.add_option('--dry', action='store_true', default=False,
                      help='If specified debug output is enabled')
    parser.add_option('--pattern', action='store',
                      default='./../../data/*.tsv.gz',
                      help='File pattern to upload')
    parser.add_option('--idfa', action='store',
                      help='Memcached URL to cache idfa data')
    parser.add_option('--gaid', action='store',
                      help='Memcached URL to cache gaid data')
    parser.add_option('--adid', action='store',
                      help='Memcached URL to cache adid data')
    parser.add_option('--dvid', action='store',
                      help='Memcached URL to cache dvid data')
    (options, args) = parser.parse_args()

    run_loader(
        options.log,
        options.dry,
        options.pattern,
        options.idfa,
        options.gaid,
        options.adid,
        options.dvid
    )


if __name__ == '__main__':
    main()
