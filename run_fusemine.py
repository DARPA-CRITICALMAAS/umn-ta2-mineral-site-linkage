import argparse
from fusemine import FuseMine

def main(commodity: str|int|None) -> None:
    # Support string type commodity, integer (indicating all critical commodities), or none (indicating all commodity)
    fusemine = FuseMine()
    fusemine.link_records()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TA2 Database Processing')

    parser.add_argument('--commodity', required=True,
                        help='Directory or file where the mineral site database is located')
    
    args = parser.parse_args()

    main(commodity=args.commodity)