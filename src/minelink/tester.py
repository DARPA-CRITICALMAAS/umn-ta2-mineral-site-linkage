from argparse import ArgumentParser

def main(args):
    print(args)

if __name__ == '__main__':
    parser = ArgumentParser(description='Testing mineral site linking result')
    parser.add_argument('--path_ground_truth', '-p',
                        help='path in which the ground truth labeled data file is saved')
    parser.add_argument('--column_name', '-c',
                        help='column name (header) in the ground truth labeled data file, where the linking result is listed')
    
    main(parser.parse_args())