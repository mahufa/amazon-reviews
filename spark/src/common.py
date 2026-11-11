import argparse


def parse_args(args: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    for arg in args:
        parser.add_argument(arg, required=True)

    known_args, _ = parser.parse_known_args()
    return known_args
