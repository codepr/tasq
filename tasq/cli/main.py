# -*- coding: utf-8 -*-

"""
tasq.cli.main.py
~~~~~~~~~~~~~~~~
"""

import argparse


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('local', store='false')
    parser.add_argument('remote', store='false')
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
