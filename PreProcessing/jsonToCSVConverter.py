#PYTHON 1


# -*- coding: utf-8 -*-
"""Convert the Yelp Dataset Challenge dataset from json format to csv.
For more information on the Yelp Dataset Challenge please visit http://yelp.com/dataset_challenge
"""
import argparse
import collections
import csv
import json as json


def read_and_write_file(json_file_path, csv_file_path, column_names):
    """Read in the json dataset file and write it out to a csv file, given the column names."""
    with open(csv_file_path, 'wb+') as fout:
        csv_file = csv.writer(fout)
        csv_file.writerow(list(column_names))
        with open(json_file_path) as fin:
            for line in fin:
                line_contents = json.loads(line)
                csv_file.writerow(get_row(line_contents, column_names))


def get_superset_of_column_names_from_file(json_file_path):
    """Read in the json dataset file and return the superset of column names."""
    column_names = set()
    with open(json_file_path) as fin:
        for line in fin:
            line_contents = json.loads(line)
            column_names.update(
                set(get_column_names(line_contents).keys())
            )
    return column_names


def get_column_names(line_contents, parent_key=''):
    """Return a list of flattened key names given a dict.
    Example:
        line_contents = {
            'a': {
                'b': 2,
                'c': 3,
                },
        }
        will return: ['a.b', 'a.c']
    These will be the column names for the eventual csv file.
    """
    column_names = []
    for k, v in line_contents.iteritems():
        column_name = "{0}.{1}".format(parent_key, k) if parent_key else k
        if isinstance(v, collections.MutableMapping):
            column_names.extend(
                get_column_names(v, column_name).items()
            )
        else:
            column_names.append((column_name, v))
    return dict(column_names)


def get_nested_value(d, key):
    """Return a dictionary item given a dictionary `d` and a flattened key from `get_column_names`.

    Example:
        d = {
            'a': {
                'b': 2,
                'c': 3,
                },
        }
        key = 'a.b'
        will return: 2

    """
    if '.' not in key:
        if key not in d:
            return None
        return d[key]
    base_key, sub_key = key.split('.', 1)
    if base_key not in d:
        return None
    sub_dict = d[base_key]
    return get_nested_value(sub_dict, sub_key)


def get_row(line_contents, column_names):
    """Return a csv compatible row given column names and a dict."""
    row = []
    for column_name in column_names:
        line_value = get_nested_value(
            line_contents,
            column_name,
        )
        if isinstance(line_value, unicode):
            row.append('{0}'.format(line_value.encode('utf-8')))
        elif line_value is not None:
            row.append('{0}'.format(line_value))
        else:
            row.append('')
    return row


if __name__ == '__main__':
    """Convert a yelp dataset file from json to csv."""

    json_file_business = "C:\\Users\\Ankitha\\Desktop\\DM DATASET\\yelp_academic_dataset_business.json"
    csv_file_business = 'Converted_Business.csv'.format(json_file_business.split('.json')[0])

    json_file_review = "C:\\Users\\Ankitha\\Desktop\\DM DATASET\\yelp_academic_dataset_review.json"
    csv_file_review = 'Converted_Reviews.csv'.format(
        json_file_review.split('.json')[0])

    json_file_users = "C:\\Users\\Ankitha\\Desktop\\DM DATASET\\yelp_academic_dataset_user.json"
    csv_file_users = 'Converted_Users.csv'.format(json_file_users.split('.json')[0])

    # column_names_business = get_superset_of_column_names_from_file(json_file_business)
    # print(get_superset_of_column_names_from_file(json_file_users))
    # print(get_superset_of_column_names_from_file(json_file_review))

    column_names_user = set(['user_id', 'friends'])
    column_names_review = set(['user_id', 'review_id', 'business_id', 'stars', 'date'])
    column_names_business = set(
        ['business_id', 'city', 'name', 'latitude', 'longitude', 'stars', 'categories', 'is_open'])

    # read_and_write_file(json_file_business, csv_file_business, column_names_business)
    read_and_write_file(json_file_review, csv_file_review, column_names_review)
    # read_and_write_file(json_file_users, csv_file_users, column_names_user)

    # 'hours.Monday', 'hours.Tuesday', 'hours.Wednesday', 'hours.Thursday', 'hours.Friday', 'hours.Saturday', 'hours.Sunday'