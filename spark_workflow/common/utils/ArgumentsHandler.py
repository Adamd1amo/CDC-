import argparse
from typing import *


def get_arguments() -> Dict:
    args = initial_argument_parser()
    args_dict = convert_params_to_dict(vars(args))

    return args_dict


def initial_argument_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='The dark hole =))')

    parser.add_argument('--function_name', type=str, help='Name of Function/Formular')
    parser.add_argument('--user_id', type=str)
    parser.add_argument('--request_id', type=str)
    parser.add_argument('--params', nargs='+', type=str,
                        help='A place in space where gravity pulls so much that even light can not get out.')
    arguments = parser.parse_args()

    return arguments


# def namespace_to_dictionary(args: argparse.Namespace) -> Dict[str, Any]:
#     """convert args.Namespace to dictionary"""
#
#     return vars(args)


def convert_params_to_dict(params: Dict) -> Dict:
    params_dict = dict()

    params_dict['function_name'] = params['function_name']
    params_dict['user_id'] = params['user_id']
    params_dict['request_id'] = params['request_id']

    for param in params['params']:
        key = param.split("=")[0]
        if key in ['where', 'join']:
            value = param[len(key) + 1:]
        else:
            value = param.split("=")[1]
        params_dict[key] = value
    return params_dict


class ArgumentHandler:
    @classmethod
    def handle_arguments(cls, args_dict: Dict):
        if not args_dict:
            raise ValueError("The argument is empty.")
        for key, value in args_dict.items():
            try:
                mapped_function = getattr(
                    cls, f"handle_{key}".lower()
                )
            except AttributeError:
                raise ValueError("The sql query is invalid.")
            args_dict[key] = mapped_function(value)
        return args_dict

    @classmethod
    def handle_where(cls, value):
        pass

    @classmethod
    def handle_order_by(cls, value):
        columns = value.split("/")[0].split(",")
        ascending = value.split("/")[1]
        asc_bool = [True if x.strip() == 'true' else False for x in ascending.split(',')]
        return columns, asc_bool


