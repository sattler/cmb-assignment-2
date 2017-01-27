#! /bin/python3

import matplotlib.pyplot as plt
import numpy as np
import argparse
import json


def main():
    parser = argparse.ArgumentParser(description='a stats file')
    parser.add_argument('client_json_path', help='path to the stats file')
    parser.add_argument('server_json_path', help='path to the stats file')
    parser.add_argument('-s', '--show-plot', action='store_true',
                        help='If set each plot will also be shown to the user')
    args = parser.parse_args()

    with open(args.client_json_path) as stats_file:
        data = json.load(stats_file)

    time_values = np.array([int(time_point) for time_point, _ in data])
    min_time_value = min(time_values)
    cor_time_values = time_values-min_time_value
    c_time_values_set = xrange(max(cor_time_values)+1)
    c_count = np.bincount(cor_time_values)
    c_count *= 1400

    with open(args.server_json_path) as stats_file:
        data = json.load(stats_file)

    time_values = np.array([int(time_point) for time_point, _ in data])
    min_time_value = min(time_values)
    cor_time_values = time_values-min_time_value
    s_time_values_set = xrange(max(cor_time_values)+1)
    s_count = np.bincount(cor_time_values)


    plt.plot(c_time_values_set, c_count, '.-k', color='green', label='client traffic received')
    plt.plot(s_time_values_set, s_count, '.-b', color='blue',
             label='server packets marked as missing')

    plt.title('Transmission rate')

    # plt.yticks([tick * 1000 for tick in range(1, max_load + 1)])
    plt.legend(loc='upper right')
    plt.xlabel('time')
    plt.ylabel('throughput [B/s]')
    plt.savefig('plot.pdf', format='pdf', dpi=2000)
    if args.show_plot:
        plt.show()


if __name__ == '__main__':
    main()
