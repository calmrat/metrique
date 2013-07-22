#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Authors: "Jan Grec" <jgrec@redhat.com>

import pandas as pd
import matplotlib.pyplot as plt


# Some nice colors, stored here for convenience.
COLORS = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
          '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
ALPHAS = ['#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5',
          '#c49c94', '#f7b6d2', '#c7c7c7', '#dbdb8d', '#9edae5']
CNAMES = {'blue': 0, 'b': 0,
          'orange': 1,
          'green': 2, 'g': 2,
          'red': 3, 'r': 3,
          'violet': 4,
          'brown': 5,
          'pink': 6,
          'grey': 7,
          'khaki': 8,
          'cyan': 9}


class Plotter():
    '''
    Convenince plotting wrapper.
    '''

    def __init__(self, figsize=(10, 6), fill=True):
        '''
        :param (int, int) figsize:
            The size of the figure.
        :param boolean fill:
            Indicates whether the area under individual plots should be filled.
        '''
        self.counter = 0
        self.fill = fill
        plt.figure(figsize=figsize)

    def plot(self, series, label='', color=None, index=None, style=None):
        '''
        Wrapper around plot.

        :param pandas.Series/list series:
            The series to be plotted. If passed in as a list, the parameter
            `index` must be also passed in.
        :param string label:
            The label of for the plot.
        :param integer/string color:
            Color for the plot. Can be an index for the color from COLORS
            or a key(string) from CNAMES.
        :param list index:
            Must be specified if `series` is a list. Otherwise not used.
        :param string style:
            Style forwarded to the plt.plot.
        '''
        if color is None:
            color = self.counter
        if isinstance(color, str):
            color = CNAMES[color]
        self.counter = color + 1
        color %= len(COLORS)
        if not isinstance(series, pd.Series):
            series = pd.Series(series, index=index)
        series.plot(label=label, c=COLORS[color], linewidth=2, style=style)
        if self.fill:
            plt.fill_between(series.index, 0, series, facecolor=ALPHAS[color])
            plt.gca().set_ylim(bottom=0)

    def line(self, x, y, label=None, color='grey'):
        '''
        Creates a vertical line in the plot.

        :param x:
            The x coordinate of the line. Should be in the same units
            as the x-axis.
        :param y:
            The y coordinate of the text-label.
        :param string label:
            The label to be displayed.
        :param color color:
            The color of the line.
        '''
        plt.axvline(x, color=color)
        if label is not None:
            plt.annotate('\n' + label, (x, y), rotation=90)
