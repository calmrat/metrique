'''
This module contains a plotter wrapper that provides
defaults and additional helper functionality for
quickly generating plots with pandas and matplotlib
'''

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


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


def timestamp_figure(figure, stamp=True):
    # drop seconds:
    t = str(datetime.utcnow()).split('.')[0][:-3]
    if isinstance(stamp, basestring):
        t = '%s %s' % (stamp, t)
    figure.text(0.95, 0.05, t, fontsize=12, color='gray',
                ha='right', va='bottom', alpha=0.5)


class Plotter(object):
    ''' Convenince plotting wrapper '''

    def __init__(self, figsize=(10, 6), fill=True, title='', stamp=True,
                 **kwargs):
        '''
        :param (int, int) figsize:
            The size of the figure.
        :param boolean fill:
            Indicates whether the area under individual plots should be filled.
        :param boolean/string stamp:
            Put a timestamp in the bottom right corner.
            If True the current time will be stamped.
            If string then the string concatenated with the current time
            will be stamped.
        '''
        self.counter = 0
        self.fill = fill
        self.fig = plt.figure(figsize=figsize)
        if title:
            plt.title(title)
        if stamp:
            timestamp_figure(self.fig, stamp)

    def get_color(self, color):
        '''
        Returns a color to use.

        :param integer/string color:
            Color for the plot. Can be an index for the color from COLORS
            or a key(string) from CNAMES.
        '''
        if color is None:
            color = self.counter
        if isinstance(color, str):
            color = CNAMES[color]
        self.counter = color + 1
        color %= len(COLORS)
        return color

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
        color = self.get_color(color)
        if not isinstance(series, pd.Series):
            series = pd.Series(series, index=index)
        series.plot(label=label, c=COLORS[color], linewidth=2, style=style)
        if self.fill:
            plt.fill_between(series.index, 0, series, facecolor=ALPHAS[color])
            plt.gca().set_ylim(bottom=0)

    def plots(self, list_of_label_series, stacked=False, colors=None):
        '''
        Plots all the series from the list.
        The assumption is that all of the series share the same index.

        :param list list_of_label_series:
            A list of (label, series) pairs which should be plotted
        :param bool stacked:
            If true then the resulting graph will be stacked
        :params list list_of_colors:
            A list of colors to use.
        '''
        colors = range(len(list_of_label_series)) if colors is None else colors
        if stacked:
            ssum = 0
            lst = []
            for label, series in list_of_label_series:
                ssum += series
                lst.append((label, ssum))
            list_of_label_series = lst
        for color, label, series in zip(colors,
                                        *zip(*list_of_label_series))[::-1]:
            self.plot(series=series, label=label, color=color)

    def line(self, x, label=None, y='bottom', color='grey', ax=None, **kwargs):
        '''
        Creates a vertical line in the plot.

        :param x:
            The x coordinate of the line. Should be in the same units
            as the x-axis.
        :param string label:
            The label to be displayed.
        :param y:
            May be 'top', 'bottom' or int.
            The y coordinate of the text-label.
        :param color color:
            The color of the line.
        '''
        if ax is None:
            ax = plt
            y0, y1 = ax.ylim()
        else:
            y0, y1 = ax.get_ylim()
        ax.axvline(x, color=color, **kwargs)
        if label is not None:
            verticalalignment = 'bottom'
            if y == 'bottom':
                y = y0 + (y1 - y0) / 25.
            if y == 'top':
                verticalalignment = 'top'
                y = y0 + (y1 - y0) * 24 / 25.
            ax.annotate('\n' + label, (x, y), rotation=90,
                        verticalalignment=verticalalignment)

    def lines(self, lines_dict, y='bottom', color='grey', **kwargs):
        '''
        Creates vertical lines in the plot.

        :param lines_dict:
            A dictionary of label, x-coordinate pairs.
        :param y:
            May be 'top', 'bottom' or int.
            The y coordinate of the text-labels.
        :param color color:
            The color of the lines.
        '''
        for l, x in lines_dict.items():
            self.line(x, l, y, color, **kwargs)

    def legend(self, **kwargs):
        plt.legend(**kwargs)


class DiffPlotter(Plotter):
    def __init__(self, figsize=(10, 7), fill=False, title='', autodiffs=True,
                 **kwargs):
        '''
        :param (int, int) figsize:
            The size of the figure.
        :param boolean fill:
            Indicates whether the area under individual plots should be filled.
        :param string title:
            Title for the plot.
        :param boolean autodiffs:
            Indicates whether the diffs should be computed automatically if
            they are not specified.
        '''
        super(DiffPlotter, self).__init__(figsize=figsize, fill=fill,
                                          **kwargs)
        self.autodiffs = autodiffs
        self.ax1 = plt.subplot2grid((4, 1), (0, 0), rowspan=3)
        plt.title(title)
        plt.setp(self.ax1.get_xticklabels(), visible=False)
        self.ax2 = plt.subplot2grid((4, 1), (3, 0), sharex=self.ax1)
        plt.subplots_adjust(hspace=.15)

    def plot(self, series, series_diff=None, label='', color=None, index=None,
             style=None):
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
        color = self.get_color(color)
        if not isinstance(series, pd.Series):
            series = pd.Series(series, index=index)
        series.plot(label=label, c=COLORS[color], linewidth=2, style=style,
                    ax=self.ax1)
        if self.fill:
            self.ax1.fill_between(series.index, 0, series,
                                  facecolor=ALPHAS[color])
            self.ax1.set_ylim(bottom=0)
        if series_diff is None and self.autodiffs:
            series_diff = series.diff()
        if series_diff is not None:
            series_diff.plot(label=label, c=COLORS[color], linewidth=2,
                             style=style, ax=self.ax2)

    def line(self, x, label=None, y='bottom', color='grey', **kwargs):
        '''
        Creates a vertical line in the plot.

        :param x:
            The x coordinate of the line. Should be in the same units
            as the x-axis.
        :param string label:
            The label to be displayed.
        :param y:
            May be 'top', 'bottom' or int.
            The y coordinate of the text-label.
        :param color color:
            The color of the line.
        '''
        super(DiffPlotter, self).line(x, label, y, color, self.ax1, **kwargs)
        super(DiffPlotter, self).line(x, '', 0, color, self.ax2, **kwargs)

    def legend(self, **kwargs):
        self.ax1.legend(**kwargs)
