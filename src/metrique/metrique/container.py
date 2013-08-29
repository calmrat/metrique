#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: Juraj Niznan <jniznan@redhat.com>


from metrique.plotting import Plotter, DiffPlotter

from matplotlib import pyplot as plt


class Container(object):
    def __init__(self, data, display='plot'):
        self.data = data
        self.method = display
        self.dmap = {'plot': self._disp_plot,
                     'stacked': self._disp_stacked,
                     'diffplot': self._disp_diffplot,
                     'diffplotinv': self._disp_diffplotinv,
                     }
        self.title = self.data['title'] if 'title' in self.data else ''

    def _get(self, field, alt='', default=None):
        if '%s%s' % (alt, field) in self.data:
            return self.data['%s%s' % (alt, field)]
        elif field in self.data:
            return self.data[field]
        else:
            return default

    def display(self, method=None, alt='', **kwargs):
        method = method if method else self.method
        return self.dmap[method](series=self._get('series', alt),
                                 diffs=self._get('diffs', alt),
                                 labels=self._get('labels', alt),
                                 title=self._get('title', alt, ''),
                                 colors=self._get('colors', alt),
                                 lines=self._get('lines', alt),
                                 today=self._get('today', alt),
                                 **kwargs)

    def _plot(self, series, labels, colors, title, lines=None, today=None,
              stacked=False, loc=0, lines_y='bottom', today_y='bottom',
              **kwargs):
        p = Plotter(fill=stacked)
        p.plots(zip(labels, series), stacked=stacked, colors=colors)
        self._lines(p, series, lines, lines_y, today, today_y)
        plt.legend(loc=loc)
        plt.title('%s, stacked' % title if stacked else title)

    def _diffplot(self, series, diffs, labels, colors, title, lines=None,
                  today=None, invert=False, loc=0, lines_y='bottom',
                  today_y='bottom', **kwargs):
        p = DiffPlotter(title=title)
        if invert:
            series, diffs = diffs, series
        for s, d, l in zip(series, diffs, labels):
            p.plot(s, d, l)
        self._lines(p, series, lines, lines_y, today, today_y)
        p.legend(loc=loc)

    def _lines(self, p, series, lines, lines_y, today, today_y):
        if lines is not None:
            # this excludes the lines that are out of bounds:
            minx = min([s.index.min() for s in series]).replace(tzinfo=None)
            maxx = max([s.index.max() for s in series]).replace(tzinfo=None)
            lines = {k: v for k, v in lines.items()
                     if v.replace(tzinfo=None) <= maxx
                     and v.replace(tzinfo=None) >= minx}
            p.lines(lines, lines_y)
        if today is not None:
            p.line(today, 'today', today_y, lw=2,  linestyle='--')

    def _disp_plot(self, **kwargs):
        self._plot(**kwargs)

    def _disp_stacked(self, **kwargs):
        self._plot(stacked=True, **kwargs)

    def _disp_diffplot(self, **kwargs):
        self._diffplot(invert=False, **kwargs)

    def _disp_diffplotinv(self, **kwargs):
        self._diffplot(invert=True, **kwargs)
