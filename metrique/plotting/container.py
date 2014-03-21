'''
container.py contains a Container class that can
be used for holding and displaying data in
simple dictionary formats.

@jniznan: Add some code examples here...

    >>> data = {...}
    >>> c = Container(data)
    >>> ...

'''

from plotting import Plotter, DiffPlotter

from matplotlib import pyplot as plt


class Container(object):
    '''
    A class that can be used to hold and display data.
    '''
    def __init__(self, data, display='plot'):
        '''
        Container class initiatlization.

        :param dictionary data:
            Dict containing the data.
        :param display plot:
            The default display method.
        '''
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

    def display(self, method=None, alt='', merge=None, **kwargs):
        '''
        Display the data.

        :param string method:
            One of None, 'plot', 'stacked', 'diffplot', 'diffplotinv'
        :param string alt:
            The prefix that specifies other data source to be displayed.
        :param list merge:
            A list of groups (lists/tuples) of labels to merge together.
        :param int loc:
            The location of the legend.
        :param boolean/string stamp:
            Put a timestamp in the bottom right corner.
            If True the current time will be stamped.
            If string then the string concatenated with the current time
            will be stamped.
        '''
        method = method if method else self.method
        labels = self._get('labels', alt)
        series = self._get('series', alt)
        diffs = self._get('diffs', alt)
        labels, series, diffs = self._merge(merge, labels, series, diffs)

        return self.dmap[method](series=series,
                                 diffs=diffs,
                                 labels=labels,
                                 title=self._get('title', alt, ''),
                                 colors=self._get('colors', alt),
                                 lines=self._get('lines', alt),
                                 today=self._get('today', alt),
                                 **kwargs)

    def _merge(self, merge, labels, series, diffs):
        '''
        Merges groups together.
        :param list merge:
            A list of groups (lists/tuples) of labels to merge together.
        '''
        if merge:
            lab = set(labels) - set([e for group in merge for e in group])
            merge += map(lambda x: [x], lab)
            merge.sort(key=lambda g: labels.index(g[0]))
            l, s, d = [], [], [] if diffs else None
            for group in merge:
                idx = [labels.index(e) for e in group]
                l.append(' + '.join([labels[i] for i in idx]))
                s.append(sum([series[i] for i in idx]))
                if diffs:
                    d.append(sum([diffs[i] for i in idx]))
            labels, series, diffs = l, s, d
        return labels, series, diffs

    def _plot(self, series, labels, colors, title, lines=None, today=None,
              stacked=False, loc=0, lines_y='bottom', today_y='bottom',
              **kwargs):
        p = Plotter(fill=stacked, **kwargs)
        p.plots(zip(labels, series), stacked=stacked, colors=colors)
        self._lines(p, series, lines, lines_y, today, today_y)
        plt.legend(loc=loc)
        plt.title('%s, stacked' % title if stacked else title)

    def _diffplot(self, series, diffs, labels, colors, title, lines=None,
                  today=None, invert=False, loc=0, lines_y='bottom',
                  today_y='bottom', **kwargs):
        p = DiffPlotter(title=title, **kwargs)
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
            # requires 2.7+
            #lines = {k: v for k, v in lines.items()
            #         if v.replace(tzinfo=None) <= maxx
            #         and v.replace(tzinfo=None) >= minx}
            # compatible with 2.6+
            lines = dict(
                [(k, v) for k, v in lines.items() if (
                    v.replace(tzinfo=None) <= maxx and
                    v.replace(tzinfo=None) >= minx)])
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
