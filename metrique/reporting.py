#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Juraj Niznan" <jniznan@redhat.com>
# Author: "Chris Ward <cward@redhat.com>

'''
metrique.reporting
~~~~~~~~~~~~~~~~~~

This module contains a basic reporting class
for quickly generating textual reports.
'''

from __future__ import unicode_literals, absolute_import

import os

from metrique.utils import read_file, write_file, make_dirs

CACHE_DIR = os.environ.get('METRIQUE_CACHE') or '/tmp'

try:
    from matplotlib import pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


class Report(object):
    def __init__(self, title, plot_template=None, save_dir=None):
        '''
        Create a report in the current working directory. The report will be
        called `{title}.html`.  Also a directory `{title}_files` will be
        created and used for storing images etc.

        Content to the report is supposed to be added linearly.

        Warning: Using Report will turn matplotlib's interactive mode off.
        After writing the report interactive mode will be turned back on.

        :param str title:
            Title of the report
        '''
        if not HAS_MATPLOTLIB:
            raise RuntimeError("`pip install matplotlib` required")
        plot_template = plot_template or 'templates/plotting_bootstrap.html'
        self._template = read_file(plot_template)
        self.title = title
        self.body = ''
        self.sidebar = ''
        self.fig_counter = 0
        self.chap_counter = 0
        self._base_dir = os.path.expanduser(save_dir or CACHE_DIR)
        self._dir = os.path.join(self._base_dir, '%s_files' % title)
        make_dirs(self._dir)
        plt.ioff()

    def add_chapter(self, title):
        '''
        Adds a new chapter to the report.

        :param str title: Title of the chapter.
        '''
        chap_id = 'chap%s' % self.chap_counter
        self.chap_counter += 1
        self.sidebar += '<a href="#%s" class="list-group-item">%s</a>\n' % (
            chap_id, title)
        self.body += '<h1 id="%s">%s</h1>\n' % (chap_id, title)

    def add_section(self, title):
        '''
        Adds a new section to the last chapter.

        :param str title: Title of the chapter.
        '''
        self.body += '<h2>%s</h2>\n' % title

    def add_text(self, text):
        '''
        Adds text to the last chapter/section.

        :param str text: Text to be added.
        '''
        self.body += '<p>%s</p>\n' % text

    def add_image(self, figure, dpi=72):
        '''
        Adds an image to the last chapter/section.
        The image will be stored in the `{self.title}_files` directory.

        :param matplotlib.figure figure:
            A matplotlib figure to be saved into the report
        '''
        name = os.path.join(self._dir, '/fig%s.png' % self.fig_counter)
        self.fig_counter += 1
        figure.savefig(name, dpi=dpi)
        plt.close(figure)
        self.body += '<img src="%s" />\n' % name

    def write_report(self, force=False):
        '''
        Writes the report to a file.
        '''
        path = self.title + '.html'
        value = self._template.format(
            title=self.title, body=self.body, sidebar=self.sidebar)
        write_file(path, value, force=force)
        plt.ion()
