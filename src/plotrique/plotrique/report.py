import os
from matplotlib import pyplot as plt


class Report(object):
    def __init__(self, title):
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
        self.title = title
        self.body = ''
        self.sidebar = ''
        self.dir = title + '_files'
        self.fig_counter = 0
        self.chap_counter = 0
        os.mkdir(self.dir)
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

    def add_image(self, figure):
        '''
        Adds an image to the last chapter/section.
        The image will be stored in the `{self.title}_files` directory.

        :param matplotlib.figure figure:
            A matplotlib figure to be saved into the report
        '''
        name = self.dir + '/fig%s.png' % self.fig_counter
        self.fig_counter += 1
        figure.savefig(name)
        plt.close(figure)
        self.body += '<img src="%s" />\n' % name

    def write_report(self):
        '''
        Writes the report to a file.
        '''
        with open(self.title + '.html', 'w') as f:
            f.write(TEMPLATE.format(title=self.title,
                                    body=self.body,
                                    sidebar=self.sidebar))
        plt.ion()


TEMPLATE = '''<!DOCTYPE html>
<html>
<head>
<title>{title}</title>
<!-- Bootstrap core CSS -->
<link href="https://netdna.bootstrapcdn.com/bootstrap/3.0.2/css/bootstrap.min.css" rel="stylesheet">
</head>
<style>
body {{
    padding-bottom: 40px;
    padding-top: 60px;
}}
.nav-fixed {{
    position:fixed;
}}
</style>

<body>
<div class="container">

<div class="col-xs-6 col-sm-3" id="sidebar" role="navigation">
<div class="list-group nav-fixed">
{sidebar}
</div>
</div><!--/span-->

<div class="col-xs-12 col-sm-9">
{body}
</div><!--/span-->

</div><!--/.container-->

<!-- Bootstrap core JavaScript
================================================== -->
<!-- Placed at the end of the document so the pages load faster -->
<script src="https://code.jquery.com/jquery-1.10.2.min.js"></script>
<script src="https://netdna.bootstrapcdn.com/bootstrap/3.0.2/js/bootstrap.min.js"></script>
</body>
</html>
'''
