#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# author: "Chris Ward" <cward@redhat.com>


def test_import_star():
    from plotrique import *
    assert Plotter and DiffPlotter and Container


def test_container():
    from plotrique import Container

    assert Container({'test': [1, 2, 3]}).data


def test_plotting():
    from plotrique.plotting import Plotter, DiffPlotter

    assert Plotter()
    assert DiffPlotter()
