#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Authors: "Jan Grec" <jgrec@redhat.com>

import pandas as pd


def plot_with_labels(figure, frame, value_field, labels_field):
    """
    Takes figure with plot based on datetime data type and adds
    additional subplot from given frame's value_field, also adding
    labels from labels_field.

    This is useful to apply 'milestone' or 'schedule' or
    'event' information to give more context to time-series
    plots.
    """

    # Extract first axes fro figure and use it to create second axes
    ax1 = figure.axes[0]
    ax2 = ax1.twiny()

    # Convert value_field to pandas datetime
    frame[value_field] = pd.to_datetime(frame[value_field])

    # Set limit for second axes based on first axes
    # (Prevents labels cumulation on graph borders
    ax2.set_xlim(ax1.axis()[0], ax1.axis()[1])

    # Set both values and labels (rotation is set to reduce overlapping)
    ax2.set_xticks(frame[value_field].tolist())
    ax2.set_xticklabels(frame[labels_field].tolist(), rotation=50)
    ax2.grid()

    # Append this new plot to the figure and return figure
    figure.axes.append(ax2)
    return figure
