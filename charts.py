"""
I generated the animated gif by using ImageMagick's 'convert' utility according to the following recipe:
convert -dispose previous $(for a in grade_velocity*all.png; do printf -- "-delay 50 %s " $a; done; ) grade_velocity.all.all.gif
"""
import cPickle, sys, os, prepare_data, gzip
import matplotlib.pyplot as plt
import numpy as N
from operator import itemgetter as nth

def nadaraya_watson(xx, xs, ys, sigma):
    K = lambda t : N.exp(-0.5*(t/sigma)**2)
    p = q = 0.0
    for (x, y) in zip(xs, ys):
        w = K(x - xx)
        p += w*y
        q += w
    return p/q

def loadMaybePickledData(segment_id):
    pickle_fname_ext = '%d.pkl.gz' % segment_id
    if os.path.isfile('data.' + pickle_fname_ext):
        sys.stdout.write('Found pickle for %d; unpickling.\n' % segment_id)
        data = cPickle.load(gzip.open('data.' + pickle_fname_ext))
        index = cPickle.load(gzip.open('index.' + pickle_fname_ext))
    else:
        sys.stdout.write('No pickle for %d; reading raw.\n' % segment_id)
        dataAndIndex = prepare_data.loadData(segment_id)
        index, data = dataAndIndex.asDataFrames()
        cPickle.dump(data, gzip.open('data.' + pickle_fname_ext, 'w'))
        cPickle.dump(index, gzip.open('index.' + pickle_fname_ext, 'w'))
    return (index, data)


def getNiceAxes(xlabel, ylabel, title, fig, sub_plot_args=(1, 1, 1)):
    ax = fig.add_subplot(*sub_plot_args)
    ax.patch.set_alpha(0)
    ax.set_title(title, fontsize=18, color='black', y=1.05)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    ax.get_xaxis().set_tick_params(direction='out')
    ax.get_yaxis().set_tick_params(direction='out')
    ax.spines['left'].set_position(('outward', 15))
    ax.spines['bottom'].set_position(('outward', 15))
    ax.set_xlabel(xlabel, fontsize=16, color='black', labelpad=20)
    ax.set_ylabel(ylabel, fontsize=16, color='black', labelpad=10)
    return ax


def makeCharts(returned_series, segment_id, color, title, pctile_lb, pctile_ub, index, data):
    """Uncomment for just one iteration to generate histograms and scatter plots.
    fig = plt.figure()
    fig.set_size_inches(16.0, 6.0)
    ax = getNiceAxes('Gradient', 'Frequency (normalised)', title, fig, (1, 2, 1))
    ax.hist(data.grade_smooth,
            bins=N.arange(-5, 25, 0.3),
            linewidth=0,
            color=color,
            normed=True)
    ax = getNiceAxes('Gradient', 'Speed (kph)', title, fig, (1, 2, 2))
    ax.set_xlim([-5, 25])
    ax.set_ylim([0, 50])
    ax.scatter(data.grade_smooth,
               data.velocity_smooth * 3.6,
               s=1,
               color=color)
    plt.tight_layout()
    fig.savefig('histogram_and_scatter.%d.png' % segment_id, transparent=True, dpi=100)"""

    xs = N.arange(-5, 25, 0.2)
    a = index.start_row[len(index) * pctile_lb / 100]
    b = index.start_row[len(index) * pctile_ub / 100]
    ys = [nadaraya_watson(x,
                          data.grade_smooth[a:b],
                          data.velocity_smooth[a:b] * 3.6,
                          1.0) for x in xs]
    returned_series.extend([xs, ys])
    fig = plt.figure()
    fig.set_size_inches(8.0, 6.0)
    ax = getNiceAxes('Gradient', 'Speed (kph)',
                     title + ' (top %d-%d%% of riders)' % (pctile_lb, pctile_ub),
                     fig)
    ax.set_xlim([0, 20])
    ax.set_ylim([0, 30])
    ax.plot(xs, ys, color=color, linewidth=2)
    plt.tight_layout()
    fig.savefig('grade_velocity_line.%d-%d.%d.png' % (pctile_lb, pctile_ub, segment_id),
                transparent=True, dpi=100)

color_from_segment_id = { 3538533 : '#FF6961',
                          665229  : '#C23B22',
                          4629741 : '#FFB347' }
index_data_from_segment_id = { segment_id : loadMaybePickledData(segment_id) for\
                                       segment_id in color_from_segment_id.keys()}

for (pctile_lb, pctile_ub) in zip(N.arange(0, 90, 10), N.arange(10, 100, 10)):
    joint_chart_series = []
    for (segment_id, title) in sorted([(3538533, "Stocking Lane"),
                                       (665229,  "Col du Tourmalet"),
                                       (4629741, "L'Alpe d'Huez")]):
        makeCharts(joint_chart_series,
                   segment_id,
                   color_from_segment_id[segment_id],
                   title,
                   pctile_lb, pctile_ub,
                   *index_data_from_segment_id[segment_id])

    fig = plt.figure()
    fig.set_size_inches(8.0, 6.0)
    ax = getNiceAxes('Gradient', 'Speed (kph)',
                     "Stocking, Tourmalet, d'Huez (top %d-%d%% of riders)" % (pctile_lb, pctile_ub),
                     fig)
    ax.set_xlim([0, 20])
    ax.set_ylim([0, 30])
    ax.set_color_cycle(map(nth(1), sorted(color_from_segment_id.items())))
    ax.plot(*joint_chart_series, linewidth=2)
    plt.tight_layout()
    fig.savefig('grade_velocity_line.%d-%d.all.png' % (pctile_lb, pctile_ub),
                transparent=True, dpi=100)
