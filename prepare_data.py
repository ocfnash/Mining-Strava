import cPickle
import numpy as N
import copy
from pandas import DataFrame
from collections import namedtuple
from operator import itemgetter as nth

class StravaSegmentEffortData(object):
    IndexRow = namedtuple('IndexRow', ['athlete_id', 'effort_id', 'start_row', 'n_rows'])
    ESType = namedtuple('ESType', ['name', 'delta_from_start'])

    es_types = (ESType('lat', False),
                ESType('lng', False),
                ESType('time', True),
                ESType('distance', True),
                ESType('grade_smooth', False),
                ESType('velocity_smooth', False))

    def __init__(self, segment_characteristics):
        self.segment_characteristics = segment_characteristics
        es_types_names = [es_type.name for es_type in self.es_types]
        self.time_col = es_types_names.index('time')
        self.distance_col = es_types_names.index('distance')
        self.grade_smooth_col = es_types_names.index('grade_smooth')
        self.velocity_smooth_col = es_types_names.index('velocity_smooth')
        self.index = []
        self.data = []

    @classmethod
    def genIthRow(cls, effort_streams, i):
        res = []
        for (j, es_type) in enumerate(cls.es_types):
            offset = effort_streams[j]['data'][0] if es_type.delta_from_start else 0
            res.append(effort_streams[j]['data'][i] - offset)
        return res

    @staticmethod
    def fixColumns(effort_streams):
        # Special case hack for Strava's annoying 2-valued 'latlng' column
        assert effort_streams[0]['type'] == 'latlng'
        def fix(i, new_type):
            l = copy.copy(effort_streams[0])
            l['type'] = new_type
            l['data'] = map(nth(i), l['data'])
            return l
        effort_streams = [fix(0, 'lat'), fix(1, 'lng')] + effort_streams[1:]
        # Fix ordering
        local_es_types = [effort_stream['type'] for effort_stream in effort_streams]
        return map(lambda es_type : effort_streams[local_es_types.index(es_type.name)], StravaSegmentEffortData.es_types)

    def isTooFewRows(self, n_rows, effort_streams):
        if n_rows < self.segment_characteristics.min_effort_rows:
            return
        for stream in effort_streams:
            if len(stream['data']) != n_rows:
                return True
        return False

    def isInconsistentTimes(self, effort_summary, effort_streams):
        ts = effort_streams[self.time_col]['data']
        return effort_summary['elapsed_time'] != ts[-1] - ts[0]

    def isNotAlwaysMoving(self, effort_summary):
        return effort_summary['elapsed_time'] != effort_summary['moving_time']

    def isInconsistentDistances(self, effort_summary, effort_streams):
        ds = effort_streams[self.distance_col]['data']
        abs_diff_p = lambda x, y : abs(x / y - 1)
        if abs_diff_p(ds[-1] - ds[0], effort_summary['distance']) > self.segment_characteristics.distance_slippage or\
            abs_diff_p(effort_summary['distance'], effort_summary['segment']['distance']) > self.segment_characteristics.distance_slippage:
                return True
        return False

    def isInadmissableAvgSpeed(self, effort_summary):
        avg_speed = (effort_summary['distance'] / 1000.0) / (effort_summary['elapsed_time'] / 3600.0)
        if avg_speed < self.segment_characteristics.min_avg_speed_kph or\
            avg_speed > self.segment_characteristics.max_avg_speed_kph:
                return True
        return False

    def getRowsToCensor(self, effort_summary, effort_streams):
        def xs_dxs_from_col(col):
            xs = N.array(effort_streams[col]['data'])
            return (xs, xs[1:] - xs[:-1])
        ts, dts = xs_dxs_from_col(self.time_col)
        ds, dds = xs_dxs_from_col(self.distance_col)
        vs, dvs = xs_dxs_from_col(self.velocity_smooth_col)
        gs, dgs = xs_dxs_from_col(self.grade_smooth_col)
        if min(dts) < 0 or min(dds) < 0:
            return N.ones_like(ts, dtype=bool) # Censor all points if distance or time ever run backwards.
        dvdts = dvs/dts
        dgdds = dgs/dds
        # The below is a pretty bad way to try and deal with extremely-noisy data points (these are fairly
        # common in 'grade_smooth' and also quite common in the 'velocity_smooth' for the Tourmalet segment ---
        # I wonder if GPS signal is worse there?). With the below we lose two data-points for every bad outlier.
        # The proper way to do it would be to use some simple ideas from signal filtering.
        # Also note that previously I was filtering the grade_smooth using:
        # (abs(gs) > (1 + self.segment_characteristics.grade_slippage) * effort_summary['segment']['maximum_grade'])
        # but having the cutoff vary by segment made various charts look ugly so I decided to for for a static value.
        return (vs > self.segment_characteristics.max_abs_speed_kph * 1000 / 3600.0) |\
               (abs(gs) > 25.0) |\
               N.append([True], (abs(dvdts) > self.segment_characteristics.max_accel_mpsps) |\
                                (abs(dgdds) > self.segment_characteristics.max_grade_delta_pm))

    def maybeConsumeEffort(self, effort_summary, effort_streams):
        effort_streams = self.fixColumns(effort_streams)
        # Double check column ordering as critically important
        assert len(effort_streams) == len(self.es_types)
        for (i, es_type) in enumerate(self.es_types):
            assert es_type.name == effort_streams[i]['type']

        n_rows = effort_summary['end_index'] - effort_summary['start_index'] + 1

        if self.isTooFewRows(n_rows, effort_streams) or\
           self.isInconsistentTimes(effort_summary, effort_streams) or\
           self.isNotAlwaysMoving(effort_summary) or\
           self.isInconsistentDistances(effort_summary, effort_streams) or\
           self.isInadmissableAvgSpeed(effort_summary):
            return

        censor_f = self.getRowsToCensor(effort_summary, effort_streams)
        if float(sum(censor_f)) / n_rows > self.segment_characteristics.max_censoring:
            return
        for (i, censor) in enumerate(censor_f):
            if not censor:
                self.data.append(self.genIthRow(effort_streams, i))
        n_rows -= sum(censor_f)
        self.index.append(self.IndexRow(effort_summary['athlete']['id'],
                                        effort_summary['id'],
                                        len(self.data) - n_rows,
                                        n_rows))

    def getIthTotalTime(self, i):
        if i < 0 or i > len(self.index):
            return 0
        indexRow = self.index[i]
        r1 = indexRow.start_row
        r2 = indexRow.start_row + indexRow.n_rows
        return self.data[r2-1][self.time_col] - self.data[r1][self.time_col]

    def sortByTotalTime(self):
        perm = map(nth(1), sorted([(self.getIthTotalTime(i), i) for i in range(len(self.index))]))
        new_data = []
        new_index = []
        r_tot = 0
        for i in perm:
            indexRow = self.index[i]
            r1 = indexRow.start_row
            r2 = indexRow.start_row + indexRow.n_rows
            new_data.extend(self.data[r1:r2])
            new_index.append(self.IndexRow(indexRow.athlete_id,
                                           indexRow.effort_id,
                                           r_tot,
                                           indexRow.n_rows))
            r_tot += indexRow.n_rows
        self.data = new_data
        self.index = new_index

    def asDataFrames(self):
        return (DataFrame(N.array(self.index), columns=self.IndexRow._fields),
                DataFrame(N.array(self.data), columns=[es_type.name for es_type in self.es_types]))

SegmentCharacteristics = namedtuple('SegmentCharacteristics', ['min_effort_rows',
                                                               'min_avg_speed_kph',
                                                               'max_avg_speed_kph',
                                                               'distance_slippage',
                                                               'grade_slippage',
                                                               'max_abs_speed_kph',
                                                               'max_accel_mpsps',
                                                               'max_grade_delta_pm',
                                                               'max_censoring'])
segment_characteristics_from_id = { 3538533 : SegmentCharacteristics(min_effort_rows=50,
                                                                     min_avg_speed_kph=6.0,
                                                                     max_avg_speed_kph=25.0,
                                                                     distance_slippage=0.02,
                                                                     grade_slippage=0.2,
                                                                     max_abs_speed_kph=60.0,
                                                                     max_accel_mpsps=5.0,
                                                                     max_grade_delta_pm=5.0,
                                                                     max_censoring=0.05) }
segment_characteristics_from_id[4629741] = segment_characteristics_from_id[3538533]
segment_characteristics_from_id[665229] = segment_characteristics_from_id[3538533]

def loadData(segment_id, max_efforts = 99999):
    path = 'effort_streams/%d/' % segment_id
    all_efforts_fname = path + 'all_efforts.%d'
    effort_stream_fname = path + 'effort_stream.%d.%d'

    effortsData = StravaSegmentEffortData(segment_characteristics_from_id[segment_id])
    for effort_summary in cPickle.load(open(all_efforts_fname % segment_id))[:max_efforts]:
        effortsData.maybeConsumeEffort(effort_summary,
                                       cPickle.load(open(effort_stream_fname % (segment_id, effort_summary['id']))))
    effortsData.sortByTotalTime()
    return effortsData
