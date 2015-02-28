import requests, cPickle, time, sys, os

access_token = sys.argv[1]

extra_headers = {'Authorization' : 'Bearer %s' % access_token}

api_base_url = 'https://www.strava.com/api/v3/'
api_segment_url = api_base_url + 'segments/%d'
api_segment_all_efforts_url = api_segment_url + '/all_efforts'
api_segment_effort_stream_url = api_base_url + 'segment_efforts/%d/streams/latlng,time,grade_smooth,velocity_smooth'

per_page = 200 # Strava max

#segment_id = 3538533 # "Stocking Lane Roundabout to Viewpoint"
#segment_id = 665229 # "Tourmalet - Eastern Approach"
segment_id = 4629741 # "L'Alpe d'Huez (Full)"
all_efforts_fname = 'all_efforts.%d' % segment_id

if os.path.isfile(all_efforts_fname):
    sys.stdout.write('Unpickling all efforts from %s\n' % all_efforts_fname)
    all_efforts = cPickle.load(open(all_efforts_fname))
else:
    all_efforts = []
    r = requests.get(api_segment_url % segment_id, headers=extra_headers)
    n_efforts = r.json()['effort_count']
    sys.stdout.write('Fetching all %d effort summaries for segment %d\n' % (n_efforts, segment_id))
    for i in range(1, 2 + n_efforts / per_page):
        sys.stdout.write('Making summary request %d\n' % i)
        r = requests.get(api_segment_all_efforts_url % segment_id, headers=extra_headers, params={'per_page' : per_page, 'page' : i})
        if r.status_code != 200:
            sys.stderr.write('Error, received code %d for summary request %d\n' % (r.status_code, i))
        else:
            all_efforts.extend(r.json())
        time.sleep(2) # Make sure do not hit Strava rate limiting
    cPickle.dump(all_efforts, open(all_efforts_fname, 'w'))

effort_ids = map(lambda d: d['id'], all_efforts)
sys.stdout.write('Fetching all %d effort streams for segment %d\n' % (len(effort_ids), segment_id))
for (i, effort_id) in enumerate(effort_ids):
    sys.stdout.write('Making stream request %d (%d of %d)\n' % (effort_id, i+1, len(effort_ids)))
    r = requests.get(api_segment_effort_stream_url % effort_id, headers=extra_headers)
    if r.status_code != 200:
        sys.stderr.write('Error, received code %d for stream request %d\n' % (r.status_code, i+1))
    else:
        cPickle.dump(r.json(), open('effort_stream.%d.%d' % (segment_id, effort_id), 'w')) # Persist to file system right away for safety.
    time.sleep(2) # Make sure do not hit Strava rate limiting
