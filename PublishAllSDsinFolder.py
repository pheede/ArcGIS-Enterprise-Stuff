# Author: ichivite@esri.com, pheede@esri.com
# Tested with ArcGIS for Server 10.2.2 and Python 2.7
# Latest here: https://github.com/Cintruenigo/ArcGIS-Server-Stuff/blob/master/PublishAllSDsinFolder
# Pieces liberally borrowed from portalpy at https://github.com/esri/portalpy

import os, sys, time
import urllib, urllib2, urlparse, httplib, json
import mimetools, mimetypes
from cStringIO import StringIO
import threading, Queue

thread_count = 2
printLock = threading.Lock()
serviceDefinitionQueue = Queue.Queue()
publishedQueue = Queue.Queue();
failedQueue = Queue.Queue()

def getToken(baseurl, username, password):
    url = urlparse.urljoin(baseurl, '/arcgis/admin/generateToken')

    """ Generates and returns a new token. """
    postdata = { 'username': username, 'password': password,
                 'client': 'requestip', 'expiration': 60, 'f': 'json' }

    resp_json = _post(url, postdata)
    
    if resp_json: return resp_json['token']
    raise Exception('Unable to authenticate with ArcGIS Server to retrieve token')

def _tostr(obj):
    if not obj:
        return ''
    if isinstance(obj, list):
        return ', '.join(map(_tostr, obj))
    return str(obj)

def _get_content_type(filename):
    return mimetypes.guess_type(filename)[0] or 'application/octet-stream'

def _encode_multipart_formdata(fields, files):
    boundary = mimetools.choose_boundary()
    buf = StringIO()
    for (key, value) in fields.iteritems():
        buf.write('--%s\r\n' % boundary)
        buf.write('Content-Disposition: form-data; name="%s"' % key)
        buf.write('\r\n\r\n' + _tostr(value) + '\r\n')
    for (key, filepath, filename) in files:
        buf.write('--%s\r\n' % boundary)
        buf.write('Content-Disposition: form-data; name="%s"; filename="%s"\r\n' % (key, filename))
        buf.write('Content-Type: %s\r\n' % (_get_content_type(filename)))
        f = open(filepath, "rb")
        try:
            buf.write('\r\n' + f.read() + '\r\n')
        finally:
            f.close()
    buf.write('--' + boundary + '--\r\n\r\n')
    buf = buf.getvalue()
    return boundary, buf

def _post(url, postdata):
    if 'f' not in postdata: postdata['f'] = 'json' # add json format parameter if format not already specified
    encoded_postdata = urllib.urlencode(postdata)
    opener = urllib2.build_opener()
    resp = opener.open(url, data=encoded_postdata)
    resp_data = resp.read() # read raw response
    resp_json = json.loads(resp_data) # parse json response
    
    return resp_json
    
def _postmultipart(host, selector, fields, files, ssl):
    boundary, body = _encode_multipart_formdata(fields, files)
    headers = { 'Content-Type': 'multipart/form-data; boundary={0}'.format(boundary) }
    if ssl: h = httplib.HTTPSConnection(host)
    else: h = httplib.HTTPConnection(host)
    
    h.request('POST', selector, body, headers)
    resp = h.getresponse()

    return resp.read()

def uploadFile(baseurl, token, file):
    url = urlparse.urljoin(baseurl, '/arcgis/admin/uploads/upload')

    files = [('itemFile', file, os.path.split(file)[1])]
    fields = { 'token' : token, 'f' : 'json' }

    ssl = url.startswith('https://')
    
    parsed_url = urlparse.urlparse(url)
    
    resp = _postmultipart(parsed_url.netloc, str(parsed_url.path), fields, files, ssl)
    resp_json = json.loads(resp)
    
    try: return resp_json['item']['itemID']
    except: raise Exception('Unable to upload file {0}'.format(file))

def getPublishingServiceMaxInstances(baseurl, token):
    url = urlparse.urljoin(baseurl, '/arcgis/admin/services/System/PublishingTools.GPServer')
    
    resp_json = _post(url, { 'token' : token })
    
    return int(resp_json['maxInstancesPerNode'])
    
def publishService(baseurl, token, itemid):
    url = urlparse.urljoin(baseurl, '/arcgis/rest/services/System/PublishingTools/GPServer/Publish%20Service%20Definition/submitJob')
    
    postdata = { 'token': token, 'f': 'json', 'in_sdp_id' : itemid }

    encoded_postdata = urllib.urlencode(postdata)
    opener = urllib2.build_opener()
    resp = opener.open(url, data=encoded_postdata)
    resp_data = resp.read() # read raw response
    resp_json = json.loads(resp_data) # parse json response

    if not resp_json: raise Exception('Unable to publish item {0}'.format(itemid))
    
    return resp_json['jobId']

def getPublishingJobStatus(baseurl, token, jobid):
    url = urlparse.urljoin(baseurl, '/arcgis/rest/services/System/PublishingTools/GPServer/Publish%20Service%20Definition/jobs/' + jobid)
    postdata = { 'token' : token }
    resp_json = _post(url, postdata)
    status = resp_json['jobStatus']
    return status
    
def main(path, baseurl, username, password):
    token = getToken(baseurl, username, password)

    print('This script publishes all Service Definitions at {0} into {1}'.format(path, baseurl))
    
    # check the max instances for the publishing endpoint and output warning if default (or less) is in use
    maxInstances = getPublishingServiceMaxInstances(baseurl, token)
    
    if maxInstances <= 2:
        print('NOTE: The site is using a max of {0} processes per server to publish services.'.format(maxInstances))
        print('Increase the max instances if server resources allow.')

    # build a Queue containing all service definition files within the input folder and its subdirectories
    for root, subFolders, files in os.walk(path):
        for fname in files:
            extension = os.path.splitext(fname)[1][1:].strip().lower()
            if extension == 'sd':
                serviceDefinitionFile = os.path.join(root,fname)
                print(' Adding to queue {0}'.format(serviceDefinitionFile))
                serviceDefinitionQueue.put(serviceDefinitionFile)

    # create and start a pool of publishing threads 
    thread_list = []
    for i in range (thread_count):
        t = threading.Thread(target=publisherThread, args = (baseurl, token))
        t.daemon = True
        thread_list.append(t)

    for thread in thread_list: thread.start()

    # wait for queue to be empty
    serviceDefinitionQueue.join()
    
    print('All .sd files have been sent to the server for publishing. Waiting for publishing jobs to complete..')
    time.sleep(2) 
    
    # poll for publishing status until all jobs are finished (successfully or not)
    pendingJobs = list(publishedQueue.queue)
    successfulJobs = []
    failedJobs = list(failedQueue.queue)
    
    while len(pendingJobs) > 0:
        stillPendingJobs = []
        for jobid, sdpath in pendingJobs:
            jobStatus = getPublishingJobStatus(baseurl, token, jobid)
            if jobStatus == 'esriJobSucceeded': successfulJobs.append((jobid, sdpath))
            elif jobStatus == 'esriJobFailed': failedJobs.append((jobid, sdpath))
            elif jobStatus in ('esriJobWaiting', 'esriJobExecuting', 'esriJobSubmitted'): stillPendingJobs.append((jobid, sdpath))
            else: failedJobs.append((jobid, sdpath)) # cancelled statuses mostly
            
        pendingJobs = stillPendingJobs
        if len(pendingJobs) > 0:
            print('Still waiting.. {0} services still being created'.format(len(pendingJobs)))
            time.sleep(2) # give the server some breathing room..

    # print out publishing results
    if len(successfulJobs) > 0: 
        print('Services successfully published:')
        for jobid, sdpath in successfulJobs: print(' ... {0}'.format(sdpath))
    if len(failedJobs) > 0:
        print('Services that FAILED to publish:')
        for jobid, sdpath in failedJobs: print(' ... {0}'.format(sdpath))
    
def publisherThread(baseurl, token):
    while not serviceDefinitionQueue.empty():
        try:
            serviceDefinitionFile = serviceDefinitionQueue.get()
            printLock.acquire() # synchronize print statement, otherwise they have a tendency to overlap in the console
            print(' ... publishing: {0}'.format(serviceDefinitionFile))
            printLock.release()
            
            try: 
                itemid = uploadFile(baseurl, token, serviceDefinitionFile) # upload the sd to the server
                jobid = publishService(baseurl, token, itemid) # start publishing job for uploaded file
                publishedQueue.put((jobid, serviceDefinitionFile)) # store publishing jobid to check status later
            except:
                failedQueue.put(('-', serviceDefinitionFile))
            
            serviceDefinitionQueue.task_done()
        except Exception as e:
            print(e.message)
        
if __name__ == '__main__':
    try:
        path = sys.argv[1]
        baseurl = sys.argv[2] # note: baseurl is expected to be the root of the server and the site name is always expected to be /arcgis
        username = sys.argv[3]
        password = sys.argv[4]
    except:
        print('Usage:')
        print('PublishAllSDsinFolder.py <folderWithSDs> <serverPath> <username> <password>')
        print('')
        print('E.g.: PublishAllSDsinFolder.py d:\\temp https://server1.example.com:6443 siteadmin sitepassword')
        sys.exit(1)

    if not (os.path.isdir(path) or os.path.isfile(path)):
        print("File or folder {0} not found. Please check input parameters.".format(path))
        sys.exit(1)
        
    main(path, baseurl, username, password)
