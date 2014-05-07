# Author: ichivite@esri.com, pheede@esri.com
# Tested with ArcGIS for Server 10.2.2
# Latest here: https://github.com/Cintruenigo/ArcGIS-Server-Stuff/blob/master/PublishAllSDsinFolder
# Pieces liberally borrowed from portalpy at https://github.com/esri/portalpy

import os, sys, time
import urllib, urllib2, urlparse, httplib, json
import mimetools, mimetypes
from cStringIO import StringIO
import threading, Queue

thread_count = 2
serviceDefinitionQueue = Queue.Queue()

# setup _referer variable with local hostname, used by various methods later on
_referer = None
   
def main(path, baseurl, token):

    print('Publishes all Service Definitions at {0} into {1}'.format(path, baseurl))
    
    #Build a Queue containing all service definition files within the input folder and its subdirectories
    for root, subFolders, files in os.walk(path):
        for fname in files:
            extension = os.path.splitext(fname)[1][1:].strip().lower()
            if extension == 'sd':
                serviceDefinitionFile = os.path.join(path,fname)
                print(' {0}'.format(serviceDefinitionFile))
                serviceDefinitionQueue.put(serviceDefinitionFile)

                    
    # Create a pool of threads
    thread_list = []
    for i in range (thread_count):
        t = threading.Thread(target=publishServiceDefinitionFile, args = (baseurl, token))
        #t.daemon = True
        t.start()
        thread_list.append(t)

    print('Publishing...')
    # Wait for threads to finish
    #for thread in thread_list:
    #    thread.join()

    # Wait for queue to be empty
    serviceDefinitionQueue.join()
   
    print('All .sd files have been sent to the server for publishing. Services will keep starting for a while, please check the server services directory for status.')


def publishServiceDefinitionFile(baseurl, token):
    
    while serviceDefinitionQueue.empty :
        try:
            serviceDefinitionFile = serviceDefinitionQueue.get()
            #print(' ... publishing: {0}'.format(serviceDefinitionFile))
            
            try:
                itemid = uploadFile(baseurl, token, serviceDefinitionFile)
                publishService(baseurl, token, itemid)
            except:
                print(' ... publishing of {0} failed'.format(serviceDefinitionFile))
            
            serviceDefinitionQueue.task_done()
        except Exception as e:
            print(e.message)

def setupReferer():
    global _referer
    import socket
    ip = socket.gethostbyname(socket.gethostname())
    _referer = socket.gethostbyaddr(ip)[0]

def getToken(baseurl, username, password):
    url = urlparse.urljoin(baseurl, '/arcgis/admin/generateToken')

    """ Generates and returns a new token. """
    postdata = { 'username': username, 'password': password,
                 'client': 'referer', 'referer': _referer,
                 'expiration': 60, 'f': 'json' }
    headers = [('Referer', _referer)]

    encoded_postdata = urllib.urlencode(postdata)
    opener = urllib2.build_opener()
    opener.addheaders = headers
    resp = opener.open(url, data=encoded_postdata)
    resp_data = resp.read() # read raw response
    resp_json = json.loads(resp_data) # parse json response

    if resp_json:
        return resp_json['token']
        
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

def _postmultipart(host, selector, fields, files, ssl):
    boundary, body = _encode_multipart_formdata(fields, files)
    headers = { 'Referer': _referer, 'Content-Type': 'multipart/form-data; boundary={0}'.format(boundary) }
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
    
    try:
        return resp_json['item']['itemID']
    except:
        raise Exception('Unable to upload file {0}'.format(file))

def publishService(baseurl, token, itemid):
    url = urlparse.urljoin(baseurl, '/arcgis/rest/services/System/PublishingTools/GPServer/Publish%20Service%20Definition/submitJob')
    
    postdata = { 'token': token, 'f': 'json', 'in_sdp_id' : itemid }
    headers = [('Referer', _referer)]

    encoded_postdata = urllib.urlencode(postdata)
    opener = urllib2.build_opener()
    opener.addheaders = headers
    resp = opener.open(url, data=encoded_postdata)
    resp_data = resp.read() # read raw response
    resp_json = json.loads(resp_data) # parse json response

    if resp_json: return
        
    raise Exception('Unable to publish item {0}'.format(itemid))
        
if __name__ == '__main__':
    try:
        #path = sys.argv[1]
        #baseurl = sys.argv[2]
        #username = sys.argv[3]
        #password = sys.argv[4]

        path = r'D:\Ismael\Demos\AdminAPI'
        baseurl = 'http://ismael.esri.com:6080'
        username = 'psa'
        password = 'psa'
    except:
        print('"Usage:')
        print('PublishAllSDsinFolder.py <folderWithSDs> <serverPath> <username> <password>')
        print('')
        print('E.g.: PublishAllSDsinFolder.py d:\\temp https://server1.example.com:6443 siteadmin sitepassword')
        sys.exit(1)

    if not (os.path.isdir(path) or os.path.isfile(path)):
        print("File or folder {0} not found. Please check input parameters.".format(path))
        sys.exit(1)
    
    print('Initializing..')
    setupReferer()
    
    token = getToken(baseurl, username, password)
    
    main(path, baseurl, token)

