#Author: ichivite@esri.com
#Built with ArcGIS 10.2.2
#Latest here: https://github.com/Cintruenigo/ArcGIS-Server-Stuff/blob/master/PublishAllSDsinFolder

import os, arcpy, Queue,threading,time


path = r"D:\Ismael\Demos\AdminAPI"                        #Path to folder containing Service Definitions
serverConnection = r"D:\Ismael\Demos\AdminAPI\ismael.ags" #ArcGIS Server connection file
thread_count = 2
serviceDefinitionQueue = Queue.Queue()

def main():

    print "This script publishes all Service Definitions at {0} into {1}".format(path, serverConnection)
    
    #Build a Queue containing all service definition files within the input folder and its subdirectories
    for root, subFolders, files in os.walk(path):
        for fname in files:
            extension = os.path.splitext(fname)[1][1:].strip().lower()
            if extension == 'sd':
                serviceDefinitionFile = os.path.join(path,fname)
                print " Adding to queue " + serviceDefinitionFile
                serviceDefinitionQueue.put(serviceDefinitionFile)

                    
    # Create a pool of threads
    thread_list = []
    for i in range (thread_count):
        t = threading.Thread(target=publishServiceDefinitionFile, args = (serverConnection,))
        t.daemon = True
        thread_list.append(t)

    # Start threads
    for thread in thread_list:
        print " Starting new thread... "
        thread.start()

    # Wait for queue to be empty
    serviceDefinitionQueue.join()


def publishServiceDefinitionFile(arcgisServerConnectionFile):
    
    while serviceDefinitionQueue.empty :
        try:
            serviceDefinitionFile = serviceDefinitionQueue.get()
            print " ... publishing: {0}".format(serviceDefinitionFile)
            arcpy.UploadServiceDefinition_server(serviceDefinitionFile, arcgisServerConnectionFile)
            serviceDefinitionQueue.task_done()
        except Exception as e:
            print e.message
            arcpy.AddError(e.message)
        
if __name__ == "__main__":
     main()
