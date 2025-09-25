# PMan
Photo (and general file) manager for windows in python.  Intended for media (photos, videos, sound files, etc.), but applicable to files generally, this windowing application is designed to scan a directory tree and 
index every file contained therein, finding every duplicate regardless of whether the files share common names and dates.  It lists these files in a table from which the user can view the files and delete or move files
by dragging amd dropping them to different locations.  It is designed to augment the windows OS and interacts seemlessly with windows explorer windows and utilises OS registered viewers except in the case of photos where 
it uses its own internal viewer with builtin face recognition.

After the master directory tree is scanned, a second directory can be scanned and the application will reveal every file from the second directory that is not also in the master directory, again by computing a unique hascode 
based on the content of the firle rather than its attributes.  (It will also optionally show those files that are already in the master directory).  The files can be viewed and dragged from the viewing table and dropped in a windows explorer folder effectively 
moving the file on disk from its source location to a new target location.

This project is very much under development, and while it supports face recognition in the internal viewer the recognition algorythm is a little rudimentary and requires frontal views of faces and offers limited functionality concerning tagging.

# Author:  
    Jonathan Bishop
