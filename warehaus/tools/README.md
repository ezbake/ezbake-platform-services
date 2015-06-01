warehaus-tools
======

Command line interface for warehaus

##### Usage #####
- CreateWHPFile
- WarehausExport
- WarehausGet
- WarehausImport
- WarehausInsert
- WarehausReplay
- WarehausVersions
- WarehausViewGet
- WarehausViewInsert

###### CreateWHPFile ######
```java -cp warehaus-tools.jar ezbake.warehaus.tools.CreateWHPFile -f [whp filename] -u [uri] -r [raw data file] -p [parsed data file]```
 
Required Arguments:
-f 
-u
-r
-p
 
###### WarehausExport ######
```java -cp warehaus-tools.jar ezbake.warehaus.tools.WarehausExport -f [export tar] -n [feed name] -s [unix starttime] -e [unix endtime]```

Required Arguments:
-f 
-n
 
###### WarehausGet ######
```java -cp warehaus-tools.jar ezbake.warehaus.tools.WarehausGet -f [export file] -u [uri] -t ["raw" | "parsed"] -v [version number]```
 
Required Arguments:
-f 
-u
-t

###### WarehausImport ######
```java -cp warehaus-tools.jar ezbake.warehaus.tools.WarehausImport -f [export tar]```
 
Required Fields:
-f 

###### WarehausInsert ######
```java -cp warehaus-tools.jar ezbake.warehaus.tools.WarehausInsert -f [whp file] - c [classification]```
 
Required Arguments:
-f 
-c

###### WarehausReplay ######
```java -cp warehaus-tools.jar ezbake.warehaus.tools.WarehausReplay -u [urn] -s [unix starttime] -e [unix endtime]```

Required Arguments:
-u
 
###### WarehausVersions ######
```java -cp warehaus-tools.jar ezbake.warehaus.tools.WarehausVersions -u [uri]```

Required Arguments:
-u

###### WarehausViewGet ######
 ```java -cp warehaus-tools.jar ezbake.warehaus.tools.WarehausViewGet -f [export file] -u [uri] -s [namespace] -n [view name] -v [version]```

Required Arguments:
-f
-u
-s
-n

###### WarehausViewInsert ######
 ```java -cp warehaus-tools.jar ezbake.warehaus.tools.WarehausViewInsert -f [view file] -u [uri] -s [namespace] -n [view name] -c [classification]```

Required Arguments:
-f
-u
-c
  