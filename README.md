This is an initial cut at some reusable Gradle tasks for:

1. Exporting an existing NiFi process group to a local XML template file that can then be stored in version control
1. Uploading that template file to create a new process group, along with setting variables, enabling controller services, and 
starting the process group

This is just example code - the intent is to copy/paste/modify this as needed. One day, it may become something more
easily reused as a library.

Primary tasks of interest:

- exportTemplate
- deployTemplate
- startProcessGroup/stopProcessGroup

See gradle.properties for the properties to configure as well.
