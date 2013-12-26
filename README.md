VeilHelpers
===========

VeilHelpers is a part of VeilFS system that unifies access to files stored at heterogeneous data storage systems that belong to geographically distributed organizations.

Goals
-----

The goal of VeilHelpers is provision of a "storage helpers" - liblaries allowing access to all supported by VeilFS (self-scalable cluster that will is a central point of each data centre that uses VeilFS) filesystems.

Getting Started
---------------
VeilHelpers is built with CMake. More informations about compiling the project in "Compilation" section.

Prerequisites
-------------

In order to compile the project, you need to have fallowing additional libraries, its headers and all its prerequisites in include/ld path:
Also you need cmake 2.8+.

* libfuse (only headers needed)
* libboost
* libprotobuf

Use this command to install the required dependency packages:

* Debian/Ubuntu Dependencies (.deb packages):

        apt-get install libfuse-dev libboost-dev libprotobuf-dev

* RHEL/CentOS/Fedora Dependencies (.rpm packages):

        yum install fuse-libs fuse-devel cmake28 boost-devel boost-static subversion protobuf-devel
        
Compilation
-----------

If 'PREFER_STATIC_LINK' env variable is set during compilation, shared library - libveilhelpers.so/dylib
will be linked statically against protobuf, boost_ and openssl (if it's possible).

#### Build
    
    make -s build
    
after this step you should have your libveilhelpers.a and libveilhelpers.so/dylib in "build" subdirectory.
    

#### Testing
    
There are two testing targets:

    make -s test

which has summarized output (per test case) and:

    make -s cunit

which shows detailed test results. 
    
Support
-------
For more information visit project Confluence or write to 'wrzeszcz@agh.edu.pl'.