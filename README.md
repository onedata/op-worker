# op-worker

*op-worker* is a component of [Onedata](http://onedata.org) distributed data management platform, which serves as a worker process of [Oneprovider] service. Oneprovider service requires at least one *op-worker* instance.  Adding more *op-worker* nodes scales the cluster allowing for processing more requests in parallel. *op-worker* instances are coordinated by [cluster-manager](https://github.com/onedata/cluster-manager) process, which should be deployed at least in one instance per entire cluster. Adding *cluster-manager* nodes increases fault tolerance of the Oneprovider service.

*op-worker* is a specialization of generic Onedata worker process [cluster-worker](https://github.com/onedata/cluster-worker). The *cluster-worker* provides generic funcitonalities such as persistence and cluster management, while *op-worker* augments it with its specific logic.

The main objective of *op-worker*, and *oneprovider* as a whole, is to unify access to files stored at heterogeneous data storage systems that belong to geographically distributed organizations. The *oneprovider* provides a self-scalable cluster, which manages the *onedata* system in a single data centre, i.e. it stores meta-data about actual users' data from the data centre, decides how to distribute users' files among available storage systems, and executes data management rules, which can be defined by administrators or users.


# Using

*oz-worker* is an internal component of Oneprovider service and it should only be started as part of its deployment.

## Dependencies

* docker client > 1.10
* python >= 2.7

## Documentation

Comprehensive [documentation](https://beta.onedata.org/docs/index.html) explains basic concepts of onedata, provides "Getting started" and introduces to advanced topics.

## Building
To build *oz-worker* use the provided build script:
```
./make.py
```

## Configuration and Running
*op-worker* can be started using [bamboo](https://github.com/onedata/bamboo) scripts that are included in the repository. *oneprovider* cluster needs to be connected to *onezone* in order to operate, so we should clone it and build. Run the following commands from root of *op-worker* repository:

```
git clone git@github.com:onedata/oz-worker.git
cd oz-worker && ./make.py
cd ..
./bamboos/docker/env_up.py --bin-worker . --bin-oz oz-worker --bin-cm cluster_manager bamboos/example_env/single_gr_and_provider.json
```

As *op-worker* won't work without *cluster_manager*, both those applications will be started and connected into a small cluster. The section "provider_domains" in the JSON file defines all instances of oneprovider clusters that should be started and allows for basic configuration. The *onezone* will be started also and its configuration is defined in "zone_domains" section.

After the script has finished, you should have a running, dockerized *oneprovider* instance, attached to *onezone*. Enter the graphical user interface of *onezone* on https://&lt;onezone-docker-ip&gt; (its name is in format node1.oz.&lt;timestamp&gt;.dev). In there you may log in to one of the test users and then select "go to your files" to move to the *oneprovider*'s graphical user interface. 


# APIs

- web - web-based, graphical user interface for managing user account and accessing data, available on https://&lt;oneprovider-hostname&gt:/
- [oneclient](https://github.com/onedata/oneclient) - FUSE client for accessing user's spaces of data,
- [CDMI](http://www.snia.org/cdmi) - Cloud Data Management Interface in version 1.1.1 is available on https://&lt;ip&gt;:8443/cdmi/ endpoint. To authorize yourself the X-Auth-Token header should be provided to each request, with valid user token obtained from [onezone](https://github.com/onedata/onezone),
- [REST](https://beta.onedata.org/docs/doc/advanced/rest.html) - rest api for operations such as data replication or reading metrics, available on  https://&lt;oneprovider-hostname&gt:8443/api/v3/oneprovider/ endpoint.

