/**
 * @file storageHelperFactory.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "helpers/storageHelperFactory.hh"
#include "directIOHelper.hh"

StorageHelperFactory::StorageHelperFactory() 
{
}

StorageHelperFactory::~StorageHelperFactory() 
{
}

shared_ptr<IStorageHelper> StorageHelperFactory::getStorageHelper(std::string sh_name, std::vector<std::string> args) {
    if(sh_name == "DirectIO")
        return shared_ptr<IStorageHelper>(new DirectIOHelper(args));
    else
    {
        return shared_ptr<IStorageHelper>();
    }
}
