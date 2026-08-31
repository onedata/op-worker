%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is deprecated and kept only because env up mechanism depends
%%% on it to setup environment. It can be deleted after last envup drops dead
%%% @end
%%%-------------------------------------------------------------------
-module(helper).
-author("Krzysztof Trzepla").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([new/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec new(helper_spec:name(), helper_spec:configuration(), helper_spec:credentials()) ->
    helper_spec:t().
new(HelperName, ConfigurationParams, CredentialsParams) ->
    DefaultCredentialsParams = default_credentials_params(HelperName),

    #helper_spec{
        name = HelperName,
        configuration = ConfigurationParams,
        credentials = maps:merge(DefaultCredentialsParams, CredentialsParams)
    }.


%% @private
default_credentials_params(HelperName) when
    HelperName == ?POSIX_HELPER_NAME;
    HelperName == ?NULL_DEVICE_HELPER_NAME;
    HelperName == ?NFS_HELPER_NAME;
    HelperName == ?GLUSTERFS_HELPER_NAME ->
    #{<<"uid">> => <<"0">>, <<"gid">> => <<"0">>};

default_credentials_params(_) ->
    #{}.
