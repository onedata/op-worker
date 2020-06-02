%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements LUMA DB table that associates storage group
%%% with Onedata group, represented by #luma_onedata_group record.
%%% Tha mappings are used by storage_sync to associate synchronized ACLs,
%%% set for specific, named group, with corresponding Onedata group.
%%%
%%% A separate table is created for each storage
%%% so the mappings are actually associated with pair (storage:id(), luma:acl_who()).
%%%
%%% Mappings may be set in 2 ways:
%%%  * preconfigured using REST API in case EMBEDDED_LUMA
%%%    is set for given storage.
%%%  * cached after querying external, 3rd party LUMA server in case
%%%    EXTERNAL_LUMA mode is set for given storage.
%%%
%%% For more info please read the docs of luma.erl module.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_onedata_groups).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([
    map_acl_group_to_onedata_group/2,
    clear_all/1
]).

-type key() :: luma:acl_who().
-type record() :: luma_onedata_group:group().

-export_type([key/0, record/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec map_acl_group_to_onedata_group(storage:data(), key()) ->
    {ok, record()} | {error, term()}.
map_acl_group_to_onedata_group(Storage, AclGroup) ->
    luma_db:get(Storage, AclGroup, ?MODULE, fun() ->
        acquire(Storage, AclGroup)
    end).

-spec clear_all(storage:id()) -> ok | {error, term()}.
clear_all(StorageId) ->
    luma_db:clear_all(StorageId, ?MODULE).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec acquire(storage:data(), key()) ->
    {ok, record()} | {error, term()}.
acquire(Storage, AclGroup) ->
    case external_reverse_luma:map_acl_group_to_onedata_group(AclGroup, Storage) of
        {ok, OnedataGroupMap} ->
            {ok, luma_onedata_group:new(OnedataGroupMap)};
        Error ->
            Error
    end.
