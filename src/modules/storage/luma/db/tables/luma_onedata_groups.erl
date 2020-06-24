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
%%% Mappings may be set only in 2 ways:
%%%  * preconfigured using REST API in case LOCAL_FEED
%%%    is set for given storage.
%%%  * acquired from external service in case EXTERNAL FEED
%%%    is set for given storage.
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
    store/3,
    delete/2,
    clear_all/1,
    get_and_describe/2
]).

-type key() :: luma:acl_who().
-type record() :: luma_onedata_group:group().
-type storage() :: storage:id() | storage:data().

-export_type([key/0, record/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec map_acl_group_to_onedata_group(storage:data(), key()) ->
    {ok, record()} | {error, term()}.
map_acl_group_to_onedata_group(Storage, AclGroup) ->
    luma_db:get_or_acquire(Storage, AclGroup, ?MODULE, fun() ->
        acquire(Storage, AclGroup)
    end, [?POSIX_STORAGE, ?IMPORTED_STORAGE]).

-spec store(storage(), key(), luma_onedata_group:group_map()) -> ok | {error, term()}.
store(Storage, AclGroup, OnedataGroupMap) ->
    case luma_sanitizer:sanitize_onedata_group(OnedataGroupMap) of
        {ok, OnedataGroupMap2} ->
            Record = luma_onedata_group:new(OnedataGroupMap2),
            luma_db:store(Storage, AclGroup, ?MODULE, Record, ?LOCAL_FEED, ?FORCE_OVERWRITE,
                [?POSIX_STORAGE, ?IMPORTED_STORAGE]);
        Error ->
            Error
    end.

-spec delete(storage:id(), key()) -> ok.
delete(StorageId, GroupId) ->
    luma_db:delete(StorageId, GroupId, ?MODULE, [?POSIX_STORAGE, ?IMPORTED_STORAGE]).

-spec clear_all(storage:id()) -> ok | {error, term()}.
clear_all(StorageId) ->
    luma_db:clear_all(StorageId, ?MODULE).

-spec get_and_describe(storage(), key()) ->
    {ok, luma_onedata_group:group_map()} | {error, term()}.
get_and_describe(Storage, AclGroup) ->
    luma_db:get_and_describe(Storage, AclGroup, ?MODULE, [?POSIX_STORAGE, ?IMPORTED_STORAGE]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec acquire(storage:data(), key()) ->
    {ok, record(), luma:feed()} | {error, term()}.
acquire(Storage, AclGroup) ->
    case storage:get_luma_feed(Storage) of
        ?EXTERNAL_FEED ->
            acquire_from_external_feed(Storage, AclGroup);
        _ ->
            {error, not_found}
    end.

-spec acquire_from_external_feed(storage:data(), key()) ->
    {ok, record(), luma:feed()} | {error, term()}.
acquire_from_external_feed(Storage, AclGroup) ->
    case luma_external_feed:map_acl_group_to_onedata_group(AclGroup, Storage) of
        {ok, OnedataGroupMap} ->
            {ok, luma_onedata_group:new(OnedataGroupMap), ?EXTERNAL_FEED};
        Error ->
            Error
    end.
