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
%%% Tha mappings are used by storage import to associate synchronized ACLs,
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

-export_type([key/0, record/0]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec map_acl_group_to_onedata_group(storage:data(), key()) ->
    {ok, record()} | {error, term()}.
map_acl_group_to_onedata_group(StorageData, AclGroup) ->
    luma_db:get_or_acquire(StorageData, AclGroup, ?MODULE, fun() ->
        acquire(StorageData, AclGroup)
    end, [?POSIX_STORAGE, ?IMPORTED_STORAGE]).


-spec store(storage:data(), key(), luma_onedata_group:group_map()) -> ok | {error, term()}.
store(StorageData, AclGroup, OnedataGroupMap) ->
    case luma_sanitizer:sanitize_onedata_group(OnedataGroupMap) of
        {ok, OnedataGroupMap2} ->
            Record = luma_onedata_group:new(OnedataGroupMap2),
            luma_db:store(StorageData, AclGroup, ?MODULE, Record, ?LOCAL_FEED, ?FORCE_OVERWRITE,
                [?POSIX_STORAGE, ?IMPORTED_STORAGE]);
        Error ->
            Error
    end.


-spec delete(storage:data(), key()) -> ok | {error, term()}.
delete(StorageData, GroupId) ->
    luma_db:delete(StorageData, GroupId, ?MODULE, [?POSIX_STORAGE, ?IMPORTED_STORAGE]).


-spec clear_all(storage:data()) -> ok | {error, term()}.
clear_all(StorageData) ->
    luma_db:clear_all(StorageData, ?MODULE).


-spec get_and_describe(storage:data(), key()) ->
    {ok, luma_onedata_group:group_map()} | {error, term()}.
get_and_describe(StorageData, AclGroup) ->
    luma_db:get_and_describe(StorageData, AclGroup, ?MODULE, [?POSIX_STORAGE, ?IMPORTED_STORAGE]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec acquire(storage:data(), key()) ->
    {luma_db:cache_policy(), record(), luma:feed()} | {error, term()}.
acquire(StorageData, AclGroup) ->
    case storage:get_luma_feed(StorageData) of
        ?EXTERNAL_FEED ->
            acquire_from_external_feed(StorageData, AclGroup);
        _ ->
            {error, not_found}
    end.


%% @private
-spec acquire_from_external_feed(storage:data(), key()) ->
    {luma_db:cache_policy(), record(), luma:feed()} | {error, term()}.
acquire_from_external_feed(StorageData, AclGroup) ->
    case luma_external_feed:map_acl_group_to_onedata_group(AclGroup, StorageData) of
        {ok, OnedataGroupMap} ->
            {cache, luma_onedata_group:new(OnedataGroupMap), ?EXTERNAL_FEED};
        Error ->
            Error
    end.
