%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for managing file metadata (requests are delegated to middleware_worker). 
%%% @TODO VFS-9578 - Move locally managed file metadata operations to middleware worker
%%% @end
%%%-------------------------------------------------------------------
-module(mi_file_metadata).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([
    set_custom_metadata/5,
    get_custom_metadata/5,
    remove_custom_metadata/3,

    gather_distribution/2,
    gather_historical_dir_size_stats/3,
    get_storage_locations/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec set_custom_metadata(
    session:id(),
    lfm:file_key(),
    custom_metadata:type(),
    custom_metadata:value(),
    custom_metadata:query()
) ->
    ok | no_return().
set_custom_metadata(SessionId, FileKey, Type, Value, Query) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, resolve_symlink),

    middleware_worker:check_exec(SessionId, FileGuid, #custom_metadata_set_request{
        type = Type,
        query = Query,
        value = Value
    }).


-spec get_custom_metadata(
    session:id(),
    lfm:file_key(),
    custom_metadata:type(),
    custom_metadata:query(),
    boolean()
) ->
    custom_metadata:value() | no_return().
get_custom_metadata(SessionId, FileKey, Type, Query, Inherited) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, resolve_symlink),

    middleware_worker:check_exec(SessionId, FileGuid, #custom_metadata_get_request{
        type = Type,
        query = Query,
        inherited = Inherited
    }).


-spec remove_custom_metadata(session:id(), lfm:file_key(), custom_metadata:type()) ->
    ok | no_return().
remove_custom_metadata(SessionId, FileKey, Type) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, resolve_symlink),

    middleware_worker:check_exec(SessionId, FileGuid, #custom_metadata_remove_request{
        type = Type
    }).


-spec gather_distribution(session:id(), lfm:file_key()) ->
    file_distribution:get_result() | no_return().
gather_distribution(SessionId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),

    middleware_worker:check_exec(SessionId, FileGuid, #file_distribution_gather_request{}).


-spec gather_historical_dir_size_stats(session:id(), lfm:file_key(), ts_browse_request:record()) ->
    ts_browse_result:record() | no_return().
gather_historical_dir_size_stats(SessionId, FileKey, Request) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),

    middleware_worker:check_exec(SessionId, FileGuid, #historical_dir_size_stats_gather_request{
        request = Request
    }).


-spec get_storage_locations(session:id(), lfm:file_key()) ->
    file_distribution:storage_locations() | no_return().
get_storage_locations(SessionId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),
    middleware_worker:check_exec(SessionId, FileGuid, #file_storage_locations_get_request{}).
