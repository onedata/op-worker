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
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([
    gather_distribution/2,
    gather_historical_dir_size_stats/3,

    set_custom_metadata/5,
    get_custom_metadata/5,
    remove_custom_metadata/3
]).


%%%===================================================================
%%% API
%%%===================================================================


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
