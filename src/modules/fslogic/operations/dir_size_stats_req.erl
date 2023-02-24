%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on dir size stats.
%%% @end
%%%--------------------------------------------------------------------
-module(dir_size_stats_req).
-author("Michal Stanisz").

-include("modules/fslogic/data_access_control.hrl").
-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("proto/oneprovider/provider_rpc_messages.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([get_historical/4]).


%%%===================================================================
%%% API
%%%===================================================================

-spec get_historical(user_ctx:ctx(), file_ctx:ctx(), od_provider:id(), ts_browse_request:record()) ->
    ts_browse_result:record().
get_historical(UserCtx, FileCtx0, ProviderId, BrowseRequest) ->
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_metadata_mask)]
    ),
    
    get_historical_insecure(FileCtx2, ProviderId, BrowseRequest).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_historical_insecure(file_ctx:ctx(), od_provider:id(), ts_browse_request:record()) ->
    ts_browse_result:record().
get_historical_insecure(FileCtx, ProviderId, BrowseRequest) ->
    Guid = file_ctx:get_logical_guid_const(FileCtx),
    RpcRequest = #{
        ProviderId => #provider_historical_dir_size_stats_browse_request{request = BrowseRequest}
    },
    case provider_rpc:gather(Guid, RpcRequest) of
        #{ProviderId := {ok, Result}} -> Result;
        #{ProviderId := {error, _} = Error} -> Error
    end.
