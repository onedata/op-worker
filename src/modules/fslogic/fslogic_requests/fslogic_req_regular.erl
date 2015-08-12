%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc FSLogic request handlers for regular files.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_req_regular).
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_file_location/3, get_new_file_location/3]).

%%%===================================================================
%%% API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Gets file location (implicit file open operation). Allows to force-select ClusterProxy helper.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(File :: fslogic_worker:file(), Flags :: fslogic_worker:open_flags(), ForceClusterProxy :: boolean()) ->
    no_return().
get_file_location(_File, _Flags, _ForceClusterProxy) ->
    ?NOT_IMPLEMENTED.


%%--------------------------------------------------------------------
%% @doc Gets new file location (implicit mknod operation).
%% @end
%%--------------------------------------------------------------------
-spec get_new_file_location(File :: file_meta:path(), Flags :: fslogic_worker:open_flags(), ForceClusterProxy :: boolean()) ->
    no_return().
get_new_file_location(_File, _Flags, _ForceClusterProxy) ->
    ?NOT_IMPLEMENTED.
