%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for obtaining and modifying things related to any
%%% fslogic request.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_request).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_file_partial_ctx/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get file_ctx record associated with request. If request does not point to
%% specific file, the function returns undefined.
%% @end
%%--------------------------------------------------------------------
-spec get_file_partial_ctx(user_ctx:ctx(), fslogic_worker:request()) ->
    file_partial_ctx:ctx() | undefined.
get_file_partial_ctx(UserCtx, #fuse_request{fuse_request = #resolve_guid{path = Path}}) ->
    file_partial_ctx:new_by_logical_path(UserCtx, Path);
get_file_partial_ctx(UserCtx, #fuse_request{fuse_request = #resolve_guid_by_canonical_path{path = Path}}) ->
    file_partial_ctx:new_by_canonical_path(UserCtx, Path);
get_file_partial_ctx(_UserCtx, #fuse_request{fuse_request = #resolve_guid_by_relative_path{root_file = RelRootGuid}}) ->
    file_partial_ctx:new_by_guid(RelRootGuid);
get_file_partial_ctx(_UserCtx, #fuse_request{fuse_request = #ensure_dir{root_file = RelRootGuid}}) ->
    file_partial_ctx:new_by_guid(RelRootGuid);
get_file_partial_ctx(_UserCtx, #fuse_request{fuse_request = #file_request{context_guid = FileGuid}}) ->
    file_partial_ctx:new_by_guid(FileGuid);
get_file_partial_ctx(_UserCtx, #fuse_request{fuse_request = #get_fs_stats{file_id = FileGuid}}) ->
    file_partial_ctx:new_by_guid(FileGuid);
get_file_partial_ctx(_UserCtx, #fuse_request{}) ->
    undefined;
get_file_partial_ctx(_UserCtx, #proxyio_request{parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}}) ->
    file_partial_ctx:new_by_guid(FileGuid);
get_file_partial_ctx(_UserCtx, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).
