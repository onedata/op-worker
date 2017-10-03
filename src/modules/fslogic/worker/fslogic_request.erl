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
-export([get_file_partial_ctx/2, get_target_providers/3]).

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
get_file_partial_ctx(_UserCtx, #fuse_request{fuse_request = #file_request{context_guid = FileGuid}}) ->
    file_partial_ctx:new_by_guid(FileGuid);
get_file_partial_ctx(_UserCtx, #fuse_request{}) ->
    undefined;
get_file_partial_ctx(_UserCtx, #provider_request{context_guid = FileGuid}) ->
    file_partial_ctx:new_by_guid(FileGuid);
get_file_partial_ctx(_UserCtx, #proxyio_request{parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}}) ->
    file_partial_ctx:new_by_guid(FileGuid);
get_file_partial_ctx(_UserCtx, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

%%--------------------------------------------------------------------
%% @doc
%% Get providers capable of handling given request.
%% @end
%%--------------------------------------------------------------------
-spec get_target_providers(user_ctx:ctx(), file_partial_ctx:ctx(), fslogic_worker:request()) ->
    [oneprovider:id()].
get_target_providers(_UserCtx, undefined, _) ->
    [oneprovider:get_provider_id()];
get_target_providers(UserCtx, File, #fuse_request{
    fuse_request = #resolve_guid{}
}) ->
    get_target_providers_for_attr_req(UserCtx, File);
get_target_providers(UserCtx, File, #fuse_request{fuse_request = #file_request{
    file_request = #get_file_attr{}
}}) ->
    get_target_providers_for_attr_req(UserCtx, File);
get_target_providers(_UserCtx, _File, #provider_request{provider_request = #replicate_file{
    provider_id = ProviderId
}}) ->
    [ProviderId];
get_target_providers(_UserCtx, _File, #provider_request{provider_request = #invalidate_file_replica{
    provider_id = ProviderId
}}) ->
    [ProviderId];
get_target_providers(UserCtx, File, _Req) ->
    get_target_providers_for_file(UserCtx, File).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get providers capable of handling resolve_guid/get_attr request.
%% @end
%%--------------------------------------------------------------------
-spec get_target_providers_for_attr_req(user_ctx:ctx(), file_partial_ctx:ctx()) ->
    [oneprovider:id()].
get_target_providers_for_attr_req(UserCtx, FileCtx) ->
    %todo TL handle guids stored in file_force_proxy
    case file_partial_ctx:is_space_dir_const(FileCtx) of
        true ->
            [oneprovider:get_provider_id()];
        false ->
            get_target_providers_for_file(UserCtx, FileCtx)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get providers cappable of handling generic request.
%% @end
%%--------------------------------------------------------------------
-spec get_target_providers_for_file(user_ctx:ctx(), file_partial_ctx:ctx()) ->
    [oneprovider:id()].
get_target_providers_for_file(UserCtx, FilePartialCtx) ->
    case file_partial_ctx:is_user_root_dir_const(FilePartialCtx, UserCtx) of
        true ->
            [oneprovider:get_provider_id()];
        false ->
            SpaceId = file_partial_ctx:get_space_id_const(FilePartialCtx),
            SessionId = user_ctx:get_session_id(UserCtx),
            {ok, Providers} = space_logic:get_provider_ids(SessionId, SpaceId),
            case lists:member(oneprovider:get_provider_id(), Providers) of
                true ->
                    [oneprovider:get_provider_id()];
                false ->
                    Providers
            end
    end.