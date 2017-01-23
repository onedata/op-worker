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
-export([get_file_ctx/2, get_target_providers/3, update_target_guid_if_file_is_phantom/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get file_ctx record associated with request. If request does not point to
%% specific file, the function returns undefined.
%% @end
%%--------------------------------------------------------------------
-spec get_file_ctx(user_ctx:ctx(), fslogic_worker:request()) ->
    file_ctx:ctx() | undefined.
get_file_ctx(UserCtx, #fuse_request{fuse_request = #resolve_guid{path = Path}}) ->
    file_ctx:new_by_logical_path(UserCtx, Path);
get_file_ctx(_UserCtx, #fuse_request{fuse_request = #file_request{context_guid = FileGuid}}) ->
    file_ctx:new_by_guid(FileGuid);
get_file_ctx(_UserCtx, #fuse_request{}) ->
    undefined;
get_file_ctx(_UserCtx, #provider_request{context_guid = FileGuid}) ->
    file_ctx:new_by_guid(FileGuid);
get_file_ctx(_UserCtx, #proxyio_request{parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}}) ->
    file_ctx:new_by_guid(FileGuid);
get_file_ctx(_UserCtx, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

%%--------------------------------------------------------------------
%% @doc
%% Get providers capable of handling given request.
%% @end
%%--------------------------------------------------------------------
-spec get_target_providers(user_ctx:ctx(), file_ctx:ctx(), fslogic_worker:request()) ->
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
get_target_providers(UserCtx, File, _Req) ->
    get_target_providers_for_file(UserCtx, File).

%%--------------------------------------------------------------------
%% @doc
%% Check if the file is of the phantom type. If so, the function updates the request to
%% point to the guid of file where the phantom points to. The file itself is also replaced
%% by the phantom target.
%% @end
%%--------------------------------------------------------------------
-spec update_target_guid_if_file_is_phantom(file_ctx:ctx() | undefined, fslogic_worker:request()) ->
    {NewFileCtx :: file_ctx:ctx() | undefined, NewRequest :: fslogic_worker:request()}.
update_target_guid_if_file_is_phantom(undefined, Request) ->
    {undefined, Request};
update_target_guid_if_file_is_phantom(FileCtx, Request) ->
    try
        {_, NewFileCtx} = file_ctx:get_file_doc(file_ctx:fill_guid(FileCtx)),
        {NewFileCtx, Request}
    catch
        _:{badmatch, {error, {not_found, file_meta}}} ->
            try
                {uuid, Uuid} = file_ctx:get_uuid_entry_const(FileCtx),
                {ok, NewGuid} = file_meta:get_guid_from_phantom_file(Uuid),
                NewRequest = change_target_guid(Request, NewGuid),
                NewFileCtx_ = file_ctx:new_by_guid(NewGuid),
                {NewFileCtx_, NewRequest}
            catch
                _:_ ->
                    {FileCtx, Request}
            end;
        _:_ ->
            {FileCtx, Request}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get providers capable of handling resolve_guid/get_attr request.
%% @end
%%--------------------------------------------------------------------
-spec get_target_providers_for_attr_req(user_ctx:ctx(), file_ctx:ctx()) ->
    [oneprovider:id()].
get_target_providers_for_attr_req(UserCtx, FileCtx) ->
    %todo TL handle guids stored in file_force_proxy
    case file_ctx:is_space_dir_const(FileCtx) of
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
-spec get_target_providers_for_file(user_ctx:ctx(), file_ctx:ctx()) ->
    [oneprovider:id()].
get_target_providers_for_file(UserCtx, FileCtx) ->
    case file_ctx:is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            [oneprovider:get_provider_id()];
        false ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            Auth = user_ctx:get_auth(UserCtx),
            UserId = user_ctx:get_user_id(UserCtx),
            {ok, #document{value = #od_space{providers = ProviderIds}}} =
                od_space:get_or_fetch(Auth, SpaceId, UserId), %todo consider caching it in file_ctx

            case lists:member(oneprovider:get_provider_id(), ProviderIds) of
                true ->
                    [oneprovider:get_provider_id()];
                false ->
                    ProviderIds
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Changes target Guid of given request.
%% @end
%%--------------------------------------------------------------------
-spec change_target_guid(any(), fslogic_worker:file_guid()) -> any().
change_target_guid(#fuse_request{fuse_request = #file_request{} = FileRequest} = Request, Guid) ->
    Request#fuse_request{fuse_request = change_target_guid(FileRequest, Guid)};
change_target_guid(#file_request{} = Request, Guid) ->
    Request#file_request{context_guid = Guid};
change_target_guid(#provider_request{} = Request, Guid) ->
    Request#provider_request{context_guid = Guid};
change_target_guid(#proxyio_request{
    parameters = #{?PROXYIO_PARAMETER_FILE_GUID := _} = Parameters
} = Request, Guid) ->
    Request#proxyio_request{
        parameters = Parameters#{?PROXYIO_PARAMETER_FILE_GUID => Guid}
    };
change_target_guid(Request, _) ->
    Request.