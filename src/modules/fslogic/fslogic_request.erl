%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for obtaining and modifying things related to any fslogic request.
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
-export([get_file/2, get_target_providers/3, update_target_guid_if_file_is_phantom/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get file_context record associated with request. If request does not point to
%% specific file, the function returns undefined.
%% @end
%%--------------------------------------------------------------------
-spec get_file(fslogic_context:ctx(), fslogic_worker:request()) ->
    file_context:ctx() | undefined.
get_file(Ctx, #fuse_request{fuse_request = #resolve_guid{path = Path}}) ->
    file_context:new_by_path(Ctx, Path);
get_file(_Ctx, #fuse_request{fuse_request = #file_request{context_guid = FileGuid}}) ->
    file_context:new_by_guid(FileGuid);
get_file(_Ctx, #fuse_request{}) ->
    undefined;
get_file(_Ctx, #provider_request{context_guid = FileGuid}) ->
    file_context:new_by_guid(FileGuid);
get_file(_Ctx, #proxyio_request{parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}}) ->
    file_context:new_by_guid(FileGuid);
get_file(_Ctx, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

%%--------------------------------------------------------------------
%% @doc
%% Get providers capable of handling given request.
%% @end
%%--------------------------------------------------------------------
-spec get_target_providers(fslogic_context:ctx(), file_context:ctx(), fslogic_worker:request()) ->
    [oneprovider:id()].
get_target_providers(_Ctx, undefined, _) ->
    [oneprovider:get_provider_id()];
get_target_providers(Ctx, File, #fuse_request{fuse_request = #resolve_guid{}}) ->
    get_target_providers_for_attr_req(Ctx, File);
get_target_providers(Ctx, File, #fuse_request{fuse_request = #file_request{file_request = #get_file_attr{}}}) ->
    get_target_providers_for_attr_req(Ctx, File);
get_target_providers(_Ctx, _File, #provider_request{provider_request = #replicate_file{provider_id = ProviderId}}) ->
    [ProviderId];
get_target_providers(Ctx, File, _Req) ->
    get_target_providers_for_file(Ctx, File).

%%--------------------------------------------------------------------
%% @doc
%% Check if the file is of the phantom type. If so, the function updates the request to
%% point to the guid of file where the phantom points to. The file itself is also replaced
%% by the phantom target.
%% @end
%%--------------------------------------------------------------------
-spec update_target_guid_if_file_is_phantom(file_context:ctx(), fslogic_worker:request()) ->
    {NewFile :: file_context:ctx(), NewRequest :: fslogic_worker:request()}.
update_target_guid_if_file_is_phantom(undefined, Request) ->
    {undefined, Request};
update_target_guid_if_file_is_phantom(File, Request) ->
    try file_context:get_file_doc(File) of
        {{error, {not_found, file_meta}}, File2} ->
            try
                {Guid, _File3} = file_context:get_guid(File2),
                Uuid = fslogic_uuid:guid_to_uuid(Guid),
                {ok, NewGuid} = file_meta:get_guid_from_phantom_file(Uuid),
                NewRequest = change_target_guid(Request, NewGuid),
                NewFile = file_context:new_by_guid(NewGuid),
                {NewFile, NewRequest}
            catch
                _:_ ->
                    {File2, Request}
            end;
        {_, NewFile} ->
            {NewFile, Request}
    catch
        _:_ ->
            {File, Request}
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
-spec get_target_providers_for_attr_req(fslogic_context:ctx(), file_context:ctx()) ->
    [oneprovider:id()].
get_target_providers_for_attr_req(Ctx, File) ->
    %todo TL handle guids stored in file_force_proxy
    case file_context:is_space_dir(File) of
        true ->
            [oneprovider:get_provider_id()];
        false ->
            get_target_providers_for_file(Ctx, File)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get providers cappable of handling generic request.
%% @end
%%--------------------------------------------------------------------
-spec get_target_providers_for_file(fslogic_context:ctx(), file_context:ctx()) ->
    [oneprovider:id()].
get_target_providers_for_file(Ctx, File) ->
    case file_context:is_user_root_dir(File, Ctx) of
        true ->
            [oneprovider:get_provider_id()];
        false ->
            SpaceId = file_context:get_space_id(File),
            Auth = fslogic_context:get_auth(Ctx),
            UserId = fslogic_context:get_user_id(Ctx),
            {ok, #document{value = #od_space{providers = ProviderIds}}} = od_space:get_or_fetch(Auth, SpaceId, UserId), %todo consider caching it in file_context

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
change_target_guid(#proxyio_request{parameters = #{?PROXYIO_PARAMETER_FILE_GUID := _} = Parameters} = Request, Guid) ->
    Request#proxyio_request{parameters = Parameters#{?PROXYIO_PARAMETER_FILE_GUID => Guid}};
change_target_guid(Request, _) ->
    Request.