%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for operations on shares.
%%% @end
%%%--------------------------------------------------------------------
-module(shares).
-author("Lukasz Opiola").
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/privileges.hrl").

%% API
-export([terminate/3, allowed_methods/2, is_authorized/2,
    content_types_accepted/2, content_types_provided/2, delete_resource/2]).

%% resource functions
-export([create_or_modify_share/2, get_share/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), maps:map()) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), maps:map() | {error, term()}) -> {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"POST">>, <<"GET">>, <<"PATCH">>, <<"DELETE">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | stop, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_share}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{atom() | binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, create_or_modify_share}
    ], Req, State}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/shares/{path}'
%% '/api/v3/oneprovider/shares-id/{dir_id}'
%% '/api/v3/oneprovider/shares-public-id/{share_public_id}'
%% @doc Deletes share specified either by directory path, directory id or share id.\n
%%
%% HTTP method: DELETE
%%
%% @end
%%--------------------------------------------------------------------
-spec delete_resource(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource(Req, State = #{resource_type := share_id}) ->
    {StateWithShareId, ReqWithShareId} = validator:parse_share_id(Req, State),
    delete_resource_internal(ReqWithShareId, StateWithShareId);
delete_resource(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_objectid(Req, State),
    delete_resource_internal(ReqWithId, StateWithId);
delete_resource(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    delete_resource_internal(ReqWithPath, StateWithPath).

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Internal version of delete_resource/2
%%--------------------------------------------------------------------
-spec delete_resource_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource_internal(Req, State) ->
    #{auth := SessionId} = State,
    ShareId = get_share_id(State),
    SpaceId = get_share_space_id(SessionId, ShareId),

    ensure_space_supported_locally(SpaceId),

    case logical_file_manager:remove_share(SessionId, ShareId) of
        {error, Error} ->
            error({error, Error});
        ok ->
            {stop, Req, State}
    end.

-spec create_or_modify_share(req(), maps:map()) -> {term(), req(), maps:map()}.
create_or_modify_share(#{method := <<"POST">>} = Req, State) ->
    create_share(Req, State);
create_or_modify_share(#{method := <<"PATCH">>} = Req, State) ->
    update_share_name(Req, State).

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/shares/{path}'
%% '/api/v3/oneprovider/shares-id/{dir_id}'
%% @doc Share directory specified by path or id.\n
%%
%% HTTP method: POST
%%
%% @param name The human readable name of the share.\n
%%--------------------------------------------------------------------
-spec create_share(req(), maps:map()) -> {term(), req(), maps:map()}.
create_share(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_objectid(Req, State),
    create_share_internal(ReqWithId, StateWithId);
create_share(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    create_share_internal(ReqWithPath, StateWithPath).

%%--------------------------------------------------------------------
%% @doc Internal version of create_share/2
%%--------------------------------------------------------------------
-spec create_share_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
create_share_internal(Req, State) ->
    {StateWithName, ReqWithName} = validator:parse_name(Req, State),
    #{auth := SessionId, name := Name} = StateWithName,
    FileGuid = get_file_guid(StateWithName),
    FileKey = {guid, FileGuid},
    SpaceId = file_id:guid_to_space_id(FileGuid),

    ensure_is_directory(SessionId, FileKey),
    ensure_space_supported_locally(SpaceId),
    ensure_valid_name(Name),

    case logical_file_manager:create_share(SessionId, FileKey, Name) of
        {error, ?EEXIST} ->
            throw(?ERROR_SHARE_ALREADY_EXISTS);
        {error, Error} ->
            error({error, Error});
        {ok, {ShareId, _ShareGuid}} ->
            Response = json_utils:encode(#{<<"shareId">> => ShareId}),
            FinalReq = cowboy_req:reply(?HTTP_200_OK, #{}, Response, ReqWithName),
            {stop, FinalReq, StateWithName}
    end.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/shares/{path}'
%% '/api/v3/oneprovider/shares-id/{dir_id}'
%% '/api/v3/oneprovider/shares-public-id/{share_public_id}'
%% @doc Updates name of the share specified either by
%% directory path, directory id or public share id.\n
%%
%% HTTP method: PATCH
%%
%% @param name The human readable name of the share.\n
%%--------------------------------------------------------------------
-spec update_share_name(req(), maps:map()) -> {term(), req(), maps:map()}.
update_share_name(Req, State = #{resource_type := share_id}) ->
    {StateWithShareId, ReqWithShareId} = validator:parse_share_id(Req, State),
    update_share_name_internal(ReqWithShareId, StateWithShareId);
update_share_name(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_objectid(Req, State),
    update_share_name_internal(ReqWithId, StateWithId);
update_share_name(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    update_share_name_internal(ReqWithPath, StateWithPath).

%%--------------------------------------------------------------------
%% @doc Internal version of update_share_name/2
%%--------------------------------------------------------------------
-spec update_share_name_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
update_share_name_internal(Req, State) ->
    {StateWithName, ReqWithName} = validator:parse_name(Req, State),
    #{auth := SessionId, name := NewName} = StateWithName,
    ShareId = get_share_id(State),
    SpaceId = get_share_space_id(SessionId, ShareId),

    ensure_space_supported_locally(SpaceId),
    ensure_valid_name(NewName),

    case share_logic:update_name(SessionId, ShareId, NewName) of
        {error, forbidden} ->
            throw(?ERROR_PERMISSION_DENIED);
        {error, not_found} ->
            throw(?ERROR_NOT_FOUND);
        {error, Error} ->
            error({error, Error});
        ok ->
            FinalReq = cowboy_req:reply(?HTTP_200_OK, ReqWithName),
            {stop, FinalReq, StateWithName}
    end.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/shares/{path}'
%% '/api/v3/oneprovider/shares-id/{dir_id}'
%% '/api/v3/oneprovider/shares-public-id/{share_public_id}'
%% @doc Returns the basic information about share specified either by
%% directory path, directory id or public share id.\n
%%
%% HTTP method: GET
%%
%% @end
%%--------------------------------------------------------------------
-spec get_share(req(), maps:map()) -> {term(), req(), maps:map()}.
get_share(Req, State = #{resource_type := share_id}) ->
    {StateWithShareId, ReqWithShareId} = validator:parse_share_id(Req, State),
    get_share_internal(ReqWithShareId, StateWithShareId);
get_share(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_objectid(Req, State),
    get_share_internal(ReqWithId, StateWithId);
get_share(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    get_share_internal(ReqWithPath, StateWithPath).

%%--------------------------------------------------------------------
%% @doc Internal version of get_share/2
%%--------------------------------------------------------------------
-spec get_share_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
get_share_internal(Req, State) ->
    #{auth := SessionId} = State,
    ShareId = get_share_id(State),
    SpaceId = get_share_space_id(SessionId, ShareId),

    ensure_space_supported_locally(SpaceId),

    case share_logic:get(SessionId, ShareId) of
        {error, forbidden} ->
            throw(?ERROR_PERMISSION_DENIED);
        {error, Error} ->
            error({error, Error});
        {ok, #document{value = Share}} ->
            Response = json_utils:encode(#{
                <<"shareId">> => ShareId,
                <<"name">> => Share#od_share.name,
                <<"publicUrl">> => Share#od_share.public_url,
                <<"rootFileId">> => root_file_guid_to_objectid(Share#od_share.root_file),
                <<"spaceId">> => Share#od_share.space,
                <<"handleId">> => utils:ensure_defined(Share#od_share.handle, undefined, null)
            }),
            FinalReq = cowboy_req:reply(?HTTP_200_OK, #{}, Response, Req),
            {stop, FinalReq, State}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_file_guid(maps:map()) -> fslogic_worker:file_guid().
get_file_guid(#{id := Id}) ->
    Id;
get_file_guid(#{auth := Auth, path := Path}) ->
    {guid, Guid} = guid_utils:ensure_guid(Auth, {path, Path}),
    Guid.

-spec get_share_id(maps:map()) -> od_share:id() | no_return().
get_share_id(#{share_id := ShareId}) ->
    ShareId;
get_share_id(State = #{auth := SessionId}) ->
    {ok, Attrs} = onedata_file_api:stat(SessionId, {guid, get_file_guid(State)}),
    case Attrs#file_attr.shares of
        [ShareId] -> ShareId;
        _ -> throw(?ERROR_NOT_FOUND)
    end.

-spec ensure_is_directory(onedata_auth_api:auth(), fslogic_worker:file_guid_or_path()) ->
    ok | no_return().
ensure_is_directory(SessionId, FileKey) ->
    case logical_file_manager:stat(SessionId, FileKey) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
            ok;
        {ok, _} ->
            throw(?ERROR_NOT_A_DIRECTORY);
        {error, Error} ->
            error({error, Error})
    end.

-spec root_file_guid_to_objectid(fslogic_worker:file_guid()) -> file_id:objectid().
root_file_guid_to_objectid(RootFileGuid) ->
    {ok, ObjectId} = file_id:guid_to_objectid(RootFileGuid),
    ObjectId.

-spec ensure_space_supported_locally(od_space:id()) -> ok | no_return().
ensure_space_supported_locally(SpaceId) ->
    case provider_logic:supports_space(SpaceId) of
        true ->
            ok;
        false ->
            throw(?ERROR_SPACE_NOT_SUPPORTED)
    end.

-spec ensure_valid_name(binary() | undefined) -> ok | no_return().
ensure_valid_name(Name) when is_binary(Name) andalso size(Name) > 0 -> ok;
ensure_valid_name(_) -> throw(?ERROR_INVALID_NAME).

-spec get_share_space_id(onedata_auth_api:auth(), od_share:id()) ->
    od_space:id() | no_return().
get_share_space_id(SessionId, ShareId) ->
    case share_logic:get(SessionId, ShareId) of
        {ok, #document{value = #od_share{space = SpaceId}}} ->
            SpaceId;
        {error, not_found} ->
            throw(?ERROR_NOT_FOUND);
        Error ->
            error({error, Error})
    end.
