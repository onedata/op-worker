%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utils functions for middleware plugins.
%%% @end
%%%-------------------------------------------------------------------
-module(middleware_utils).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/storage/import/storage_import.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/errors.hrl").

-export([
    is_file_access_error/1
]).
-export([
    resolve_file_path/2,
    switch_context_if_shared_file_request/1,
    is_shared_file_request/4,

    is_eff_space_member/2,
    assert_space_supported_locally/1,
    assert_space_supported_by/2,
    assert_space_supported_with_storage/2,

    decode_object_id/2,
    assert_file_exists/2,
    has_access_to_file_space/2,
    assert_file_managed_locally/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec is_file_access_error(errors:error()) -> boolean().
is_file_access_error(?ERROR_POSIX(?EACCES)) -> true;
is_file_access_error(?ERROR_POSIX(?EPERM)) -> true;
is_file_access_error(?ERROR_POSIX(?ENOENT)) -> true;
is_file_access_error(?ERROR_UNAUTHORIZED) -> true;
is_file_access_error(?ERROR_FORBIDDEN) -> true;
is_file_access_error(?ERROR_NOT_FOUND) -> true;
is_file_access_error({error, {file_meta_missing, _Uuid}}) -> true; % error thrown by paths_cache
is_file_access_error(_) -> false.


%% TODO VFS-10809 uncomment
%%-spec resolve_file_path(session:id(), file_meta:path()) ->
%%    {ok, file_id:file_guid()} | no_return().
%%resolve_file_path(SessionId, Path) ->
%%    ?lfm_check(remote_utils:call_fslogic(
%%        SessionId,
%%        fuse_request,
%%        #resolve_guid_by_canonical_path{path = ensure_canonical_path(SessionId, Path)},
%%        fun(#guid{guid = Guid}) -> {ok, Guid} end
%%    )).


%% TODO VFS-10809 rm
-spec resolve_file_path(session:id(), file_meta:path()) ->
    {ok, file_id:file_guid()} | no_return().
resolve_file_path(SessionId, Path) ->
    {ok, UserId} = session:get_user_id(SessionId),

    case filepath_utils:split_and_skip_dots(Path) of
        {ok, [<<"/">>]} ->
            {ok, fslogic_file_id:user_root_dir_guid(UserId)};
        {ok, [<<"/">>, SpaceName | Rest]} ->
            case user_logic:get_space_by_name(SessionId, UserId, SpaceName) of
                false ->
                    throw(?ERROR_POSIX(?ENOENT));
                {true, SpaceId} ->
                    assert_space_supported_locally(SpaceId),
                    {RootGuid, RelPathTokens} = case Rest of
                        [<<".__onedata__tmp">> | Rest2] ->
                            {fslogic_file_id:spaceid_to_tmp_dir_guid(SpaceId), Rest2};
                        _ ->
                            {fslogic_file_id:spaceid_to_space_dir_guid(SpaceId), Rest}
                    end,
                    resolve_guid_by_relative_path(SessionId, RootGuid, filepath_utils:join(RelPathTokens))
            end;
        _ ->
            throw(?ERROR_POSIX(?ENOENT))
    end.


%% TODO VFS-10809 rm
%% @private
-spec resolve_guid_by_relative_path(session:id(), file_id:file_guid(), file_meta:path()) ->
    {ok, file_id:file_guid()} | no_return().
resolve_guid_by_relative_path(SessionId, RootFileGuid, RelativePath) ->
    case lfm:resolve_guid_by_relative_path(SessionId, RootFileGuid, RelativePath) of
        {ok, Guid} -> {ok, Guid};
        {error, Errno} -> throw(?ERROR_POSIX(Errno))
    end.


-spec switch_context_if_shared_file_request(middleware:req()) -> middleware:req().
switch_context_if_shared_file_request(#op_req{gri = #gri{
    type = Type,
    id = Id,
    aspect = Aspect,
    scope = Scope
}} = OpReq) ->
    case is_shared_file_request(Type, Aspect, Scope, Id) of
        true -> OpReq#op_req{auth = ?GUEST};
        false -> OpReq
    end.


-spec is_shared_file_request(gri:entity_type(), gri:aspect(), gri:scope(), gri:entity_id()) ->
    boolean().
is_shared_file_request(op_file, download_url, Scope, _) ->
    Scope == public;
is_shared_file_request(op_file, _, _, Id) when is_binary(Id) ->
    file_id:is_share_guid(Id);
is_shared_file_request(op_replica, As, _, Id) when
    As =:= instance;
    As =:= distribution
->
    file_id:is_share_guid(Id);
is_shared_file_request(_, _, _, _) ->
    false.


-spec is_eff_space_member(aai:auth(), od_space:id()) -> boolean().
is_eff_space_member(?GUEST, _SpaceId) ->
    false;
is_eff_space_member(?USER(UserId, SessionId), SpaceId) ->
    user_logic:has_eff_space(SessionId, UserId, SpaceId).


-spec assert_space_supported_locally(od_space:id()) -> ok | no_return().
assert_space_supported_locally(SpaceId) ->
    assert_space_supported_by(SpaceId, oneprovider:get_id()).


-spec assert_space_supported_by(od_space:id(), od_provider:id()) ->
    ok | no_return().
assert_space_supported_by(SpaceId, ProviderId) ->
    case space_logic:is_supported(?ROOT_SESS_ID, SpaceId, ProviderId) of
        true ->
            ok;
        false ->
            throw(?ERROR_SPACE_NOT_SUPPORTED_BY(SpaceId, ProviderId))
    end.


-spec assert_space_supported_with_storage(od_space:id(), storage:id()) -> ok | no_return().
assert_space_supported_with_storage(SpaceId, StorageId) ->
    case storage_logic:is_local_storage_supporting_space(StorageId, SpaceId) of
        true ->
            ok;
        false ->
            throw(?ERROR_NOT_A_LOCAL_STORAGE_SUPPORTING_SPACE(oneprovider:get_id(), StorageId, SpaceId))
    end.


-spec decode_object_id(file_id:objectid(), binary() | atom()) ->
    file_id:file_guid() | no_return().
decode_object_id(ObjectId, Key) ->
    try
        {ok, Guid} = file_id:objectid_to_guid(ObjectId),
        {_, _, _} = file_id:unpack_share_guid(Guid),
        Guid
    catch _:_ ->
        throw(?ERROR_BAD_VALUE_IDENTIFIER(Key))
    end.


-spec assert_file_exists(aai:auth(), file_id:file_guid()) ->
    ok | no_return().
assert_file_exists(#auth{session_id = SessionId}, FileGuid) ->
    ?lfm_check(lfm:stat(SessionId, ?FILE_REF(FileGuid))),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Checks user membership in space containing specified file. Returns true
%% in case of user root dir since it doesn't belong to any space.
%% @end
%%--------------------------------------------------------------------
-spec has_access_to_file_space(aai:auth(), file_id:file_guid()) -> boolean().
has_access_to_file_space(?GUEST, _Guid) ->
    false;
has_access_to_file_space(?USER(UserId) = Auth, Guid) ->
    case fslogic_file_id:user_root_dir_guid(UserId) of
        Guid ->
            true;
        _ ->
            SpaceId = file_id:guid_to_space_id(Guid),
            is_eff_space_member(Auth, SpaceId)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Asserts that space containing specified file is supported by this provider.
%% Omit this check in case of user root dir which doesn't belong to any space
%% and can be reached from any provider.
%% @end
%%--------------------------------------------------------------------
-spec assert_file_managed_locally(file_id:file_guid()) ->
    ok | no_return().
assert_file_managed_locally(FileGuid) ->
    {FileUuid, SpaceId} = file_id:unpack_guid(FileGuid),
    case fslogic_file_id:is_root_dir_uuid(FileUuid) of
        true ->
            ok;
        false ->
            assert_space_supported_locally(SpaceId)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% TODO VFS-10809 uncomment
%%%% @private
%%-spec ensure_canonical_path(session:id(), file_meta:path()) -> file_meta:path() | no_return().
%%ensure_canonical_path(SessionId, Path) ->
%%    case filepath_utils:split_and_skip_dots(Path) of
%%        {ok, [<<"/">>]} ->
%%            <<"/">>;
%%        {ok, [<<"/">>, SpaceName | Rest]} ->
%%            {ok, UserId} = session:get_user_id(SessionId),
%%            case user_logic:get_space_by_name(SessionId, UserId, SpaceName) of
%%                false ->
%%                    throw(?ERROR_POSIX(?ENOENT));
%%                {true, SpaceId} ->
%%                    assert_space_supported_locally(SpaceId),
%%                    filename:join([<<"/">>, SpaceId | Rest])
%%            end;
%%        _ ->
%%            throw(?ERROR_POSIX(?ENOENT))
%%    end.
