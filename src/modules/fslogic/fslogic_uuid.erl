%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Conversions between paths and uuids.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_uuid).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([user_root_dir_uuid/1, user_root_dir_guid/1, ensure_guid/2]).
-export([uuid_to_path/2]).
-export([uuid_to_guid/2, uuid_to_guid/1, guid_to_uuid/1]).
-export([spaceid_to_space_dir_uuid/1, space_dir_uuid_to_spaceid/1, spaceid_to_space_dir_guid/1]).
-export([uuid_to_phantom_uuid/1, phantom_uuid_to_uuid/1]).
-export([uuid_to_share_guid/3, unpack_share_guid/1]).
-export([guid_to_share_guid/2, share_guid_to_guid/1, is_share_guid/1,
    guid_to_share_id/1, guid_to_space_id/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Returns Uuid of user's root directory.
%% @end
%%--------------------------------------------------------------------
-spec user_root_dir_uuid(UserId :: od_user:id()) -> file_meta:uuid().
user_root_dir_uuid(UserId) ->
    http_utils:base64url_encode(term_to_binary({root_space, UserId})).

%%--------------------------------------------------------------------
%% @doc Returns Uuid of user's root directory.
%% @end
%%--------------------------------------------------------------------
-spec user_root_dir_guid(Uuid :: file_meta:uuid()) -> fslogic_worker:file_guid().
user_root_dir_guid(Uuid) ->
    uuid_to_guid(Uuid, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Converts given file entry to FileGuid.
%% @end
%%--------------------------------------------------------------------
-spec ensure_guid(user_ctx:ctx() | session:id(), fslogic_worker:file_guid_or_path()) ->
    {guid, fslogic_worker:file_guid()}.
ensure_guid(_, {guid, FileGuid}) ->
    {guid, FileGuid};
ensure_guid(UserCtx, {path, Path}) when is_tuple(UserCtx) -> %todo TL use only sessionId
    ensure_guid(user_ctx:get_session_id(UserCtx), {path, Path});
ensure_guid(SessionId, {path, Path}) ->
    remote_utils:call_fslogic(SessionId, fuse_request,
        #resolve_guid{path = Path},
        fun(#guid{guid = Guid}) ->
            {guid, Guid}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Gets full file path.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_path(user_ctx:ctx() | session:id(), file_meta:uuid()) -> file_meta:path().
uuid_to_path(UserCtx, FileUuid) when is_tuple(UserCtx) ->
    UserId = user_ctx:get_user_id(UserCtx),
    SessId = user_ctx:get_session_id(UserCtx),
    UserRoot = user_root_dir_uuid(UserId),
    case FileUuid of
        UserRoot -> <<"/">>;
        _ ->
            {ok, Path} = gen_path({uuid, FileUuid}, SessId),
            Path
    end;
uuid_to_path(SessionId, FileUuid) ->
    {ok, UserId} = session:get_user_id(SessionId),
    UserRoot = user_root_dir_uuid(UserId),
    case FileUuid of
        UserRoot -> <<"/">>;
        _ ->
            {ok, Path} = gen_path({uuid, FileUuid}, SessionId),
            Path
    end.

%%--------------------------------------------------------------------
%% @doc
%% For given file Uuid and spaceId generates file's Guid.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_guid(file_meta:uuid(), od_space:id() | undefined) ->
    fslogic_worker:file_guid().
uuid_to_guid(FileUuid, SpaceId) ->
    http_utils:base64url_encode(term_to_binary({guid, FileUuid, SpaceId})).

%%--------------------------------------------------------------------
%% @doc
%% For given file Uuid generates file's Guid. SpaceId is calculated in process.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_guid(file_meta:uuid()) -> fslogic_worker:file_guid().
uuid_to_guid(FileUuid) ->
    try get_space_id({uuid, FileUuid}) of
        SpaceId ->
            uuid_to_guid(FileUuid, SpaceId)
    catch
        {not_a_space, _} ->
            uuid_to_guid(FileUuid, undefined)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's Uuid for given file's Guid.
%% @end
%%--------------------------------------------------------------------
-spec guid_to_uuid(fslogic_worker:file_guid()) -> file_meta:uuid().
guid_to_uuid(FileGuid) ->
    {FileUuid, _} = unpack_guid(FileGuid),
    FileUuid.

%%--------------------------------------------------------------------
%% @doc Convert SpaceId to uuid of file_meta document of this space directory.
%%--------------------------------------------------------------------
-spec spaceid_to_space_dir_uuid(od_space:id()) -> file_meta:uuid().
spaceid_to_space_dir_uuid(SpaceId) ->
    http_utils:base64url_encode(term_to_binary({space, SpaceId})).

%%--------------------------------------------------------------------
%% @doc Convert SpaceId to guid of file_meta document of this space directory.
%%--------------------------------------------------------------------
-spec spaceid_to_space_dir_guid(od_space:id()) -> fslogic_worker:file_guid().
spaceid_to_space_dir_guid(SpaceId) ->
    uuid_to_guid(http_utils:base64url_encode(term_to_binary({space, SpaceId})), SpaceId).

%%--------------------------------------------------------------------
%% @doc Convert file_meta uuid of space directory to SpaceId
%%--------------------------------------------------------------------
-spec space_dir_uuid_to_spaceid(file_meta:uuid()) -> od_space:id().
space_dir_uuid_to_spaceid(SpaceUuid) ->
    case binary_to_term(http_utils:base64url_decode(SpaceUuid)) of
        {space, SpaceId} ->
            SpaceId;
        _ ->
            throw({not_a_space, {uuid, SpaceUuid}})
    end.

%%--------------------------------------------------------------------
%% @doc
%% For given file Uuid generates phantom's Uuid.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_phantom_uuid(file_meta:uuid()) -> file_meta:uuid().
uuid_to_phantom_uuid(FileUuid) ->
    http_utils:base64url_encode(term_to_binary({phantom, FileUuid})).

%%--------------------------------------------------------------------
%% @doc
%% For given file Uuid generates phantom's Uuid.
%% @end
%%--------------------------------------------------------------------
-spec phantom_uuid_to_uuid(file_meta:uuid()) -> file_meta:uuid(). %todo why is it unused
phantom_uuid_to_uuid(PhantomUuid) ->
    {phantom, FileUuid} = binary_to_term(http_utils:base64url_decode(PhantomUuid)),
    FileUuid.

%%--------------------------------------------------------------------
%% @doc
%% Convert Guid and share id to share guid (allowing for guest read)
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_share_guid(file_meta:uuid(), od_space:id(), od_share:id() | undefined) ->
    od_share:share_guid().
uuid_to_share_guid(FileUuid, SpaceId, undefined) ->
    uuid_to_guid(FileUuid, SpaceId);
uuid_to_share_guid(FileUuid, SpaceId, ShareId) ->
    http_utils:base64url_encode(term_to_binary({share_guid, FileUuid, SpaceId, ShareId})).

%%--------------------------------------------------------------------
%% @doc
%% Convert Guid and share id to share guid (allowing for guest read)
%% @end
%%--------------------------------------------------------------------
-spec guid_to_share_guid(fslogic_worker:file_guid(), od_share:id()) ->
    od_share:share_guid().
guid_to_share_guid(Guid, ShareId) ->
    {FileUuid, SpaceId} = unpack_guid(Guid),
    http_utils:base64url_encode(term_to_binary({share_guid, FileUuid, SpaceId, ShareId})).

%%--------------------------------------------------------------------
%% @doc
%% Convert Share guid to Guid.
%% @end
%%--------------------------------------------------------------------
-spec share_guid_to_guid(od_share:share_guid()) -> fslogic_worker:file_guid().
share_guid_to_guid(ShareGuid) ->
    {FileUuid, SpaceId, _} = unpack_share_guid(ShareGuid),
    uuid_to_guid(FileUuid, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's Uuid, its SpaceId and its ShareId for given file's share Guid.
%% @end
%%--------------------------------------------------------------------
-spec unpack_share_guid(od_share:share_guid()) ->
    {file_meta:uuid(), undefined | od_space:id(), od_share:id() | undefined}.
unpack_share_guid(ShareGuid) ->
    try binary_to_term(http_utils:base64url_decode(ShareGuid)) of
        {share_guid, FileUuid, SpaceId, ShareId} ->
            {FileUuid, SpaceId, ShareId};
        {guid, FileUuid, SpaceId} ->
            {FileUuid, SpaceId, undefined};
        _ ->
            {ShareGuid, undefined, undefined}
    catch
        _:_ ->
            {ShareGuid, undefined, undefined}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Predicate checking if given Id is a share guid.
%% @end
%%--------------------------------------------------------------------
-spec is_share_guid(binary()) -> boolean().
is_share_guid(Id) ->
    case unpack_share_guid(Id) of
        {_, _, undefined} -> false;
        _ -> true
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get share id connected with given guid (returns undefined if guid is
%% not of shared type).
%% @end
%%--------------------------------------------------------------------
-spec guid_to_share_id(od_share:share_guid()) -> od_share:id() | undefined.
guid_to_share_id(Guid) ->
    {_, _, ShareId} = unpack_share_guid(Guid),
    ShareId.

%%--------------------------------------------------------------------
%% @doc
%% Get space id connected with given guid.
%% @end
%%--------------------------------------------------------------------
-spec guid_to_space_id(fslogic_worker:guid()) -> od_space:id() | undefined.
guid_to_space_id(Guid) ->
    try binary_to_term(http_utils:base64url_decode(Guid)) of
        {share_guid, _FileUuid, SpaceId, _ShareId} ->
            SpaceId;
        {guid, _FileUuid, SpaceId} ->
            SpaceId;
        {space, SpaceId} ->
            SpaceId;
        _ ->
            undefined
    catch
        _:_ ->
            undefined
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns file's Uuid and its SpaceId for given file's Guid.
%% @end
%%--------------------------------------------------------------------
-spec unpack_guid(FileGuid :: fslogic_worker:file_guid()) ->
    {file_meta:uuid(), od_space:id() | undefined}.
unpack_guid(FileGuid) ->
    try binary_to_term(http_utils:base64url_decode(FileGuid)) of
        {guid, FileUuid, SpaceId} ->
            {FileUuid, SpaceId};
        {share_guid, FileUuid, SpaceId, _ShareId} ->
            {FileUuid, SpaceId};
        _ ->
            {FileGuid, undefined}
    catch
        _:_ ->
            {FileGuid, undefined}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Generate file_meta:path() for given file_meta:entry()
%% @end
%%--------------------------------------------------------------------
-spec gen_path(file_meta:entry(), SessId :: session:id()) ->
    {ok, file_meta:path()} | datastore:generic_error().
gen_path({path, Path}, _SessId) when is_binary(Path) ->
    {ok, Path};
gen_path(Entry, SessId) ->
    {ok, UserId} = session:get_user_id(SessId),
    gen_path(Entry, UserId, []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Internal helper for gen_path/2. Accumulates all file meta names
%% and concatenates them into path().
%% @end
%%--------------------------------------------------------------------
-spec gen_path(file_meta:entry(), od_user:id(), [file_meta:name()]) ->
    {ok, file_meta:path()} | datastore:generic_error() | no_return().
gen_path(Entry, UserId, Tokens) ->
    {ok, #document{key = UUID, value = #file_meta{name = Name}} = Doc} = file_meta:get(Entry),
    case file_meta:get_parent(Doc) of
        {ok, #document{key = ?ROOT_DIR_UUID}} ->
            SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(UUID),
            {ok, #document{value = #od_space{name = SpaceName}}} =
                od_space:get(SpaceId, UserId),
            {ok, fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, SpaceName | Tokens])};
        {ok, #document{key = ParentUUID}} ->
            gen_path({uuid, ParentUUID}, UserId, [Name | Tokens])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns space ID for given file.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id(Entry :: {uuid, file_meta:uuid()} | {guid, fslogic_worker:file_guid()}) ->
    SpaceId :: binary().
get_space_id({uuid, FileUUID}) ->
    {ok, #document{key = SpaceUUID}} = get_space({uuid, FileUUID}),
    fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID);
get_space_id({guid, FileGUID}) ->
    case fslogic_uuid:unpack_guid(FileGUID) of
        {FileUUID, undefined} ->
            get_space_id({uuid, FileUUID});
        {_, SpaceId} ->
            SpaceId
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns space document for given file or space id. Note: This function
%% works only with absolute, user independent paths (cannot be used
%% with paths to default space).
%% @end
%%--------------------------------------------------------------------
-spec get_space(FileEntry :: fslogic_worker:file() | {guid, fslogic_worker:file_guid()}|
{id, SpaceId :: datastore:id()}) ->
    {ok, ScopeDoc :: datastore:document()} | {error, Reason :: term()}.
get_space({id, SpaceId}) ->
    SpaceUUID = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    case file_meta:get({uuid, SpaceUUID}) of
        {error, {not_found, _}} ->
            file_meta:make_space_exist(SpaceId),
            file_meta:get({uuid, SpaceUUID});
        Res -> Res
    end;
get_space({guid, FileGUID}) ->
    case fslogic_uuid:unpack_guid(FileGUID) of
        {FileUUID, undefined} -> get_space({uuid, FileUUID});
        {_, SpaceId} ->
            get_space({id, SpaceId})
    end;
get_space(FileEntry) ->
    {ok, FileUUID} = file_meta:to_uuid(FileEntry),

    case FileUUID of
        <<"">> ->
            throw({not_a_space, FileEntry});
        _ ->
            {ok, #document{key = SpaceUUID}} = Res = file_meta:get_scope({uuid, FileUUID}),
            _ = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID), %% Crash if given UUID is not a space
            Res

    end.