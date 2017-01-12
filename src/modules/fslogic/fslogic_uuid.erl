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
-export([user_root_dir_uuid/1, user_root_dir_guid/1, gen_file_uuid/0, ensure_guid/2]).
-export([path_to_uuid/2, uuid_to_path/2, parent_uuid/2]).
-export([uuid_to_guid/2, uuid_to_guid/1, guid_to_uuid/1, unpack_guid/1]).
-export([spaceid_to_space_dir_uuid/1, space_dir_uuid_to_spaceid/1, spaceid_to_space_dir_guid/1]).
-export([uuid_to_phantom_uuid/1, phantom_uuid_to_uuid/1]).
-export([uuid_to_share_guid/3, unpack_share_guid/1]).
-export([guid_to_share_guid/2, share_guid_to_guid/1, is_share_guid/1,
    guid_to_share_id/1, guid_to_space_id/1]).
-export([is_root_dir/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns Uuid file's parent.
%% If the file is a child of the top level (i.e. a space directory), user's
%% root dir Uuid is returned as in {@link user_root_dir_uuid}.
%% If the file is a root directory itself, returns undefined.
%% @end
%%--------------------------------------------------------------------
-spec parent_uuid(file_meta:entry(), onedata_user:id()) -> file_meta:uuid() | no_return().
parent_uuid(Entry, UserId) ->
    UserRootUuid = user_root_dir_uuid(UserId),
    case file_meta:get(Entry) of
        {ok, #document{key = ?ROOT_DIR_UUID}} -> undefined;
        {ok, #document{key = UserRootUuid}} -> undefined;
        {ok, FileDoc} ->
            case file_meta:get_parent_uuid(FileDoc) of
                {ok, ?ROOT_DIR_UUID} -> UserRootUuid;
                {ok, ParentUuid} -> ParentUuid
            end
    end.

%%--------------------------------------------------------------------
%% @doc Checks if uuid represents user's root dir
%% @end
%%--------------------------------------------------------------------
-spec is_root_dir(Uuid :: file_meta:uuid()) -> boolean().
is_root_dir(?ROOT_DIR_UUID) ->
    true;
is_root_dir(Uuid) ->
    case (catch binary_to_term(http_utils:base64url_decode(Uuid))) of
        {root_space, _UserId} ->
            true;
        _ ->
            false
    end.

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
%% Generates generic file's Uuid that will be not placed in any Space.
%% @end
%%--------------------------------------------------------------------
-spec gen_file_uuid() -> file_meta:uuid().
gen_file_uuid() ->
    PID = oneprovider:get_provider_id(),
    Rand = crypto:rand_bytes(16),
    http_utils:base64url_encode(<<PID/binary, "##", Rand/binary>>).

%%--------------------------------------------------------------------
%% @doc
%% Converts given file entry to FileGuid.
%% @end
%%--------------------------------------------------------------------
-spec ensure_guid(user_ctx:ctx() | session:id(), fslogic_worker:file_guid_or_path()) ->
    {guid, fslogic_worker:file_guid()}.
ensure_guid(_, {guid, FileGuid}) ->
    {guid, FileGuid};
ensure_guid(Ctx, {path, Path}) when is_tuple(Ctx) -> %todo TL use only sessionId
    ensure_guid(user_ctx:get_session_id(Ctx), {path, Path});
ensure_guid(SessionId, {path, Path}) ->
    lfm_utils:call_fslogic(SessionId, fuse_request,
        #resolve_guid{path = Path},
        fun(#uuid{uuid = Guid}) ->
            {guid, Guid}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Converts given file path to Uuid.
%% @end
%%--------------------------------------------------------------------
-spec path_to_uuid(user_ctx:ctx(), file_meta:path()) -> file_meta:uuid().
path_to_uuid(Ctx, Path) when is_tuple(Ctx) ->
    {ok, Tokens} = fslogic_path:tokenize_skipping_dots(Path),
    Entry = fslogic_path:get_canonical_file_entry(Ctx, Tokens),
    {ok, #document{key = Uuid}} = file_meta:get(Entry),
    Uuid.

%%--------------------------------------------------------------------
%% @doc
%% Gets full file path.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_path(user_ctx:ctx() | session:id(), file_meta:uuid()) -> file_meta:path().
uuid_to_path(Ctx, FileUuid) when is_tuple(Ctx) ->
    UserId = user_ctx:get_user_id(Ctx),
    SessId = user_ctx:get_session_id(Ctx),
    UserRoot = user_root_dir_uuid(UserId),
    case FileUuid of
        UserRoot -> <<"/">>;
        _ ->
            {ok, Path} = fslogic_path:gen_path({uuid, FileUuid}, SessId),
            Path
    end;
uuid_to_path(SessionId, FileUuid) ->
    UserId = session:get_user_id(SessionId),
    UserRoot = user_root_dir_uuid(UserId),
    case FileUuid of
        UserRoot -> <<"/">>;
        _ ->
            {ok, Path} = fslogic_path:gen_path({uuid, FileUuid}, SessionId),
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
    try fslogic_spaces:get_space_id({uuid, FileUuid}) of
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
-spec phantom_uuid_to_uuid(file_meta:uuid()) -> file_meta:uuid().
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
