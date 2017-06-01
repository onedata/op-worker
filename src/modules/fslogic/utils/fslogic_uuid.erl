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
-spec ensure_guid(session:id(), fslogic_worker:file_guid_or_path()) ->
    {guid, fslogic_worker:file_guid()}.
ensure_guid(_, {guid, FileGuid}) ->
    {guid, FileGuid};
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
-spec uuid_to_path(session:id(), file_meta:uuid()) -> file_meta:path().
uuid_to_path(SessionId, FileUuid) ->
    {ok, UserId} = session:get_user_id(SessionId),
    UserRoot = user_root_dir_uuid(UserId),
    case FileUuid of
        UserRoot -> <<"/">>;
        _ ->
            {ok, UserId} = session:get_user_id(SessionId),
            {ok, Path} = gen_path({uuid, FileUuid}, UserId, []),
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
    try uuid_to_space_id(FileUuid) of
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
            throw({not_a_space, {uuid, SpaceUuid}}) %todo remove this throw and return undefined instead
    end.

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
%% @private
%% @doc
%% Internal helper for gen_path/2. Accumulates all file meta names
%% and concatenates them into path().
%% @end
%%--------------------------------------------------------------------
-spec gen_path(file_meta:entry(), od_user:id(), [file_meta:name()]) ->
    {ok, file_meta:path()} | datastore:generic_error() | no_return().
gen_path(Entry, UserId, Tokens) ->
    {ok, #document{key = Uuid, value = #file_meta{name = Name}} = Doc} = file_meta:get(Entry),
    case file_meta:get_parent(Doc) of
        {ok, #document{key = ?ROOT_DIR_UUID}} ->
            SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(Uuid),
            {ok, #document{value = #od_space{name = SpaceName}}} =
                od_space:get(SpaceId, UserId),
            {ok, fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, SpaceName | Tokens])};
        {ok, #document{key = ParentUuid}} ->
            gen_path({uuid, ParentUuid}, UserId, [Name | Tokens])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns space ID for given file.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_space_id(file_meta:uuid()) ->
    SpaceId :: od_space:id().
uuid_to_space_id(FileUuid) ->
    case FileUuid of
        ?ROOT_DIR_UUID ->
            undefined;
        _ ->
            {ok, #document{key = SpaceUuid}} = file_meta:get_scope({uuid, FileUuid}),
            fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUuid)
    end.