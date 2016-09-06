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
-export([user_root_dir_uuid/1, gen_file_uuid/0, ensure_guid/2]).
-export([path_to_uuid/2, uuid_to_path/2]).
-export([to_file_guid/2, to_file_guid/1, file_guid_to_uuid/1, unpack_file_guid/1]).
-export([spaceid_to_space_dir_uuid/1, space_dir_uuid_to_spaceid/1]).
-export([uuid_to_phantom_uuid/1, phantom_uuid_to_uuid/1]).
-export([guid_to_share_guid/2, unpack_share_guid/1]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Returns UUID of user's root directory.
%% @end
%%--------------------------------------------------------------------
-spec user_root_dir_uuid(UserId :: onedata_user:id()) -> file_meta:uuid().
user_root_dir_uuid(UserId) ->
    http_utils:base64url_encode(term_to_binary({root_space, UserId})).

%%--------------------------------------------------------------------
%% @doc
%% Generates generic file's UUID that will be not placed in any Space.
%% @end
%%--------------------------------------------------------------------
-spec gen_file_uuid() -> file_meta:uuid().
gen_file_uuid() ->
    http_utils:base64url_encode(crypto:rand_bytes(16)).

%%--------------------------------------------------------------------
%% @doc
%% Converts given file entry to FileGUID.
%% @end
%%--------------------------------------------------------------------
-spec ensure_guid(fslogic_worker:ctx(), fslogic_worker:file_guid_or_path()) ->
    {guid, fslogic_worker:file_guid()}.
ensure_guid(_CTX, {guid, FileGUID}) ->
    {guid, FileGUID};
ensure_guid(#fslogic_ctx{session_id = SessId}, {path, Path}) ->
    lfm_utils:call_fslogic(SessId, fuse_request,
        #resolve_guid{path = Path},
        fun (#file_attr{uuid = GUID}) ->
            {guid, GUID}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Converts given file path to UUID.
%% @end
%%--------------------------------------------------------------------
-spec path_to_uuid(fslogic_worker:ctx(), file_meta:path()) -> file_meta:uuid().
path_to_uuid(CTX, Path) ->
    {ok, Tokens} = fslogic_path:verify_file_path(Path),
    Entry = fslogic_path:get_canonical_file_entry(CTX, Tokens),
    {ok, #document{key = UUID}} = file_meta:get(Entry),
    UUID.

%%--------------------------------------------------------------------
%% @doc
%% Gets full file path.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_path(fslogic_worker:ctx(), file_meta:uuid()) -> file_meta:path().
uuid_to_path(#fslogic_ctx{session_id = SessId, session = #session{
    identity = #user_identity{user_id = UserId}}}, FileUuid) ->
    UserRoot = user_root_dir_uuid(UserId),
    case FileUuid of
        UserRoot -> <<"/">>;
        _ ->
            {ok, Path} = fslogic_path:gen_path({uuid, FileUuid}, SessId),
            Path
    end.

%%--------------------------------------------------------------------
%% @doc
%% For given file UUID and spaceId generates file's GUID.
%% @end
%%--------------------------------------------------------------------
-spec to_file_guid(file_meta:uuid(), space_info:id()) ->
    fslogic_worker:file_guid().
to_file_guid(FileUUID, SpaceId) -> %todo rename
    http_utils:base64url_encode(term_to_binary({guid, FileUUID, SpaceId})).

%%--------------------------------------------------------------------
%% @doc
%% For given file UUID generates file's GUID. SpaceId is calculated in process.
%% @end
%%--------------------------------------------------------------------
-spec to_file_guid(file_meta:uuid()) -> fslogic_worker:file_guid().
to_file_guid(FileUUID) ->
    try fslogic_spaces:get_space_id({uuid, FileUUID}) of
        SpaceId ->
            to_file_guid(FileUUID, SpaceId)
    catch
        {not_a_space, _} ->
            to_file_guid(FileUUID, undefined)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's UUID for given file's GUID.
%% @end
%%--------------------------------------------------------------------
-spec file_guid_to_uuid(fslogic_worker:file_guid()) -> file_meta:uuid().
file_guid_to_uuid(FileGUID) ->
    {FileUUID, _} = unpack_file_guid(FileGUID),
    FileUUID.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's UUID and its SpaceId for given file's GUID.
%% @end
%%--------------------------------------------------------------------
-spec unpack_file_guid(FileGUID :: fslogic_worker:file_guid()) ->
    {file_meta:uuid(), space_info:id()}.
unpack_file_guid(FileGUID) ->
    try binary_to_term(http_utils:base64url_decode(FileGUID)) of
        {guid, FileUUID, SpaceId} ->
            {FileUUID, SpaceId};
        {share_guid, FileUUID, SpaceId, ShareId} ->
            {FileUUID, SpaceId};
        _ ->
            {FileGUID, undefined}
    catch
        _:_ ->
            {FileGUID, undefined}
    end.

%%--------------------------------------------------------------------
%% @doc Convert SpaceId to uuid of file_meta document of this space directory.
%%--------------------------------------------------------------------
-spec spaceid_to_space_dir_uuid(space_info:id()) -> file_meta:uuid().
spaceid_to_space_dir_uuid(SpaceId) ->
    http_utils:base64url_encode(term_to_binary({space, SpaceId})).

%%--------------------------------------------------------------------
%% @doc Convert file_meta uuid of space directory to SpaceId
%%--------------------------------------------------------------------
-spec space_dir_uuid_to_spaceid(file_meta:uuid()) -> space_info:id().
space_dir_uuid_to_spaceid(SpaceUuid) ->
    case binary_to_term(http_utils:base64url_decode(SpaceUuid)) of
        {space, SpaceId} ->
            SpaceId;
        _ ->
            throw({not_a_space, {uuid, SpaceUuid}})
    end.

%%--------------------------------------------------------------------
%% @doc
%% For given file UUID generates phantom's UUID.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_phantom_uuid(file_meta:uuid()) -> file_meta:uuid().
uuid_to_phantom_uuid(FileUUID) ->
    http_utils:base64url_encode(term_to_binary({phantom, FileUUID})).

%%--------------------------------------------------------------------
%% @doc
%% For given file UUID generates phantom's UUID.
%% @end
%%--------------------------------------------------------------------
-spec phantom_uuid_to_uuid(file_meta:uuid()) -> file_meta:uuid().
phantom_uuid_to_uuid(PhantomUUID) ->
    {phantom, FileUUID} = binary_to_term(http_utils:base64url_decode(PhantomUUID)),
    FileUUID.

%%--------------------------------------------------------------------
%% @doc
%% Convert Guid and share id to share guid (allowing for guest read)
%% @end
%%--------------------------------------------------------------------
-spec guid_to_share_guid(fslogic_worker:file_guid(), share_info:id()) ->
    share_info:share_guid().
guid_to_share_guid(Guid, ShareId) ->
    {FileUUID, SpaceId} = unpack_file_guid(Guid),
    http_utils:base64url_encode(term_to_binary({share_guid, FileUUID, SpaceId, ShareId})).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's UUID, its SpaceId and its ShareId for given file's share GUID.
%% @end
%%--------------------------------------------------------------------
-spec unpack_share_guid(share_info:share_guid()) ->
    {file_meta:uuid(), space_info:id(), share_info:id()}.
unpack_share_guid(ShareGUID) ->
    {share_guid, FileUUID, SpaceId, ShareId} =
        binary_to_term(http_utils:base64url_decode(ShareGUID)),
    {FileUUID, SpaceId, ShareId}.

%%%===================================================================
%%% Internal functions
%%%===================================================================