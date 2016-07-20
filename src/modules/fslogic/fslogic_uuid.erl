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
-export([user_root_dir_uuid/1, path_to_uuid/2, uuid_to_path/2,
    guid_to_path/2, spaceid_to_space_dir_uuid/1, space_dir_uuid_to_spaceid/1, ensure_uuid/2,
    default_space_owner/1]).
-export([gen_file_uuid/1, gen_file_uuid/0]).
-export([to_file_guid/2, unpack_file_guid/1, file_guid_to_uuid/1, to_file_guid/1, ensure_guid/2]).
-export([uuid_to_phantom_uuid/1, phantom_uuid_to_uuid/1]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% For given file UUID and spaceId generates file's GUID.
%% @end
%%--------------------------------------------------------------------
-spec to_file_guid(file_meta:uuid(), SpaceId :: binary() | undefined) ->
    fslogic_worker:file_guid().
to_file_guid(FileUUID, SpaceId) ->
    http_utils:base64url_encode(term_to_binary({guid, FileUUID, SpaceId})).


%%--------------------------------------------------------------------
%% @doc
%% For given file UUID generates file's GUID. SpaceId is calculated in process.
%% @end
%%--------------------------------------------------------------------
-spec to_file_guid(file_meta:uuid()) ->
    fslogic_worker:file_guid().
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
%% Returns file's UUID and its SpaceId for given file's GUID.
%% @end
%%--------------------------------------------------------------------
-spec unpack_file_guid(FileGUID :: fslogic_worker:file_guid()) ->
    {file_meta:uuid(), SpaceId :: binary() | undefined}.
unpack_file_guid(FileGUID) ->
    try binary_to_term(http_utils:base64url_decode(FileGUID)) of
        {guid, FileUUID, SpaceId} ->
            {FileUUID, SpaceId};
        _ ->
            {FileGUID, undefined}
    catch
        _:_ ->
            {FileGUID, undefined}
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
%% Generates file's UUID that will be placed in given Space.
%% @end
%%--------------------------------------------------------------------
-spec gen_file_uuid(SpaceId :: binary()) -> file_meta:uuid().
gen_file_uuid(SpaceId) ->
    http_utils:base64url_encode(term_to_binary({{s, SpaceId}, crypto:rand_bytes(10)})).


%%--------------------------------------------------------------------
%% @doc
%% Generates generic file's UUID that will be not placed in any Space.
%% @end
%%--------------------------------------------------------------------
-spec gen_file_uuid() -> file_meta:uuid().
gen_file_uuid() ->
    PID = oneprovider:get_provider_id(),
    Rand = crypto:rand_bytes(16),
    http_utils:base64url_encode(<<PID/binary, "##", Rand/binary>>).

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
%% Converts given file entry to UUID.
%% @end
%%--------------------------------------------------------------------
-spec ensure_uuid(fslogic_worker:ctx(), fslogic_worker:ext_file()) ->
    {uuid, file_meta:uuid()}.
ensure_uuid(_CTX, {guid, FileGUID}) ->
    {uuid, fslogic_uuid:file_guid_to_uuid(FileGUID)};
ensure_uuid(_CTX, {uuid, UUID}) ->
    {uuid, UUID};
ensure_uuid(_CTX, #document{key = UUID}) ->
    {uuid, UUID};
ensure_uuid(CTX, {path, Path}) ->
    {uuid, path_to_uuid(CTX, Path)}.


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
        #get_file_attr{entry = {path, Path}},
        fun (#file_attr{uuid = GUID}) ->
            {guid, GUID}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Gets full file path.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_path(fslogic_worker:ctx(), file_meta:uuid()) -> file_meta:path().
uuid_to_path(#fslogic_ctx{session_id = SessId, session = #session{
    identity = #identity{user_id = UserId}}}, FileUuid) ->
    UserRoot = user_root_dir_uuid(UserId),
    case FileUuid of
        UserRoot -> <<"/">>;
        _ ->
            {ok, Path} = fslogic_path:gen_path({uuid, FileUuid}, SessId),
            Path
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets full file path.
%% @end
%%--------------------------------------------------------------------
-spec guid_to_path(fslogic_worker:ctx(), fslogic_worker:file_guid()) -> file_meta:path().
guid_to_path(CTX = #fslogic_ctx{}, FileGUID) ->
    uuid_to_path(CTX, fslogic_uuid:file_guid_to_uuid(FileGUID)).

%%--------------------------------------------------------------------
%% @doc Convert SpaceId to uuid of file_meta document of this space directory.
%%--------------------------------------------------------------------
-spec spaceid_to_space_dir_uuid(SpaceId :: binary()) -> binary().
spaceid_to_space_dir_uuid(SpaceId) ->
    http_utils:base64url_encode(term_to_binary({space, SpaceId})).

%%--------------------------------------------------------------------
%% @doc Convert file_meta uuid of space directory to SpaceId
%%--------------------------------------------------------------------
-spec space_dir_uuid_to_spaceid(SpaceUuid :: binary()) -> binary().
space_dir_uuid_to_spaceid(SpaceUuid) ->
    case binary_to_term(http_utils:base64url_decode(SpaceUuid)) of
        {space, SpaceId} ->
            SpaceId;
        _ ->
            throw({not_a_space, {uuid, SpaceUuid}})
    end.

%%--------------------------------------------------------------------
%% @doc Returns UUID of user's root directory.
%% @end
%%--------------------------------------------------------------------
-spec user_root_dir_uuid(UserId :: onedata_user:id()) -> file_meta:uuid().
user_root_dir_uuid(UserId) ->
    http_utils:base64url_encode(term_to_binary({root_space, UserId})).


%%--------------------------------------------------------------------
%% @doc Returns user id of default space directory owner.
%% @end
%%--------------------------------------------------------------------
-spec default_space_owner(DefaultSpaceUUID :: file_meta:uuid()) ->
    onedata_user:id().
default_space_owner(DefaultSpaceUUID) ->
    {root_space, UserId} = binary_to_term(http_utils:base64url_decode(DefaultSpaceUUID)),
    UserId.

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

%%%===================================================================
%%% Internal functions
%%%===================================================================