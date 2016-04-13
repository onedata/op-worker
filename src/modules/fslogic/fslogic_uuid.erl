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
-include_lib("ctool/include/logging.hrl").

%% API
-export([spaces_uuid/1, default_space_uuid/1, path_to_uuid/2, uuid_to_path/2,
    spaceid_to_space_dir_uuid/1, space_dir_uuid_to_spaceid/1, ensure_uuid/2]).
-export([file_uuid_to_space_id/1, gen_file_uuid/1, gen_file_uuid/0]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% For given file's UUID returns Space's ID that contains this file.
%% @end
%%--------------------------------------------------------------------
-spec file_uuid_to_space_id(file_meta:uuid()) ->
    {ok, binary()} | {error, {not_in_space_scope, file_meta:uuid(), Reason :: term()}}.
file_uuid_to_space_id(FileUUID) ->
    BinParentUUID = http_utils:base64url_decode(FileUUID),
    try binary_to_term(BinParentUUID) of
        {space, SpaceId} ->
            {ok, SpaceId};
        {{s, SpaceId}, _} ->
            {ok, SpaceId}
    catch
        _:Reason ->
            ?error("Unable to decode file UUID ~p due to: ~p", [FileUUID, Reason]),
            {error, {not_in_space_scope, FileUUID, Reason}}
    end.


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
    http_utils:base64url_encode(crypto:rand_bytes(16)).

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
-spec ensure_uuid(fslogic_worker:ctx(), fslogic_worker:file()) ->
    {uuid, file_meta:uuid()}.
ensure_uuid(_CTX, {uuid, UUID}) ->
    {uuid, UUID};
ensure_uuid(_CTX, #document{key = UUID}) ->
    {uuid, UUID};
ensure_uuid(CTX, {path, Path}) ->
    {uuid, path_to_uuid(CTX, Path)}.

%%--------------------------------------------------------------------
%% @doc
%% Gets full file path.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_path(fslogic_worker:ctx(), file_meta:uuid()) -> file_meta:path().
uuid_to_path(#fslogic_ctx{session_id = SessId, session = #session{
    identity = #identity{user_id = UserId}}}, FileUuid) ->
    case default_space_uuid(UserId) =:= FileUuid of
        true -> <<"/">>;
        false ->
            {ok, Path} = fslogic_path:gen_path({uuid, FileUuid}, SessId),
            Path
    end.

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
%% @doc Returns UUID of user's main 'spaces' directory.
%% @end
%%--------------------------------------------------------------------
-spec spaces_uuid(UserId :: onedata_user:id()) -> file_meta:uuid().
spaces_uuid(UserId) ->
    http_utils:base64url_encode(term_to_binary({UserId, ?SPACES_BASE_DIR_NAME})).

%%--------------------------------------------------------------------
%% @doc Returns UUID of user's default space directory.
%% @end
%%--------------------------------------------------------------------
-spec default_space_uuid(UserId :: onedata_user:id()) -> file_meta:uuid().
default_space_uuid(UserId) ->
    http_utils:base64url_encode(UserId).

%%%===================================================================
%%% Internal functions
%%%===================================================================