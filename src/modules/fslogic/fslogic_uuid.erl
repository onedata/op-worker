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

%% API
-export([spaces_uuid/1, default_space_uuid/1, path_to_uuid/2, uuid_to_path/2,
    spaceid_to_space_dir_uuid/1, space_dir_uuid_to_spaceid/1]).

%%%===================================================================
%%% API
%%%===================================================================

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
uuid_to_path(#fslogic_ctx{session = #session{identity = #identity{user_id = Uid}}}, FileUuid) ->
    case default_space_uuid(Uid) =:= FileUuid of
        true -> <<"/">>;
        false ->
            {ok, Path} = file_meta:gen_path({uuid, FileUuid}),
            Path
    end.

%%--------------------------------------------------------------------
%% @doc Convert SpaceId to uuid of file_meta document of this space directory.
%%--------------------------------------------------------------------
-spec spaceid_to_space_dir_uuid(SpaceId :: binary()) -> binary().
spaceid_to_space_dir_uuid(SpaceId) ->
    base64:encode(SpaceId).

%%--------------------------------------------------------------------
%% @doc Convert file_meta uuid of space directory to SpaceId
%%--------------------------------------------------------------------
-spec space_dir_uuid_to_spaceid(SpaceUuid :: binary()) -> binary().
space_dir_uuid_to_spaceid(SpaceUuid) ->
    base64:decode(SpaceUuid).

%%--------------------------------------------------------------------
%% @doc Returns UUID of user's main 'spaces' directory.
%% @end
%%--------------------------------------------------------------------
-spec spaces_uuid(UserId :: onedata_user:id()) -> file_meta:uuid().
spaces_uuid(UserId) ->
    base64:encode(term_to_binary({UserId, ?SPACES_BASE_DIR_NAME})).

%%--------------------------------------------------------------------
%% @doc Returns UUID of user's default space directory.
%% @end
%%--------------------------------------------------------------------
-spec default_space_uuid(UserId :: onedata_user:id()) -> file_meta:uuid().
default_space_uuid(UserId) ->
    base64:encode(UserId).

%%%===================================================================
%%% Internal functions
%%%===================================================================