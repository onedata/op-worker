%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides spaces-oriented helper methods for fslogic.
%% @end
%% ===================================================================
-module(fslogic_spaces).
-author("Rafal Slota").

-include("veil_modules/dao/dao.hrl").
-include("files_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([initialize/1, map_to_grp_owner/1, get_storage_space_name/1]).


%% initialize/1
%% ====================================================================
%% @doc Initializes or updates local data about given space (by its ID or struct #space_info).
%%      In both cases, space is updated/created both in DB and on storage.
%% @end
-spec initialize(SpaceInfo :: #space_info{} | binary()) -> {ok, #space_info{}} | {error, Reason :: any()}.
%% ====================================================================
initialize(#space_info{space_id = SpaceId, name = SpaceName} = SpaceInfo) ->
    case user_logic:create_space_dir(SpaceInfo) of
        {ok, SpaceUUID} ->
            try user_logic:create_dirs_at_storage([SpaceInfo]) of
                ok -> {ok, SpaceInfo};
                {error, Reason} ->
                    ?error("Filed to create space's (~p) dir on storage due to ~p", [SpaceInfo, Reason]),
                    dao_lib:apply(dao_vfs, remove_file, [{uuid, SpaceUUID}], fslogic_context:get_protocol_version()),
                    {error, Reason}
            catch
                Type:Error ->
                    ?error_stacktrace("Cannot initialize space on storage due to: ~p:~p", [Type, Error]),
                    dao_lib:apply(dao_vfs, remove_file, [{uuid, SpaceUUID}], fslogic_context:get_protocol_version()),
                    {error, {Type, Error}}
            end;
        {error, dir_exists} ->
            {ok, #veil_document{record = #file{extensions = Ext} = File} = FileDoc} = dao_lib:apply(dao_vfs, get_space_file, [{uuid, SpaceId}], 1),
            NewExt = lists:keyreplace(?file_space_info_extestion, 1, Ext, {?file_space_info_extestion, SpaceInfo}),
            NewFile = File#file{extensions = NewExt, name = unicode:characters_to_list(SpaceName)},
            {ok, _} = dao_lib:apply(vfs, save_file, [FileDoc#veil_document{record = NewFile}], 1),
            ok = user_logic:create_dirs_at_storage([SpaceInfo]),

            {ok, SpaceInfo};
        {error, Reason} ->
            ?error("Filed to initialize space (~p) due to ~p", [SpaceInfo, Reason]),
            {error, Reason}
    end;
initialize(SpaceId) ->
    case gr_adapter:get_space_info(SpaceId, fslogic_context:get_access_token()) of
        {ok, #space_info{} = SpaceInfo} ->
            initialize(SpaceInfo);
        {error, Reason} ->
            ?error("Cannot fetch data for space ~p due to: ~p", [SpaceId, Reason]),
            {error, Reason}
    end.


%% map_to_grp_owner/1
%% ====================================================================
%% @doc Maps given space or list of spaces to gid that shall be used on storage.
%% @end
-spec map_to_grp_owner([#space_info{}] | #space_info{}) -> GID :: integer() | [integer()].
%% ====================================================================
map_to_grp_owner([]) ->
    [];
map_to_grp_owner([SpaceInfo | T]) ->
    [map_to_grp_owner(SpaceInfo)] ++ map_to_grp_owner(T);
map_to_grp_owner(#space_info{name = SpaceName, space_id = SpaceId}) ->
    case os:cmd("getent group \"" ++ unicode:characters_to_list(SpaceName) ++ "\" | cut -d: -f3") -- [10, 13] of
        "" ->
            <<GID0:16/big-unsigned-integer-unit:8>> = crypto:hash(md5, SpaceId),
            70000 + GID0 rem 1000000;
        StrGID ->
            list_to_integer(StrGID)
    end.


%% get_storage_space_name/1
%% ====================================================================
%% @doc Generates space storage directory's name.
%% @end
-spec get_storage_space_name(SpaceInfo :: #space_info{}) -> SpaceStorageName :: string().
%% ====================================================================
get_storage_space_name(#space_info{space_id = SpaceId}) ->
    vcn_utils:ensure_list(SpaceId).
