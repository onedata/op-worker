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

-include("oneprovider_modules/dao/dao.hrl").
-include("files_common.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([initialize/1, map_to_grp_owner/1, get_storage_space_name/1, sync_all_supported_spaces/0]).


%% sync_all_supported_spaces/0
%% ====================================================================
%% @doc Synchronizes all spaces that are supported by this provider.
%% @end
-spec sync_all_supported_spaces() -> ok | {error, Reason :: any()} | {error, [Reason :: any()]}.
%% ====================================================================
sync_all_supported_spaces() ->
    case gr_providers:get_spaces(provider) of
        {ok, SpaceIds} ->
            Result = [catch fslogic_spaces:initialize(SpaceId) || SpaceId <- SpaceIds],
            {_GoodRes, BadRes} = lists:partition(
                fun(Elem) ->
                    case Elem of
                        {ok, _} -> true;
                        _       -> false
                    end
                end, Result),

            case BadRes of
                [] -> ok;
                _  ->
                    {error, BadRes}
            end;
        {error, Reason} ->
            ?error("Cannot get supported spaces due to: ~p", [Reason]),
            {error, Reason}
    end.


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
            {ok, #db_document{record = #file{extensions = Ext} = File} = FileDoc} = dao_lib:apply(dao_vfs, get_space_file, [{uuid, SpaceId}], 1),
            NewExt = lists:keyreplace(?file_space_info_extestion, 1, Ext, {?file_space_info_extestion, SpaceInfo}),
            NewFile = File#file{extensions = NewExt, name = unicode:characters_to_list(SpaceName)},
            {ok, _} = dao_lib:apply(vfs, save_file, [FileDoc#db_document{record = NewFile}], 1),
            ok = user_logic:create_dirs_at_storage([SpaceInfo]),

            {ok, SpaceInfo};
        {error, Reason} ->
            ?error("Filed to initialize space (~p) due to ~p", [SpaceInfo, Reason]),
            {error, Reason}
    end;
initialize(SpaceId) ->
    case gr_adapter:get_space_info(SpaceId, fslogic_context:get_gr_auth()) of
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
    case ets:lookup(?STORAGE_GROUP_IDS_CACHE, SpaceName) of
        [] ->
            <<GID0:16/big-unsigned-integer-unit:8>> = crypto:hash(md5, SpaceId),
            {ok, LowestGID} = oneprovider_node_app:get_env(lowest_generated_storage_gid),
            LowestGID + GID0 rem 1000000;
        [{_, GID}] ->
            GID
    end.


%% get_storage_space_name/1
%% ====================================================================
%% @doc Generates space storage directory's name.
%% @end
-spec get_storage_space_name(SpaceInfo :: #space_info{}) -> SpaceStorageName :: string().
%% ====================================================================
get_storage_space_name(#space_info{space_id = SpaceId}) ->
    utils:ensure_list(SpaceId).
