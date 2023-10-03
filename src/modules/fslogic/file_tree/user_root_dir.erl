%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% User root dir is a virtual directory that represents mount root after mounting Oneclient.
%%% Listing it returns all spaces that user belongs to that have at least one supporting provider.
%%% It is local to each provider and cannot be modified.
%%% Because spaces can have duplicated names, in such a case when there are at least 2 spaces with the same name
%%% each space's name is extended with its id (see disambiguate_space_name/2).
%%%
%%% This module contains logic of events production for all spaces changes.
%%% Events production should be checked as follows:
%%%     * file_attr_changed (represents new file for Oneclient):
%%%         - for each new space for given user (as user might have been added to an already supported space);
%%%         - for each new space support (as users with mounted oneclient could belong to such a space);
%%%     * file_removed:
%%%         - when space was deleted;
%%%         - when space support was removed;
%%%     * file_renamed:
%%%         - space name has changed
%%%         - new space was created (as it may be second space with such a name, therefore the other space needs to be renamed);
%%%         - user was added to new space (the same as above);
%%%           @TODO VFS-10923 currently not working properly as user can not fetch space name of space he no longer belongs to
%%%         - space was deleted (as this may result in removing last space with duplicated name);
%%%         - user was removed from space (the same as above);
%%% @TODO VFS-11415 - cleanup user root dir docs after user deletion
%%% @end
%%%-------------------------------------------------------------------
-module(user_root_dir).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-export([
    list_spaces/5,
    get_space_name_and_conflicts/4
]).

-export([
    report_new_spaces_appeared/2,
    report_spaces_removed/2,
    report_space_name_change/4
]).

-export([
    ensure_cache_updated/0,
    ensure_docs_exist/1
]).

-define(FM_DOC(UserId), #document{
    key = fslogic_file_id:user_root_dir_uuid(UserId),
    value = #file_meta{
        name = UserId,
        type = ?DIRECTORY_TYPE,
        mode = 8#1755,
        owner = ?ROOT_USER_ID,
        is_scope = true,
        parent_uuid = ?GLOBAL_ROOT_DIR_UUID
    }
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec list_spaces(session:id(), od_user:id(), file_listing:offset(), file_listing:limit(),
    file_listing:whitelist() | undefined) -> [{file_meta:name(), od_space:id()}].
list_spaces(SessId, UserId, Offset, Limit, WhiteList) ->
    SpacesWithSupport = get_user_supported_spaces(SessId, UserId),
    
    FilteredSpaces = case WhiteList of
        undefined ->
            SpacesWithSupport;
        _ ->
            lists:filter(fun(Space) ->
                lists:member(Space, WhiteList)
            end, SpacesWithSupport)
    end,
    
    case Offset < length(FilteredSpaces) of
        true ->
            lists:sublist(resolve_unique_names_for_spaces(group_spaces_by_name(SessId, FilteredSpaces)),
                Offset + 1, Limit);
        false ->
            []
    end.


-spec get_space_name_and_conflicts(session:id(), od_user:id(), file_meta:name(), od_space:id()) ->
    {file_meta:name(), file_meta:conflicts()}.
get_space_name_and_conflicts(SessId, UserId, Name, SpaceId) ->
    SpacesWithSupport = get_user_supported_spaces(SessId, UserId),
    SpacesByName = group_spaces_by_name(SessId, SpacesWithSupport),
    [BaseFileName | _] = binary:split(Name, ?SPACE_NAME_ID_SEPARATOR),
    case maps:get(BaseFileName, SpacesByName, []) of
        [_] ->
            {BaseFileName, []};
        Spaces ->
            Conflicts = [{BaseFileName, fslogic_file_id:spaceid_to_space_dir_uuid(S)} || S <- Spaces -- [SpaceId]],
            ExtendedName = disambiguate_space_name(BaseFileName, SpaceId),
            {ExtendedName, Conflicts}
    end.


-spec report_new_spaces_appeared([od_user:id()], [od_space:id()]) -> ok.
report_new_spaces_appeared(UserList, NewSpaces) ->
    lists:foreach(fun file_meta:ensure_space_docs_exist/1, NewSpaces),
    apply_for_user_with_fuse_session_spaces(fun(UserRootDirGuid, SpaceId, SpacesByName) ->
        SpaceName = maps_utils:fold_while(fun(Name, SpacesWithName, _Acc) ->
            case lists:member(SpaceId, SpacesWithName) of
                true -> {halt, Name};
                false -> {cont, undefined}
            end
        end, undefined, SpacesByName),
        handle_space_name_appeared_events(SpaceId, SpaceName, UserRootDirGuid, maps:get(SpaceName, SpacesByName), create)
    end, UserList, NewSpaces).


-spec report_spaces_removed([od_user:id()], [od_space:id()]) -> ok.
report_spaces_removed(UserList, SpacesDiff) ->
    %% @TODO VFS-10923 user does not have access to space anymore so he cannot get its name and therefore unnecessary
    %% space name suffix can remain (when there were 2 spaces with the same name and user left one of this spaces).
    lists:foreach(fun(SpaceId) ->
        lists:foreach(fun(UserId) ->
            emit_space_dir_deleted(SpaceId, UserId)
        end, UserList)
    end, SpacesDiff).


-spec report_space_name_change([od_user:id()], od_space:id(), PrevName :: od_space:name() | undefined,
    NewName :: od_space:name() | undefined) -> ok.
report_space_name_change(UserList, SpaceId, undefined, NewName) ->
    % first appearance of this space in provider
    apply_for_user_with_fuse_session_spaces(fun(UserRootDirGuid, _SpaceId, SpacesByName) ->
        handle_space_name_appeared_events(SpaceId, NewName, UserRootDirGuid, maps:get(NewName, SpacesByName), create)
    end, UserList, [SpaceId]);
report_space_name_change(UserList, SpaceId, PrevName, undefined) ->
    % space was deleted
    apply_for_user_with_fuse_session_spaces(fun(UserRootDirGuid, _SpaceId, SpacesByName) ->
        handle_events_space_name_disappeared(PrevName, UserRootDirGuid, maps:get(PrevName, SpacesByName, []))
    end, UserList, [SpaceId]);
report_space_name_change(UserList, SpaceId, PrevName, NewName) ->
    % space was renamed
    apply_for_user_with_fuse_session_spaces(fun(UserRootDirGuid, _SpaceId, SpacesByName) ->
        handle_events_space_name_disappeared(PrevName, UserRootDirGuid, maps:get(PrevName, SpacesByName, [])),
        handle_space_name_appeared_events(SpaceId, NewName, UserRootDirGuid, maps:get(NewName, SpacesByName), {rename, PrevName})
    end, UserList, [SpaceId]).


-spec ensure_docs_exist(od_user:id()) -> ok.
ensure_docs_exist(UserId) ->
    case file_meta:create({uuid, ?GLOBAL_ROOT_DIR_UUID}, ?FM_DOC(UserId)) of
        {ok, #document{key = Uuid}} ->
            ?extract_ok(times:save_with_current_times(Uuid));
        {error, already_exists} ->
            ok
    end.


-spec ensure_cache_updated() -> ok.
ensure_cache_updated() ->
    {ok, SessList} = session:list(),
    lists:foreach(fun
        % NOTE: % as events are only produced for Oneclient only fuse sessions or of interest.
        (#document{key = SessId, value = #session{type = fuse, identity = ?SUB(user, UserId)}}) ->
            case user_logic:get(SessId, UserId) of
                {ok, #document{value = #od_user{eff_spaces = Spaces}}} ->
                    lists:foreach(fun(SpaceId) -> {ok, _} = space_logic:get(SessId, SpaceId) end, Spaces);
                {error, _} = Error ->
                    ?warning("Could not fetch spaces for user ~s due to ~p", [Error])
            end;
        (_) ->
            ok
    end, SessList).

%%%===================================================================
%%% Helper functions
%%%===================================================================

%% @private
-spec handle_space_name_appeared_events(od_space:id(), od_space:name(), file_id:file_guid(), [od_space:id()],
    create | {rename, od_space:name()}) -> ok.
handle_space_name_appeared_events(SpaceId, Name, UserRootDirGuid, SpacesWithSameName, Operation) ->
    FinalNewName = case SpacesWithSameName of
        [SpaceId] ->
            Name;
        [_S1, _S2] = L ->
            [OtherSpaceId] = L -- [SpaceId],
            emit_renamed_event(OtherSpaceId, UserRootDirGuid, disambiguate_space_name(Name, OtherSpaceId), Name),
            disambiguate_space_name(Name, SpaceId);
        _ ->
            disambiguate_space_name(Name, SpaceId)
    end,
    case Operation of
        create -> emit_space_dir_created(SpaceId, FinalNewName);
        {rename, PrevName} -> emit_renamed_event(SpaceId, UserRootDirGuid, FinalNewName, PrevName)
    end.


%% @private
-spec handle_events_space_name_disappeared(od_space:name(), file_id:file_guid(), [od_space:id()]) -> ok.
handle_events_space_name_disappeared(SpaceName, UserRootDirGuid, [SpaceId] = _SpacesWithTheSameName) ->
    emit_renamed_event(SpaceId, UserRootDirGuid, SpaceName, disambiguate_space_name(SpaceName, SpaceId));
handle_events_space_name_disappeared(_SpaceName, _UserRootDirGuid, _SpacesWithSameName) ->
    ok.


%% @private
-spec apply_for_user_with_fuse_session_spaces(fun((file_id:file_guid(), od_space:id(), #{od_space:name() => [od_space:id()]}) -> ok),
    [od_user:id()], [od_space:id()]) -> ok.
apply_for_user_with_fuse_session_spaces(Fun, UsersList, Spaces) ->
    % user to session mapping is done only to fetch space name in user context (as provider may not have access to such space)
    maps:fold(fun(UserId, [SessId | _], _) ->
        UserRootDirGuid = fslogic_file_id:user_root_dir_guid(UserId),
        SpacesByName = group_spaces_by_name(SessId, get_user_supported_spaces(SessId, UserId)),
        lists:foreach(fun(SpaceId) ->
            Fun(UserRootDirGuid, SpaceId, SpacesByName)
        end, lists_utils:intersect(lists:merge(maps:values(SpacesByName)), Spaces))
    end, ok, maps:with(UsersList, map_fuse_sessions_to_users())).


%% @private
-spec map_fuse_sessions_to_users() -> #{od_user:id() => [session:id()]}.
map_fuse_sessions_to_users() ->
    {ok, SessList} = session:list(),
    lists:foldl(fun
        % NOTE: % as events are only produced for Oneclient only fuse sessions or of interest.
        (#document{key = SessId, value = #session{type = fuse, identity = ?SUB(user, UserId)}}, Acc) ->
            Acc#{UserId => [SessId | maps:get(UserId, Acc, [])]};
        (_, Acc) ->
            Acc
    end, #{}, SessList).


%% @private
-spec get_user_supported_spaces(session:id(), od_user:id()) -> [od_space:id()].
get_user_supported_spaces(SessId, UserId) ->
    {ok, AllUserSpaces} = user_logic:get_eff_spaces(SessId, UserId),
    filter_spaces_with_support(SessId, lists:usort(AllUserSpaces)).


%% @private
-spec filter_spaces_with_support(session:id(), [od_space:id()]) -> [od_space:id()].
filter_spaces_with_support(SessId, Spaces) ->
    lists:filter(fun(SpaceId) ->
        case space_logic:get(SessId, SpaceId) of
            {ok, #document{value = #od_space{providers = P}}} ->
                maps:size(P) > 0;
            _ ->
                false
        end
    end, Spaces).


%%%===================================================================
%%% Space name related functions
%%%===================================================================

%% @private
-spec group_spaces_by_name(gs_client_worker:client(), [od_space:id()]) -> #{od_space:name() => [od_space:id()]}.
group_spaces_by_name(SessId, SpaceIds) ->
    lists:foldl(fun(SpaceId, Acc) ->
        case space_logic:get_name(SessId, SpaceId) of
            {ok, SpaceName} ->
                Acc#{SpaceName => [SpaceId | maps:get(SpaceName, Acc, [])]};
            ?ERROR_NOT_FOUND ->
                Acc;
            ?ERROR_FORBIDDEN ->
                Acc
        end
    end, #{}, SpaceIds).


%% @private
-spec resolve_unique_names_for_spaces(#{od_space:name() => [od_space:id()]}) -> [{file_meta:name(), od_space:id()}].
resolve_unique_names_for_spaces(SpacesByName) ->
    lists:sort(maps:fold(
        fun (Name, [SpaceId], Acc) -> [{Name, SpaceId} | Acc];
            (Name, Spaces, Acc) -> Acc ++ lists:map(fun(SpaceId) ->
                {disambiguate_space_name(Name, SpaceId), SpaceId}
            end, Spaces)
        end, [], SpacesByName)).


%% @private
-spec disambiguate_space_name(od_space:name(), od_space:id()) -> file_meta:name().
disambiguate_space_name(SpaceName, SpaceId) ->
    <<SpaceName/binary, (?SPACE_NAME_ID_SEPARATOR)/binary, SpaceId/binary>>.


%%%===================================================================
%%% Event emitting functions
%%%===================================================================

%% @private
-spec emit_renamed_event(od_space:id(), file_id:file_guid(), od_space:name(), od_space:name()) -> ok.
emit_renamed_event(SpaceId, UserRootDirGuid, NewName, PrevName) ->
    FileCtx = file_ctx:new_by_guid(fslogic_file_id:spaceid_to_space_dir_guid(SpaceId)),
    fslogic_event_emitter:emit_file_renamed_no_exclude(FileCtx, UserRootDirGuid, UserRootDirGuid, NewName, PrevName).


%% @private
-spec emit_space_dir_created(od_space:id(), od_space:name()) -> ok.
emit_space_dir_created(SpaceId, SpaceName) ->
    FileCtx = file_ctx:new_by_guid(fslogic_file_id:spaceid_to_space_dir_guid(SpaceId)),
    #fuse_response{fuse_response = FileAttr} =
        attr_req:get_file_attr_insecure(user_ctx:new(?ROOT_SESS_ID), FileCtx, #{
            allow_deleted_files => false,
            name_conflicts_resolution_policy => resolve_name_conflicts
        }),
    FileAttr2 = FileAttr#file_attr{size = 0, name = SpaceName},
    ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, FileAttr2, []).


%% @private
-spec emit_space_dir_deleted(od_space:id(), od_user:id()) -> ok.
emit_space_dir_deleted(SpaceId, UserId) ->
    FileCtx = file_ctx:new_by_uuid(fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId), SpaceId),
    ok = fslogic_event_emitter:emit_file_removed(FileCtx, [], fslogic_file_id:user_root_dir_guid(UserId)).
