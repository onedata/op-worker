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
%%% each space's name is extended with its id (see space_logic:disambiguate_space_name/2).
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
%%%         - space name has changed;
%%%         - new space was created (as it may be second space with such a name, therefore the other space needs to be renamed);
%%%         - user was added to new space (the same as ^);
%%%         - space was deleted (as this may result in removing last space with duplicated name);
%%%             @TODO VFS-10923 currently not working properly as user can not fetch space name of already deleted space
%%%         - user was removed from space (the same as ^); @TODO VFS-10923
% fixme what can be moved here
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
    ensure_cache_updated/0,
    ensure_docs_exist/1
]).

-export([
    report_new_spaces_appeared/2,
    report_spaces_removed/2,
    report_space_name_change/4
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

-spec ensure_docs_exist(od_user:id()) -> ok.
ensure_docs_exist(UserId) ->
    case file_meta:create({uuid, ?GLOBAL_ROOT_DIR_UUID}, ?FM_DOC(UserId)) of
        {ok, #document{key = Uuid}} ->
            ?extract_ok(times:save_with_current_times(Uuid));
        {error, already_exists} ->
            ok
    end.


-spec report_new_spaces_appeared([od_user:id()], [od_space:id()]) -> ok.
report_new_spaces_appeared(UserList, NewSpaces) ->
    apply_for_user_spaces(fun(UserRootDirGuid, SpaceId, SpacesByName) ->
        SpaceName = maps_utils:fold_while(fun(Name, SpacesWithName, _Acc) ->
            case lists:member(SpaceId, SpacesWithName) of
                true -> {halt, Name};
                false -> {cont, undefined}
            end
        end, undefined, SpacesByName),
        file_meta:ensure_space_docs_exist(SpaceId),
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
    apply_for_user_spaces(fun(UserRootDirGuid, _SpaceId, SpacesByName) ->
        handle_space_name_appeared_events(SpaceId, NewName, UserRootDirGuid, maps:get(NewName, SpacesByName), create)
    end, UserList, [SpaceId]);
report_space_name_change(UserList, SpaceId, PrevName, undefined) ->
    % space was deleted
    apply_for_user_spaces(fun(UserRootDirGuid, _SpaceId, SpacesByName) ->
        handle_events_space_name_disappeared(PrevName, UserRootDirGuid, maps:get(PrevName, SpacesByName, []))
    end, UserList, [SpaceId]);
report_space_name_change(UserList, SpaceId, PrevName, NewName) ->
    % space was renamed
    apply_for_user_spaces(fun(UserRootDirGuid, _SpaceId, SpacesByName) ->
        handle_events_space_name_disappeared(PrevName, UserRootDirGuid, maps:get(PrevName, SpacesByName, [])),
        handle_space_name_appeared_events(SpaceId, NewName, UserRootDirGuid, maps:get(NewName, SpacesByName), {rename, PrevName})
    end, UserList, [SpaceId]).


% fixme explain that only fuse as this is for events creation
-spec ensure_cache_updated() -> ok.
ensure_cache_updated() -> % fixme name
    {ok, SessList} = session:list(),
    lists:foreach(fun
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
%%% Internal functions
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
            emit_renamed_event(OtherSpaceId, UserRootDirGuid, space_logic:disambiguate_space_name(Name, OtherSpaceId), Name),
            space_logic:disambiguate_space_name(Name, SpaceId);
        _ ->
            space_logic:disambiguate_space_name(Name, SpaceId)
    end,
    case Operation of
        create -> emit_space_dir_created(SpaceId, FinalNewName);
        {rename, PrevName} -> emit_renamed_event(SpaceId, UserRootDirGuid, FinalNewName, PrevName)
    end.


%% @private
-spec handle_events_space_name_disappeared(od_space:name(), file_id:file_guid(), [od_space:id()]) -> ok.
handle_events_space_name_disappeared(SpaceName, UserRootDirGuid, [SpaceId] = _SpacesWithTheSameName) ->
    emit_renamed_event(SpaceId, UserRootDirGuid, SpaceName, space_logic:disambiguate_space_name(SpaceName, SpaceId));
handle_events_space_name_disappeared(_SpaceName, _UserRootDirGuid, _SpacesWithSameName) ->
    ok.


%% @private
-spec apply_for_user_spaces(fun((file_id:file_guid(), od_space:id(), #{od_space:name() => [od_space:id()]}) -> ok),
    [od_user:id()], [od_space:id()]) -> ok.
apply_for_user_spaces(Fun, UsersList, Spaces) ->
    % user to session mapping is done only to fetch space name in user context (as provider may not have access to such space)
    maps:fold(fun(UserId, [SessId | _], _) ->
        {ok, AllUserSpaces} = user_logic:get_eff_spaces(SessId, UserId),
        UserRootDirGuid = fslogic_file_id:user_root_dir_guid(UserId),
        SpacesWithSupport = filter_spaces_with_support(SessId, lists:usort(AllUserSpaces ++ Spaces)),
        SpacesByName = space_logic:group_spaces_by_name(SessId, SpacesWithSupport),
        lists:foreach(fun(SpaceId) ->
            Fun(UserRootDirGuid, SpaceId, SpacesByName)
        end, lists_utils:intersect(lists:merge(maps:values(SpacesByName)), Spaces))
    end, ok, maps:with(UsersList, map_fuse_sessions_to_users())).


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
