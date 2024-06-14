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
%%%         - for each new space support (as users with mounted Oneclient could belong to such a space);
%%%     * file_removed:
%%%         - when space was deleted;
%%%         - when space support was removed;
%%%     * file_renamed:
%%%         - always when file_attr_changed is considered (see above) - the new space may have a name that conflicts
%%%           with one of already existing spaces, which results in rename of this preexisting space to disambiguated name;
%%%         - always when file_removed is considered (see above) - it may cause an existing conflict to cease existing;
%%%           @TODO VFS-10923 currently not working properly in case of space removal, as user can not fetch name of space he no longer belongs to
%%%         - when any user space changes name - this may cause new name conflict with the rest of user spaces,
%%%           as well as cause an existing conflict to cease existing.
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
    list_spaces/4,
    get_space_name_and_conflicts/3, get_space_name_and_conflicts/4
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

-define(FILE_META_DOC(UserId), #document{
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

-type apply_fun() :: fun((session:id(), file_id:file_guid(), od_space:id(), #{od_space:name() => [od_space:id()]}) -> ok).

%%%===================================================================
%%% API
%%%===================================================================

-spec list_spaces(user_ctx:ctx(), file_listing:offset(), file_listing:limit(),
    file_listing:whitelist() | undefined) -> [{file_meta:name(), od_space:id()}].
list_spaces(UserCtx, Offset, Limit, WhiteList) ->
    SpaceIdsWithSupport = get_user_supported_spaces(UserCtx),
    
    FilteredSpaceIds = case WhiteList of
        undefined -> SpaceIdsWithSupport;
        _ -> lists_utils:intersect(SpaceIdsWithSupport, WhiteList)
    end,
    
    case Offset < length(FilteredSpaceIds) of
        true ->
            lists:sublist(resolve_unambiguous_names_for_spaces(group_spaces_by_name(
                user_ctx:get_session_id(UserCtx), FilteredSpaceIds)), Offset + 1, Limit);
        false ->
            []
    end.


-spec get_space_name_and_conflicts(user_ctx:ctx(), file_meta:name() | file_meta:disambiguated_name(), od_space:id()) ->
    {file_meta:name(), file_meta:conflicts()}.
get_space_name_and_conflicts(UserCtx, PossiblyDisambiguatedName, SpaceId) ->
    get_space_name_and_conflicts(user_ctx:get_session_id(UserCtx), user_ctx:get_user_id(UserCtx),
        PossiblyDisambiguatedName, SpaceId).


-spec get_space_name_and_conflicts(session:id(), od_user:id(), file_meta:name() | file_meta:disambiguated_name(),
    od_space:id()) -> {file_meta:name(), file_meta:conflicts()}.
get_space_name_and_conflicts(SessId, UserId, PossiblyDisambiguatedName, SpaceId) ->
    SpaceIdsWithSupport = get_user_supported_spaces(SessId, UserId),
    SpaceIdsByName = group_spaces_by_name(SessId, SpaceIdsWithSupport),
    [BaseSpaceName | _] = binary:split(PossiblyDisambiguatedName, ?SPACE_NAME_ID_SEPARATOR),
    case maps:get(BaseSpaceName, SpaceIdsByName, []) of
        [_] ->
            {BaseSpaceName, []};
        SpaceIds ->
            Conflicts = [{BaseSpaceName, fslogic_file_id:spaceid_to_space_dir_uuid(Sid)} || Sid <- SpaceIds, Sid /= SpaceId],
            ExtendedName = disambiguate_conflicted_space_name(BaseSpaceName, SpaceId),
            {ExtendedName, Conflicts}
    end.


-spec report_new_spaces_appeared([od_user:id()], [od_space:id()]) -> ok.
report_new_spaces_appeared(UserIds, NewSpaceIds) ->
    apply_for_spaces_of_users_with_active_fuse_sessions(fun(SessId, UserRootDirGuid, SpaceId, SpaceIdsByName) ->
        SpaceName = maps_utils:fold_while(fun(Name, SpacesWithName, _Acc) ->
            case lists:member(SpaceId, SpacesWithName) of
                true -> {halt, Name};
                false -> {cont, undefined}
            end
        end, undefined, SpaceIdsByName),
        handle_space_name_appeared_events(
            SessId, SpaceId, SpaceName, UserRootDirGuid, maps:get(SpaceName, SpaceIdsByName), create)
    end, UserIds, NewSpaceIds).


-spec report_spaces_removed([od_user:id()], [od_space:id()]) -> ok.
report_spaces_removed(UserIds, RemovedSpaceIds) ->
    %% @TODO VFS-10923 user does not have access to space anymore so he cannot get its name and therefore unnecessary
    %% space name suffix can remain (when there were 2 spaces with the same name and user left one of this spaces).
    lists:foreach(fun(SpaceId) ->
        lists:foreach(fun(UserId) ->
            emit_space_dir_deleted(SpaceId, UserId)
        end, UserIds)
    end, RemovedSpaceIds).


-spec report_space_name_change([od_user:id()], od_space:id(), PrevName :: od_space:name() | undefined,
    NewName :: od_space:name() | undefined) -> ok.
report_space_name_change(UserIds, SpaceId, undefined, NewName) ->
    % first appearance of this space in provider
    apply_for_spaces_of_users_with_active_fuse_sessions(fun(SessId, UserRootDirGuid, _SpaceId, SpaceIdsByName) ->
        handle_space_name_appeared_events(SessId, SpaceId, NewName, UserRootDirGuid, maps:get(NewName, SpaceIdsByName), create)
    end, UserIds, [SpaceId]);
report_space_name_change(UserIds, SpaceId, PrevName, undefined) ->
    % space was deleted
    apply_for_spaces_of_users_with_active_fuse_sessions(fun(_SessId, UserRootDirGuid, _SpaceId, SpaceIdsByName) ->
        handle_space_name_disappeared_events(PrevName, UserRootDirGuid, maps:get(PrevName, SpaceIdsByName, []))
    end, UserIds, [SpaceId]);
report_space_name_change(UserIds, SpaceId, PrevName, NewName) ->
    % space was renamed
    apply_for_spaces_of_users_with_active_fuse_sessions(fun(SessId, UserRootDirGuid, _SpaceId, SpaceIdsByName) ->
        handle_space_name_disappeared_events(PrevName, UserRootDirGuid, maps:get(PrevName, SpaceIdsByName, [])),
        handle_space_name_appeared_events(
            SessId, SpaceId, NewName, UserRootDirGuid, maps:get(NewName, SpaceIdsByName), {rename, PrevName})
    end, UserIds, [SpaceId]).


-spec ensure_docs_exist(od_user:id()) -> ok.
ensure_docs_exist(UserId) ->
    case file_meta:create({uuid, ?GLOBAL_ROOT_DIR_UUID}, ?FILE_META_DOC(UserId)) of
        {ok, #document{}} ->
            ?extract_ok(times_api:report_file_created(
                file_ctx:new_by_guid(fslogic_file_id:user_root_dir_guid(UserId))
            ));
        {error, already_exists} ->
            ok
    end.


-spec ensure_cache_updated() -> ok.
ensure_cache_updated() ->
    maps:fold(fun(UserId, [SessId | _], AlreadyFetchedSpaces) ->
        case user_logic:get(SessId, UserId) of
            {ok, #document{value = #od_user{eff_spaces = SpaceIds}}} ->
                lists:foldl(fun(SpaceId, AccFetchedSpaces) ->
                    case gb_sets:is_element(SpaceId, AccFetchedSpaces) of
                        true ->
                            AccFetchedSpaces;
                        false ->
                            {ok, _} = space_logic:get(SessId, SpaceId),
                            gb_sets:add(SpaceId, AccFetchedSpaces)
                    end
                end, AlreadyFetchedSpaces, SpaceIds);
            ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_INVALID) ->
                AlreadyFetchedSpaces;
            ?ERROR_TOKEN_INVALID ->
                AlreadyFetchedSpaces;
            {error, _} = Error ->
                ?warning(?autoformat_with_msg("Could not fetch spaces", [UserId, Error])),
                AlreadyFetchedSpaces
        end
    end, gb_sets:new(), find_fuse_sessions_of_all_users()),
    ok.

%%%===================================================================
%%% Helper functions
%%%===================================================================

%% @private
-spec handle_space_name_appeared_events(session:id(), od_space:id(), od_space:name(), file_id:file_guid(),
    [od_space:id()], create | {rename, od_space:name()}) -> ok.
handle_space_name_appeared_events(SessId, SpaceId, Name, UserRootDirGuid, SpaceIdsWithSameName, Operation) ->
    FinalNewName = case SpaceIdsWithSameName of
        [SpaceId] ->
            Name;
        [_S1, _S2] = L ->
            [OtherSpaceId] = L -- [SpaceId],
            emit_renamed_event(
                OtherSpaceId, UserRootDirGuid, disambiguate_conflicted_space_name(Name, OtherSpaceId), Name),
            disambiguate_conflicted_space_name(Name, SpaceId);
        _ ->
            disambiguate_conflicted_space_name(Name, SpaceId)
    end,
    case Operation of
        create -> emit_space_dir_created(SessId, SpaceId, FinalNewName);
        {rename, PrevName} -> emit_renamed_event(SpaceId, UserRootDirGuid, FinalNewName, PrevName)
    end.


%% @private
-spec handle_space_name_disappeared_events(od_space:name(), file_id:file_guid(), [od_space:id()]) -> ok.
handle_space_name_disappeared_events(SpaceName, UserRootDirGuid, [SpaceId] = _SpaceIdsWithTheSameName) ->
    emit_renamed_event(SpaceId, UserRootDirGuid, SpaceName, disambiguate_conflicted_space_name(SpaceName, SpaceId));
handle_space_name_disappeared_events(_SpaceName, _UserRootDirGuid, _SpaceIdsWithSameName) ->
    ok.


%% @private
-spec apply_for_spaces_of_users_with_active_fuse_sessions(apply_fun(), [od_user:id()], [od_space:id()]) -> ok.
apply_for_spaces_of_users_with_active_fuse_sessions(Fun, UserIds, SpaceIds) ->
    % user to session mapping is done only to fetch space name in user context (as provider may not have access to such space)
    maps:foreach(fun(UserId, [SessId | _]) ->
        UserRootDirGuid = fslogic_file_id:user_root_dir_guid(UserId),
        SpaceIdsByName = group_spaces_by_name(SessId, get_user_supported_spaces(SessId, UserId)),
        lists:foreach(fun(SpaceId) ->
            Fun(SessId, UserRootDirGuid, SpaceId, SpaceIdsByName)
        end, lists_utils:intersect(lists:merge(maps:values(SpaceIdsByName)), SpaceIds))
    end, maps:with(UserIds, find_fuse_sessions_of_all_users())).


%% @private
-spec find_fuse_sessions_of_all_users() -> #{od_user:id() => [session:id()]}.
find_fuse_sessions_of_all_users() ->
    {ok, SessList} = session:list(),
    lists:foldl(fun
        % NOTE: as events are only produced for Oneclient, only fuse sessions are of interest.
        (#document{key = SessId, value = #session{type = fuse, identity = ?SUB(user, UserId)}}, Acc) ->
            Acc#{UserId => [SessId | maps:get(UserId, Acc, [])]};
        (_, Acc) ->
            Acc
    end, #{}, SessList).


%% @private
-spec get_user_supported_spaces(user_ctx:ctx()) -> [od_space:id()].
get_user_supported_spaces(UserCtx) ->
    get_user_supported_spaces(user_ctx:get_session_id(UserCtx), user_ctx:get_user_id(UserCtx)).
    

%% @private
-spec get_user_supported_spaces(session:id(), od_user:id()) -> [od_space:id()].
get_user_supported_spaces(SessId, UserId) ->
    case user_logic:get_eff_spaces(SessId, UserId) of
        {ok, AllUserSpaceIds} -> filter_spaces_with_support(SessId, AllUserSpaceIds);
        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_INVALID) -> []; % race with token invalidation
        ?ERROR_TOKEN_INVALID -> []
    end.


%% @private
-spec filter_spaces_with_support(session:id(), [od_space:id()]) -> [od_space:id()].
filter_spaces_with_support(SessId, SpaceIds) ->
    lists:filter(fun(SpaceId) ->
        case space_logic:get(SessId, SpaceId) of
            {ok, #document{value = #od_space{providers = P}}} ->
                maps:size(P) > 0;
            _ ->
                false
        end
    end, SpaceIds).


%%%===================================================================
%%% Space name related functions
%%%===================================================================

%% @private
-spec group_spaces_by_name(session:id(), [od_space:id()]) -> #{od_space:name() => [od_space:id()]}.
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
-spec resolve_unambiguous_names_for_spaces(#{od_space:name() => [od_space:id()]}) ->
    [{file_meta:name(), od_space:id()}].
resolve_unambiguous_names_for_spaces(SpaceIdsByName) ->
    lists:sort(lists:flatmap(
        fun
            ({Name, [SpaceId]}) ->
                [{Name, SpaceId}];
            ({Name, Spaces}) -> lists:map(fun(SpaceId) ->
                {disambiguate_conflicted_space_name(Name, SpaceId), SpaceId}
            end, Spaces)
        end, maps:to_list(SpaceIdsByName))).


%% @private
-spec disambiguate_conflicted_space_name(od_space:name(), od_space:id()) -> file_meta:name().
disambiguate_conflicted_space_name(SpaceName, SpaceId) ->
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
-spec emit_space_dir_created(session:id(), od_space:id(), od_space:name()) -> ok.
emit_space_dir_created(SessId, SpaceId, SpaceName) ->
    FileCtx = file_ctx:new_by_guid(fslogic_file_id:spaceid_to_space_dir_guid(SpaceId)),
    #fuse_response{fuse_response = FileAttr} =
        attr_req:get_file_attr_insecure(user_ctx:new(SessId), FileCtx, #{
            allow_deleted_files => false,
            attributes => ?ONECLIENT_FILE_ATTRS -- [?attr_name]
        }),
    FileAttr2 = FileAttr#file_attr{size = 0, name = SpaceName},
    ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, FileAttr2, []).


%% @private
-spec emit_space_dir_deleted(od_space:id(), od_user:id()) -> ok.
emit_space_dir_deleted(SpaceId, UserId) ->
    FileCtx = file_ctx:new_by_uuid(fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId), SpaceId),
    ok = fslogic_event_emitter:emit_file_removed(FileCtx, [], fslogic_file_id:user_root_dir_guid(UserId)).
