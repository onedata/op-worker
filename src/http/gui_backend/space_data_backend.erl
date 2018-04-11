%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the data-space model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(space_data_backend).
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").
-author("Jakub Liput").

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/privileges.hrl").

-define(CURRENT_TRANSFERS_PREFIX, <<"current">>).
-define(SCHEDULED_TRANSFERS_PREFIX, <<"scheduled">>).
-define(COMPLETED_TRANSFERS_PREFIX, <<"completed">>).
-define(MAX_TRANSFERS_TO_LIST, application:get_env(?APP_NAME, max_transfers_to_list, all)).
-define(TRANSFERS_LIST_OFFSET, application:get_env(?APP_NAME, transfers_list_offset, 0)).

%% API
-export([init/0, terminate/0]).
-export([find_record/2, find_all/1, query/2, query_record/2]).
-export([create_record/2, update_record/3, delete_record/2]).

-export([space_record/1, space_record/2]).

%%%===================================================================
%%% data_backend_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback init/0.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_record/2.
%% @end
%%--------------------------------------------------------------------
-spec find_record(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_record(<<"space">>, SpaceId) ->
    SessionId = gui_session:get_session_id(),
    % Check if the user belongs to this space -
    % he should be able to get protected space data.
    case space_logic:get_protected_data(SessionId, SpaceId) of
        {ok, _} ->
            {ok, space_record(SpaceId)};
        _ ->
            gui_error:unauthorized()
    end;

% PermissionsRecord matches <<"space-(user|group)-(list|permission)">>
find_record(PermissionsRecord, RecordId) ->
    SessionId = gui_session:get_session_id(),
    SpaceId = case PermissionsRecord of
        <<"space-user-list">> ->
            RecordId;
        <<"space-group-list">> ->
            RecordId;
        <<"space-provider-list">> ->
            RecordId;
        <<"space-transfer-link-state">> ->
            RecordId;
        _ -> % covers space-(user|group)-permission and space-transfer-list
            {_, Id} = op_gui_utils:association_to_ids(RecordId),
            Id
    end,
    UserId = gui_session:get_user_id(),
    % Make sure that user is allowed to view requested privileges - he must have
    % view privileges in this space.
    Authorized = space_logic:has_eff_privilege(
        SessionId, SpaceId, UserId, ?SPACE_VIEW
    ),
    case Authorized of
        false ->
            gui_error:unauthorized();
        true ->
            case PermissionsRecord of
                <<"space-user-list">> ->
                    {ok, space_user_list_record(RecordId)};
                <<"space-group-list">> ->
                    {ok, space_group_list_record(RecordId)};
                <<"space-provider-list">> ->
                    {ok, space_provider_list_record(RecordId)};
                <<"space-transfer-list">> ->
                    {ok, space_transfer_list_record(RecordId)};
                <<"space-transfer-time-stat">> ->
                    {ok, space_transfer_time_stat_record(RecordId)};
                <<"space-transfer-link-state">> ->
                    {ok, space_transfer_active_links_record(RecordId)};
                <<"space-user-permission">> ->
                    {ok, space_user_permission_record(RecordId)};
                <<"space-group-permission">> ->
                    {ok, space_group_permission_record(RecordId)}
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(<<"space">>) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
query(<<"space">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
query_record(<<"space">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"space">>, Data) ->
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    Name = proplists:get_value(<<"name">>, Data),
    case Name of
        <<"">> ->
            gui_error:report_warning(
                <<"Cannot create space with empty name.">>);
        _ ->
            case user_logic:create_space(SessionId, UserId, Name) of
                {ok, SpaceId} ->
                    gui_async:push_updated(
                        <<"user">>,
                        user_data_backend:user_record(SessionId, UserId)
                    ),
                    SpaceRecord = space_record(SpaceId, true),
                    {ok, SpaceRecord};
                _ ->
                    gui_error:report_warning(
                        <<"Cannot create new space due to unknown error.">>)
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"space">>, SpaceId, [{<<"name">>, Name}]) ->
    SessionId = gui_session:get_session_id(),
    case Name of
        undefined ->
            ok;
        <<"">> ->
            gui_error:report_warning(
                <<"Cannot set space name to empty string.">>);
        NewName ->
            case space_logic:update_name(SessionId, SpaceId, NewName) of
                ok ->
                    ok;
                {error, {403, <<>>, <<>>}} ->
                    gui_error:report_warning(
                        <<"You do not have privileges to modify this space.">>);
                {error, _} ->
                    gui_error:report_warning(
                        <<"Cannot change space name due to unknown error.">>)
            end
    end;

update_record(<<"space-user-permission">>, AssocId, Data) ->
    SessionId = gui_session:get_session_id(),
    {UserId, SpaceId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_space{
            direct_users = UsersAndPerms
        }}} = space_logic:get(SessionId, SpaceId),
    UserPerms = maps:get(UserId, UsersAndPerms),
    NewUserPerms = lists:foldl(
        fun({PermGui, Flag}, PermsAcc) ->
            Perm = perm_gui_to_db(PermGui),
            case Flag of
                true ->
                    PermsAcc ++ [Perm];
                false ->
                    PermsAcc -- [Perm]
            end
        end, UserPerms, Data),

    Result = space_logic:update_user_privileges(
        SessionId, SpaceId, UserId, lists:usort(NewUserPerms)),
    case Result of
        ok ->
            ok;
        {error, {403, <<>>, <<>>}} ->
            gui_error:report_warning(
                <<"You do not have privileges to modify space privileges.">>);
        {error, _} ->
            gui_error:report_warning(
                <<"Cannot change user privileges due to unknown error.">>)
    end;

update_record(<<"space-group-permission">>, AssocId, Data) ->
    SessionId = gui_session:get_session_id(),
    {GroupId, SpaceId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_space{
            direct_groups = GroupsAndPerms
        }}} = space_logic:get(SessionId, SpaceId),
    GroupPerms = maps:get(GroupId, GroupsAndPerms),
    NewGroupPerms = lists:foldl(
        fun({PermGui, Flag}, PermsAcc) ->
            Perm = perm_gui_to_db(PermGui),
            case Flag of
                true ->
                    PermsAcc ++ [Perm];
                false ->
                    PermsAcc -- [Perm]
            end
        end, GroupPerms, Data),

    Result = space_logic:update_group_privileges(
        SessionId, SpaceId, GroupId, lists:usort(NewGroupPerms)),
    case Result of
        ok ->
            ok;
        {error, {403, <<>>, <<>>}} ->
            gui_error:report_warning(
                <<"You do not have privileges to modify space privileges.">>);
        {error, _} ->
            gui_error:report_warning(
                <<"Cannot change group privileges due to unknown error.">>)
    end;

update_record(_ResourceType, _Id, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"space">>, SpaceId) ->
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    case space_logic:delete(SessionId, SpaceId) of
        ok ->
            gui_async:push_updated(
                <<"user">>,
                user_data_backend:user_record(SessionId, UserId)
            ),
            ok;
        {error, {403, <<>>, <<>>}} ->
            gui_error:report_warning(
                <<"You do not have privileges to modify this space.">>);
        {error, _} ->
            gui_error:report_warning(
                <<"Cannot remove space due to unknown error.">>)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space record based on space id. Automatically
%% checks if the user has view privileges in that space and returns proper data.
%% @end
%%--------------------------------------------------------------------
-spec space_record(SpaceId :: binary()) -> proplists:proplist().
space_record(SpaceId) ->
    SessionId = gui_session:get_session_id(),
    % Check if that user has view privileges in that space
    HasViewPrivileges = space_logic:has_eff_privilege(
        SessionId, SpaceId, gui_session:get_user_id(), ?SPACE_VIEW
    ),
    space_record(SpaceId, HasViewPrivileges).


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space record based on space id. Allows to
%% override HasViewPrivileges.
%% @end
%%--------------------------------------------------------------------
-spec space_record(SpaceId :: binary(), HasViewPrivileges :: boolean()) ->
    proplists:proplist().
space_record(SpaceId, HasViewPrivileges) ->
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    {ok, #document{value = #od_space{
        name = Name,
        providers = Providers
    }}} = space_logic:get_protected_data(SessionId, SpaceId),
    RootDir = case Providers of
        EmptyMap when map_size(EmptyMap) =:= 0 ->
            null;
        _ ->
            fslogic_uuid:uuid_to_guid(
                fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId), SpaceId
            )
    end,

    % Depending on view privileges, show or hide info about members, privileges,
    % providers and transfers.
    {
        RelationWithViewPrivileges, CurrentTransferListId, 
        ScheduledTransferListId, CompletedTransferListId, TransferMinStat, 
        TransferHourStat, TransferDayStat, TransferMonthStat
    } = case HasViewPrivileges of
        true -> {
            SpaceId,
            op_gui_utils:ids_to_association(?CURRENT_TRANSFERS_PREFIX, SpaceId),
            op_gui_utils:ids_to_association(?SCHEDULED_TRANSFERS_PREFIX, SpaceId),
            op_gui_utils:ids_to_association(?COMPLETED_TRANSFERS_PREFIX, SpaceId),
            op_gui_utils:ids_to_association(?MINUTE_STAT_TYPE, SpaceId),
            op_gui_utils:ids_to_association(?HOUR_STAT_TYPE, SpaceId),
            op_gui_utils:ids_to_association(?DAY_STAT_TYPE, SpaceId),
            op_gui_utils:ids_to_association(?MONTH_STAT_TYPE, SpaceId)
        };
        false ->
            {null, null, null, null, null, null, null, null}
    end,
    [
        {<<"id">>, SpaceId},
        {<<"name">>, Name},
        {<<"rootDir">>, RootDir},
        {<<"hasViewPrivilege">>, HasViewPrivileges},
        {<<"userList">>, RelationWithViewPrivileges},
        {<<"groupList">>, RelationWithViewPrivileges},
        {<<"providerList">>, RelationWithViewPrivileges},
        {<<"currentTransferList">>, CurrentTransferListId},
        {<<"scheduledTransferList">>, ScheduledTransferListId},
        {<<"completedTransferList">>, CompletedTransferListId},
        {<<"transferMinuteStat">>, TransferMinStat},
        {<<"transferHourStat">>, TransferHourStat},
        {<<"transferDayStat">>, TransferDayStat},
        {<<"transferMonthStat">>, TransferMonthStat},
        {<<"transferLinkState">>, RelationWithViewPrivileges},
        {<<"user">>, UserId}
    ].


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space-user-list record based on space id.
%% @end
%%--------------------------------------------------------------------
-spec space_user_list_record(SpaceId :: binary()) -> proplists:proplist().
space_user_list_record(SpaceId) ->
    SessionId = gui_session:get_session_id(),
    {ok, #document{value = #od_space{
        direct_users = UsersWithPerms
    }}} = space_logic:get(SessionId, SpaceId),
    UserPermissions = lists:map(
        fun(UsId) ->
            op_gui_utils:ids_to_association(UsId, SpaceId)
        end, maps:keys(UsersWithPerms)),
    [
        {<<"id">>, SpaceId},
        {<<"space">>, SpaceId},
        {<<"permissions">>, UserPermissions}
    ].


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space-group-list record based on space id.
%% @end
%%--------------------------------------------------------------------
-spec space_group_list_record(SpaceId :: binary()) -> proplists:proplist().
space_group_list_record(SpaceId) ->
    SessionId = gui_session:get_session_id(),
    {ok, #document{value = #od_space{
        direct_groups = GroupsWithPerms
    }}} = space_logic:get(SessionId, SpaceId),
    GroupPermissions = lists:map(
        fun(GrId) ->
            op_gui_utils:ids_to_association(GrId, SpaceId)
        end, maps:keys(GroupsWithPerms)),
    [
        {<<"id">>, SpaceId},
        {<<"space">>, SpaceId},
        {<<"permissions">>, GroupPermissions}
    ].


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space-provider-list record based on space id.
%% @end
%%--------------------------------------------------------------------
-spec space_provider_list_record(SpaceId :: binary()) -> proplists:proplist().
space_provider_list_record(SpaceId) ->
    SessionId = gui_session:get_session_id(),
    {ok, Providers} = space_logic:get_provider_ids(SessionId, SpaceId),
    [
        {<<"id">>, SpaceId},
        {<<"list">>, Providers}
    ].


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space-transfer-list record based on space id.
%% @end
%%--------------------------------------------------------------------
-spec space_transfer_list_record(RecordId :: binary()) -> proplists:proplist().
space_transfer_list_record(RecordId) ->
    {Prefix, SpaceId} = op_gui_utils:association_to_ids(RecordId),
    {ok, Transfers} = case Prefix of
        ?CURRENT_TRANSFERS_PREFIX ->
            transfer:list_current_transfers(SpaceId, ?TRANSFERS_LIST_OFFSET,
                ?MAX_TRANSFERS_TO_LIST);
        ?SCHEDULED_TRANSFERS_PREFIX ->
            {ok, Scheduled} = transfer:list_scheduled_transfers(SpaceId, ?TRANSFERS_LIST_OFFSET,
               ?MAX_TRANSFERS_TO_LIST),
            {ok, Current} = transfer:list_current_transfers(SpaceId, ?TRANSFERS_LIST_OFFSET,
               ?MAX_TRANSFERS_TO_LIST),
            {ok, Completed} = transfer:list_past_transfers(SpaceId, ?TRANSFERS_LIST_OFFSET,
               ?MAX_TRANSFERS_TO_LIST),
            {ok, (Scheduled -- Current) -- Completed};
        ?COMPLETED_TRANSFERS_PREFIX ->
            transfer:list_past_transfers(SpaceId, ?TRANSFERS_LIST_OFFSET,
                ?MAX_TRANSFERS_TO_LIST)
    end,
    [
        {<<"id">>, RecordId},
        {<<"list">>, Transfers}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space-user-permission record based on its id.
%% @end
%%--------------------------------------------------------------------
-spec space_user_permission_record(AssocId :: binary()) -> proplists:proplist().
space_user_permission_record(AssocId) ->
    SessionId = gui_session:get_session_id(),
    {UserId, SpaceId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_space{
            direct_users = UsersAndPerms
        }}} = space_logic:get(SessionId, SpaceId),
    UserPerms = maps:get(UserId, UsersAndPerms),
    PermsMapped = perms_db_to_gui(UserPerms),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"space">>, SpaceId},
        {<<"systemUserId">>, UserId}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space-group-permission record based on its id.
%% @end
%%--------------------------------------------------------------------
-spec space_group_permission_record(AssocId :: binary()) ->
    proplists:proplist().
space_group_permission_record(AssocId) ->
    SessionId = gui_session:get_session_id(),
    {GroupId, SpaceId} = op_gui_utils:association_to_ids(AssocId),
    {ok, #document{
        value = #od_space{
            direct_groups = GroupsAndPerms
        }}} = space_logic:get(SessionId, SpaceId),
    GroupPerms = maps:get(GroupId, GroupsAndPerms),
    PermsMapped = perms_db_to_gui(GroupPerms),
    PermsMapped ++ [
        {<<"id">>, AssocId},
        {<<"space">>, SpaceId},
        {<<"systemGroupId">>, GroupId}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space-transfer-time-stat record based on stat id
%% (combined space id and prefix defining time span of histograms).
%% @end
%%--------------------------------------------------------------------
-spec space_transfer_time_stat_record(StatId :: binary()) ->
    proplists:proplist().
space_transfer_time_stat_record(StatId) ->
    {TypePrefix, SpaceId} = op_gui_utils:association_to_ids(StatId),
    #space_transfer_stats_cache{
        timestamp = Timestamp,
        stats_in = StatsIn,
        stats_out = StatsOut
    } = space_transfer_stats_cache:get(SpaceId, TypePrefix),

    [
        {<<"id">>, StatId},
        {<<"timestamp">>, Timestamp},
        {<<"type">>, TypePrefix},
        {<<"statsIn">>, maps:to_list(StatsIn)},
        {<<"statsOut">>, maps:to_list(StatsOut)}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space-transfer-provider-map record based on space id
%% @end
%%--------------------------------------------------------------------
-spec space_transfer_active_links_record(StatId :: binary()) ->
    proplists:proplist().
space_transfer_active_links_record(SpaceId) ->
    {ok, ActiveLinks} = space_transfer_stats_cache:get_active_links(SpaceId),

    [
        {<<"id">>, SpaceId},
        {<<"activeLinks">>, maps:to_list(ActiveLinks)}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a list of space permissions from internal form to client-compliant form.
%% @end
%%--------------------------------------------------------------------
-spec perms_db_to_gui(atom()) -> proplists:proplist().
perms_db_to_gui(Perms) ->
    lists:foldl(
        fun(Perm, Acc) ->
            case perm_db_to_gui(Perm) of
                undefined ->
                    Acc;
                PermBin ->
                    HasPerm = lists:member(Perm, Perms),
                    [{PermBin, HasPerm} | Acc]
            end
        end, [], privileges:space_privileges()).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a space permission from internal form to client-compliant form.
%% @end
%%--------------------------------------------------------------------
-spec perm_db_to_gui(atom()) -> binary() | undefined.
perm_db_to_gui(?SPACE_VIEW) -> <<"permViewSpace">>;
perm_db_to_gui(?SPACE_UPDATE) -> <<"permModifySpace">>;
perm_db_to_gui(?SPACE_DELETE) -> <<"permRemoveSpace">>;
perm_db_to_gui(?SPACE_SET_PRIVILEGES) -> <<"permSetPrivileges">>;
perm_db_to_gui(?SPACE_INVITE_USER) -> <<"permInviteUser">>;
perm_db_to_gui(?SPACE_REMOVE_USER) -> <<"permRemoveUser">>;
perm_db_to_gui(?SPACE_INVITE_GROUP) -> <<"permInviteGroup">>;
perm_db_to_gui(?SPACE_REMOVE_GROUP) -> <<"permRemoveGroup">>;
perm_db_to_gui(?SPACE_INVITE_PROVIDER) -> <<"permInviteProvider">>;
perm_db_to_gui(?SPACE_REMOVE_PROVIDER) -> <<"permRemoveProvider">>;
perm_db_to_gui(?SPACE_MANAGE_SHARES) -> <<"permManageShares">>;
perm_db_to_gui(?SPACE_WRITE_DATA) -> <<"permWriteFiles">>;
perm_db_to_gui(_) -> undefined.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a space permission from client-compliant form to internal form.
%% @end
%%--------------------------------------------------------------------
-spec perm_gui_to_db(binary()) -> atom().
perm_gui_to_db(<<"permViewSpace">>) -> ?SPACE_VIEW;
perm_gui_to_db(<<"permModifySpace">>) -> ?SPACE_UPDATE;
perm_gui_to_db(<<"permRemoveSpace">>) -> ?SPACE_DELETE;
perm_gui_to_db(<<"permSetPrivileges">>) -> ?SPACE_SET_PRIVILEGES;
perm_gui_to_db(<<"permInviteUser">>) -> ?SPACE_INVITE_USER;
perm_gui_to_db(<<"permRemoveUser">>) -> ?SPACE_REMOVE_USER;
perm_gui_to_db(<<"permInviteGroup">>) -> ?SPACE_INVITE_GROUP;
perm_gui_to_db(<<"permRemoveGroup">>) -> ?SPACE_REMOVE_GROUP;
perm_gui_to_db(<<"permInviteProvider">>) -> ?SPACE_INVITE_PROVIDER;
perm_gui_to_db(<<"permRemoveProvider">>) -> ?SPACE_REMOVE_PROVIDER;
perm_gui_to_db(<<"permManageShares">>) -> ?SPACE_MANAGE_SHARES;
perm_gui_to_db(<<"permWriteFiles">>) -> ?SPACE_WRITE_DATA.
