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
-include_lib("ctool/include/errors.hrl").

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
    {ok, proplists:proplist()} | op_gui_error:error_result().
find_record(<<"space">>, SpaceId) ->
    SessionId = op_gui_session:get_session_id(),
    % Check if the user belongs to this space -
    % he should be able to get protected space data.
    case space_logic:get_protected_data(SessionId, SpaceId) of
        {ok, _} ->
            {ok, space_record(SpaceId)};
        _ ->
            op_gui_error:unauthorized()
    end;

find_record(<<"space-provider-list">>, SpaceId) ->
    SessionId = op_gui_session:get_session_id(),
    UserId = op_gui_session:get_user_id(),
    case user_logic:has_eff_space(SessionId, UserId, SpaceId) of
        false -> op_gui_error:unauthorized();
        true -> {ok, space_provider_list_record(SpaceId)}
    end;

find_record(RecordType, RecordId) ->
    {SpaceId, AdditionalPrivs} = case RecordType of
        <<"space-user-list">> ->
            {RecordId, []};
        <<"space-group-list">> ->
            {RecordId, []};
        <<"space-transfer-link-state">> ->
            {RecordId, [?SPACE_VIEW_TRANSFERS]};
        <<"space-on-the-fly-transfer-list">> ->
            {RecordId, [?SPACE_VIEW_TRANSFERS]};
        <<"space-transfer-stat">> ->
            {_, _, Id} = op_gui_utils:association_to_ids(RecordId),
            {Id, [?SPACE_VIEW_TRANSFERS]};
        <<"space-transfer-time-stat">> ->
            {_, _, _, Id} = op_gui_utils:association_to_ids(RecordId),
            {Id, [?SPACE_VIEW_TRANSFERS]};
        <<"space-transfer-list">> ->
            {_, Id} = op_gui_utils:association_to_ids(RecordId),
            {Id, [?SPACE_VIEW_TRANSFERS]}
    end,
    UserId = op_gui_session:get_user_id(),
    % Make sure that user is allowed to view requested privileges - he must have
    % view privileges in this space.
    case space_logic:has_eff_privileges(SpaceId, UserId, [?SPACE_VIEW | AdditionalPrivs]) of
        false ->
            op_gui_error:unauthorized();
        true ->
            case RecordType of
                <<"space-user-list">> ->
                    {ok, space_user_list_record(RecordId)};
                <<"space-group-list">> ->
                    {ok, space_group_list_record(RecordId)};
                <<"space-on-the-fly-transfer-list">> ->
                    {ok, space_on_the_fly_transfer_list_record(RecordId)};
                <<"space-transfer-stat">> ->
                    {ok, space_transfer_stat_record(RecordId)};
                <<"space-transfer-time-stat">> ->
                    {ok, space_transfer_time_stat_record(RecordId)};
                <<"space-transfer-link-state">> ->
                    {ok, space_transfer_active_links_record(RecordId)}
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
find_all(<<"space">>) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
query(<<"space">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
query_record(<<"space">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
create_record(<<"space">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | op_gui_error:error_result().
update_record(_ResourceType, _Id, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | op_gui_error:error_result().
delete_record(<<"space">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


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
    % Check if that user has view privileges in that space
    HasViewPrivileges = space_logic:has_eff_privilege(
        SpaceId, op_gui_session:get_user_id(), ?SPACE_VIEW
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
    SessionId = op_gui_session:get_session_id(),
    UserId = op_gui_session:get_user_id(),
    {ok, #document{value = #od_space{
        name = Name,
        providers = Providers
    }}} = space_logic:get_protected_data(SessionId, SpaceId),
    RootDir = case Providers of
        EmptyMap when map_size(EmptyMap) =:= 0 ->
            null;
        _ ->
            file_id:pack_guid(
                fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId), SpaceId
            )
    end,
    % Depending on view privileges, show or hide info about members, privileges,
    % providers and transfers.
    {
        RelationWithViewPrivileges,
        TransferOnTheFlyStatId,
        TransferJobStatId,
        TransferAllStatId,
        TransferProviderStat
    } = case HasViewPrivileges of
        true ->
            ProvidersStats = lists:map(fun(ProviderId) ->
                ProviderStat = [
                    {<<"onTheFlyStat">>, op_gui_utils:ids_to_association(
                        ?ON_THE_FLY_TRANSFERS_TYPE, ProviderId, SpaceId)},
                    {<<"jobStat">>, op_gui_utils:ids_to_association(
                        ?JOB_TRANSFERS_TYPE, ProviderId, SpaceId)},
                    {<<"allStat">>, op_gui_utils:ids_to_association(
                        ?ALL_TRANSFERS_TYPE, ProviderId, SpaceId)}
                ],
                {ProviderId, ProviderStat}
            end, maps:keys(Providers)),

            {
                SpaceId,
                op_gui_utils:ids_to_association(
                    ?ON_THE_FLY_TRANSFERS_TYPE, <<"undefined">>, SpaceId),
                op_gui_utils:ids_to_association(
                    ?JOB_TRANSFERS_TYPE, <<"undefined">>, SpaceId),
                op_gui_utils:ids_to_association(
                    ?ALL_TRANSFERS_TYPE, <<"undefined">>, SpaceId),
                ProvidersStats
            };
        false -> {
            null, null, null, null, null
        }
    end,
    [
        {<<"id">>, SpaceId},
        {<<"name">>, Name},
        {<<"rootDir">>, RootDir},
        {<<"hasViewPrivilege">>, HasViewPrivileges},
        {<<"userList">>, RelationWithViewPrivileges},
        {<<"groupList">>, RelationWithViewPrivileges},
        {<<"providerList">>, SpaceId},
        {<<"onTheFlyTransferList">>, RelationWithViewPrivileges},
        {<<"transferOnTheFlyStat">>, TransferOnTheFlyStatId},
        {<<"transferJobStat">>, TransferJobStatId},
        {<<"transferAllStat">>, TransferAllStatId},
        {<<"transferProviderStat">>, TransferProviderStat},
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
    SessionId = op_gui_session:get_session_id(),
    {ok, EffUsers} = space_logic:get_eff_users(SessionId, SpaceId),
    [
        {<<"id">>, SpaceId},
        {<<"space">>, SpaceId},
        {<<"systemUsers">>, maps:keys(EffUsers)}
    ].


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space-group-list record based on space id.
%% @end
%%--------------------------------------------------------------------
-spec space_group_list_record(SpaceId :: binary()) -> proplists:proplist().
space_group_list_record(SpaceId) ->
    SessionId = op_gui_session:get_session_id(),
    {ok, EffGroups} = space_logic:get_eff_groups(SessionId, SpaceId),
    [
        {<<"id">>, SpaceId},
        {<<"space">>, SpaceId},
        {<<"systemGroups">>, maps:keys(EffGroups)}
    ].


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space-provider-list record based on space id.
%% @end
%%--------------------------------------------------------------------
-spec space_provider_list_record(SpaceId :: binary()) -> proplists:proplist().
space_provider_list_record(SpaceId) ->
    SessionId = op_gui_session:get_session_id(),
    {ok, Providers} = space_logic:get_provider_ids(SessionId, SpaceId),
    [
        {<<"id">>, SpaceId},
        {<<"list">>, Providers}
    ].


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space-on-the-fly-transfer-list record
%% based on space id.
%% @end
%%--------------------------------------------------------------------
-spec space_on_the_fly_transfer_list_record(SpaceId :: od_space:id()) ->
    proplists:proplist().
space_on_the_fly_transfer_list_record(SpaceId) ->
    SessionId = op_gui_session:get_session_id(),
    {ok, Providers} = space_logic:get_provider_ids(SessionId, SpaceId),
    Ids = lists:map(fun(ProviderId) ->
        op_gui_utils:ids_to_association(ProviderId, SpaceId)
    end, Providers),

    [
        {<<"id">>, SpaceId},
        {<<"list">>, Ids}
    ].


%%--------------------------------------------------------------------
%% @doc
%% Returns a client-compliant space-transfer-stat record based on record id
%% (combined transfer category and space id).
%% @end
%%--------------------------------------------------------------------
-spec space_transfer_stat_record(RecordId :: binary()) -> proplists:proplist().
space_transfer_stat_record(RecordId) ->
    {TransferType, TargetProvider, SpaceId} =
        op_gui_utils:association_to_ids(RecordId),

    [
        {<<"id">>, RecordId},
        {<<"minuteStat">>, op_gui_utils:ids_to_association(
            TransferType, ?MINUTE_PERIOD, TargetProvider, SpaceId)},
        {<<"hourStat">>, op_gui_utils:ids_to_association(
            TransferType, ?HOUR_PERIOD, TargetProvider, SpaceId)},
        {<<"dayStat">>, op_gui_utils:ids_to_association(
            TransferType, ?DAY_PERIOD, TargetProvider, SpaceId)},
        {<<"monthStat">>, op_gui_utils:ids_to_association(
            TransferType, ?MONTH_PERIOD, TargetProvider, SpaceId)}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space-transfer-time-stat record based on stat id
%% (combined space id, target provider id, transfer type and prefix
%% defining time span of histograms).
%% @end
%%--------------------------------------------------------------------
-spec space_transfer_time_stat_record(StatId :: binary()) ->
    proplists:proplist().
space_transfer_time_stat_record(StatId) ->
    % Some functions from transfer_histograms module require specifying
    % start time parameter. But there is no conception of start time for
    % space_transfer_stats doc. So a long past value like 0 (year 1970) is used.
    StartTime = 0,
    {TransferType, StatsType, Provider, SpaceId} =
        op_gui_utils:association_to_ids(StatId),
    TargetProvider = case Provider of
        <<"undefined">> -> undefined;
        _ -> Provider
    end,
    TimeWindow = transfer_histograms:period_to_time_window(StatsType),

    #space_transfer_stats_cache{
        timestamp = LastUpdate,
        stats_in = StatsIn,
        stats_out = StatsOut
    } = space_transfer_stats_cache:get(
        TargetProvider, SpaceId, TransferType, StatsType),

    SpeedStatsIn = transfer_histograms:to_speed_charts(
        StatsIn, StartTime, LastUpdate, TimeWindow
    ),
    SpeedStatsOut = transfer_histograms:to_speed_charts(
        StatsOut, StartTime, LastUpdate, TimeWindow
    ),

    [
        {<<"id">>, StatId},
        {<<"timestamp">>, LastUpdate},
        {<<"type">>, StatsType},
        {<<"statsIn">>, maps:to_list(SpeedStatsIn)},
        {<<"statsOut">>, maps:to_list(SpeedStatsOut)}
    ].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a client-compliant space-transfer-provider-map record
%% based on space id
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
