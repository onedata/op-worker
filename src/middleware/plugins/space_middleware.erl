%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to space aspects such as:
%%% - space,
%%% - views,
%%% - transfers.
%%% @end
%%%-------------------------------------------------------------------
-module(space_middleware).
-author("Bartosz Walkowicz").

-behaviour(middleware_plugin).

-include("middleware/middleware.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).

-define(MAX_LIST_LIMIT, 1000).
-define(DEFAULT_VIEW_LIST_LIMIT, 100).
-define(DEFAULT_TRANSFER_LIST_LIMIT, 100).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(middleware:operation(), gri:aspect(),
    middleware:scope()) -> boolean().
operation_supported(create, {view, _}, private) -> true;
operation_supported(create, {view_reduce_function, _}, private) -> true;

operation_supported(get, list, private) -> true;
operation_supported(get, instance, private) -> true;
operation_supported(get, views, private) -> true;
operation_supported(get, {view, _}, private) -> true;
operation_supported(get, {query_view, _}, private) -> true;
operation_supported(get, eff_users, private) -> true;
operation_supported(get, eff_groups, private) -> true;
operation_supported(get, shares, private) -> true;
operation_supported(get, providers, private) -> true;
operation_supported(get, transfers, private) -> true;
operation_supported(get, transfers_active_channels, private) -> true;
operation_supported(get, {transfers_throughput_charts, _}, private) -> true;

operation_supported(update, {view, _}, private) -> true;

operation_supported(delete, {view, _}, private) -> true;
operation_supported(delete, {view_reduce_function, _}, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = {view, _}}}) -> #{
    required => #{
        <<"mapFunction">> => {binary, non_empty}
    },
    optional => #{
        <<"spatial">> => {boolean, any},
        <<"update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"replica_update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"providers[]">> => {any,
            fun
                (ProviderId) when is_binary(ProviderId) ->
                    {true, [ProviderId]};
                (Providers) when is_list(Providers) ->
                    lists:all(fun(ProvId) -> is_binary(ProvId) end, Providers);
                (_) ->
                    false
            end
        }
    }
};

data_spec(#op_req{operation = create, gri = #gri{aspect = {view_reduce_function, _}}}) -> #{
    required => #{<<"reduceFunction">> => {binary, non_empty}}
};

data_spec(#op_req{operation = get, gri = #gri{aspect = list}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = views}}) -> #{
    optional => #{
        <<"limit">> => {integer, {between, 1, ?MAX_LIST_LIMIT}},
        <<"page_token">> => {binary, non_empty}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = {view, _}}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = {query_view, _}}}) -> #{
    optional => #{
        <<"descending">> => {boolean, any},
        <<"limit">> => {integer, {not_lower_than, 1}},
        <<"skip">> => {integer, {not_lower_than, 1}},
        <<"stale">> => {binary, [<<"ok">>, <<"update_after">>, <<"false">>]},
        <<"spatial">> => {boolean, any},
        <<"inclusive_end">> => {boolean, any},
        <<"start_range">> => {binary, any},
        <<"end_range">> => {binary, any},
        <<"startkey">> => {binary, any},
        <<"endkey">> => {binary, any},
        <<"key">> => {binary, any},
        <<"keys">> => {binary, any},
        <<"bbox">> => {binary, any}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = eff_users}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = eff_groups}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = shares}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = providers}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = transfers}}) -> #{
    optional => #{
        <<"state">> => {binary, [
            ?WAITING_TRANSFERS_STATE,
            ?ONGOING_TRANSFERS_STATE,
            ?ENDED_TRANSFERS_STATE
        ]},
        <<"offset">> => {integer, any},
        <<"limit">> => {integer, {between, 1, ?MAX_LIST_LIMIT}},
        <<"page_token">> => {page_token, any}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = transfers_active_channels}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = {transfers_throughput_charts, _}}}) -> #{
    required => #{
        <<"transfer_type">> => {binary, [
            ?JOB_TRANSFERS_TYPE,
            ?ON_THE_FLY_TRANSFERS_TYPE,
            ?ALL_TRANSFERS_TYPE
        ]},
        <<"charts_type">> => {binary, [
            ?MINUTE_PERIOD,
            ?HOUR_PERIOD,
            ?DAY_PERIOD,
            ?MONTH_PERIOD
        ]}
    }
};

data_spec(#op_req{operation = update, gri = #gri{aspect = {view, _}}}) -> #{
    optional => #{
        <<"mapFunction">> => {binary, any},
        <<"spatial">> => {boolean, any},
        <<"update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"replica_update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"providers[]">> => {any,
            fun
                (ProviderId) when is_binary(ProviderId) ->
                    {true, [ProviderId]};
                (Providers) when is_list(Providers) ->
                    lists:all(fun(ProvId) -> is_binary(ProvId) end, Providers);
                (_) ->
                    false
            end
        }
    }
};

data_spec(#op_req{operation = delete, gri = #gri{aspect = {view, _}}}) ->
    undefined;

data_spec(#op_req{operation = delete, gri = #gri{aspect = {view_reduce_function, _}}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(_) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?GUEST}, _) ->
    false;

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_VIEWS);

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {view_reduce_function, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_VIEWS);

authorize(#op_req{operation = get, gri = #gri{aspect = list}}, _) ->
    % User is always authorized to list his spaces
    true;

authorize(#op_req{operation = get, auth = Auth, gri = #gri{
    id = SpaceId,
    aspect = instance
}}, _) ->
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = views
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_VIEWS);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_VIEWS);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {query_view, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_QUERY_VIEWS);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = As
}}, _) when
    As =:= eff_users;
    As =:= eff_groups;
    As =:= shares;
    As =:= providers
->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = As
}}, _) when
    As =:= transfers;
    As =:= transfers_active_channels
->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_TRANSFERS);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {transfers_throughput_charts, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_TRANSFERS);

authorize(#op_req{operation = update, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_VIEWS);

authorize(#op_req{operation = delete, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_VIEWS);

authorize(#op_req{operation = delete, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {view_reduce_function, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_VIEWS).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, data = Data, gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId),

    % In case of undefined `providers[]` local provider is chosen instead
    case maps:get(<<"providers[]">>, Data, undefined) of
        undefined ->
            ok;
        Providers ->
            lists:foreach(fun(ProviderId) ->
                middleware_utils:assert_space_supported_by(SpaceId, ProviderId)
            end, Providers)
    end;

validate(#op_req{operation = create, gri = #gri{
    id = SpaceId,
    aspect = {view_reduce_function, _}
}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{aspect = list}}, _) ->
    % User spaces are listed by fetching information from zone,
    % whether they are supported locally is irrelevant.
    ok;

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = instance}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = views}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = {view, _}}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = {query_view, _}}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = As}}, _) when
    As =:= eff_users;
    As =:= eff_groups;
    As =:= shares;
    As =:= providers
->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = As}}, _) when
    As =:= transfers;
    As =:= transfers_active_channels
->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{
    id = SpaceId,
    aspect = {transfers_throughput_charts, _}
}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = update, gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}} = Req, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId),

    % In case of undefined `providers[]` local provider is chosen instead
    case maps:get(<<"providers[]">>, Req#op_req.data, undefined) of
        undefined ->
            ok;
        Providers ->
            lists:foreach(fun(ProviderId) ->
                middleware_utils:assert_space_supported_by(SpaceId, ProviderId)
        end, Providers)
    end;

validate(#op_req{operation = delete, gri = #gri{id = SpaceId, aspect = {view, _}}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, gri = #gri{
    id = SpaceId,
    aspect = {view_reduce_function, _}
}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = {view, ViewName}}}) ->
    index:save(
        SpaceId, ViewName,
        maps:get(<<"mapFunction">>, Data), undefined,
        prepare_view_options(Data),
        maps:get(<<"spatial">>, Data, false),
        maps:get(<<"providers[]">>, Data, [oneprovider:get_id()])
    );

create(#op_req{gri = #gri{id = SpaceId, aspect = {view_reduce_function, ViewName}}} = Req) ->
    ReduceFunction = maps:get(<<"reduceFunction">>, Req#op_req.data),
    case index:update_reduce_function(SpaceId, ViewName, ReduceFunction) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        Result ->
            Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = ?USER(UserId, SessionId), gri = #gri{aspect = list}}, _) ->
    case user_logic:get_eff_spaces(SessionId, UserId) of
        {ok, EffSpaces} ->
            {ok ,lists:map(fun(SpaceId) ->
                {ok, SpaceName} = space_logic:get_name(SessionId, SpaceId),
                #{<<"spaceId">> => SpaceId, <<"name">> => SpaceName}
            end, EffSpaces)};
        {error, _} = Error ->
            Error
    end;

get(#op_req{auth = Auth, gri = #gri{id = SpaceId, aspect = instance}}, _) ->
    case space_logic:get(Auth#auth.session_id, SpaceId) of
        {ok, #document{value = Space}} ->
            {ok, Space};
        {error, _} = Error ->
            Error
    end;

get(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = views}}, _) ->
    PageToken = maps:get(<<"page_token">>, Data, <<"null">>),
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_VIEW_LIST_LIMIT),

    {StartId, Offset} = case PageToken of
        <<"null">> ->
            {undefined, 0};
        _ ->
            % Start after the page token (link key from last listing) if it is given
            {PageToken, 1}
    end,

    case index:list(SpaceId, StartId, Offset, Limit) of
        {ok, Views} ->
            NextPageToken = case length(Views) of
                Limit -> #{<<"nextPageToken">> => lists:last(Views)};
                _ -> #{}
            end,
            {ok, maps:merge(#{
                <<"indexes">> => Views,   % TODO VFS-5608
                <<"views">> => Views
            }, NextPageToken)};
        {error, _} = Error ->
            Error
    end;

get(#op_req{gri = #gri{id = SpaceId, aspect = {view, ViewName}}}, _) ->
    case index:get_json(SpaceId, ViewName) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        Result ->
            Result
    end;

get(#op_req{gri = #gri{id = SpaceId, aspect = {query_view, ViewName}}} = Req, _) ->
    Options = view_utils:sanitize_query_options(Req#op_req.data),
    case index:query(SpaceId, ViewName, Options) of
        {ok, #{<<"rows">> := Rows}} ->
            {ok, Rows};
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        {error, _} = Error ->
            Error
    end;

get(#op_req{auth = Auth, gri = #gri{id = SpaceId, aspect = eff_users}}, _) ->
    space_logic:get_eff_users(Auth#auth.session_id, SpaceId);

get(#op_req{auth = Auth, gri = #gri{id = SpaceId, aspect = eff_groups}}, _) ->
    space_logic:get_eff_groups(Auth#auth.session_id, SpaceId);

get(#op_req{auth = Auth, gri = #gri{id = SpaceId, aspect = shares}}, _) ->
    space_logic:get_shares(Auth#auth.session_id, SpaceId);

get(#op_req{auth = Auth, gri = #gri{id = SpaceId, aspect = providers}}, _) ->
    space_logic:get_provider_ids(Auth#auth.session_id, SpaceId);

get(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = transfers}}, _) ->
    StartId = maps:get(<<"page_token">>, Data, undefined),
    TransferState = maps:get(<<"state">>, Data, <<"ongoing">>),
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_TRANSFER_LIST_LIMIT),
    Offset = case {StartId, maps:get(<<"offset">>, Data, undefined)} of
        {undefined, undefined} ->
            % Start from the beginning if no page_token and offset given
            0;
        {_, undefined} ->
            % Start after given page token (link key from last listing)
            % if no offset given
            1;
        {_, Int} when is_integer(Int) ->
            Int
    end,

    {ok, Transfers} = case TransferState of
        ?WAITING_TRANSFERS_STATE ->
            transfer:list_waiting_transfers(SpaceId, StartId, Offset, Limit);
        ?ONGOING_TRANSFERS_STATE ->
            transfer:list_ongoing_transfers(SpaceId, StartId, Offset, Limit);
        ?ENDED_TRANSFERS_STATE ->
            transfer:list_ended_transfers(SpaceId, StartId, Offset, Limit)
    end,

    Result = case length(Transfers) of
        Limit ->
            % The list returned by the link tree can contain transfers for which
            % the doc was not synchronized yet. In such case it is not possible
            % to get it's link key. Find the last transfer id for which the doc
            % is synchronized and drop the remaining ids from the result
            {SynchronizedTransfers, NextPageToken} = lists:foldr(
                fun
                    (Tid, {TransfersAcc, undefined}) ->
                        case transfer:get_link_key_by_state(Tid, TransferState) of
                            {ok, LinkName} ->
                                {[Tid | TransfersAcc], LinkName};
                            {error, not_found} ->
                                {TransfersAcc, undefined}
                        end;
                    (Tid, {TransfersAcc, LinkName}) ->
                        {[Tid | TransfersAcc], LinkName}
                end,
                {[], undefined},
                Transfers
            ),
            #{
                <<"transfers">> => SynchronizedTransfers,
                <<"nextPageToken">> => NextPageToken
            };
        _ ->
            #{<<"transfers">> => Transfers}
    end,
    {ok, Result};

get(#op_req{gri = #gri{id = SpaceId, aspect = transfers_active_channels}}, _) ->
    {ok, ActiveChannels} = space_transfer_stats_cache:get_active_channels(SpaceId),
    {ok, value, #{<<"channelDestinations">> => ActiveChannels}};

get(#op_req{data = Data, gri = #gri{
    id = SpaceId,
    aspect = {transfers_throughput_charts, ProviderId}
}}, _) ->
    TargetProvider = case ProviderId of
        <<"undefined">> -> undefined;
        _ -> ProviderId
    end,
    TransferType = maps:get(<<"transfer_type">>, Data),
    ChartsType = maps:get(<<"charts_type">>, Data),
    TimeWindow = transfer_histograms:period_to_time_window(ChartsType),

    % Some functions from transfer_histograms module require specifying
    % start time parameter. But there is no conception of start time for
    % space_transfer_stats doc. So a long past value like 0 (year 1970) is used.
    StartTime = 0,

    #space_transfer_stats_cache{
        timestamp = LastUpdate,
        stats_in = StatsIn,
        stats_out = StatsOut
    } = space_transfer_stats_cache:get(
        TargetProvider, SpaceId, TransferType, ChartsType
    ),

    InputThroughputCharts = transfer_histograms:to_speed_charts(
        StatsIn, StartTime, LastUpdate, TimeWindow
    ),
    OutputThroughputCharts = transfer_histograms:to_speed_charts(
        StatsOut, StartTime, LastUpdate, TimeWindow
    ),

    {ok, value, #{
        <<"timestamp">> => LastUpdate,
        <<"inputCharts">> => InputThroughputCharts,
        <<"outputCharts">> => OutputThroughputCharts
    }}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = {view, ViewName}}}) ->
    MapFunctionRaw = maps:get(<<"mapFunction">>, Data),
    MapFun = utils:ensure_defined(MapFunctionRaw, <<>>, undefined),

    case index:update(
        SpaceId, ViewName,
        MapFun,
        prepare_view_options(Data),
        maps:get(<<"spatial">>, Data, undefined),
        maps:get(<<"providers[]">>, Data, undefined)
    ) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        Result ->
            Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{gri = #gri{id = SpaceId, aspect = {view, ViewName}}}) ->
    case index:delete(SpaceId, ViewName) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        Result ->
            Result
    end;

delete(#op_req{gri = #gri{id = SpaceId, aspect = {view_reduce_function, ViewName}}}) ->
    case index:update_reduce_function(SpaceId, ViewName, undefined) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        Result ->
            Result
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec prepare_view_options(map()) -> list().
prepare_view_options(Data) ->
    Options = case maps:get(<<"replica_update_min_changes">>, Data, undefined) of
        undefined ->
            [];
        ReplicaUpdateMinChanges ->
            [{replica_update_min_changes, ReplicaUpdateMinChanges}]
    end,

    case maps:get(<<"update_min_changes">>, Data, undefined) of
        undefined ->
            Options;
        UpdateMinChanges ->
            [{update_min_changes, UpdateMinChanges} | Options]
    end.
