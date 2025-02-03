%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to space transfers aspects.
%%% @end
%%%-------------------------------------------------------------------
-module(space_transfers_middleware_handler).
-author("Bartosz Walkowicz").

-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


-define(MAX_LIST_LIMIT, 1000).
-define(DEFAULT_LIST_LIMIT, 1000).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
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
}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) -> {ok, middleware:versioned_entity()}.
fetch_entity(_) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?GUEST}, _) ->
    false;

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
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_TRANSFERS).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = As}}, _) when
    As =:= transfers;
    As =:= transfers_active_channels
->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{
    id = SpaceId,
    aspect = {transfers_throughput_charts, _}
}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = transfers}}, _) ->
    StartId = maps:get(<<"page_token">>, Data, undefined),
    TransferState = maps:get(<<"state">>, Data, <<"ongoing">>),
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_LIST_LIMIT),
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
            %% TODO VFS-7806 add isLast field
            #{
                <<"transfers">> => SynchronizedTransfers,
                <<"nextPageToken">> => utils:undefined_to_null(NextPageToken)
            };
        _ ->
            %% TODO VFS-7806 add isLast field
            #{
                <<"transfers">> => Transfers,
                <<"nextPageToken">> => null
            }
    end,
    {ok, value, Result};

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
        last_update = LastUpdate,
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
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.
