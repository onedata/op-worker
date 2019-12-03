%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to transfer aspects such as:
%%% - viewing,
%%% - cancelling,
%%% - rerunning.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_middleware).
-author("Bartosz Walkowicz").

-behaviour(middleware_plugin).

-include("middleware/middleware.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/fslogic/fslogic_common.hrl").
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
operation_supported(create, rerun, private) -> true;

operation_supported(get, instance, private) -> true;
operation_supported(get, progress, private) -> true;
operation_supported(get, throughput_charts, private) -> true;

operation_supported(delete, cancel, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = rerun}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = As}}) when
    As =:= instance;
    As =:= progress
->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = throughput_charts}}) -> #{
    required => #{<<"chartsType">> => {binary, [
        ?MINUTE_PERIOD,
        ?HOUR_PERIOD,
        ?DAY_PERIOD,
        ?MONTH_PERIOD
    ]}}
};

data_spec(#op_req{operation = delete, gri = #gri{aspect = cancel}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(#op_req{gri = #gri{id = TransferId}}) ->
    case transfer:get(TransferId) of
        {ok, #document{value = Transfer, revs = [DbRev | _]}} ->
            % Transfer doc is synchronized only with providers supporting space
            % so if it was fetched then space must be supported locally
            {Revision, _Hash} = datastore_utils:parse_rev(DbRev),
            {ok, {Transfer, Revision}};
        _ ->
            ?ERROR_NOT_FOUND
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?NOBODY}, _) ->
    false;

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{
    aspect = rerun
}}, #transfer{space_id = SpaceId} = Transfer) ->

    ViewPrivileges = case Transfer#transfer.index_name of
        undefined -> [];
        _ -> [?SPACE_QUERY_VIEWS]
    end,
    TransferPrivileges = case transfer:type(Transfer) of
        replication -> [?SPACE_SCHEDULE_REPLICATION];
        eviction -> [?SPACE_SCHEDULE_EVICTION];
        migration -> [?SPACE_SCHEDULE_REPLICATION, ?SPACE_SCHEDULE_EVICTION]
    end,
    space_logic:has_eff_privileges(SpaceId, UserId, ViewPrivileges ++ TransferPrivileges);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    aspect = As
}}, #transfer{space_id = SpaceId}) when
    As =:= instance;
    As =:= progress;
    As =:= throughput_charts
->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_TRANSFERS);

authorize(#op_req{operation = delete, auth = ?USER(UserId), gri = #gri{
    aspect = cancel
}} = Req, #transfer{space_id = SpaceId} = Transfer) ->
    case Transfer#transfer.user_id of
        UserId ->
            % User doesn't need cancel privileges to cancel his transfer but
            % must still be member of space.
            middleware_utils:is_eff_space_member(Req#op_req.auth, SpaceId);
        _ ->
            case transfer:type(Transfer) of
                replication ->
                    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_CANCEL_REPLICATION);
                eviction ->
                    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_CANCEL_EVICTION);
                migration ->
                    space_logic:has_eff_privileges(
                        SpaceId, UserId, [?SPACE_CANCEL_REPLICATION, ?SPACE_CANCEL_EVICTION]
                    )
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback validate/2.
%%
%% Does not check if space is locally supported because if it wasn't
%% it would not be possible to fetch transfer doc (it is synchronized
%% only between providers supporting given space).
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{aspect = rerun}}, _) ->
    ok;

validate(#op_req{operation = get, gri = #gri{aspect = As}}, _) when
    As =:= instance;
    As =:= progress;
    As =:= throughput_charts
->
    ok;

validate(#op_req{operation = delete, gri = #gri{aspect = cancel}}, _) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = ?USER(UserId), gri = #gri{id = TransferId, aspect = rerun}}) ->
    case transfer:rerun_ended(UserId, TransferId) of
        {ok, NewTransferId} ->
            {ok, value, NewTransferId};
        {error, not_ended} ->
            ?ERROR_TRANSFER_NOT_ENDED;
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{gri = #gri{aspect = instance}}, Transfer) ->
    {ok, Transfer};
get(#op_req{gri = #gri{aspect = progress}}, #transfer{
    bytes_replicated = BytesReplicated,
    files_replicated = FilesReplicated,
    files_evicted = FilesEvicted
} = Transfer) ->
    {ok, #{
        <<"status">> => get_status(Transfer),
        <<"timestamp">> => get_last_update(Transfer),
        <<"replicatedBytes">> => BytesReplicated,
        <<"replicatedFiles">> => FilesReplicated,
        <<"evictedFiles">> => FilesEvicted
    }};
get(#op_req{data = Data, gri = #gri{aspect = throughput_charts}}, Transfer) ->
    StartTime = Transfer#transfer.start_time,
    ChartsType = maps:get(<<"chartsType">>, Data),

    % Return historical statistics of finished transfers intact. As for active
    % ones, pad them with zeroes to current time and erase recent n-seconds to
    % avoid fluctuations on charts
    {Histograms, LastUpdate, TimeWindow} = case transfer:is_ongoing(Transfer) of
        false ->
            RequestedHistograms = transfer_histograms:get(Transfer, ChartsType),
            Window = transfer_histograms:period_to_time_window(ChartsType),
            {RequestedHistograms, get_last_update(Transfer), Window};
        true ->
            LastUpdates = Transfer#transfer.last_update,
            CurrentTime = provider_logic:zone_time_seconds(),
            transfer_histograms:prepare(
                Transfer, ChartsType, CurrentTime, LastUpdates
            )
    end,

    ThroughputCharts = transfer_histograms:to_speed_charts(
        Histograms, StartTime, LastUpdate, TimeWindow
    ),

    {ok, value, #{
        <<"timestamp">> => LastUpdate,
        <<"charts">> => ThroughputCharts
    }}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{gri = #gri{id = TransferId, aspect = cancel}}) ->
    case transfer:cancel(TransferId) of
        ok ->
            ok;
        {error, already_ended} ->
            ?ERROR_TRANSFER_ALREADY_ENDED;
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns status of given transfer. Replaces active status with 'replicating'
%% for replication and 'evicting' for eviction.
%% In case of migration 'evicting' indicates that the replication itself has
%% finished, but source replica eviction is still in progress.
%% @end
%%--------------------------------------------------------------------
-spec get_status(transfer:transfer()) ->
    transfer:status() | evicting | replicating.
get_status(T = #transfer{
    replication_status = completed,
    replicating_provider = P1,
    evicting_provider = P2
}) when is_binary(P1) andalso is_binary(P2) ->
    case T#transfer.eviction_status of
        scheduled -> evicting;
        enqueued -> evicting;
        active -> evicting;
        Status -> Status
    end;
get_status(T = #transfer{replication_status = skipped}) ->
    case T#transfer.eviction_status of
        active -> evicting;
        Status -> Status
    end;
get_status(#transfer{replication_status = active}) -> replicating;
get_status(#transfer{replication_status = Status}) -> Status.


-spec get_last_update(#transfer{}) -> non_neg_integer().
get_last_update(#transfer{start_time = StartTime, last_update = LastUpdateMap}) ->
    % It is possible that there is no last update, if 0 bytes were
    % transferred, in this case take the start time.
    lists:max([StartTime | maps:values(LastUpdateMap)]).
