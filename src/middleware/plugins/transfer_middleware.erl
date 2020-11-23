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
operation_supported(create, instance, private) -> true;
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
data_spec(#op_req{operation = create, data = Data, gri = #gri{aspect = instance}}) ->
    AlwaysRequired = #{
        <<"type">> => {atom, [replication, eviction, migration]},
        <<"dataSourceType">> => {atom, [file, view]}
    },
    AlwaysOptional = #{<<"callback">> => {binary, non_empty}},

    RequiredDependingOnType = case maps:get(<<"type">>, Data, undefined) of
        <<"replication">> ->
            AlwaysRequired#{<<"replicatingProviderId">> => {binary, non_empty}};
        <<"eviction">> ->
            AlwaysRequired#{<<"evictingProviderId">> => {binary, non_empty}};
        <<"migration">> ->
            AlwaysRequired#{
                <<"replicatingProviderId">> => {binary, non_empty},
                <<"evictingProviderId">> => {binary, non_empty}
            };
        _ ->
            % Do not do anything - exception will be raised by middleware_sanitizer
            AlwaysRequired
    end,
    {AllRequired, AllOptional} = case maps:get(<<"dataSourceType">>, Data, undefined) of
        <<"file">> ->
            {RequiredDependingOnType#{
                <<"fileId">> => {binary, fun(ObjectId) ->
                    {true, middleware_utils:decode_object_id(ObjectId, <<"fileId">>)}
                end}
            }, AlwaysOptional};
        <<"view">> ->
            ViewRequired = RequiredDependingOnType#{
                <<"spaceId">> => {binary, non_empty},
                <<"viewName">> => {binary, non_empty}
            },
            ViewOptional = AlwaysOptional#{
                <<"queryViewParams">> => {json, fun(QueryViewParams) ->
                    {true, view_utils:sanitize_query_options(QueryViewParams)}
                end}
            },
            {ViewRequired, ViewOptional};
        _ ->
            % Do not do anything - exception will be raised by middleware_sanitizer
            {RequiredDependingOnType, AlwaysOptional}
    end,

    #{required => AllRequired, optional => AllOptional};

data_spec(#op_req{operation = create, gri = #gri{aspect = rerun}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = As}}) when
    As =:= instance;
    As =:= progress
->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = throughput_charts}}) -> #{
    required => #{<<"charts_type">> => {binary, [
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
        {ok, #document{value = Transfer}} ->
            % Transfer doc is synchronized only with providers supporting space
            % so if it was fetched then space must be supported locally
            {ok, {Transfer, 1}};
        _ ->
            ?ERROR_NOT_FOUND
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?GUEST}, _) ->
    false;

authorize(#op_req{operation = create, auth = ?USER(UserId), data = Data, gri = #gri{
    aspect = instance
}}, _) ->
    {SpaceId, ViewPrivileges} = case maps:get(<<"dataSourceType">>, Data) of
        file ->
            FileGuid = maps:get(<<"fileId">>, Data),
            {file_id:guid_to_space_id(FileGuid), []};
        view ->
            {maps:get(<<"spaceId">>, Data), [?SPACE_QUERY_VIEWS]}
    end,
    TransferPrivileges = create_transfer_privileges(maps:get(<<"type">>, Data)),

    space_logic:has_eff_privileges(SpaceId, UserId, ViewPrivileges ++ TransferPrivileges);

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{
    aspect = rerun
}}, #transfer{space_id = SpaceId} = Transfer) ->

    ViewPrivileges = case transfer:data_source_type(Transfer) of
        file -> [];
        view -> [?SPACE_QUERY_VIEWS]
    end,
    TransferPrivileges = create_transfer_privileges(transfer:type(Transfer)),

    space_logic:has_eff_privileges(SpaceId, UserId, ViewPrivileges ++ TransferPrivileges);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    aspect = As
}}, #transfer{space_id = SpaceId}) when
    As =:= instance;
    As =:= progress;
    As =:= throughput_charts
->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_TRANSFERS);

authorize(#op_req{operation = delete, auth = Auth = ?USER(UserId), gri = #gri{
    aspect = cancel
}}, #transfer{space_id = SpaceId, user_id = Creator} = Transfer) ->

    case UserId of
        Creator ->
            % User doesn't need cancel privileges to cancel his transfer but
            % must still be member of space.
            middleware_utils:is_eff_space_member(Auth, SpaceId);
        _ ->
            RequiredPrivileges = case transfer:type(Transfer) of
                replication -> [?SPACE_CANCEL_REPLICATION];
                eviction -> [?SPACE_CANCEL_EVICTION];
                migration -> [?SPACE_CANCEL_REPLICATION, ?SPACE_CANCEL_EVICTION]
            end,
            space_logic:has_eff_privileges(SpaceId, UserId, RequiredPrivileges)
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
validate(#op_req{operation = create, data = Data, gri = #gri{aspect = instance}}, _) ->
    ReplicatingProviderId = maps:get(<<"replicatingProviderId">>, Data, undefined),
    EvictingProviderId = maps:get(<<"evictingProviderId">>, Data, undefined),

    case maps:get(<<"dataSourceType">>, Data) of
        file ->
            validate_file_transfer_creation(
                maps:get(<<"fileId">>, Data),
                ReplicatingProviderId, EvictingProviderId
            );
        view ->
            validate_view_transfer_creation(
                maps:get(<<"spaceId">>, Data),
                maps:get(<<"viewName">>, Data),
                ReplicatingProviderId, EvictingProviderId
            )
    end;

validate(#op_req{operation = create, gri = #gri{aspect = rerun}}, #transfer{
    space_id = SpaceId,
    replicating_provider = ReplicatingProviderId,
    evicting_provider = EvictingProviderId,

    file_uuid = FileUuid,
    index_name = ViewName
} = Transfer) ->
    case transfer:data_source_type(Transfer) of
        file ->
            validate_file_transfer_creation(
                file_id:pack_guid(FileUuid, SpaceId),
                ReplicatingProviderId, EvictingProviderId
            );
        view ->
            validate_view_transfer_creation(
                SpaceId, ViewName,
                ReplicatingProviderId, EvictingProviderId
            )
    end;

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
create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance}}) ->
    SessionId = Auth#auth.session_id,

    ReplicatingProviderId = maps:get(<<"replicatingProviderId">>, Data, undefined),
    EvictingProviderId = maps:get(<<"evictingProviderId">>, Data, undefined),
    Callback = maps:get(<<"callback">>, Data, undefined),

    Result = case maps:get(<<"dataSourceType">>, Data) of
        file ->
            lfm:schedule_file_transfer(
                SessionId, maps:get(<<"fileId">>, Data),
                ReplicatingProviderId, EvictingProviderId,
                Callback
            );
        view ->
            lfm:schedule_view_transfer(
                SessionId,
                maps:get(<<"spaceId">>, Data),
                maps:get(<<"viewName">>, Data),
                maps:get(<<"queryViewParams">>, Data, []),
                ReplicatingProviderId, EvictingProviderId,
                Callback
            )
    end,
    case Result of
        {ok, TransferId} ->
            {ok, value, TransferId};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end;

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
        <<"status">> => transfer:status(Transfer),
        <<"timestamp">> => get_last_update(Transfer),
        <<"replicatedBytes">> => BytesReplicated,
        <<"replicatedFiles">> => FilesReplicated,
        <<"evictedFiles">> => FilesEvicted
    }};
get(#op_req{data = Data, gri = #gri{aspect = throughput_charts}}, Transfer) ->
    StartTime = Transfer#transfer.start_time,
    ChartsType = maps:get(<<"charts_type">>, Data),

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
            CurrentTime = global_clock:timestamp_seconds(),
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


%% @private
-spec create_transfer_privileges(transfer:type()) -> [privileges:space_privilege()].
create_transfer_privileges(replication) -> [?SPACE_SCHEDULE_REPLICATION];
create_transfer_privileges(eviction)    -> [?SPACE_SCHEDULE_EVICTION];
create_transfer_privileges(migration)   -> [?SPACE_SCHEDULE_REPLICATION, ?SPACE_SCHEDULE_EVICTION].


%% @private
-spec validate_file_transfer_creation(
    FileGuid :: file_id:file_guid(),
    ReplicatingProvider :: undefined | od_provider:id(),
    EvictingProvider :: undefined | od_provider:id()
) ->
    ok | no_return().
validate_file_transfer_creation(FileGuid, ReplicatingProvider, EvictingProvider) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    middleware_utils:assert_space_supported_locally(SpaceId),

    assert_space_supported_by(SpaceId, ReplicatingProvider),
    assert_space_supported_by(SpaceId, EvictingProvider).


%% @private
-spec validate_view_transfer_creation(
    SpaceId :: od_space:id(),
    ViewName :: index:name(),
    ReplicatingProvider :: undefined | od_provider:id(),
    EvictingProvider :: undefined | od_provider:id()
) ->
    ok | no_return().
validate_view_transfer_creation(SpaceId, ViewName, ReplicatingProvider, EvictingProvider) ->
    middleware_utils:assert_space_supported_locally(SpaceId),

    assert_space_supported_by(SpaceId, ReplicatingProvider),
    assert_view_exists_on_provider(SpaceId, ViewName, ReplicatingProvider),

    assert_space_supported_by(SpaceId, EvictingProvider),
    assert_view_exists_on_provider(SpaceId, ViewName, EvictingProvider).


%% @private
-spec assert_space_supported_by(od_space:id(), undefined | od_provider:id()) ->
    ok | no_return().
assert_space_supported_by(_SpaceId, undefined) ->
    ok;
assert_space_supported_by(SpaceId, ProviderId) ->
    middleware_utils:assert_space_supported_by(SpaceId, ProviderId).


%% @private
-spec assert_view_exists_on_provider(od_space:id(), index:name(),
    undefined | od_provider:id()) -> ok | no_return().
assert_view_exists_on_provider(_SpaceId, _ViewName, undefined) ->
    ok;
assert_view_exists_on_provider(SpaceId, ViewName, ProviderId) ->
    case index:exists_on_provider(SpaceId, ViewName, ProviderId) of
        true ->
            ok;
        false ->
            throw(?ERROR_VIEW_NOT_EXISTS_ON(ProviderId))
    end.


-spec get_last_update(#transfer{}) -> non_neg_integer().
get_last_update(#transfer{start_time = StartTime, last_update = LastUpdateMap}) ->
    % It is possible that there is no last update, if 0 bytes were
    % transferred, in this case take the start time.
    lists:max([StartTime | maps:values(LastUpdateMap)]).
