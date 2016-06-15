%% ===================================================================
%% @author Michal Wrona
%% @copyright (C): 2016 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module exports utility tools for rrd files.
%% @end
%% ===================================================================
-module(rrd_utils).
-author("Michal Wrona").

-include_lib("ctool/include/logging.hrl").
-include("global_definitions.hrl").
-include("modules/monitoring/rrd_definitions.hrl").

%% API
-export([create_rrd/3, update_rrd/4, export_rrd/6]).

-type rrd_file() :: binary().
%% Params: [Heartbeat, MinValue, MaxValue]
-type datastore() :: {DSName :: string(), StoreType :: atom(), Params :: []}.
-type rra() :: {ConsolidationFunction :: atom(), XffFactor :: float(),
    PDPsPerCDP :: non_neg_integer(), CDPsPerRRA :: non_neg_integer()}.
-type options() :: proplists:proplists().

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates rrd with given parameters if database entry for it is empty.
%% @end
%%--------------------------------------------------------------------
-spec create_rrd(atom(), datastore:id(), atom()) -> ok | already_exists.
create_rrd(SubjectType, SubjectId, MetricType) ->
    #rrd_definition{datastore = Datastore, rras_map = RRASMap, options = Options} =
        get_rrd_definition(SubjectType, MetricType),
    StepInSeconds = proplists:get_value(step, Options),

    case monitoring_state:exists(SubjectType, SubjectId, MetricType, oneprovider:get_provider_id()) of
        false ->
            Path = get_path(),
            poolboy:transaction(?RRDTOOL_POOL_NAME, fun(Pid) ->
                ok = rrdtool:create(Pid, Path, [Datastore],
                    parse_rras_map(RRASMap), Options)
            end, ?RRDTOOL_POOL_TRANSACTION_TIMEOUT),

            {ok, RRDFile} = read_rrd_from_file(Path),

            {ok, _} = monitoring_state:save(SubjectType, SubjectId, MetricType,
                #monitoring_state{
                    rrd_file = RRDFile,
                    monitoring_interval = timer:seconds(StepInSeconds)
                }),
            ok;
        true ->
            {ok, State} = monitoring_state:get(SubjectType, SubjectId, MetricType),
            {ok, _} = monitoring_state:save(SubjectType, SubjectId, MetricType,
                State#monitoring_state{active = true}),
            already_exists
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates RRD file content with given data.
%% @end
%%--------------------------------------------------------------------
-spec update_rrd(atom(), datastore:id(), atom(), term()) ->
    {ok, #monitoring_state{}}.
update_rrd(SubjectType, SubjectId, MetricType, UpdateValue) ->
    #rrd_definition{datastore = Datastore} = get_rrd_definition(SubjectType, MetricType),
    {DSName, _, _} = Datastore,

    #monitoring_state{rrd_file = RRDFile} = MonitoringState =
        case monitoring_state:get(SubjectType, SubjectId, MetricType) of
            {ok, State} ->
                State;
            {error, {not_found, _}} ->
                ok = create_rrd(SubjectType, SubjectId, MetricType),
                {ok, State} = monitoring_state:get(SubjectType, SubjectId, MetricType),
                State
        end,

    {ok, Path} = write_rrd_to_file(RRDFile),
    poolboy:transaction(?RRDTOOL_POOL_NAME, fun(Pid) ->
        ok = rrdtool:update(Pid, Path, [{DSName, UpdateValue}])
    end, ?RRDTOOL_POOL_TRANSACTION_TIMEOUT),

    {ok, UpdatedRRDFile} = read_rrd_from_file(Path),
    UpdatedMonitoringState = MonitoringState#monitoring_state{rrd_file = UpdatedRRDFile},
    {ok, _} = monitoring_state:save(SubjectType, SubjectId, MetricType,
        UpdatedMonitoringState),
    {ok, UpdatedMonitoringState}.

%%--------------------------------------------------------------------
%% @doc
%% Exports RRD for given parameters in given format.
%% @end
%%--------------------------------------------------------------------
-spec export_rrd(atom(), datastore:id(), atom(), atom(), Format :: json | xml,
    oneprovider:id()) -> {ok, binary()}.
export_rrd(SubjectType, SubjectId, MetricType, Step, Format, ProviderId) ->
    #rrd_definition{datastore = Datastore, rras_map = RRASMap, options = Options} =
        get_rrd_definition(SubjectType, MetricType),
    StepInSeconds = proplists:get_value(step, Options),
    {DSName, _, _} = Datastore,

    {ok, #monitoring_state{rrd_file = RRDFile}} =
        monitoring_state:get(SubjectType, SubjectId, MetricType, ProviderId),

    {ok, Path} = write_rrd_to_file(RRDFile),
    {CF, _, PDPsPerCDP, _} = maps:get(Step, RRASMap),

    FormatOptions = case Format of
        json ->
            "--json";
        xml ->
            ""
    end,

    Description = "\"" ++ atom_to_list(SubjectType) ++ " " ++ binary_to_list(SubjectId)
        ++ "; metric " ++ atom_to_list(MetricType)
        ++ "; oneprovider ID " ++ binary_to_list(ProviderId) ++ "\"",

    {ok, Data} = poolboy:transaction(?RRDTOOL_POOL_NAME, fun(Pid) ->
        rrdtool:xport(Pid,
            "DEF:export=" ++ Path ++ ":" ++ DSName ++ ":"
                ++ atom_to_list(CF) ++ ":step="
                ++ integer_to_list(StepInSeconds * PDPsPerCDP),
            "XPORT:export:" ++ Description,
            FormatOptions ++ " --start now-" ++ maps:get(Step, ?MAKESPAN_FOR_STEP)
                ++ " --end now")
    end, ?RRDTOOL_POOL_TRANSACTION_TIMEOUT),

    file:delete(Path),
    {ok, list_to_binary(Data)}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Writes RRD file content to tmp file.
%% @end
%%--------------------------------------------------------------------
-spec write_rrd_to_file(rrd_file()) -> {ok, Path :: string()}.
write_rrd_to_file(RRDFile) ->
    Path = get_path(),
    ok = file:write_file(Path, base64:decode(RRDFile)),
    {ok, Path}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Reads RRD file content from given path and deletes file.
%% @end
%%--------------------------------------------------------------------
-spec read_rrd_from_file(Path :: string()) -> {ok, rrd_file()}.
read_rrd_from_file(Path) ->
    {ok, RRDFile} = file:read_file(Path),
    file:delete(Path),
    {ok, base64:encode(RRDFile)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns path to temporary rrd file location.
%% @end
%%--------------------------------------------------------------------
-spec get_path() -> Path :: string().
get_path() ->
    filelib:ensure_dir("/tmp/.rrd/"),
    "/tmp/.rrd/" ++ integer_to_list(erlang:unique_integer([positive])).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns rrd definition for given SubjectType and MetricType.
%% @end
%%--------------------------------------------------------------------
-spec get_rrd_definition(SubjectType :: atom(), MetricType :: atom()) ->
    #rrd_definition{}.
get_rrd_definition(space, storage_used) ->
    ?STORAGE_USED_PER_SPACE_RRD;
get_rrd_definition(space, storage_quota) ->
    ?STORAGE_QUOTA_PER_SPACE_RRD.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns rras map parsed to list.
%% @end
%%--------------------------------------------------------------------
-spec parse_rras_map(#{atom() =>rra()}) -> [rra()].
parse_rras_map(RRASMap) ->
    lists:map(fun({_Key, Value}) -> Value end, maps:to_list(RRASMap)).
