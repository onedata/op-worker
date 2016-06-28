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
-export([create_rrd/2, update_rrd/3, export_rrd/3]).

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
-spec create_rrd(#monitoring_id{}, maps:map()) -> ok | already_exists.
create_rrd(MonitoringId, StateBuffer) ->
    #rrd_definition{datastores = Datastores, rras_map = RRASMap, options = Options} =
        get_rrd_definition(MonitoringId),
    StepInSeconds = proplists:get_value(step, Options),

    case monitoring_state:exists(MonitoringId) of
        false ->
            Path = get_path(),
            poolboy:transaction(?RRDTOOL_POOL_NAME, fun(Pid) ->
                ok = rrdtool:create(Pid, Path, Datastores,
                    parse_rras_map(RRASMap), Options)
            end, ?RRDTOOL_POOL_TRANSACTION_TIMEOUT),

            {ok, RRDFile} = read_rrd_from_file(Path),

            {ok, _} = monitoring_state:save(#document{key = MonitoringId,
                value = #monitoring_state{
                    rrd_file = RRDFile,
                    monitoring_interval = timer:seconds(StepInSeconds),
                    state_buffer = StateBuffer
                }}),
            {ok, _} = monitoring_init_state:save(#document{key = MonitoringId,
                value = #monitoring_init_state{
                    monitoring_interval = timer:seconds(StepInSeconds)
                }}),
            ok;
        true ->
            {ok, #document{value = State}} = monitoring_state:get(MonitoringId),
            {ok, _} = monitoring_state:save(#document{key = MonitoringId,
                value = State#monitoring_state{active = true}}),
            already_exists
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates RRD file content with given data. Does not saves rrd to database.
%% @end
%%--------------------------------------------------------------------
-spec update_rrd(#monitoring_id{}, #monitoring_state{}, [term()]) -> {ok, #monitoring_state{}}.
update_rrd(MonitoringId, MonitoringState, UpdateValues) ->
    #rrd_definition{datastores = Datastores} = get_rrd_definition(MonitoringId),
    #monitoring_state{rrd_file = RRDFile} = MonitoringState,

    UpdatesList = lists:zip(
        lists:map(fun({DSName, _, _}) -> DSName end, Datastores),
        UpdateValues
    ),

    {ok, Path} = write_rrd_to_file(RRDFile),
    poolboy:transaction(?RRDTOOL_POOL_NAME, fun(Pid) ->
        ok = rrdtool:update(Pid, Path, UpdatesList)
    end, ?RRDTOOL_POOL_TRANSACTION_TIMEOUT),

    {ok, UpdatedRRDFile} = read_rrd_from_file(Path),
    UpdatedMonitoringState = MonitoringState#monitoring_state{rrd_file = UpdatedRRDFile},
    {ok, UpdatedMonitoringState}.

%%--------------------------------------------------------------------
%% @doc
%% Exports RRD for given parameters in given format.
%% @end
%%--------------------------------------------------------------------
-spec export_rrd(#monitoring_id{}, atom(), Format :: json | xml) -> {ok, binary()}.
export_rrd(MonitoringId, Step, Format) ->
    #rrd_definition{datastores = Datastores, rras_map = RRASMap, options = Options, unit = Unit} =
        get_rrd_definition(MonitoringId),
    StepInSeconds = proplists:get_value(step, Options),

    {ok, #document{value = #monitoring_state{rrd_file = RRDFile}}} =
        monitoring_state:get(MonitoringId),

    {ok, Path} = write_rrd_to_file(RRDFile),
    {CF, _, PDPsPerCDP, _} = maps:get(Step, RRASMap),

    FormatOptions = case Format of
        json ->
            "--json";
        xml ->
            ""
    end,

    #monitoring_id{
        main_subject_type = MainSubjectType,
        main_subject_id = MainSubjectId,
        metric_type = MetricType,
        secondary_subject_type = SecondarySubjectType,
        secondary_subject_id = SecondarySubjectId,
        provider_id = ProviderId
    } = MonitoringId,

    SecondaryDescription = case SecondarySubjectType of
        undefined ->
            "";
        _ ->
            "; " ++ atom_to_list(SecondarySubjectType) ++ " " ++ binary_to_list(SecondarySubjectId)
    end,

    DescriptionBase = lists:flatten(io_lib:format("\"~s ~s; metric ~s~s; oneprovider ID ~s",
        [atom_to_list(MainSubjectType), binary_to_list(MainSubjectId), atom_to_list(MetricType),
            SecondaryDescription, binary_to_list(ProviderId)])),

    Sources = lists:foldl(
        fun({DSName, _, _}, Acc) ->
            lists:flatten(io_lib:format("~s DEF:~s=~s:~s:~s:step=~b",
                [Acc, DSName, Path, DSName, atom_to_list(CF), StepInSeconds * PDPsPerCDP]))
        end, "", Datastores),

    Exports = lists:foldl(
        fun({DSName, _, _}, Acc) ->
            lists:flatten(io_lib:format("~s XPORT:~s:~s; ~s[~s]\"",
                [Acc, DSName, DescriptionBase, DSName, Unit]))
        end, "", Datastores),

    {ok, Data} = poolboy:transaction(?RRDTOOL_POOL_NAME, fun(Pid) ->
        rrdtool:xport(Pid,
            Sources,
            Exports,
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
%% Returns rrd definition for given monitoring id record.
%% @end
%%--------------------------------------------------------------------
-spec get_rrd_definition(#monitoring_id{}) -> #rrd_definition{}.
get_rrd_definition(#monitoring_id{main_subject_type = space, metric_type = storage_used}) ->
    ?STORAGE_USED_RRD;

get_rrd_definition(#monitoring_id{main_subject_type = space, metric_type = storage_quota}) ->
    ?STORAGE_QUOTA_RRD;

get_rrd_definition(#monitoring_id{main_subject_type = space, metric_type = connected_users}) ->
    ?CONNECTED_USERS_RRD;

get_rrd_definition(#monitoring_id{main_subject_type = space, metric_type = data_access}) ->
    ?DATA_ACCESS_RRD;

get_rrd_definition(#monitoring_id{main_subject_type = space, metric_type = block_access}) ->
    ?BLOCK_ACCESS_IOPS_RRD.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns rras map parsed to list.
%% @end
%%--------------------------------------------------------------------
-spec parse_rras_map(#{atom() =>rra()}) -> [rra()].
parse_rras_map(RRASMap) ->
    lists:map(fun({_Key, Value}) -> Value end, maps:to_list(RRASMap)).
