%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour to provide
%% central logging functionalities.
%% @end
%% ===================================================================
-module(central_logger).
-behaviour(worker_plugin_behaviour).

-include_lib("ctool/include/logging.hrl").
-include("logging_pb.hrl").

%% ====================================================================
%% API functions
%% ====================================================================

% Worker behaviour
-export([init/1, handle/2, cleanup/0]).
% Utilities for client loglevel conversion
-export([client_loglevel_int_to_atom/1, client_loglevel_atom_to_int/1]).

% Subscribers ETS name
-define(SUBSCRIBERS_ETS, subscribers_ets).

%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1 <br />
%% Sets up the worker for propagating logs to CMT sessions and
%% configures lager trace files.
%% @end
-spec init(Args :: term()) -> Result when
    Result :: {ok, term()}.
%% ====================================================================
init(_) ->
    ets:new(?SUBSCRIBERS_ETS, [named_table, public, bag, {read_concurrency, true}]),
    % change trace console to omit duplicate logs
    gen_event:delete_handler(lager_event, lager_console_backend, []),
    supervisor:start_child(lager_handler_watcher_sup, [lager_event, lager_console_backend,
        [info, {lager_default_formatter, [{destination, "", [time, " [", severity, "] ", message, "\n"]}]}]]),

    % remove standard file traces
    gen_event:delete_handler(lager_event, {lager_file_backend, "log/debug.log"}, []),
    gen_event:delete_handler(lager_event, {lager_file_backend, "log/info.log"}, []),
    gen_event:delete_handler(lager_event, {lager_file_backend, "log/error.log"}, []),

    %  install proper file traces for a central_logger
    install_trace_file("log/debug.log", debug, 10485760, "$D0", 10, [{source, cluster}, {destination, local}], false),
    install_trace_file("log/info.log", info, 104857600, "$W5D23", 100, [{source, cluster}, {destination, local}], false),
    install_trace_file("log/error.log", error, 1048576000, "$M1D1", 1000, [{source, cluster}, {destination, local}], false),

    install_trace_file("log/global_debug.log", debug, 10485760, "$D0", 10, [{source, cluster}, {destination, '*'}], true),
    install_trace_file("log/global_info.log", info, 104857600, "$W5D23", 100, [{source, cluster}, {destination, '*'}], true),
    install_trace_file("log/global_error.log", error, 1048576000, "$M1D1", 1000, [{source, cluster}, {destination, '*'}], true),

    % trace files for clients
    install_trace_file("log/client_debug.log", debug, 10485760, "$D0", 10, [{source, client}], false),
    install_trace_file("log/client_info.log", info, 104857600, "$W5D23", 100, [{source, client}], false),
    install_trace_file("log/client_error.log", error, 1048576000, "$M1D1", 1000, [{source, client}], false),

    ok.


%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1
-spec handle(ProtocolVersion :: term(), Request) -> Result when
    Request :: ping | healthcheck | get_version | {subscribe, Source, Subscriber} |
    {unsubscribe, Source, Subscriber} | {dispatch_log, Message, Timestamp, Severity, Metadata},
    Result :: ok | {ok, Response} | {error, Error} | pong | Version,
    Source :: cluster | client,
    Subscriber :: pid(),
    Message :: string(),
    Timestamp :: term(),
    Severity :: atom(),
    Metadata :: list(),
    Response :: term(),
    Version :: term(),
    Error :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(_ProtocolVersion, {subscribe, Source, Subscriber}) when Source =:= client orelse Source =:= cluster ->
    add_subscriber(Source, Subscriber),
    ok;

handle(_ProtocolVersion, {unsubscribe, Source, Subscriber}) when Source =:= client orelse Source =:= cluster ->
    remove_subscriber(Source, Subscriber),
    ok;

handle(_ProtocolVersion, {get_subscribers, Source}) when Source =:= client orelse Source =:= cluster ->
    get_subscribers(Source);

handle(_ProtocolVersion, {dispatch_log, Message, Timestamp, Severity, Metadata}) ->
    dispatch_cluster_log(Message, Timestamp, Severity, Metadata),
    ok;

handle(_ProtocolVersion, LogMessage) when is_record(LogMessage, logmessage) ->
    User = case fslogic_context:get_user_dn() of
               undefined ->
                   "unknown";
               UserDN ->
                   case user_logic:get_user({dn, UserDN}) of
                       {ok, UserDoc} -> user_logic:get_login(UserDoc);
                       _ -> "unknown"
                   end
           end,
    FuseID = case fslogic_context:get_fuse_id() of
                 undefined ->
                     "unknown";
                 FID ->
                     FID
             end,
    #logmessage{level = Severity, file_name = Filename, line = Line, pid = Pid, timestamp = UnixTimestamp, message = Message} = LogMessage,
    Metadata = [
        {user, User},
        {fuse_id, FuseID},
        {file, Filename},
        {line, Line},
        {pid, Pid}
    ],
    Timestamp = {UnixTimestamp div 1000000, UnixTimestamp rem 1000000, 0},
    SeverityAsInt = logging_pb:enum_to_int(loglevel, Severity),
    dispatch_client_log(Message, Timestamp, client_loglevel_int_to_atom(SeverityAsInt), Metadata),
    ok;

handle(_ProtocolVersion, _Request) ->
    wrong_request.

%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0 <br />
%% Reconfigures lager back to standard
%% @end
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
%% ====================================================================
cleanup() ->
    % Delete ets table
    ets:delete(?SUBSCRIBERS_ETS),

    % Restart lager completely, which will remove all traces and install default ones
    application:stop(lager),
    application:load(lager),
    lager:start(),

    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% dispatch_cluster_log/4
%% ====================================================================
%% @doc Sends the log to subscribing pids, adds a tag meaning if the log is
%% from this or external node and calls do_log()
%% @end
-spec dispatch_cluster_log(Message :: string(), Timestamp :: term(), Severity :: atom(), Metadata :: list()) -> Result when
    Result :: ok.
%% ====================================================================
dispatch_cluster_log(Message, Timestamp, Severity, OldMetadata) ->
    try
        % Send log to subscribers
        lists:foreach(
            fun(Sub) ->
                Sub ! {log, {Message, Timestamp, Severity, OldMetadata}}
            end, get_subscribers(cluster)),

        % Log it to lager system (so it is printed to files)
        ThisNode = node(),
        {node, LogNode} = lists:keyfind(node, 1, OldMetadata),
        Metadata = case LogNode of
        % Local log
                       ThisNode -> [{source, cluster}, {destination, local} | OldMetadata];
        % Log from remote node
                       _ -> [{source, cluster}, {destination, global} | OldMetadata]
                   end,
        do_log(Message, Timestamp, Severity, Metadata)
    catch
        Type:Msg ->
            lager:log(warning, ?gather_metadata ++ [{destination, global}], "Error dispatching log: ~p:~p~nStacktrace: ~p", [Type, Msg, erlang:get_stacktrace()])
    end.

%% dispatch_client_log/4
%% ====================================================================
%% @doc Sends the log to subscribing pids, adds a tag meaning if the log is
%% from this or external node and calls do_log()
%% @end
-spec dispatch_client_log(Message :: string(), Timestamp :: term(), Severity :: atom(), Metadata :: list()) -> Result when
    Result :: ok.
%% ====================================================================
dispatch_client_log(Message, Timestamp, Severity, Metadata) ->
    try
        % Send log to subscribers
        lists:foreach(
            fun(Sub) ->
                Sub ! {log, {Message, Timestamp, Severity, Metadata}}
            end, get_subscribers(client)),
        NewMetadata = [{source, client}, {destination, global}] ++ Metadata,
        do_log(Message, Timestamp, Severity, NewMetadata)
    catch
        Type:Msg ->
            lager:log(warning, ?gather_metadata ++ [{destination, global}], "Error dispatching log: ~p:~p~nStacktrace: ~p", [Type, Msg, erlang:get_stacktrace()])
    end.


%% do_log/4
%% ====================================================================
%% @doc Checks if there are any traces to consume the log and if so,
%% notifies the lager_event
%% @end
-spec do_log(Message :: string(), Timestamp :: term(), Severity :: atom(), Metadata :: list()) -> Result when
    Result :: ok.
%% ====================================================================
do_log(Message, Timestamp, Severity, Metadata) ->
    {LevelThreshold, Traces} = lager_config:get(loglevel, {0, []}),
    SeverityAsInt = lager_util:level_to_num(Severity),
    Destinations = case Traces of
                       [] ->
                           [];
                       _ ->
                           lager_util:check_traces(Metadata, SeverityAsInt, Traces, [])
                   end,
    case (LevelThreshold band SeverityAsInt) /= 0 orelse Destinations /= [] of
        true ->
            LagerMsg = lager_msg:new(Message, Timestamp, Severity, Metadata, Destinations),
            case lager_config:get(async, false) of
                true ->
                    gen_event:notify(whereis(lager_event), {log, LagerMsg});
                false ->
                    gen_event:sync_notify(whereis(lager_event), {log, LagerMsg})
            end;
        false ->
            ok
    end.


%% add_subscriber/2
%% ====================================================================
%% @doc Adds a subscriber to ets table
-spec add_subscriber(Source :: cluster | client, Subscriber :: pid()) -> Result when
    Result :: ok | {error, Error :: term()}.
%% ====================================================================
add_subscriber(Source, Subscriber) ->
    ets:insert(?SUBSCRIBERS_ETS, {Source, Subscriber}).


%% get_subscribers/1
%% ====================================================================
%% @doc Returns list of subscribing Pids
%% @end
-spec get_subscribers(Source :: cluster | client) -> Result when
    Result :: list() | {error, Error :: term()}.
%% ====================================================================
get_subscribers(Source) ->
    lists:map(
        fun({_, Sub}) ->
            Sub
        end, ets:lookup(?SUBSCRIBERS_ETS, Source)).


%% remove_subscriber/2
%% ====================================================================
%% @doc Removes a subscriber from ets table
-spec remove_subscriber(Source :: cluster | client, Subscriber :: pid()) -> Result when
    Result :: ok | {error, Error :: term()}.
%% ====================================================================
remove_subscriber(Source, Subscriber) ->
    ets:delete_object(?SUBSCRIBERS_ETS, {Source, Subscriber}).


%% install_trace_file/7
%% ====================================================================
%% @doc Installs a trace file into lager_event. Depending on args, recalculates
%% traces according to filter or changes the default formatting.
%% @end
-spec install_trace_file(File :: string(), Level :: atom(), MaxSize :: integer(), DateSpec :: string(),
    MaxCount :: integer(), Filter :: list(), ChangeFormatting :: atom()) -> Result when
    Result :: ok | {error, Error :: term()}.
%% ====================================================================
install_trace_file(File, Level, MaxSize, DateSpec, MaxCount, Filter, ChangeFormatting) ->
    CfgLevel = case Filter of
                   [] -> Level;
                   _ -> none
               end,
    BaseCfg =
        [
            {file, File},
            {level, CfgLevel},
            {size, MaxSize},
            {date, DateSpec},
            {count, MaxCount}
        ],
    Cfg = case ChangeFormatting of
              true -> BaseCfg ++ custom_log_format();
              false -> BaseCfg
          end,

    supervisor:start_child(lager_handler_watcher_sup, [lager_event, {lager_file_backend, File}, Cfg]),

    case Filter of
        [] -> ok;
        Filters ->
            {ok, Trace} = lager_util:validate_trace({Filters, Level, {lager_file_backend, File}}),
            {MinLevel, Traces} = lager_config:get(loglevel),
            NewTraces = [Trace | lists:delete(Trace, Traces)],
            lager_util:trace_filter([element(1, T) || T <- NewTraces]),
            lager_config:set(loglevel, {MinLevel, NewTraces})
    end.


%% custom_log_format/0
%% ====================================================================
%% @doc Convenience function returning formatter args to lager trace files.
%% @end
-spec custom_log_format() -> Result when
    Result :: list().
%% ====================================================================
custom_log_format() ->
    [
        {formatter, lager_default_formatter},
        {formatter_config,
            [date, " ", time, " ", color, "[", severity, "] ",
                {node, ["(", node, ")"], ""},
                {pid, [":", pid], ""},
                {module,
                    [
                        {pid, ["@"], ""},
                        module,
                        {function, [":", function], ""},
                        {line, [":", line], ""}
                    ], ""},
                " ", message, "\n"]
        }
    ].


%% client_loglevel_int_to_atom/0
%% ====================================================================
%% @doc Converts client loglevel from integer to atom representation.
%% @end
-spec client_loglevel_int_to_atom(LevelAsInt :: integer()) -> atom().
%% ====================================================================
client_loglevel_int_to_atom(0) -> debug;
client_loglevel_int_to_atom(1) -> info;
client_loglevel_int_to_atom(2) -> warning;
client_loglevel_int_to_atom(3) -> error;
client_loglevel_int_to_atom(4) -> fatal;
client_loglevel_int_to_atom(5) -> none.


%% client_loglevel_atom_to_int/0
%% ====================================================================
%% @doc Converts client loglevel from atom to integer representation.
%% @end
-spec client_loglevel_atom_to_int(LevelAsAtom :: atom()) -> integer().
%% ====================================================================
client_loglevel_atom_to_int(debug) -> 0;
client_loglevel_atom_to_int(info) -> 1;
client_loglevel_atom_to_int(warning) -> 2;
client_loglevel_atom_to_int(error) -> 3;
client_loglevel_atom_to_int(fatal) -> 4;
client_loglevel_atom_to_int(none) -> 5.
