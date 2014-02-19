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

-include("logging.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).


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
    ets:new(subscribers_ets, [named_table, public, bag, {read_concurrency, true}]),
    % change trace console to omit duplicate logs
    gen_event:delete_handler(lager_event, lager_console_backend, []),
    supervisor:start_child(lager_handler_watcher_sup, [lager_event, lager_console_backend,
        [info, {lager_default_formatter, [{destination, "", [time, " [", severity, "] ", message, "\n"]}]}]]),

    % remove standard file traces
    gen_event:delete_handler(lager_event, {lager_file_backend, "log/debug.log"}, []),
    gen_event:delete_handler(lager_event, {lager_file_backend, "log/info.log"}, []),
    gen_event:delete_handler(lager_event, {lager_file_backend, "log/error.log"}, []),

    %  install proper file traces for a central_logger
    install_trace_file("log/debug.log", debug, 10485760, "$D0", 10, [{destination, local}], false),
    install_trace_file("log/info.log", info, 104857600, "$W5D23", 100, [{destination, local}], false),
    install_trace_file("log/error.log", error, 1048576000, "$M1D1", 1000, [{destination, local}], false),

    install_trace_file("log/global_debug.log", debug, 10485760, "$D0", 10, [{destination, '*'}], true),
    install_trace_file("log/global_info.log", info, 104857600, "$W5D23", 100, [{destination, '*'}], true),
    install_trace_file("log/global_error.log", error, 1048576000, "$M1D1", 1000, [{destination, '*'}], true),

    ok.


%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1
-spec handle(ProtocolVersion :: term(), Request) -> Result when
    Request :: ping | healthcheck | get_version | {subscribe, Subscriber} |
    {unsubscribe, Subscriber} | {dispatch_log, Message, Timestamp, Severity, Metadata},
    Result :: ok | {ok, Response} | {error, Error} | pong | Version,
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

handle(_ProtocolVersion, {subscribe, Subscriber}) ->
    add_subscriber(Subscriber);

handle(_ProtocolVersion, get_subscribers) ->
    get_subscribers();

handle(_ProtocolVersion, {unsubscribe, Subscriber}) ->
    remove_subscriber(Subscriber);

handle(_ProtocolVersion, {dispatch_log, Message, Timestamp, Severity, Metadata}) ->
    dispatch_log(Message, Timestamp, Severity, Metadata);

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
    ets:delete(subscribers_ets),

    % Restart lager completely, which will remove all traces and install default ones
    application:stop(lager),
    application:load(lager),
    lager:start(),

    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================


%% dispatch_log/4
%% ====================================================================
%% @doc Sends the log to subscribing pids, adds a tag meaning if the log is
%% from this or external node and calls do_log()
-spec dispatch_log(Message :: string(), Timestamp :: term(), Severity :: atom(), Metadata :: list()) -> Result when
    Result :: ok.
%% ====================================================================
dispatch_log(Message, Timestamp, Severity, OldMetadata) ->
    try
        send_log_to_subscribers(Message, Timestamp, Severity, OldMetadata),
        ThisNode = node(),
        {node, LogNode} = lists:keyfind(node, 1, OldMetadata),
        Metadata = case LogNode of
        % Local log
                       ThisNode -> [{destination, local} | OldMetadata];
        % Log from remote node
                       _ -> [{destination, global} | OldMetadata]
                   end,
        do_log(Message, Timestamp, Severity, Metadata)
    catch
        Type:Msg -> ?warning_stacktrace("Error dispatching log: ~p:~p", [Type, Msg])
    end,
    ok.

%% do_log/4
%% ====================================================================
%% @doc Checks if there are any traces to consume the log and if so,
%% notifies the lager_event
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

%% send_log_to_subscribers/4
%% ====================================================================
%% @doc Propagates the log to all subscribing pids
-spec send_log_to_subscribers(Message :: string(), Timestamp :: term(), Severity :: atom(), Metadata :: list()) -> Result when
    Result :: ok.
%% ====================================================================
send_log_to_subscribers(Message, Timestamp, Severity, Metadata) ->
    lists:foreach
    (
        fun(Sub) ->
            Sub ! {log, {Message, Timestamp, Severity, Metadata}}
        end,
        get_subscribers()
    ).

%% add_subscriber/1
%% ====================================================================
%% @doc Adds a subscriber to ets table
-spec add_subscriber(Subscriber :: pid()) -> Result when
    Result :: ok | {error, Error :: term()}.
%% ====================================================================
add_subscriber(Subscriber) ->
    ets:insert(subscribers_ets, {subscriber, Subscriber}).

%% get_subscribers/0
%% ====================================================================
%% @doc Returns list of subscribing Pids
-spec get_subscribers() -> Result when
    Result :: list() | {error, Error :: term()}.
%% ====================================================================
get_subscribers() ->
    lists:map
    (
        fun({subscriber, Sub}) -> Sub end,
        ets:lookup(subscribers_ets, subscriber)
    ).

%% remove_subscriber/1
%% ====================================================================
%% @doc Removes a subscriber from ets table
-spec remove_subscriber(Subscriber :: pid()) -> Result when
    Result :: ok | {error, Error :: term()}.
%% ====================================================================
remove_subscriber(Subscriber) ->
    ets:delete_object(subscribers_ets, {subscriber, Subscriber}).

%% install_trace_file/7
%% ====================================================================
%% @doc Installs a trace file into lager_event. Depending on args, recalculates
%% traces according to filter or changes the default formatting.
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
