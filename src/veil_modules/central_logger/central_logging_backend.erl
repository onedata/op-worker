%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This is a gen_event module (lager backend), responsible for intercepting logs
%% and sending them to central sink via request_dispatcher.
%% @end
%% ===================================================================

-module(central_logging_backend).
-behaviour(gen_event).
-include("registered_names.hrl").

%% This record is a replica of lager_msg record, it contains all data
%% of a single log.
-record(lager_msg,
{
    destinations :: list(),
    metadata :: [tuple()],
    severity :: lager:log_level(),
    datetime :: {string(), string()},
    timestamp :: erlang:timestamp(),
    message :: list()
}).


%% ====================================================================
%% API functions
%% ====================================================================
-export([
    init/1, 
    handle_call/2, 
    handle_event/2, 
    handle_info/2, 
    terminate/2,
    code_change/3
]).


%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc gen_event callback init/1 <br />
%% Called after installing this handler into lager_event.
%% Returns its loglevel ( {mask, 255} ) as Status.
%% @end
-spec init(Args :: term()) -> Result when
    Result :: {ok, term()}.
%% ====================================================================
init(_Args) ->
    {ok, {mask, 255}}.

%% handle_call/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:handle_call-2">gen_event:handle_call/2</a>
-spec handle_call(Request :: term(), State :: term()) -> Result when
    Result :: {ok, Reply, NewState},
    Reply :: term(),
    NewState :: term().
%% ====================================================================
handle_call(get_loglevel, State) ->
    {ok, State, State};

handle_call({set_loglevel, Level}, State) ->
    try lager_util:config_to_mask(Level) of
        Levels ->
            {ok, ok, Levels}
    catch
        _:_ ->
            {ok, {error, bad_log_level}, State}
    end;

handle_call(_Request, State) ->
    {ok, ok, State}.

%% handle_event/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:handle_event-2">gen_event:handle_event/2</a>
-spec handle_event(Request :: term(), State :: term()) -> Result when
    Result :: {ok, NewState},
    NewState :: term().
%% ====================================================================
handle_event({log,    #lager_msg {metadata = Metadata, severity = Severity, timestamp = Timestamp, message = Message} }, State) ->
    case lists:keyfind(destination, 1, Metadata) of
        {destination, _} -> already_logged;
        _ -> spawn(fun() -> dispatch_log(Message, Timestamp, Severity, Metadata) end)
    end,
    {ok, State};

handle_event(_Event, State) ->
    {ok, State}.

%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:handle_info-2">gen_event:handle_info/2</a>
-spec handle_info(Info :: term(), State :: term()) -> Result when
    Result :: {ok, NewState},
    NewState :: term().
%% ====================================================================
handle_info(_Info, State) ->
    {ok, State}.

%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:terminate-2">gen_event:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
    Reason :: normal
    | shutdown
    | {shutdown, term()}
    | term().
%% ====================================================================
terminate(_Reason, _State) ->
    ok.

%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_event.html#Module:code_change-3">gen_event:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term()},
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

%% dispatch_log/4
%% ====================================================================
%% @doc Adds proper tags if they don't exist and sends the log to central_logger.
-spec dispatch_log(Message :: string(), Timestamp :: term(), Severity :: atom(), OldMetadata :: list()) -> Result when
    Result :: ok.
%% ====================================================================
dispatch_log(Message, Timestamp, Severity, OldMetadata) ->
    Metadata = case OldMetadata of
        [] ->
            [{node, node()}, {source, unknown}];
        [{pid, Pid}] ->
            [{node, node()}, {source, error_logger}, {pid, Pid}];
        OldMetadata2 ->
            % Make sure node metadata is at the beginning
            [{node, node()}|lists:keydelete(node, 1, OldMetadata2)]
    end,
    try
        gen_server:call(?Dispatcher_Name, {central_logger, 1, {dispatch_log, Message, Timestamp, Severity, Metadata}})
    catch _:_ ->
        central_logger_not_running
    end.