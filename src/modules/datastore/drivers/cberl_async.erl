%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-module(cberl_async).
-author("Rafal Slota").

%% API
-export([]).

%%%===================================================================
%%% API
%%%===================================================================

%%%===================================================================
%%% Internal functions
%%%===================================================================


-ifdef(skip_debug).
-define(debug(_Message), ok).
-define(debug(_Format, _Args), ok).
-define(debug_stacktrace(_Message), ok).
-define(debug_stacktrace(_Format, _Args), ok).
-endif.

-ifndef(skip_debug).
-define(debug(_Message), ?do_log(0, _Message, false)).
-define(debug(_Format, _Args), ?do_log(0, _Format, _Args, false)).
-define(debug_stacktrace(_Message), ?do_log(0, _Message, true)).
-define(debug_stacktrace(_Format, _Args), ?do_log(0, _Format, _Args, true)).
-endif.

-define(info(_Message), ?do_log(1, _Message, false)).
-define(info(_Format, _Args), ?do_log(1, _Format, _Args, false)).
-define(info_stacktrace(_Message), ?do_log(1, _Message, true)).
-define(info_stacktrace(_Format, _Args), ?do_log(1, _Format, _Args, true)).

-define(notice(_Message), ?do_log(2, _Message, false)).
-define(notice(_Format, _Args), ?do_log(2, _Format, _Args, false)).
-define(notice_stacktrace(_Message), ?do_log(2, _Message, true)).
-define(notice_stacktrace(_Format, _Args), ?do_log(2, _Format, _Args, true)).

-define(warning(_Message), ?do_log(3, _Message, false)).
-define(warning(_Format, _Args), ?do_log(3, _Format, _Args, false)).
-define(warning_stacktrace(_Message), ?do_log(3, _Message, true)).
-define(warning_stacktrace(_Format, _Args), ?do_log(3, _Format, _Args, true)).

-define(error(_Message), ?do_log(4, _Message, false)).
-define(error(_Format, _Args), ?do_log(4, _Format, _Args, false)).
-define(error_stacktrace(_Message), ?do_log(4, _Message, true)).
-define(error_stacktrace(_Format, _Args), ?do_log(4, _Format, _Args, true)).

-define(critical(_Message), ?do_log(5, _Message, false)).
-define(critical(_Format, _Args), ?do_log(5, _Format, _Args, false)).
-define(critical_stacktrace(_Message), ?do_log(5, _Message, true)).
-define(critical_stacktrace(_Format, _Args), ?do_log(5, _Format, _Args, true)).

-define(alert(_Message), ?do_log(6, _Message, false)).
-define(alert(_Format, _Args), ?do_log(6, _Format, _Args, false)).
-define(alert_stacktrace(_Message), ?do_log(6, _Message, true)).
-define(alert_stacktrace(_Format, _Args), ?do_log(6, _Format, _Args, true)).

-define(emergency(_Message), ?do_log(7, _Message, false)).
-define(emergency(_Format, _Args), ?do_log(7, _Format, _Args, false)).
-define(emergency_stacktrace(_Message), ?do_log(7, _Message, true)).
-define(emergency_stacktrace(_Format, _Args), ?do_log(7, _Format, _Args, true)).

% Convienience macros for development purposes

% Prints bad request warning (frequently used in gen_servers)
-define(log_bad_request(Request),
    ?warning("~p:~p - bad request ~p", [?MODULE, ?LINE, Request])
).

% Prints abnormal termination warning
-define(log_terminate(Reason, State),
    case Reason of
        normal -> ok;
        shutdown -> ok;
        {shutdown, _} -> ok;
        _ ->
            ?warning("~p terminated in state ~p due to: ~p", [?MODULE, State, Reason])
    end
).

% Prints a single variable
-define(dump(_Arg), io:format(user, "[DUMP] ~s: ~p~n~n", [??_Arg, _Arg])).

% Prints a list of variables
-define(dump_all(_ListOfVariables),
    lists:foreach(
        fun({_Name, _Value}) ->
            io:format(user, "[DUMP] ~s: ~p~n~n", [_Name, _Value])
        end, lists:zip(string:tokens(??_ListOfVariables, "[] ,"), _ListOfVariables))
).

%% Macros used internally

-define(do_log(_LoglevelAsInt, _Message, _IncludeStackTrace),
    ?do_log(_LoglevelAsInt, _Message, [], _IncludeStackTrace)
).

-define(do_log(_LoglevelAsInt, _Format, _Args, _IncludeStackTrace),
    case logger:should_log(_LoglevelAsInt) of
        false -> ok;
        true ->
            logger:dispatch_log(_LoglevelAsInt, ?gather_metadata, _Format, _Args, _IncludeStackTrace)
    end
).

% Resolves current process's state and returns it as metadata proplist
% Must be called from original function where the log is,
% so that the process info makes sense
-define(gather_metadata,
    [{pid, self()}, {line, ?LINE}] ++
    logger:parse_process_info(process_info(self(), current_function))
).

-compile([export_all]).
-export([handle_call2/3]).



-define('CBE_ADD',      1).
-define('CBE_REPLACE',  2).
-define('CBE_SET',      3).
-define('CBE_APPEND',   4).
-define('CBE_PREPEND',  5).

-define('CMD_CONNECT',    0).
-define('CMD_STORE',      1).
-define('CMD_MGET',       2).
-define('CMD_UNLOCK',     3).
-define('CMD_MTOUCH',     4).
-define('CMD_ARITHMETIC', 5).
-define('CMD_REMOVE',     6).
-define('CMD_HTTP',       7).

-type handle() :: binary().

-record(instance, {handle :: handle(),
    bucketname :: string(),
    transcoder :: module(),
    connected :: true | false,
    opts :: list()}).

-type key() :: string().
-type value() :: string() | list() | integer() | binary().
-type operation_type() :: add | replace | set | append | prepend.
-type instance() :: #instance{}.
-type http_type() :: view | management | raw.
-type http_method() :: get | post | put | delete.


handle_call2({mtouch, Keys, ExpTimesE}, _From, State) ->
    {Connected, Reply} = case connect(State) of
                             ok -> {true, mtouch(Keys, ExpTimesE, State)};
                             {error, _} -> {false, {error, unavailable}}
                         end,
    {reply, Reply, State#instance{connected = Connected}};
handle_call2({unlock, Key, Cas}, _From, State) ->
    {Connected, Reply} = case connect(State) of
                             ok -> {true, unlock(Key, Cas, State)};
                             {error, _} = E -> {false, E}
                         end,
    {reply, Reply, State#instance{connected = Connected}};
handle_call2({store, Op, Key, Value, TranscoderOpts, Exp, Cas}, _From, State) ->
    {Connected, Reply} = case connect(State) of
                             ok -> {true, store(Op, Key, Value, TranscoderOpts, Exp, Cas, State)};
                             {error, _} = E -> {false, E}
                         end,
    {reply, Reply, State#instance{connected = Connected}};
handle_call2({mget, Keys, Exp, Lock}, _From, State) ->
    {Connected, Reply} = case connect(State) of
                             ok -> {true, mget(Keys, Exp, Lock, State)};
                             {error, _} = E -> {false, E}
                         end,
    {reply, Reply, State#instance{connected = Connected}};
handle_call2({arithmetic, Key, OffSet, Exp, Create, Initial}, _From, State) ->
    {Connected, Reply} = case connect(State) of
                             ok -> {true, arithmetic(Key, OffSet, Exp, Create, Initial, State)};
                             {error, _} = E -> {false, E}
                         end,
    {reply, Reply, State#instance{connected = Connected}};
handle_call2({remove, Key, N}, _From, State) ->
    {Connected, Reply} = case connect(State) of
                             ok -> {true, remove(Key, N, State)};
                             {error, _} = E -> {false, E}
                         end,
    {reply, Reply, State#instance{connected = Connected}};
handle_call2({http, Path, Body, ContentType, Method, Chunked}, _From, State) ->
    {Connected, Reply} = case connect(State) of
                             ok -> {true, http(Path, Body, ContentType, Method, Chunked, State)};
                             {error, _} = E -> {false, E}
                         end,
    {reply, Reply, State#instance{connected = Connected}};
handle_call2(bucketname, _From, State = #instance{bucketname = BucketName}) ->
    {reply, {ok, BucketName}, State};
handle_call2(_Request, _From, State) ->
    Reply = ok,
    ?info("UNKNOWN ~p ~p", [_Request, State]),
    {reply, Reply, State}.


connect(#instance{connected = true}) ->
    ok;
connect(#instance{connected = false, handle = Handle, opts = Opts}) ->
    ok = cberl_nif:control(Handle, op(connect), Opts),
    receive
        ok -> ok;
        {error, _} = E -> E
    end.

mtouch(Keys, ExpTimesE, #instance{handle = Handle}) ->
    ok = cberl_nif:control(Handle, op(mtouch), [Keys, ExpTimesE]),
    receive
        Reply -> Reply
    end.

unlock(Key, Cas, #instance{handle = Handle}) ->
    cberl_nif:control(Handle, op(unlock), [Key, Cas]),
    receive
        Reply -> Reply
    end.

store(Op, Key, Value, TranscoderOpts, Exp, Cas,
    #instance{handle = Handle, transcoder = Transcoder}) ->
    StoreValue = Transcoder:encode_value(TranscoderOpts, Value),
    ok = cberl_nif:control(Handle, op(store), [operation_value(Op), Key, StoreValue,
        Transcoder:flag(TranscoderOpts), Exp, Cas]),
    receive
        Reply -> Reply
    end.

mget(Keys, Exp, Lock, #instance{handle = Handle, transcoder = Transcoder}) ->
    ok = cberl_nif:control(Handle, op(mget), [Keys, Exp, Lock]),
    receive
        {error, Error} -> {error, Error};
        {ok, Results} ->
            lists:map(fun(Result) ->
                case Result of
                    {Cas, Flag, Key, Value} ->
                        DecodedValue = Transcoder:decode_value(Flag, Value),
                        {Key, Cas, DecodedValue};
                    {_Key, {error, _Error}} ->
                        Result
                end
            end, Results)
    end.

arithmetic(Key, OffSet, Exp, Create, Initial,
    #instance{handle = Handle, transcoder = Transcoder}) ->
    ok = cberl_nif:control(Handle, op(arithmetic), [Key, OffSet, Exp, Create, Initial]),
    receive
        {error, Error} -> {error, Error};
        {ok, {Cas, Flag, Value}} ->
            DecodedValue = Transcoder:decode_value(Flag, Value),
            {ok, Cas, DecodedValue}
    end.

remove(Key, N, #instance{handle = Handle}) ->
    ok = cberl_nif:control(Handle, op(remove), [Key, N]),
    receive
        Reply -> Reply
    end.

http(Path, Body, ContentType, Method, Chunked, #instance{handle = Handle}) ->
    ok = cberl_nif:control(Handle, op(http), [Path, Body, ContentType, Method, Chunked]),
    receive
        Reply -> Reply
    end.

-spec operation_value(operation_type()) -> integer().
operation_value(add) -> ?'CBE_ADD';
operation_value(replace) -> ?'CBE_REPLACE';
operation_value(set) -> ?'CBE_SET';
operation_value(append) -> ?'CBE_APPEND';
operation_value(prepend) -> ?'CBE_PREPEND'.

-spec op(atom()) -> integer().
op(connect) -> ?'CMD_CONNECT';
op(store) -> ?'CMD_STORE';
op(mget) -> ?'CMD_MGET';
op(unlock) -> ?'CMD_UNLOCK';
op(mtouch) -> ?'CMD_MTOUCH';
op(arithmetic) -> ?'CMD_ARITHMETIC';
op(remove) -> ?'CMD_REMOVE';
op(http) -> ?'CMD_HTTP'.

-spec canonical_bucket_name(string()) -> string().
canonical_bucket_name(Name) ->
    case Name of
        [] -> "default";
        BucketName -> BucketName
    end.
