%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module coordinates access to Round Robin Database.
%% @end
%% ===================================================================
-module(rrderlang).

-behaviour(gen_server).

-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% TEST
-ifdef(TEST).
-export([select_header/2, select_row/2]).
-endif.

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-record(state, {port}).

%%%===================================================================
%%% API
%%%===================================================================


%% start_link/1
%% ====================================================================
%% @doc Starts the server
%% @end
-spec start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
%% ====================================================================
start_link() ->
  gen_server:start_link({local, ?RrdErlang_Name}, ?MODULE, [], []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
%% @end
-spec init(Args :: term()) -> Result when
  Result :: {ok, State}
  | {ok, State, Timeout}
  | {ok, State, hibernate}
  | {stop, Reason :: term()}
  | ignore,
  State :: term(),
  Timeout :: non_neg_integer() | infinity.
%% ====================================================================
init([]) ->
  case os:find_executable("rrdtool") of
    false ->
      {stop, no_rrdtool};
    RRDtool ->
      Port = open_port({spawn_executable, RRDtool}, [{line, 1024}, {args, ["-"]}, binary]),
      {ok, #state{port = Port}}
  end.


%% handle_call/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
%% @end
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
  Result :: {reply, Reply, NewState}
  | {reply, Reply, NewState, Timeout}
  | {reply, Reply, NewState, hibernate}
  | {noreply, NewState}
  | {noreply, NewState, Timeout}
  | {noreply, NewState, hibernate}
  | {stop, Reason, Reply, NewState}
  | {stop, Reason, NewState},
  Reply :: term(),
  NewState :: term(),
  Timeout :: non_neg_integer() | infinity,
  Reason :: term().
%% ====================================================================
handle_call({create, Filename, Options, DSs, RRAs}, _From, #state{port = Port} = State) ->
  Command = format_command([<<"create">>, Filename, Options, format_command(DSs), format_command(RRAs), <<"\n">>]),
  Result = execute_command(Port, Command),
  {reply, Result, State};
handle_call({update, Filename, Options, Values, Timestamp}, _From, #state{port = Port} = State) ->
  BinaryValues = lists:map(fun
    (Elem) when is_float(Elem) -> float_to_binary(Elem);
    (Elem) when is_integer(Elem) -> integer_to_binary(Elem);
    (_) -> <<"U">> end, Values),
  Command = format_command([<<"update">>, Filename, Options, format_command([Timestamp | BinaryValues], <<":">>), <<"\n">>]),
  Result = execute_command(Port, Command),
  {reply, Result, State};
handle_call({fetch, Filename, Options, CF}, _From, #state{port = Port} = State) ->
  Command = format_command([<<"fetch">>, Filename, CF, Options, <<"\n">>]),
  Result = execute_command(Port, Command, all),
  {reply, Result, State};
handle_call({fetch, Filename, Options, CF, Columns}, _From, #state{port = Port} = State) ->
  Command = format_command([<<"fetch">>, Filename, CF, Options, <<"\n">>]),
  Result = execute_command(Port, Command, Columns),
  {reply, Result, State};
handle_call({graph, Filename, Options}, _From, #state{port = Port} = State) ->
  Command = format_command([<<"graph">>, Filename, Options]),
  Result = execute_command(Port, Command),
  {reply, Result, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.


%% handle_cast/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
%% @end
-spec handle_cast(Request :: term(), State :: term()) -> Result when
  Result :: {noreply, NewState}
  | {noreply, NewState, Timeout}
  | {noreply, NewState, hibernate}
  | {stop, Reason :: term(), NewState},
  NewState :: term(),
  Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_cast(_Request, State) ->
  {noreply, State}.


%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
%% @end
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
  Result :: {noreply, NewState}
  | {noreply, NewState, Timeout}
  | {noreply, NewState, hibernate}
  | {stop, Reason :: term(), NewState},
  NewState :: term(),
  Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_info(_Info, State) ->
  {noreply, State}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
%% @end
-spec terminate(Reason, State :: term()) -> Any :: term() when
  Reason :: normal
  | shutdown
  | {shutdown, term()}
  | term().
%% ====================================================================
terminate(_Reason, State) ->
  port_command(State#state.port, [<<"quit\n">>]),
  port_close(State#state.port),
  ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
%% @end
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
  Result :: {ok, NewState :: term()} | {error, Reason :: term()},
  OldVsn :: Vsn | {down, Vsn},
  Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% format_command/1
%% ====================================================================
%% @doc Returns formatted command from command parts using default separator.
%% @end
-spec format_command(Parts :: [binary()]) -> Command :: binary().
%% ====================================================================
format_command(Parts) ->
  format_command(Parts, <<" ">>).


%% format_command/2
%% ====================================================================
%% @doc Returns formatted command from command parts.
%% Should not be used directly, use format_command/1 instead.
%% @end
-spec format_command(Parts :: [binary()], Separator :: binary()) -> Command :: binary().
%% ====================================================================
format_command([], _) ->
  <<>>;
format_command([Part | Parts], Separator) ->
  lists:foldl(fun(Elem, Acc) -> <<Acc/binary, Separator/binary, Elem/binary>> end, Part, Parts).


%% execute_command/2
%% ====================================================================
%% @doc Executes command on Round Robin Database and returns answer.
%% @end
-spec execute_command(Port :: port(), Command :: binary()) -> {ok, Result :: binary()} | {error, Error :: binary()}.
%% ====================================================================
execute_command(Port, Command) ->
  port_command(Port, Command),
  receive_answer(Port).


%% execute_command/3
%% ====================================================================
%% @doc Executes command on Round Robin Database and returns answer.
%% @end
-spec execute_command(Port :: port(), Command :: binary(), Columns) -> {ok, Result :: binary()} | {error, Error :: binary()} when
  Columns :: all | {name, [binary()]} | {starts_with, [binary()]} | {index, [integer()]}.
%% ====================================================================
execute_command(Port, Command, Columns) ->
  port_command(Port, Command),
  case receive_header(Port, Columns) of
    {ok, {Header, NewColumns}} ->
      case receive_body(Port, NewColumns) of
        {ok, Body} ->
          {ok, {Header, Body}};
        {error, Error} ->
          ?error("RRD execute_command error: ~p", [Error]),
          {error, Error}
      end;
    {error, Error} ->
      ?error("RRD execute_command error: ~p", [Error]),
      {error, Error}
  end.


%% receive_answer/1
%% ====================================================================
%% @doc Receives answer from Round Robin Database using erlang port.
%% @end
-spec receive_answer(Port :: port()) -> {ok, Result :: binary()} | {error, Error :: binary()}.
%% ====================================================================
receive_answer(Port) ->
  receive_answer(Port, []).


%% receive_answer/2
%% ====================================================================
%% @doc Receives answer from Round Robin Database using erlang port.
%% Should not be used directly, use receive_answer/1 instead.
%% @end
-spec receive_answer(Port :: port(), Acc :: [binary()]) -> {ok, Result :: binary()} | {error, Error :: binary()}.
%% ====================================================================
receive_answer(Port, Acc) ->
  {ok, Timeout} = application:get_env(?APP_Name, rrd_timeout),
  receive
    {Port, {data, {eol, <<"OK", _/binary>>}}} ->
      {ok, Acc};
    {Port, {data, {eol, <<"ERROR: ", Error/binary>>}}} ->
      {error, Error};
    {Port, {data, {eol, Data}}} ->
      receive_answer(Port, [Data | Acc])
  after Timeout ->
    ?error("RRD receive_answer timeout"),
    {error, <<"timeout">>}
  end.


%% receive_header/2
%% ====================================================================
%% @doc Receives header of Round Robin Archive fetched from database.
%% @end
-spec receive_header(Port :: port(), Columns) -> {ok, Result :: binary()} | {error, Error :: binary()} when
  Columns :: all | {name, [binary()]} | {starts_with, [binary()]} | {index, [integer()]}.
%% ====================================================================
receive_header(Port, Columns) ->
  receive_header(Port, Columns, []).


%% receive_header/3
%% ====================================================================
%% @doc Receives header of Round Robin Archive fetched from database.
%% Should not be used directly, use receive_header/2 instead.
%% @end
-spec receive_header(Port :: port(), Columns, BinaryHeader :: binary()) -> {ok, Result :: binary()} | {error, Error :: binary()} when
  Columns :: all | {name, [binary()]} | {starts_with, [binary()]} | {index, [integer()]}.
%% ====================================================================
receive_header(Port, Columns, BinaryHeader) ->
  {ok, Timeout} = application:get_env(?APP_Name, rrd_timeout),
  receive
    {Port, {data, {eol, <<>>}}} ->
      Header = split(BinaryHeader),
      select_header(Header, Columns);
    {Port, {data, {eol, <<"ERROR: ", Error/binary>>}}} ->
      {error, Error};
    {Port, {data, {eol, Data}}} ->
      receive_header(Port, Columns, Data)
  after Timeout ->
    ?error("RRD receive_header timeout"),
    {error, <<"timeout">>}
  end.


%% select_header/2
%% ====================================================================
%% @doc Select columns from header and returns new header with associated column indexes.
%% @end
-spec select_header(Header :: [binary()], Columns) -> {ok, {NewHeader :: [binary()], NewColumns :: [integer()]}} when
  Columns :: all | {name, [binary()]} | {starts_with, [binary()]} | {index, [integer()]}.
%% ====================================================================
select_header(Header, Columns) ->
  try
    select_header(Header, Columns, 1, [], [])
  catch
    E1:E2 ->
      ?error("RRD select_header error ~p:~p", [E1, E2]),
      {error, <<"Header selection error.">>}
  end.


%% select_header/5
%% ====================================================================
%% @doc Select columns from header and returns new header with associated column indexes.
%% @end
-spec select_header(Header :: [binary()], Columns, N :: integer(), NewHeader, NewColumns) -> {ok, {NewHeader, NewColumns}} when
  Columns :: all | {name, [binary()]} | {starts_with, [binary()]} | {index, [integer()]},
  NewHeader :: [binary()],
  NewColumns :: [integer()].
%% ====================================================================
select_header(Header, all, N, NewHeader, NewColumns) ->
  select_header(Header, {index, lists:seq(1, length(Header))}, N, NewHeader, NewColumns);
select_header([], _, _, NewHeader, NewColumns) ->
  {ok, {lists:reverse(NewHeader), lists:reverse(NewColumns)}};
select_header([Value | Values], {index, Columns}, N, NewHeader, NewColumns) ->
  case exists(N, {index, Columns}) of
    true -> select_header(Values, {index, Columns}, N + 1, [Value | NewHeader], [N | NewColumns]);
    _ -> select_header(Values, {index, Columns}, N + 1, NewHeader, NewColumns)
  end;
select_header([Value | Values], Columns, N, NewHeader, NewColumns) ->
  case exists(Value, Columns) of
    true -> select_header(Values, Columns, N + 1, [Value | NewHeader], [N | NewColumns]);
    _ -> select_header(Values, Columns, N + 1, NewHeader, NewColumns)
  end.


%% exists/2
%% ====================================================================
%% @doc Checks whether value matches any selection rule. Following rules
%% are acceptable:
%% * name - matches value by full name
%% * starts_with - matches value by prefix
%% * index - matches value by position in list
%% @end
-spec exists(Value :: binary(), Rule) -> true | false when
  Rule :: {name, [binary()]} | {starts_with, [binary()]} | {index, [integer()]}.
%% ====================================================================
exists(_, {_, []}) ->
  false;
exists(Value, {index, Columns}) when is_integer(Value) ->
  lists:member(Value, Columns);
exists(Value, {name, [Value | _]}) ->
  true;
exists(Value, {name, [_ | Columns]}) ->
  exists(Value, {name, Columns});
exists(Value, {starts_with, [Column | Columns]}) ->
  Size = size(Column),
  case binary:longest_common_prefix([Value, Column]) of
    Size -> true;
    _ -> exists(Value, {starts_with, Columns})
  end;
exists(_, _) ->
  false.


%% receive_body/2
%% ====================================================================
%% @doc Receives content of Round Robin Archive fetched from database.
%% @end
-spec receive_body(Port :: port(), Columns :: [integer()]) -> {ok, Result :: binary()} | {error, Error :: binary()}.
%% ====================================================================
receive_body(Port, Columns) ->
  receive_body(Port, Columns, []).


%% receive_body/3
%% ====================================================================
%% @doc Receives content of Round Robin Archive fetched from database.
%% Should not be used directly, use receive_body/2 instead.
%% @end
-spec receive_body(Port :: port(), Columns :: [integer()], Body :: [{Timestamp :: integer(), Values :: [float()]}]) ->
  {ok, Result :: binary()} | {error, Error :: binary()}.
%% ====================================================================
receive_body(Port, Columns, Body) ->
  {ok, Timeout} = application:get_env(?APP_Name, rrd_timeout),
  receive
    {Port, {data, {eol, <<"OK", _/binary>>}}} ->
      {ok, lists:reverse(Body)};
    {Port, {data, {eol, <<"ERROR: ", Error/binary>>}}} ->
      ?error("RRD receive_body error ~p", [Error]),
      {error, Error};
    {Port, {data, {eol, Data}}} ->
      case select_row(Data, Columns) of
        {ok, Row} ->
          receive_body(Port, Columns, [Row | Body]);
        {error, Error} ->
          ?error("RRD receive_body error ~p", [Error]),
          {error, Error}
      end
  after Timeout ->
    ?error("RRD receive_body timeout"),
    {error, <<"timeout">>}
  end.


%% select_row/2
%% ====================================================================
%% @doc Select columns from row and returns formatted values.
%% @end
-spec select_row(Data :: binary(), Columns :: [integer()]) -> {ok, {Timestamp :: integer(), Values :: [float()]}}.
%% ====================================================================
select_row(Data, Columns) ->
  try
    [TimeStamp, Values | _] = binary:split(Data, <<":">>, [global]),
    case select_row(split(Values), Columns, 1, []) of
      {ok, Row} -> {ok, {binary_to_integer(TimeStamp), lists:reverse(Row)}};
      Error ->
        ?error("RRD select_row error ~p", [Error]),
        {error, <<"Body selection error.">>}
    end
  catch
    E1:E2 ->
      ?error("RRD select_row error ~p:~p", [E1,E2]),
      {error, <<"Body selection error.">>}
  end.


%% select_row/4
%% ====================================================================
%% @doc Select columns from row and returns formatted values.
%% Should not be used directly, use select_row/2 instead.
%% @end
-spec select_row(Data :: binary(), Columns :: [integer()], N :: integer(), Acc :: [float()]) -> Values :: [float()].
%% ====================================================================
select_row(_, [], _, Acc) ->
  {ok, Acc};
select_row([], _, _, _) ->
  {error, <<"Body selection error: no more data.">>};
select_row([Value | Values], [N | Columns], N, Acc) ->
  select_row(Values, Columns, N + 1, [to_float(Value) | Acc]);
select_row([_ | Values], Columns, N, Acc) ->
  select_row(Values, Columns, N + 1, Acc).


%% split/1
%% ====================================================================
%% @doc Splits binary using defaulf separator.
%% @end
-spec split(Data :: binary()) -> Values :: [binary()].
%% ====================================================================
split(Data) ->
  lists:filter(fun
    (Value) -> Value =/= <<>>
  end, binary:split(Data, <<" ">>, [global])).


%% to_float/1
%% ====================================================================
%% @doc Converts binary to float or returns 'nan' if value is not convertible.
%% @end
-spec to_float(Data :: binary()) -> Values :: [binary()].
%% ====================================================================
to_float(Binary) ->
  try
    binary_to_float(Binary)
  catch
    _:_ -> nan
  end.